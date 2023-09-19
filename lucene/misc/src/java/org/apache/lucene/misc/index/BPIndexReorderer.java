/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.misc.index;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.BufferSize;

/**
 * Implementation of "recursive graph bisection", also called "bipartite graph partitioning" and
 * often abbreviated BP, an approach to doc ID assignment that aims at reducing the sum of the log
 * gap between consecutive postings. While originally targeted at reducing the size of postings,
 * this algorithm has been observed to also speed up queries significantly by clustering documents
 * that have similar sets of terms together.
 *
 * <p>This algorithm was initially described by Dhulipala et al. in "Compressing graphs and inverted
 * indexes with recursive graph bisection". This implementation takes advantage of some
 * optimizations suggested by Mackenzie et al. in "Tradeoff Options for Bipartite Graph
 * Partitioning".
 *
 * <p>Typical usage would look like this:
 *
 * <pre class="prettyprint">
 * LeafReader reader; // reader to reorder
 * Directory targetDir; // Directory where to write the reordered index
 *
 * Directory targetDir = FSDirectory.open(targetPath);
 * BPIndexReorderer reorderer = new BPIndexReorderer();
 * ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
 *     p -&gt; new ForkJoinWorkerThread(p) {}, null, false);
 * reorderer.setForkJoinPool(pool);
 * reorderer.setFields(Collections.singleton("body"));
 * CodecReader reorderedReaderView = reorderer.reorder(SlowCodecReaderWrapper.wrap(reader), targetDir);
 * try (IndexWriter w = new IndexWriter(targetDir, new IndexWriterConfig().setOpenMode(OpenMode.CREATE))) {
 *   w.addIndexes(reorderedReaderView);
 * }
 * DirectoryReader reorderedReader = DirectoryReader.open(targetDir);
 * </pre>
 *
 * <p>Note: This is a slow operation that consumes O(maxDoc + numTerms * numThreads) memory.
 */
public final class BPIndexReorderer {

  /** Exception that is thrown when not enough RAM is available. */
  public static class NotEnoughRAMException extends RuntimeException {
    private NotEnoughRAMException(String message) {
      super(message);
    }
  }

  /** Block size for terms in the forward index */
  private static final int TERM_IDS_BLOCK_SIZE = 17;

  /** Minimum problem size that will result in tasks being splitted. */
  private static final int FORK_THRESHOLD = 8192;

  /** Minimum required document frequency for terms to be considered: 4,096. */
  public static final int DEFAULT_MIN_DOC_FREQ = 4096;

  /**
   * Minimum size of partitions. The algorithm will stop recursing when reaching partitions below
   * this number of documents: 32.
   */
  public static final int DEFAULT_MIN_PARTITION_SIZE = 32;

  /**
   * Default maximum number of iterations per recursion level: 20. Higher numbers of iterations
   * typically don't help significantly.
   */
  public static final int DEFAULT_MAX_ITERS = 20;

  private int minDocFreq;
  private int minPartitionSize;
  private int maxIters;
  private ForkJoinPool forkJoinPool;
  private double ramBudgetMB;
  private Set<String> fields;

  /** Constructor. */
  public BPIndexReorderer() {
    setMinDocFreq(DEFAULT_MIN_DOC_FREQ);
    setMinPartitionSize(DEFAULT_MIN_PARTITION_SIZE);
    setMaxIters(DEFAULT_MAX_ITERS);
    setForkJoinPool(null);
    // 10% of the available heap size by default
    setRAMBudgetMB(Runtime.getRuntime().totalMemory() / 1024d / 1024d / 10d);
    setFields(null);
  }

  /** Set the minimum document frequency for terms to be considered, 4096 by default. */
  public void setMinDocFreq(int minDocFreq) {
    if (minDocFreq < 1) {
      throw new IllegalArgumentException("minDocFreq must be at least 1, got " + minDocFreq);
    }
    this.minDocFreq = minDocFreq;
  }

  /** Set the minimum partition size, when the algorithm stops recursing, 32 by default. */
  public void setMinPartitionSize(int minPartitionSize) {
    if (minPartitionSize < 1) {
      throw new IllegalArgumentException(
          "minPartitionSize must be at least 1, got " + minPartitionSize);
    }
    this.minPartitionSize = minPartitionSize;
  }

  /**
   * Set the maximum number of iterations on each recursion level, 20 by default. Experiments
   * suggests that values above 20 do not help much. However, values below 20 can be used to trade
   * effectiveness for faster reordering.
   */
  public void setMaxIters(int maxIters) {
    if (maxIters < 1) {
      throw new IllegalArgumentException("maxIters must be at least 1, got " + maxIters);
    }
    this.maxIters = maxIters;
  }

  /**
   * Set the {@link ForkJoinPool} to run graph partitioning concurrently.
   *
   * <p>NOTE: A value of {@code null} can be used to run in the current thread, which is the
   * default.
   */
  public void setForkJoinPool(ForkJoinPool forkJoinPool) {
    this.forkJoinPool = forkJoinPool;
  }

  private int getParallelism() {
    return forkJoinPool == null ? 1 : forkJoinPool.getParallelism();
  }

  /**
   * Set the amount of RAM that graph partitioning is allowed to use. More RAM allows running
   * faster. If not enough RAM is provided, a {@link NotEnoughRAMException} will be thrown. This is
   * 10% of the total heap size by default.
   */
  public void setRAMBudgetMB(double ramBudgetMB) {
    this.ramBudgetMB = ramBudgetMB;
  }

  /**
   * Sets the fields to use to perform partitioning. A {@code null} value indicates that all indexed
   * fields should be used.
   */
  public void setFields(Set<String> fields) {
    this.fields = fields == null ? null : Set.copyOf(fields);
  }

  private static class PerThreadState {

    final ForwardIndex forwardIndex;
    final int[] leftDocFreqs;
    final int[] rightDocFreqs;

    PerThreadState(int numTerms, ForwardIndex forwardIndex) {
      this.forwardIndex = forwardIndex;
      this.leftDocFreqs = new int[numTerms];
      this.rightDocFreqs = new int[numTerms];
    }
  }

  private abstract class BaseRecursiveAction extends RecursiveAction {

    protected final int depth;

    BaseRecursiveAction(int depth) {
      this.depth = depth;
    }

    protected final boolean shouldFork(int problemSize, int totalProblemSize) {
      if (forkJoinPool == null) {
        return false;
      }
      if (getSurplusQueuedTaskCount() > 3) {
        // Fork tasks if this worker doesn't have more queued work than other workers
        // See javadocs of #getSurplusQueuedTaskCount for more details
        return false;
      }
      if (problemSize == totalProblemSize) {
        // Sometimes fork regardless of the problem size to make sure that unit tests also exercise
        // forking
        return true;
      }
      return problemSize > FORK_THRESHOLD;
    }
  }

  private class IndexReorderingTask extends BaseRecursiveAction {

    private final IntsRef docIDs;
    private final float[] gains;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    IndexReorderingTask(
        IntsRef docIDs,
        float[] gains,
        CloseableThreadLocal<PerThreadState> threadLocal,
        int depth) {
      super(depth);
      this.docIDs = docIDs;
      this.gains = gains;
      this.threadLocal = threadLocal;
    }

    private void computeDocFreqs(IntsRef docs, ForwardIndex forwardIndex, int[] docFreqs) {
      try {
        Arrays.fill(docFreqs, 0);
        for (int i = docs.offset, end = docs.offset + docs.length; i < end; ++i) {
          final int doc = docs.ints[i];
          forwardIndex.seek(doc);
          for (IntsRef terms = forwardIndex.nextTerms();
              terms.length != 0;
              terms = forwardIndex.nextTerms()) {
            for (int j = 0; j < terms.length; ++j) {
              final int termID = terms.ints[terms.offset + j];
              ++docFreqs[termID];
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    protected void compute() {
      if (depth > 0) {
        Arrays.sort(docIDs.ints, docIDs.offset, docIDs.offset + docIDs.length);
      } else {
        assert sorted(docIDs);
      }

      int leftSize = docIDs.length / 2;
      if (leftSize < minPartitionSize) {
        return;
      }

      int rightSize = docIDs.length - leftSize;
      IntsRef left = new IntsRef(docIDs.ints, docIDs.offset, leftSize);
      IntsRef right = new IntsRef(docIDs.ints, docIDs.offset + leftSize, rightSize);

      PerThreadState state = threadLocal.get();
      ForwardIndex forwardIndex = state.forwardIndex;
      int[] leftDocFreqs = state.leftDocFreqs;
      int[] rightDocFreqs = state.rightDocFreqs;

      Arrays.fill(leftDocFreqs, 0);
      computeDocFreqs(left, forwardIndex, leftDocFreqs);
      Arrays.fill(rightDocFreqs, 0);
      computeDocFreqs(right, forwardIndex, rightDocFreqs);

      for (int iter = 0; iter < maxIters; ++iter) {
        boolean moved;
        try {
          moved = shuffle(forwardIndex, left, right, leftDocFreqs, rightDocFreqs, gains, iter);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        if (moved == false) {
          break;
        }
      }

      // It is fine for all tasks to share the same docs / gains array since they all work on
      // different slices of the array at a given point in time.
      IndexReorderingTask leftTask = new IndexReorderingTask(left, gains, threadLocal, depth + 1);
      IndexReorderingTask rightTask = new IndexReorderingTask(right, gains, threadLocal, depth + 1);

      if (shouldFork(docIDs.length, docIDs.ints.length)) {
        invokeAll(leftTask, rightTask);
      } else {
        leftTask.compute();
        rightTask.compute();
      }
    }

    /**
     * Shuffle doc IDs across both partitions so that each partition has lower gaps between
     * consecutive postings.
     */
    private boolean shuffle(
        ForwardIndex forwardIndex,
        IntsRef left,
        IntsRef right,
        int[] leftDocFreqs,
        int[] rightDocFreqs,
        float[] gains,
        int iter)
        throws IOException {
      assert left.ints == right.ints;
      assert left.offset + left.length == right.offset;

      // Computing gains is typically a bottleneck, because each iteration needs to iterate over all
      // postings to recompute gains, and the total number of postings is usually one order of
      // magnitude or more larger than the number of docs. So we try to parallelize it.
      ComputeGainsTask leftGainsTask =
          new ComputeGainsTask(
              left.ints,
              gains,
              left.offset,
              left.offset + left.length,
              leftDocFreqs,
              rightDocFreqs,
              threadLocal,
              depth);
      ComputeGainsTask rightGainsTask =
          new ComputeGainsTask(
              right.ints,
              gains,
              right.offset,
              right.offset + right.length,
              rightDocFreqs,
              leftDocFreqs,
              threadLocal,
              depth);
      if (shouldFork(docIDs.length, docIDs.ints.length)) {
        invokeAll(leftGainsTask, rightGainsTask);
      } else {
        leftGainsTask.compute();
        rightGainsTask.compute();
      }

      class ByDescendingGainSorter extends IntroSorter {

        int pivotDoc;
        float pivotGain;

        @Override
        protected void setPivot(int i) {
          pivotDoc = left.ints[i];
          pivotGain = gains[i];
        }

        @Override
        protected int comparePivot(int j) {
          // Compare in reverse order to get a descending sort
          int cmp = Float.compare(gains[j], pivotGain);
          if (cmp == 0) {
            // Tie break on the doc ID to preserve doc ID ordering as much as possible
            cmp = pivotDoc - left.ints[j];
          }
          return cmp;
        }

        @Override
        protected void swap(int i, int j) {
          int tmpDoc = left.ints[i];
          left.ints[i] = left.ints[j];
          left.ints[j] = tmpDoc;

          float tmpGain = gains[i];
          gains[i] = gains[j];
          gains[j] = tmpGain;
        }
      }

      Runnable leftSorter =
          () -> new ByDescendingGainSorter().sort(left.offset, left.offset + left.length);
      Runnable rightSorter =
          () -> new ByDescendingGainSorter().sort(right.offset, right.offset + right.length);

      if (shouldFork(docIDs.length, docIDs.ints.length)) {
        // TODO: run it on more than 2 threads at most
        invokeAll(adapt(leftSorter), adapt(rightSorter));
      } else {
        leftSorter.run();
        rightSorter.run();
      }

      for (int i = 0; i < left.length; ++i) {
        // This uses the simulated annealing proposed by Mackenzie et al in "Tradeoff Options for
        // Bipartite Graph Partitioning" by comparing the gain against `iter` rather than zero.
        if (gains[left.offset + i] + gains[right.offset + i] <= iter) {
          if (i == 0) {
            return false;
          }
          break;
        }

        swap(
            left.ints,
            left.offset + i,
            right.offset + i,
            forwardIndex,
            leftDocFreqs,
            rightDocFreqs);
      }

      return true;
    }

    private void swap(
        int[] docs,
        int left,
        int right,
        ForwardIndex forwardIndex,
        int[] leftDocFreqs,
        int[] rightDocFreqs)
        throws IOException {
      assert left < right;

      int leftDoc = docs[left];
      int rightDoc = docs[right];

      // Now update the cache, this makes things much faster than invalidating it and having to
      // recompute doc freqs on the next iteration.
      forwardIndex.seek(leftDoc);
      for (IntsRef terms = forwardIndex.nextTerms();
          terms.length != 0;
          terms = forwardIndex.nextTerms()) {
        for (int i = 0; i < terms.length; ++i) {
          final int termID = terms.ints[terms.offset + i];
          --leftDocFreqs[termID];
          ++rightDocFreqs[termID];
        }
      }

      forwardIndex.seek(rightDoc);
      for (IntsRef terms = forwardIndex.nextTerms();
          terms.length != 0;
          terms = forwardIndex.nextTerms()) {
        for (int i = 0; i < terms.length; ++i) {
          final int termID = terms.ints[terms.offset + i];
          ++leftDocFreqs[termID];
          --rightDocFreqs[termID];
        }
      }

      docs[left] = rightDoc;
      docs[right] = leftDoc;
    }
  }

  private class ComputeGainsTask extends BaseRecursiveAction {

    private final int[] docs;
    private final float[] gains;
    private final int from;
    private final int to;
    private final int[] fromDocFreqs;
    private final int[] toDocFreqs;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    ComputeGainsTask(
        int[] docs,
        float[] gains,
        int from,
        int to,
        int[] fromDocFreqs,
        int[] toDocFreqs,
        CloseableThreadLocal<PerThreadState> threadLocal,
        int depth) {
      super(depth);
      this.docs = docs;
      this.gains = gains;
      this.from = from;
      this.to = to;
      this.fromDocFreqs = fromDocFreqs;
      this.toDocFreqs = toDocFreqs;
      this.threadLocal = threadLocal;
    }

    @Override
    protected void compute() {
      final int problemSize = to - from;
      if (problemSize > 1 && shouldFork(problemSize, docs.length)) {
        final int mid = (from + to) >>> 1;
        invokeAll(
            new ComputeGainsTask(
                docs, gains, from, mid, fromDocFreqs, toDocFreqs, threadLocal, depth),
            new ComputeGainsTask(
                docs, gains, mid, to, fromDocFreqs, toDocFreqs, threadLocal, depth));
      } else {
        ForwardIndex forwardIndex = threadLocal.get().forwardIndex;
        try {
          for (int i = from; i < to; ++i) {
            gains[i] = computeGain(docs[i], forwardIndex, fromDocFreqs, toDocFreqs);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    /**
     * Compute a float that is negative when a document is attracted to the left and positive
     * otherwise.
     */
    private float computeGain(
        int docID, ForwardIndex forwardIndex, int[] fromDocFreqs, int[] toDocFreqs)
        throws IOException {
      forwardIndex.seek(docID);
      double gain = 0;
      for (IntsRef terms = forwardIndex.nextTerms();
          terms.length != 0;
          terms = forwardIndex.nextTerms()) {
        for (int i = 0; i < terms.length; ++i) {
          final int termID = terms.ints[terms.offset + i];
          final int fromDocFreq = fromDocFreqs[termID];
          final int toDocFreq = toDocFreqs[termID];
          assert fromDocFreq >= 0;
          assert toDocFreq >= 0;
          gain +=
              (toDocFreq == 0 ? 0 : fastLog2(toDocFreq))
                  - (fromDocFreq == 0 ? 0 : fastLog2(fromDocFreq));
        }
      }
      return (float) gain;
    }
  }

  /**
   * A forward index. Like term vectors, but only for a subset of terms, and it produces term IDs
   * instead of whole terms.
   */
  private static final class ForwardIndex implements Cloneable, Closeable {

    private final RandomAccessInput startOffsets;
    private final IndexInput startOffsetsInput, terms;
    private final int maxTerm;

    private long endOffset = -1;
    private final int[] buffer = new int[TERM_IDS_BLOCK_SIZE];
    private final IntsRef bufferRef = new IntsRef(buffer, 0, 0);

    ForwardIndex(IndexInput startOffsetsInput, IndexInput terms, int maxTerm) {
      this.startOffsetsInput = startOffsetsInput;
      try {
        this.startOffsets =
            startOffsetsInput.randomAccessSlice(
                0, startOffsetsInput.length() - CodecUtil.footerLength());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      this.terms = terms;
      this.maxTerm = maxTerm;
    }

    void seek(int docID) throws IOException {
      final long startOffset = startOffsets.readLong((long) docID * Long.BYTES);
      endOffset = startOffsets.readLong((docID + 1L) * Long.BYTES);
      terms.seek(startOffset);
    }

    IntsRef nextTerms() throws IOException {
      if (terms.getFilePointer() >= endOffset) {
        assert terms.getFilePointer() == endOffset;
        bufferRef.length = 0;
      } else {
        bufferRef.length = readMonotonicInts(terms, buffer);
      }
      return bufferRef;
    }

    @Override
    public ForwardIndex clone() {
      return new ForwardIndex(startOffsetsInput.clone(), terms.clone(), maxTerm);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(startOffsetsInput, terms);
    }
  }

  private int writePostings(
      CodecReader reader, Set<String> fields, Directory tempDir, DataOutput postingsOut)
      throws IOException {
    final int maxNumTerms =
        (int)
            ((ramBudgetMB * 1024 * 1024 - docRAMRequirements(reader.maxDoc()))
                / getParallelism()
                / termRAMRequirementsPerThreadPerTerm());

    int numTerms = 0;
    for (String field : fields) {
      Terms terms = reader.terms(field);
      if (terms == null) {
        continue;
      }
      if (terms.size() != -1) {
        // Skip ID-like fields that have many terms where none is of interest
        final long maxPossibleDocFreq = 1 + terms.getSumDocFreq() - terms.size();
        if (maxPossibleDocFreq < minDocFreq) {
          continue;
        }
      }
      TermsEnum iterator = terms.iterator();
      PostingsEnum postings = null;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        if (iterator.docFreq() < minDocFreq) {
          continue;
        }
        if (numTerms >= ArrayUtil.MAX_ARRAY_LENGTH) {
          throw new IllegalArgumentException(
              "Cannot perform recursive graph bisection on more than "
                  + ArrayUtil.MAX_ARRAY_LENGTH
                  + " terms, the maximum array length");
        } else if (numTerms >= maxNumTerms) {
          throw new NotEnoughRAMException(
              "Too many terms are matching given the RAM budget of "
                  + ramBudgetMB
                  + "MB. Consider raising the RAM budget, raising the minimum doc frequency, or decreasing concurrency");
        }
        final int termID = numTerms++;
        postings = iterator.postings(postings, PostingsEnum.NONE);
        for (int doc = postings.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = postings.nextDoc()) {
          // reverse bytes so that byte order matches natural order
          postingsOut.writeInt(Integer.reverseBytes(doc));
          postingsOut.writeInt(Integer.reverseBytes(termID));
        }
      }
    }
    return numTerms;
  }

  private ForwardIndex buildForwardIndex(
      Directory tempDir, String postingsFileName, int maxDoc, int maxTerm) throws IOException {
    String sortedPostingsFile =
        new OfflineSorter(
            tempDir,
            "forward-index",
            // Implement BytesRefComparator to make OfflineSorter use radix sort
            new BytesRefComparator(2 * Integer.BYTES) {
              @Override
              protected int byteAt(BytesRef ref, int i) {
                return ref.bytes[ref.offset + i] & 0xFF;
              }

              @Override
              public int compare(BytesRef o1, BytesRef o2) {
                assert o1.length == 2 * Integer.BYTES;
                assert o2.length == 2 * Integer.BYTES;
                return ArrayUtil.compareUnsigned8(o1.bytes, o1.offset, o2.bytes, o2.offset);
              }
            },
            BufferSize.megabytes((long) (ramBudgetMB / getParallelism())),
            OfflineSorter.MAX_TEMPFILES,
            2 * Integer.BYTES,
            forkJoinPool,
            getParallelism()) {

          @Override
          protected ByteSequencesReader getReader(ChecksumIndexInput in, String name)
              throws IOException {
            return new ByteSequencesReader(in, postingsFileName) {
              {
                ref.grow(2 * Integer.BYTES);
                ref.setLength(2 * Integer.BYTES);
              }

              @Override
              public BytesRef next() throws IOException {
                if (in.getFilePointer() >= end) {
                  return null;
                }
                // optimized read of 8 bytes
                in.readBytes(ref.bytes(), 0, 2 * Integer.BYTES);
                return ref.get();
              }
            };
          }

          @Override
          protected ByteSequencesWriter getWriter(IndexOutput out, long itemCount)
              throws IOException {
            return new ByteSequencesWriter(out) {
              @Override
              public void write(byte[] bytes, int off, int len) throws IOException {
                assert len == 2 * Integer.BYTES;
                // optimized read of 8 bytes
                out.writeBytes(bytes, off, len);
              }
            };
          }
        }.sort(postingsFileName);

    String termIDsFileName;
    String startOffsetsFileName;
    int prevDoc = -1;
    try (IndexInput sortedPostings = tempDir.openInput(sortedPostingsFile, IOContext.READONCE);
        IndexOutput termIDs = tempDir.createTempOutput("term-ids", "", IOContext.DEFAULT);
        IndexOutput startOffsets =
            tempDir.createTempOutput("start-offsets", "", IOContext.DEFAULT)) {
      termIDsFileName = termIDs.getName();
      startOffsetsFileName = startOffsets.getName();
      final long end = sortedPostings.length() - CodecUtil.footerLength();
      int[] buffer = new int[TERM_IDS_BLOCK_SIZE];
      int bufferLen = 0;
      while (sortedPostings.getFilePointer() < end) {
        final int doc = Integer.reverseBytes(sortedPostings.readInt());
        final int termID = Integer.reverseBytes(sortedPostings.readInt());
        if (doc != prevDoc) {
          if (bufferLen != 0) {
            writeMonotonicInts(buffer, bufferLen, termIDs);
            bufferLen = 0;
          }

          assert doc > prevDoc;
          for (int d = prevDoc + 1; d <= doc; ++d) {
            startOffsets.writeLong(termIDs.getFilePointer());
          }
          prevDoc = doc;
        }
        assert termID < maxTerm : termID + " " + maxTerm;
        if (bufferLen == buffer.length) {
          writeMonotonicInts(buffer, bufferLen, termIDs);
          bufferLen = 0;
        }
        buffer[bufferLen++] = termID;
      }
      if (bufferLen != 0) {
        writeMonotonicInts(buffer, bufferLen, termIDs);
      }
      for (int d = prevDoc + 1; d <= maxDoc; ++d) {
        startOffsets.writeLong(termIDs.getFilePointer());
      }
      CodecUtil.writeFooter(termIDs);
      CodecUtil.writeFooter(startOffsets);
    }

    IndexInput termIDsInput = tempDir.openInput(termIDsFileName, IOContext.READ);
    IndexInput startOffsets = tempDir.openInput(startOffsetsFileName, IOContext.READ);
    return new ForwardIndex(startOffsets, termIDsInput, maxTerm);
  }

  /**
   * Reorder the given {@link CodecReader} into a reader that tries to minimize the log gap between
   * consecutive documents in postings, which usually helps improve space efficiency and query
   * evaluation efficiency. Note that the returned {@link CodecReader} is slow and should typically
   * be used in a call to {@link IndexWriter#addIndexes(CodecReader...)}.
   *
   * @throws NotEnoughRAMException if not enough RAM is provided
   */
  public CodecReader reorder(CodecReader reader, Directory tempDir) throws IOException {

    if (docRAMRequirements(reader.maxDoc()) >= ramBudgetMB * 1024 * 1024) {
      throw new NotEnoughRAMException(
          "At least "
              + Math.ceil(docRAMRequirements(reader.maxDoc()) / 1024. / 1024.)
              + "MB of RAM are required to hold metadata about documents in RAM, but current RAM budget is "
              + ramBudgetMB
              + "MB");
    }

    Set<String> fields = this.fields;
    if (fields == null) {
      fields = new HashSet<>();
      for (FieldInfo fi : reader.getFieldInfos()) {
        if (fi.getIndexOptions() != IndexOptions.NONE) {
          fields.add(fi.name);
        }
      }
    }

    int[] newToOld = computePermutation(reader, fields, tempDir);
    int[] oldToNew = new int[newToOld.length];
    for (int i = 0; i < newToOld.length; ++i) {
      oldToNew[newToOld[i]] = i;
    }
    final Sorter.DocMap docMap =
        new Sorter.DocMap() {

          @Override
          public int size() {
            return newToOld.length;
          }

          @Override
          public int oldToNew(int docID) {
            return oldToNew[docID];
          }

          @Override
          public int newToOld(int docID) {
            return newToOld[docID];
          }
        };
    return SortingCodecReader.wrap(reader, docMap, null);
  }

  /**
   * Compute a permutation of the doc ID space that reduces log gaps between consecutive postings.
   */
  private int[] computePermutation(CodecReader reader, Set<String> fields, Directory dir)
      throws IOException {
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

    final int maxDoc = reader.maxDoc();
    ForwardIndex forwardIndex = null;
    IndexOutput postingsOutput = null;
    boolean success = false;
    try {
      postingsOutput = trackingDir.createTempOutput("postings", "", IOContext.DEFAULT);
      int numTerms = writePostings(reader, fields, trackingDir, postingsOutput);
      CodecUtil.writeFooter(postingsOutput);
      postingsOutput.close();
      final ForwardIndex finalForwardIndex =
          forwardIndex = buildForwardIndex(trackingDir, postingsOutput.getName(), maxDoc, numTerms);
      trackingDir.deleteFile(postingsOutput.getName());
      postingsOutput = null;

      int[] sortedDocs = new int[maxDoc];
      for (int i = 0; i < maxDoc; ++i) {
        sortedDocs[i] = i;
      }

      try (CloseableThreadLocal<PerThreadState> threadLocal =
          new CloseableThreadLocal<>() {
            @Override
            protected PerThreadState initialValue() {
              return new PerThreadState(numTerms, finalForwardIndex.clone());
            }
          }) {
        IntsRef docs = new IntsRef(sortedDocs, 0, sortedDocs.length);
        IndexReorderingTask task = new IndexReorderingTask(docs, new float[maxDoc], threadLocal, 0);
        if (forkJoinPool != null) {
          forkJoinPool.execute(task);
          task.join();
        } else {
          task.compute();
        }
      }

      success = true;
      return sortedDocs;
    } finally {
      if (success) {
        IOUtils.close(forwardIndex);
        IOUtils.deleteFiles(trackingDir, trackingDir.getCreatedFiles());
      } else {
        IOUtils.closeWhileHandlingException(postingsOutput, forwardIndex);
        IOUtils.deleteFilesIgnoringExceptions(trackingDir, trackingDir.getCreatedFiles());
      }
    }
  }

  /** Returns true if, and only if, the given {@link IntsRef} is sorted. */
  private static boolean sorted(IntsRef intsRef) {
    for (int i = 1; i < intsRef.length; ++i) {
      if (intsRef.ints[intsRef.offset + i - 1] > intsRef.ints[intsRef.offset + i]) {
        return false;
      }
    }
    return true;
  }

  private static long docRAMRequirements(int maxDoc) {
    // We need one int per doc for the doc map, plus one float to store the gain associated with
    // this doc.
    return 2L * Integer.BYTES * maxDoc;
  }

  private static long termRAMRequirementsPerThreadPerTerm() {
    // Each thread needs two ints per term, one for left document frequencies, and one for right
    // document frequencies
    return 2L * Integer.BYTES;
  }

  private static final float[] LOG2_TABLE = new float[256];

  static {
    LOG2_TABLE[0] = 1f;
    // float that has the biased exponent of 1f and zeros for sign and mantissa bits
    final int one = Float.floatToIntBits(1f);
    for (int i = 0; i < 256; ++i) {
      float f = Float.intBitsToFloat(one | (i << (23 - 8)));
      LOG2_TABLE[i] = (float) (Math.log(f) / Math.log(2));
    }
  }

  /** An approximate log() function in base 2 which trades accuracy for much better performance. */
  static final float fastLog2(int i) {
    assert i > 0 : "Cannot compute log of i=" + i;
    // floorLog2 would be the exponent in the float representation of i
    int floorLog2 = 31 - Integer.numberOfLeadingZeros(i);
    // tableIndex would be the first 8 mantissa bits in the float representation of i, excluding
    // the implicit bit
    // the left shift clears the high bit, which is implicit in the float representation
    // the right shift moves the 8 higher mantissa bits to the lower 8 bits
    int tableIndex = i << (32 - floorLog2) >>> (32 - 8);
    // i = 1.tableIndex * 2 ^ floorLog2
    // log(i) = log2(1.tableIndex) + floorLog2
    return floorLog2 + LOG2_TABLE[tableIndex];
  }

  // Simplified bit packing that focuses on the common / efficient case when term IDs can be encoded
  // on 16 bits
  // The decoding logic should auto-vectorize.

  /**
   * Simple bit packing that focuses on the common / efficient case when term IDs can be encoded on
   * 16 bits.
   */
  static void writeMonotonicInts(int[] ints, int len, DataOutput out) throws IOException {
    assert len > 0;
    assert len <= TERM_IDS_BLOCK_SIZE;

    if (len >= 3 && ints[len - 1] - ints[0] <= 0xFFFF) {
      for (int i = 1; i < len; ++i) {
        ints[i] -= ints[0];
      }
      final int numPacked = (len - 1) / 2;
      final int encodedLen = 1 + len / 2;
      for (int i = 0; i < numPacked; ++i) {
        ints[1 + i] |= ints[encodedLen + i] << 16;
      }
      out.writeByte((byte) ((len << 1) | 1));
      for (int i = 0; i < encodedLen; ++i) {
        out.writeInt(ints[i]);
      }
    } else {
      out.writeByte((byte) (len << 1));
      for (int i = 0; i < len; ++i) {
        out.writeInt(ints[i]);
      }
    }
  }

  /**
   * Decoding logic for {@link #writeMonotonicInts(int[], int, DataOutput)}. It should get
   * auto-vectorized.
   */
  static int readMonotonicInts(DataInput in, int[] ints) throws IOException {
    int token = in.readByte() & 0xFF;
    int len = token >>> 1;
    boolean packed = (token & 1) != 0;

    if (packed) {
      final int encodedLen = 1 + len / 2;
      in.readInts(ints, 0, encodedLen);
      final int numPacked = (len - 1) / 2;
      for (int i = 0; i < numPacked; ++i) {
        ints[encodedLen + i] = ints[1 + i] >>> 16;
        ints[1 + i] &= 0xFFFF;
      }
      for (int i = 1; i < len; ++i) {
        ints[i] += ints[0];
      }
    } else {
      in.readInts(ints, 0, len);
    }
    return len;
  }
}
