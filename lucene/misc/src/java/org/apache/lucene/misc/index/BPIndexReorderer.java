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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.Sorter.DocMap;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.packed.PackedInts;

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
  private float maxDocFreq;
  private int minPartitionSize;
  private int maxIters;
  private ForkJoinPool forkJoinPool;
  private double ramBudgetMB;
  private Set<String> fields;

  /** Constructor. */
  public BPIndexReorderer() {
    setMinDocFreq(DEFAULT_MIN_DOC_FREQ);
    setMaxDocFreq(1f);
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

  /**
   * Set the maximum document frequency for terms to be considered, as a ratio of {@code maxDoc}.
   * This is useful because very frequent terms (stop words) add significant overhead to the
   * reordering logic while not being very relevant for ordering. This value must be in (0, 1].
   * Default value is 1.
   */
  public void setMaxDocFreq(float maxDocFreq) {
    if (maxDocFreq > 0 == false || maxDocFreq <= 1 == false) {
      throw new IllegalArgumentException("maxDocFreq must be in (0, 1], got " + maxDocFreq);
    }
    this.maxDocFreq = maxDocFreq;
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
    private final float[] biases;
    private final CloseableThreadLocal<PerThreadState> threadLocal;
    private final BitSet parents;

    IndexReorderingTask(
        IntsRef docIDs,
        float[] biases,
        CloseableThreadLocal<PerThreadState> threadLocal,
        BitSet parents,
        int depth) {
      super(depth);
      this.docIDs = docIDs;
      this.biases = biases;
      this.threadLocal = threadLocal;
      this.parents = parents;
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
      assert assertParentStructure();

      int halfLength = docIDs.length / 2;
      if (halfLength < minPartitionSize) {
        return;
      }

      IntsRef left = new IntsRef(docIDs.ints, docIDs.offset, halfLength);
      IntsRef right =
          new IntsRef(docIDs.ints, docIDs.offset + halfLength, docIDs.length - halfLength);

      PerThreadState state = threadLocal.get();
      ForwardIndex forwardIndex = state.forwardIndex;
      int[] leftDocFreqs = state.leftDocFreqs;
      int[] rightDocFreqs = state.rightDocFreqs;

      computeDocFreqs(left, forwardIndex, leftDocFreqs);
      computeDocFreqs(right, forwardIndex, rightDocFreqs);

      for (int iter = 0; iter < maxIters; ++iter) {
        boolean moved;
        try {
          moved =
              shuffle(
                  forwardIndex,
                  docIDs,
                  right.offset,
                  leftDocFreqs,
                  rightDocFreqs,
                  biases,
                  parents,
                  iter);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        if (moved == false) {
          break;
        }
      }

      if (parents != null) {
        // Make sure we split just after a parent doc
        int lastLeftDocID = docIDs.ints[right.offset - 1];
        int split = right.offset + parents.nextSetBit(lastLeftDocID) - lastLeftDocID;

        if (split == docIDs.offset + docIDs.length) {
          // No good split on the right side, look on the left side then.
          split = right.offset - (lastLeftDocID - parents.prevSetBit(lastLeftDocID));
          if (split == docIDs.offset) {
            // No good split on the left side either: this slice has a single parent document, no
            // reordering is possible. Stop recursing.
            return;
          }
        }

        assert parents.get(docIDs.ints[split - 1]);

        left = new IntsRef(docIDs.ints, docIDs.offset, split - docIDs.offset);
        right = new IntsRef(docIDs.ints, split, docIDs.offset + docIDs.length - split);
      }

      // It is fine for all tasks to share the same docs / biases array since they all work on
      // different slices of the array at a given point in time.
      IndexReorderingTask leftTask =
          new IndexReorderingTask(left, biases, threadLocal, parents, depth + 1);
      IndexReorderingTask rightTask =
          new IndexReorderingTask(right, biases, threadLocal, parents, depth + 1);

      if (shouldFork(docIDs.length, docIDs.ints.length)) {
        invokeAll(leftTask, rightTask);
      } else {
        leftTask.compute();
        rightTask.compute();
      }
    }

    // used for asserts
    private boolean assertParentStructure() {
      if (parents == null) {
        return true;
      }
      int i = docIDs.offset;
      final int end = docIDs.offset + docIDs.length;
      while (i < end) {
        final int firstChild = docIDs.ints[i];
        final int parent = parents.nextSetBit(firstChild);
        final int numChildren = parent - firstChild;
        assert i + numChildren < end;
        for (int j = 1; j <= numChildren; ++j) {
          assert docIDs.ints[i + j] == firstChild + j : "Parent structure has not been preserved";
        }
        i += numChildren + 1;
      }
      assert i == end : "Last doc ID must be a parent doc";

      return true;
    }

    /**
     * Shuffle doc IDs across both partitions so that each partition has lower gaps between
     * consecutive postings.
     */
    private boolean shuffle(
        ForwardIndex forwardIndex,
        IntsRef docIDs,
        int midPoint,
        int[] leftDocFreqs,
        int[] rightDocFreqs,
        float[] biases,
        BitSet parents,
        int iter)
        throws IOException {

      // Computing biases is typically a bottleneck, because each iteration needs to iterate over
      // all postings to recompute biases, and the total number of postings is usually one order of
      // magnitude or more larger than the number of docs. So we try to parallelize it.
      new ComputeBiasTask(
              docIDs.ints,
              biases,
              docIDs.offset,
              docIDs.offset + docIDs.length,
              leftDocFreqs,
              rightDocFreqs,
              threadLocal,
              depth)
          .compute();

      if (parents != null) {
        for (int i = docIDs.offset, end = docIDs.offset + docIDs.length; i < end; ) {
          final int firstChild = docIDs.ints[i];
          final int numChildren = parents.nextSetBit(firstChild) - firstChild;
          assert parents.get(docIDs.ints[i + numChildren]);
          double cumulativeBias = 0;
          for (int j = 0; j <= numChildren; ++j) {
            cumulativeBias += biases[i + j];
          }
          // Give all docs from the same block the same bias, which is the sum of biases of all
          // documents in the block. This helps ensure that the follow-up sort() call preserves the
          // block structure.
          Arrays.fill(biases, i, i + numChildren + 1, (float) cumulativeBias);
          i += numChildren + 1;
        }
      }

      float maxLeftBias = Float.NEGATIVE_INFINITY;
      for (int i = docIDs.offset; i < midPoint; ++i) {
        maxLeftBias = Math.max(maxLeftBias, biases[i]);
      }
      float minRightBias = Float.POSITIVE_INFINITY;
      for (int i = midPoint, end = docIDs.offset + docIDs.length; i < end; ++i) {
        minRightBias = Math.min(minRightBias, biases[i]);
      }
      float gain = maxLeftBias - minRightBias;
      // This uses the simulated annealing proposed by Mackenzie et al in "Tradeoff Options for
      // Bipartite Graph Partitioning" by comparing the gain of swapping the doc from the left side
      // that is most attracted to the right and the doc from the right side that is most attracted
      // to the left against `iter` rather than zero.
      if (gain <= iter) {
        return false;
      }

      class Selector extends IntroSelector {

        int pivotDoc;
        float pivotBias;

        @Override
        public void setPivot(int i) {
          pivotDoc = docIDs.ints[i];
          pivotBias = biases[i];
        }

        @Override
        public int comparePivot(int j) {
          int cmp = Float.compare(pivotBias, biases[j]);
          if (cmp == 0) {
            // Tie break on the doc ID to preserve doc ID ordering as much as possible
            cmp = pivotDoc - docIDs.ints[j];
          }
          return cmp;
        }

        @Override
        public void swap(int i, int j) {
          float tmpBias = biases[i];
          biases[i] = biases[j];
          biases[j] = tmpBias;

          if (i < midPoint == j < midPoint) {
            int tmpDoc = docIDs.ints[i];
            docIDs.ints[i] = docIDs.ints[j];
            docIDs.ints[j] = tmpDoc;
          } else {
            // If we're swapping docs across the left and right sides, we need to keep doc freqs
            // up-to-date.
            int left = Math.min(i, j);
            int right = Math.max(i, j);
            try {
              swapDocsAndFreqs(docIDs.ints, left, right, forwardIndex, leftDocFreqs, rightDocFreqs);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        }
      }

      Selector selector = new Selector();

      if (parents == null) {
        selector.select(docIDs.offset, docIDs.offset + docIDs.length, midPoint);
      } else {
        // When we have parents, we need to do a full sort to make sure we're not breaking the
        // parent structure.
        new IntroSorter() {
          @Override
          protected void setPivot(int i) {
            selector.setPivot(i);
          }

          @Override
          protected int comparePivot(int j) {
            return selector.comparePivot(j);
          }

          @Override
          protected void swap(int i, int j) {
            selector.swap(i, j);
          }
        }.sort(docIDs.offset, docIDs.offset + docIDs.length);
      }

      return true;
    }

    private void swapDocsAndFreqs(
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

  private class ComputeBiasTask extends BaseRecursiveAction {

    private final int[] docs;
    private final float[] biases;
    private final int from;
    private final int to;
    private final int[] fromDocFreqs;
    private final int[] toDocFreqs;
    private final CloseableThreadLocal<PerThreadState> threadLocal;

    ComputeBiasTask(
        int[] docs,
        float[] biases,
        int from,
        int to,
        int[] fromDocFreqs,
        int[] toDocFreqs,
        CloseableThreadLocal<PerThreadState> threadLocal,
        int depth) {
      super(depth);
      this.docs = docs;
      this.biases = biases;
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
            new ComputeBiasTask(
                docs, biases, from, mid, fromDocFreqs, toDocFreqs, threadLocal, depth),
            new ComputeBiasTask(
                docs, biases, mid, to, fromDocFreqs, toDocFreqs, threadLocal, depth));
      } else {
        ForwardIndex forwardIndex = threadLocal.get().forwardIndex;
        try {
          for (int i = from; i < to; ++i) {
            biases[i] = computeBias(docs[i], forwardIndex, fromDocFreqs, toDocFreqs);
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
    private float computeBias(
        int docID, ForwardIndex forwardIndex, int[] fromDocFreqs, int[] toDocFreqs)
        throws IOException {
      forwardIndex.seek(docID);
      double bias = 0;
      for (IntsRef terms = forwardIndex.nextTerms();
          terms.length != 0;
          terms = forwardIndex.nextTerms()) {
        for (int i = 0; i < terms.length; ++i) {
          final int termID = terms.ints[terms.offset + i];
          final int fromDocFreq = fromDocFreqs[termID];
          final int toDocFreq = toDocFreqs[termID];
          assert fromDocFreq >= 0;
          assert toDocFreq >= 0;
          bias +=
              (toDocFreq == 0 ? 0 : fastLog2(toDocFreq))
                  - (fromDocFreq == 0 ? 0 : fastLog2(fromDocFreq));
        }
      }
      return (float) bias;
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
    final int maxDocFreq = (int) ((double) this.maxDocFreq * reader.maxDoc());

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
        final int docFreq = iterator.docFreq();
        if (docFreq < minDocFreq || docFreq > maxDocFreq) {
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
          postingsOut.writeLong(Integer.toUnsignedLong(termID) << 32 | Integer.toUnsignedLong(doc));
        }
      }
    }
    return numTerms;
  }

  private ForwardIndex buildForwardIndex(
      Directory tempDir, String postingsFileName, int maxDoc, int maxTerm) throws IOException {

    String termIDsFileName;
    String startOffsetsFileName;
    try (IndexOutput termIDs = tempDir.createTempOutput("term-ids", "", IOContext.DEFAULT);
        IndexOutput startOffsets =
            tempDir.createTempOutput("start-offsets", "", IOContext.DEFAULT)) {
      termIDsFileName = termIDs.getName();
      startOffsetsFileName = startOffsets.getName();
      int[] buffer = new int[TERM_IDS_BLOCK_SIZE];
      new ForwardIndexSorter(tempDir)
          .sortAndConsume(
              postingsFileName,
              maxDoc,
              new LongConsumer() {

                int prevDoc = -1;
                int bufferLen = 0;

                @Override
                public void accept(long value) throws IOException {
                  int doc = (int) value;
                  int termID = (int) (value >>> 32);
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

                @Override
                public void onFinish() throws IOException {
                  if (bufferLen != 0) {
                    writeMonotonicInts(buffer, bufferLen, termIDs);
                  }
                  for (int d = prevDoc + 1; d <= maxDoc; ++d) {
                    startOffsets.writeLong(termIDs.getFilePointer());
                  }
                  CodecUtil.writeFooter(termIDs);
                  CodecUtil.writeFooter(startOffsets);
                }
              });
    }

    IndexInput termIDsInput = tempDir.openInput(termIDsFileName, IOContext.READ);
    IndexInput startOffsets = tempDir.openInput(startOffsetsFileName, IOContext.READ);
    return new ForwardIndex(startOffsets, termIDsInput, maxTerm);
  }

  /**
   * Expert: Compute the {@link DocMap} that holds the new doc ID numbering. This is exposed to
   * enable integration into {@link BPReorderingMergePolicy}, {@link #reorder(CodecReader,
   * Directory)} should be preferred in general.
   */
  public Sorter.DocMap computeDocMap(CodecReader reader, Directory tempDir) throws IOException {
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
    return new Sorter.DocMap() {

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
    Sorter.DocMap docMap = computeDocMap(reader, tempDir);
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

      BitSet parents = null;
      String parentField = reader.getFieldInfos().getParentField();
      if (parentField != null) {
        parents = BitSet.of(DocValues.getNumeric(reader, parentField), maxDoc);
      }

      try (CloseableThreadLocal<PerThreadState> threadLocal =
          new CloseableThreadLocal<>() {
            @Override
            protected PerThreadState initialValue() {
              return new PerThreadState(numTerms, finalForwardIndex.clone());
            }
          }) {
        IntsRef docs = new IntsRef(sortedDocs, 0, sortedDocs.length);
        IndexReorderingTask task =
            new IndexReorderingTask(docs, new float[maxDoc], threadLocal, parents, 0);
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
    // We need one int per doc for the doc map, plus one float to store the bias associated with
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
  static float fastLog2(int i) {
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

  /**
   * Use a LSB Radix Sorter to sort the (docID, termID) entries. We only need to compare docIds
   * because LSB Radix Sorter is stable and termIDs already sorted.
   *
   * <p>This sorter will require at least 16MB ({@link #BUFFER_BYTES} * {@link #HISTOGRAM_SIZE})
   * RAM.
   */
  static class ForwardIndexSorter {

    private static final int HISTOGRAM_SIZE = 256;
    private static final int BUFFER_SIZE = 8192;
    private static final int BUFFER_BYTES = BUFFER_SIZE * Long.BYTES;
    private final Directory directory;
    private final Bucket[] buckets = new Bucket[HISTOGRAM_SIZE];

    private static class Bucket {
      private final ByteBuffersDataOutput fps = new ByteBuffersDataOutput();
      private final long[] buffer = new long[BUFFER_SIZE];
      private IndexOutput output;
      private int bufferUsed;
      private int blockNum;
      private long lastFp;
      private int finalBlockSize;

      private void addEntry(long l) throws IOException {
        buffer[bufferUsed++] = l;
        if (bufferUsed == BUFFER_SIZE) {
          flush(false);
        }
      }

      private void flush(boolean isFinal) throws IOException {
        if (isFinal) {
          finalBlockSize = bufferUsed;
        }
        long fp = output.getFilePointer();
        fps.writeVLong(encode(fp - lastFp));
        lastFp = fp;
        for (int i = 0; i < bufferUsed; i++) {
          output.writeLong(buffer[i]);
        }
        lastFp = fp;
        blockNum++;
        bufferUsed = 0;
      }

      private void reset(IndexOutput resetOutput) {
        output = resetOutput;
        finalBlockSize = 0;
        bufferUsed = 0;
        blockNum = 0;
        lastFp = 0;
        fps.reset();
      }
    }

    private static long encode(long fpDelta) {
      assert (fpDelta & 0x07) == 0 : "fpDelta should be multiple of 8";
      if (fpDelta % BUFFER_BYTES == 0) {
        return ((fpDelta / BUFFER_BYTES) << 1) | 1;
      } else {
        return fpDelta;
      }
    }

    private static long decode(long fpDelta) {
      if ((fpDelta & 1) == 1) {
        return (fpDelta >>> 1) * BUFFER_BYTES;
      } else {
        return fpDelta;
      }
    }

    ForwardIndexSorter(Directory directory) {
      this.directory = directory;
      for (int i = 0; i < HISTOGRAM_SIZE; i++) {
        buckets[i] = new Bucket();
      }
    }

    private void consume(String fileName, LongConsumer consumer) throws IOException {
      try (IndexInput in = directory.openInput(fileName, IOContext.READONCE)) {
        final long end = in.length() - CodecUtil.footerLength();
        while (in.getFilePointer() < end) {
          consumer.accept(in.readLong());
        }
      }
      consumer.onFinish();
    }

    private void consume(String fileName, long indexFP, LongConsumer consumer) throws IOException {
      try (IndexInput index = directory.openInput(fileName, IOContext.READONCE);
          IndexInput value = directory.openInput(fileName, IOContext.READONCE)) {
        index.seek(indexFP);
        for (int i = 0; i < buckets.length; i++) {
          int blockNum = index.readVInt();
          int finalBlockSize = index.readVInt();
          long fp = decode(index.readVLong());
          for (int block = 0; block < blockNum - 1; block++) {
            value.seek(fp);
            for (int j = 0; j < BUFFER_SIZE; j++) {
              consumer.accept(value.readLong());
            }
            fp += decode(index.readVLong());
          }
          value.seek(fp);
          for (int j = 0; j < finalBlockSize; j++) {
            consumer.accept(value.readLong());
          }
        }
        consumer.onFinish();
      }
    }

    private LongConsumer consumer(int shift) {
      return new LongConsumer() {
        @Override
        public void accept(long value) throws IOException {
          int b = (int) ((value >>> shift) & 0xFF);
          Bucket bucket = buckets[b];
          bucket.addEntry(value);
        }

        @Override
        public void onFinish() throws IOException {
          for (Bucket bucket : buckets) {
            bucket.flush(true);
          }
        }
      };
    }

    void sortAndConsume(String fileName, int maxDoc, LongConsumer consumer) throws IOException {
      int bitsRequired = PackedInts.bitsRequired(maxDoc);
      String sourceFileName = fileName;
      long indexFP = -1;
      for (int shift = 0; shift < bitsRequired; shift += 8) {
        try (IndexOutput output = directory.createTempOutput(fileName, "sort", IOContext.DEFAULT)) {
          Arrays.stream(buckets).forEach(b -> b.reset(output));
          if (shift == 0) {
            consume(sourceFileName, consumer(shift));
          } else {
            consume(sourceFileName, indexFP, consumer(shift));
            directory.deleteFile(sourceFileName);
          }
          indexFP = output.getFilePointer();
          for (Bucket bucket : buckets) {
            output.writeVInt(bucket.blockNum);
            output.writeVInt(bucket.finalBlockSize);
            bucket.fps.copyTo(output);
          }
          CodecUtil.writeFooter(output);
          sourceFileName = output.getName();
        }
      }
      consume(sourceFileName, indexFP, consumer);
    }
  }

  interface LongConsumer {
    void accept(long value) throws IOException;

    default void onFinish() throws IOException {}
  }
}
