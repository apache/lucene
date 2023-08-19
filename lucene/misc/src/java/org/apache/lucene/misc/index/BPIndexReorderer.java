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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.BufferSize;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Implementation of "recursive graph bisection", also called "bipartite graph partitioning" and
 * often abbreviated BP, an approach to doc ID assignment that aims at reducing the sum of the log
 * gap between consecutive postings.
 *
 * <p>This algorithm was initially described by Dhulipala et al. in "Compressing graphs and inverted
 * indexes with recursive graph bisection". This implementation takes advantage of some
 * optimizations suggested by Mackenzie et al. in "Tradeoff Options for Bipartite Graph
 * Partitioning".
 *
 * <p>Note: This is a slow operation that consumes O(maxDoc + numTerms) memory per thread.
 */
public final class BPIndexReorderer {

  /** Exception that is thrown when not enough RAM is available. */
  public static class NotEnoughRAMException extends RuntimeException {
    NotEnoughRAMException(String message) {
      super(message);
    }
  }

  private static final int TERM_IDS_BLOCK_SIZE = 17;

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

  /** Default RAM budget: 64MB. Consider raising it when working on large indexes. */
  public static final int DEFAULT_RAM_BUDGET_MB = 64;

  private int minDocFreq;
  private int minPartitionSize;
  private int maxIters;
  private ExecutorService executorService;
  private int concurrency;
  private double ramBudgetMB;
  private Set<String> fields;

  /** Constructor. */
  public BPIndexReorderer() {
    setMinDocFreq(DEFAULT_MIN_DOC_FREQ);
    setMinPartitionSize(DEFAULT_MIN_PARTITION_SIZE);
    setMaxIters(DEFAULT_MAX_ITERS);
    setExecutorService(new SameThreadExecutorService(), 1);
    setRAMBudgetMB(DEFAULT_RAM_BUDGET_MB);
    setFields(null);
  }

  /** Set the minimum document frequency for terms to be considered. */
  public void setMinDocFreq(int minDocFreq) {
    if (minDocFreq < 1) {
      throw new IllegalArgumentException("minDocFreq must be at least 1, got " + minDocFreq);
    }
    this.minDocFreq = minDocFreq;
  }

  /** Set the minimum partition size, when the algorithm stops recursing. */
  public void setMinPartitionSize(int minPartitionSize) {
    if (minPartitionSize < 1) {
      throw new IllegalArgumentException(
          "minPartitionSize must be at least 1, got " + minPartitionSize);
    }
    this.minPartitionSize = minPartitionSize;
  }

  /** Set the maximum number of iterations on each recursion level. */
  public void setMaxIters(int maxIters) {
    if (maxIters < 1) {
      throw new IllegalArgumentException("maxIters must be at least 1, got " + maxIters);
    }
    this.maxIters = maxIters;
  }

  /**
   * Set the {@link ExecutorService} to run graph partitioning concurrently. {@code concurrency}
   * must indicate the concurrency of the {@link ExecutorService}.
   */
  public void setExecutorService(ExecutorService executorService, int concurrency) {
    if (executorService == null) {
      throw new IllegalArgumentException("The executor service must be non-null");
    }
    if (concurrency < 1) {
      throw new IllegalArgumentException("concurrency must be at least 1, got " + concurrency);
    }
    this.executorService = executorService;
    this.concurrency = concurrency;
  }

  /**
   * Set the amount of RAM that graph partitioning is allowed to use. More RAM allows running
   * faster. If not enough RAM is provided, reordering will not happen.
   */
  public void setRAMBudgetMB(double ramBudgetMB) {
    this.ramBudgetMB = ramBudgetMB;
  }

  /**
   * Sets the fields to use to perform partitioning. A {@code null} value indicates that all indexed
   * fields should be used.
   */
  public void setFields(Set<String> fields) {
    this.fields = fields == null ? null : new HashSet<>(fields);
  }

  private class RecursionHelper implements Cloneable {

    private final int numTerms;
    private final ForwardIndex forwardIndex;
    private final int[] leftDocFreqs;
    private final int[] rightDocFreqs;
    private final float[] biases;

    private RecursionHelper(int numTerms, ForwardIndex forwardIndex, float[] biases) {
      this.numTerms = numTerms;
      this.forwardIndex = forwardIndex;
      this.leftDocFreqs = new int[numTerms];
      this.rightDocFreqs = new int[numTerms];
      this.biases = biases;
    }

    @Override
    public RecursionHelper clone() {
      // No need to clone biases since each clone will manage its own slice of the biases array
      return new RecursionHelper(numTerms, forwardIndex.clone(), biases);
    }

    private void initLeftDocFreqs(IntsRef docs) {
      try {
        Arrays.fill(leftDocFreqs, 0);
        for (int i = docs.offset, end = docs.offset + docs.length; i < end; ++i) {
          final int doc = docs.ints[i];
          forwardIndex.seek(doc);
          for (int termID = forwardIndex.nextTerm();
              termID != -1;
              termID = forwardIndex.nextTerm()) {
            ++leftDocFreqs[termID];
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Update doc freqs. {@code leftDocFreqs} must initially contain doc freqs across left and
     * right, and {@code rightDocFreqs} must be empty.
     */
    private void updateDocFreqs(IntsRef rightDocs, int[] leftDocFreqs, int[] rightDocFreqs) {
      try {
        for (int i = rightDocs.offset, end = rightDocs.offset + rightDocs.length; i < end; ++i) {
          final int doc = rightDocs.ints[i];
          forwardIndex.seek(doc);
          for (int termID = forwardIndex.nextTerm();
              termID != -1;
              termID = forwardIndex.nextTerm()) {
            --leftDocFreqs[termID];
            assert leftDocFreqs[termID] >= 0;
            ++rightDocFreqs[termID];
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Helper method to control recursion of {@link BPIndexReorderer#computePermutation(CodecReader,
     * Set, Directory)}.
     */
    private void recurse(
        IntsRef docIDs,
        ExecutorService executor,
        Collection<Future<?>> futures,
        CloseableThreadLocal<RecursionHelper> threadLocal) {
      int leftSize = docIDs.length / 2;
      int rightSize = docIDs.length - leftSize;
      IntsRef left = new IntsRef(docIDs.ints, docIDs.offset, leftSize);
      IntsRef right = new IntsRef(docIDs.ints, docIDs.offset + leftSize, rightSize);

      if (left.length < minPartitionSize) {
        return;
      }

      Arrays.fill(rightDocFreqs, 0);
      updateDocFreqs(right, leftDocFreqs, rightDocFreqs);

      for (int iter = 0; iter < maxIters; ++iter) {
        boolean moved;
        try {
          moved = shuffle(forwardIndex, left, right, leftDocFreqs, rightDocFreqs, biases, iter);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        if (moved == false) {
          break;
        }
      }

      Runnable recurseRight =
          () -> {
            RecursionHelper rgb = threadLocal.get();
            if (rgb == null) {
              rgb = clone();
              threadLocal.set(rgb);
            }
            Arrays.fill(rgb.leftDocFreqs, 0);
            rgb.initLeftDocFreqs(right);
            rgb.recurse(right, executor, futures, threadLocal);
          };

      Future<?> rightFuture = tryEnqueue(executor, recurseRight);
      recurse(left, executor, futures, threadLocal);
      if (rightFuture == null) {
        rightFuture = executor.submit(recurseRight);
      }
      futures.add(rightFuture);
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
    private int bufferOffset;
    private int bufferLen;

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
      bufferOffset = bufferLen = 0;
    }

    int nextTerm() throws IOException {
      if (bufferOffset < bufferLen) {
        return buffer[bufferOffset++];
      } else if (terms.getFilePointer() >= endOffset) {
        assert terms.getFilePointer() == endOffset;
        return -1;
      } else {
        bufferLen = readMonotonicInts(terms, buffer);
        bufferOffset = 1;
        return buffer[0];
      }
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
                / concurrency
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
            BufferSize.megabytes((long) (ramBudgetMB / concurrency)),
            OfflineSorter.MAX_TEMPFILES,
            2 * Integer.BYTES,
            executorService,
            concurrency) {

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

    final Set<String> tempFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
    Directory tempDir =
        new FilterDirectory(dir) {
          @Override
          public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
              throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            tempFiles.add(out.getName());
            return out;
          }

          @Override
          public void deleteFile(String name) throws IOException {
            super.deleteFile(name);
            tempFiles.remove(name);
          }
        };

    final int maxDoc = reader.maxDoc();
    ForwardIndex forwardIndex = null;
    IndexOutput postingsOutput = null;
    boolean success = false;
    try {
      postingsOutput = tempDir.createTempOutput("postings", "", IOContext.DEFAULT);
      int numTerms = writePostings(reader, fields, tempDir, postingsOutput);
      CodecUtil.writeFooter(postingsOutput);
      postingsOutput.close();
      forwardIndex = buildForwardIndex(tempDir, postingsOutput.getName(), maxDoc, numTerms);
      tempDir.deleteFile(postingsOutput.getName());
      postingsOutput = null;

      int[] sortedDocs = new int[maxDoc];
      for (int i = 0; i < maxDoc; ++i) {
        sortedDocs[i] = i;
      }

      try (CloseableThreadLocal<RecursionHelper> threadLocal = new CloseableThreadLocal<>()) {
        Queue<Future<?>> futures = new ConcurrentLinkedQueue<>();
        final ForwardIndex finalForwardIndex = forwardIndex;
        final CountDownLatch latch = new CountDownLatch(1);
        executorService.submit(
            () -> {
              try {
                RecursionHelper rgb =
                    new RecursionHelper(numTerms, finalForwardIndex, new float[maxDoc]);
                threadLocal.set(rgb);
                IntsRef docs = new IntsRef(sortedDocs, 0, sortedDocs.length);
                rgb.initLeftDocFreqs(docs);
                rgb.recurse(docs, executorService, futures, threadLocal);
              } finally {
                latch.countDown();
              }
            });

        try {
          latch.await();
          for (Future<?> future = futures.poll(); future != null; future = futures.poll()) {
            future.get();
          }
        } catch (InterruptedException e) {
          throw new ThreadInterruptedException(e);
        } catch (ExecutionException e) {
          IOUtils.rethrowAlways(e.getCause());
        }
      }

      success = true;
      return sortedDocs;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(postingsOutput);
      }
      IOUtils.close(forwardIndex);
      for (String file : tempFiles) {
        dir.deleteFile(file);
      }
    }
  }

  private static Future<?> tryEnqueue(ExecutorService executor, Runnable runnable) {
    final AtomicBoolean queued = new AtomicBoolean(true);
    final AtomicBoolean mayRunInSameThread = new AtomicBoolean(false);
    Thread currentThread = Thread.currentThread();
    Future<?> future =
        executor.submit(
            () -> {
              if (Thread.currentThread() == currentThread && mayRunInSameThread.get() == false) {
                queued.set(false);
              } else {
                runnable.run();
              }
            });
    mayRunInSameThread.set(true);
    if (queued.get() == false) {
      return null;
    } else {
      return future;
    }
  }

  /**
   * Shuffle doc IDs across both partitions so that each partition has lower gaps between
   * consecutive postings.
   */
  private static boolean shuffle(
      ForwardIndex forwardIndex,
      IntsRef left,
      IntsRef right,
      int[] leftDocFreqs,
      int[] rightDocFreqs,
      float[] biases,
      int iter)
      throws IOException {
    assert left.ints == right.ints;
    assert left.offset + left.length == right.offset;
    assert sorted(left);
    assert sorted(right);

    for (int i = left.offset, end = left.offset + left.length; i < end; ++i) {
      biases[i] = computeBias(left.ints[i], forwardIndex, leftDocFreqs, rightDocFreqs);
    }
    for (int i = right.offset, end = right.offset + right.length; i < end; ++i) {
      biases[i] = computeBias(right.ints[i], forwardIndex, rightDocFreqs, leftDocFreqs);
    }

    IntroSorter byDescendingGainSorter =
        new IntroSorter() {

          int pivotDoc;
          float pivotGain;

          @Override
          protected void setPivot(int i) {
            pivotDoc = left.ints[i];
            pivotGain = biases[i];
          }

          @Override
          protected int comparePivot(int j) {
            // Compare in reverse order to get a descending sort
            int cmp = Float.compare(biases[j], pivotGain);
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

            float tmpGain = biases[i];
            biases[i] = biases[j];
            biases[j] = tmpGain;
          }
        };

    byDescendingGainSorter.sort(left.offset, left.offset + left.length);
    byDescendingGainSorter.sort(right.offset, right.offset + right.length);

    // This uses the simulated annealing proposed by Mackenzie et al in "Tradeoff Options for
    // Bipartite Graph Partitioning" by comparing the gain against `iter` rather than zero.
    if (biases[left.offset] + biases[right.offset] <= iter) {
      Arrays.sort(left.ints, left.offset, left.offset + left.length);
      Arrays.sort(right.ints, right.offset, right.offset + right.length);
      return false;
    }
    swap(left.ints, left.offset, right.offset, forwardIndex, leftDocFreqs, rightDocFreqs);

    for (int i = 1; i < left.length; ++i) {
      if (biases[left.offset + i] + biases[right.offset + i] <= iter) {
        break;
      }

      swap(left.ints, left.offset + i, right.offset + i, forwardIndex, leftDocFreqs, rightDocFreqs);
    }

    Arrays.sort(left.ints, left.offset, left.offset + left.length);
    Arrays.sort(right.ints, right.offset, right.offset + right.length);

    return true;
  }

  /**
   * Compute a float that is negative when a document is attracted to the left and positive
   * otherwise.
   */
  private static float computeBias(
      int docID, ForwardIndex forwardIndex, int[] leftDocFreqs, int[] rightDocFreqs)
      throws IOException {
    forwardIndex.seek(docID);
    double bias = 0;
    for (int termID = forwardIndex.nextTerm(); termID != -1L; termID = forwardIndex.nextTerm()) {
      // This uses the simpler estimator proposed by Mackenzie et al in "Tradeoff Options for
      // Bipartite Graph Partitioning"
      final int leftDocFreq = leftDocFreqs[termID];
      final int rightDocFreq = rightDocFreqs[termID];
      bias +=
          (rightDocFreq == 0 ? 0 : fastLog2(rightDocFreq))
              - (leftDocFreq == 0 ? 0 : fastLog2(leftDocFreq));
    }
    return (float) bias;
  }

  private static void swap(
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
    for (int term = forwardIndex.nextTerm(); term != -1; term = forwardIndex.nextTerm()) {
      --leftDocFreqs[term];
      ++rightDocFreqs[term];
    }

    forwardIndex.seek(rightDoc);
    for (int term = forwardIndex.nextTerm(); term != -1; term = forwardIndex.nextTerm()) {
      ++leftDocFreqs[term];
      --rightDocFreqs[term];
    }

    docs[left] = rightDoc;
    docs[right] = leftDoc;
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
