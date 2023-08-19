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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.BufferSize;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Implementation of recursive graph bisection, an approach to doc ID assignment that aims at
 * reducing the gap between consecutive postings.
 *
 * <p>This algorithm was initially described by Dhulipala et al. in "Compressing graphs and inverted
 * indexes with recursive graph bisection". This implementation takes advantage of some
 * optimizations suggested by Mackenzie et al. in "Tradeoff Options for Bipartite Graph
 * Partitioning".
 *
 * <p>Note: This is a slow operation that consumes O(maxDoc + numTerms) memory per thread.
 */
public final class RecursiveGraphBisection implements Cloneable {

  /** Minimum required document frequency for terms to be considered. */
  public static final int DEFAULT_MIN_DOC_FREQ = 4096;

  /**
   * Minimum size of partitions. The algorithm will stop recursing when reaching partitions below
   * this number of documents.
   */
  public static final int DEFAULT_MIN_PARTITION_SIZE = 32;

  /**
   * Default maximum number of iterations per recursion level. Higher numbers of iterations
   * typically don't help significantly.
   */
  public static final int DEFAULT_MAX_ITERS = 20;

  private final int minPartitionSize;
  private final int maxIters;
  private final int numTerms;
  private final ForwardIndex forwardIndex;
  private final DocFreqs leftDocFreqs;
  private final DocFreqs rightDocFreqs;
  private final float[] biases;

  private RecursiveGraphBisection(
      int minPartitionSize, int maxIters, int numTerms, ForwardIndex forwardIndex, float[] biases) {
    this.minPartitionSize = minPartitionSize;
    this.maxIters = maxIters;
    this.numTerms = numTerms;
    this.forwardIndex = forwardIndex;
    this.leftDocFreqs = new DocFreqs(numTerms, forwardIndex);
    this.rightDocFreqs = new DocFreqs(numTerms, forwardIndex);
    this.biases = biases;
  }

  @Override
  public RecursiveGraphBisection clone() {
    // No need to clone biases since each clone will manage its own slice of the biases array
    return new RecursiveGraphBisection(
        minPartitionSize, maxIters, numTerms, forwardIndex.clone(), biases);
  }

  /**
   * A forward index. Like term vectors, but only for a subset of terms, and it produces term IDs
   * instead of whole terms.
   */
  private static final class ForwardIndex implements Cloneable, Closeable {

    private final long[] startOffsets;
    private final IndexInput terms;
    private final int maxTerm;

    private long endOffset = -1;
    private int prevTerm;

    ForwardIndex(long[] startIndexes, IndexInput terms, int maxTerm) {
      this.startOffsets = startIndexes;
      this.terms = terms;
      this.maxTerm = maxTerm;
    }

    void seek(int docID) throws IOException {
      final long startOffset = startOffsets[docID];
      endOffset = startOffsets[docID + 1];
      terms.seek(startOffset);
      prevTerm = 0;
    }

    int nextTerm() throws IOException {
      if (terms.getFilePointer() >= endOffset) {
        assert terms.getFilePointer() == endOffset;
        return -1;
      }
      int term = prevTerm + terms.readVInt();
      assert terms.getFilePointer() <= endOffset;
      assert term < maxTerm : term + " " + maxTerm + " " + prevTerm;
      prevTerm = term;
      return term;
    }

    @Override
    public ForwardIndex clone() {
      return new ForwardIndex(startOffsets, terms.clone(), maxTerm);
    }

    @Override
    public void close() throws IOException {
      terms.close();
    }
  }

  private static class DocFreqs {

    private final ForwardIndex forwardIndex;
    private final int[] dfCache;

    DocFreqs(int numTerms, ForwardIndex forwardIndex) {
      this.forwardIndex = forwardIndex;
      dfCache = new int[numTerms];
    }

    void reset(IntsRef docIDs) {
      // And otherwise using the forward index
      Arrays.fill(dfCache, 0);
      try {
        for (int i = docIDs.offset, end = docIDs.offset + docIDs.length; i < end; ++i) {
          final int doc = docIDs.ints[i];
          forwardIndex.seek(doc);
          for (int termID = forwardIndex.nextTerm();
              termID != -1;
              termID = forwardIndex.nextTerm()) {
            dfCache[termID]++;
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    void increment(int termID) throws IOException {
      assert dfCache[termID] >= 0;
      ++dfCache[termID];
    }

    void decrement(int termID) throws IOException {
      assert dfCache[termID] > 0;
      --dfCache[termID];
    }

    int docFreq(int termID) throws IOException {
      return dfCache[termID];
    }
  }

  private static int writePostings(
      Terms terms, int minDocFreq, Directory tempDir, DataOutput postingsOut) throws IOException {
    int numTerms = 0;
    List<TermState> termStates = new ArrayList<>();
    TermsEnum iterator = terms.iterator();
    PostingsEnum postings = null;
    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if (iterator.docFreq() < minDocFreq) {
        continue;
      }
      if (termStates.size() >= ArrayUtil.MAX_ARRAY_LENGTH) {
        throw new IllegalArgumentException(
            "Cannot perform recursive graph bisection on more than "
                + ArrayUtil.MAX_ARRAY_LENGTH
                + " terms");
      }
      final int termID = numTerms++;
      termStates.add(iterator.termState());
      postings = iterator.postings(postings, PostingsEnum.NONE);
      for (int doc = postings.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = postings.nextDoc()) {
        // reverse bytes so that byte order matches natural order
        postingsOut.writeInt(Integer.reverseBytes(doc));
        postingsOut.writeInt(Integer.reverseBytes(termID));
      }
    }
    return numTerms;
  }

  private static ForwardIndex buildForwardIndex(
      Directory tempDir, String postingsFileName, int maxDoc, int maxTerm, ExecutorService executor)
      throws IOException {
    // TODO: what are some good parameters here?
    String sortedPostingsFile =
        new OfflineSorter(
            tempDir,
            "forward-index",
            (a, b) -> ArrayUtil.compareUnsigned8(a.bytes, a.offset, b.bytes, b.offset),
            BufferSize.megabytes(16),
            OfflineSorter.MAX_TEMPFILES,
            2 * Integer.BYTES,
            executor,
            Runtime.getRuntime().availableProcessors()) {

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
                BitUtil.VH_LE_LONG.set(ref.bytes(), 0, in.readLong());
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
                out.writeLong((long) BitUtil.VH_LE_LONG.get(bytes, off));
              }
            };
          }
        }.sort(postingsFileName);

    final long[] startOffsets = new long[maxDoc + 1];
    String termIDsFileName;
    int prevDoc = -1;
    try (IndexInput sortedPostings = tempDir.openInput(sortedPostingsFile, IOContext.READONCE);
        IndexOutput termIDs = tempDir.createTempOutput("term-ids", "", IOContext.DEFAULT)) {
      termIDsFileName = termIDs.getName();
      final long end = sortedPostings.length() - CodecUtil.footerLength();
      int prevTermID = 0;
      while (sortedPostings.getFilePointer() < end) {
        final int doc = Integer.reverseBytes(sortedPostings.readInt());
        final int termID = Integer.reverseBytes(sortedPostings.readInt());
        if (doc != prevDoc) {
          assert doc > prevDoc;
          for (int d = prevDoc + 1; d <= doc; ++d) {
            startOffsets[d] = termIDs.getFilePointer();
          }
          prevDoc = doc;
          prevTermID = 0;
        }
        assert termID >= prevTermID : prevTermID + " " + termID;
        assert termID < maxTerm : termID + " " + maxTerm;
        termIDs.writeVInt(termID - prevTermID);
        prevTermID = termID;
      }
      for (int d = prevDoc + 1; d <= maxDoc; ++d) {
        startOffsets[d] = termIDs.getFilePointer();
      }
      CodecUtil.writeFooter(termIDs);
    }

    IndexInput termIDsInput = tempDir.openInput(termIDsFileName, IOContext.READ);
    return new ForwardIndex(startOffsets, termIDsInput, maxTerm);
  }

  /** Same as the other reorder method with sensible default values. */
  public static CodecReader reorder(CodecReader reader, Directory tempDir, Terms terms)
      throws IOException {
    ExecutorService executor =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    try {
      return reorder(
          reader,
          tempDir,
          terms,
          DEFAULT_MIN_DOC_FREQ,
          DEFAULT_MIN_PARTITION_SIZE,
          DEFAULT_MAX_ITERS,
          executor);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Reorder the given {@link CodecReader} into a reader that tries to minimize the log gap between
   * consecutive documents in postings, which usually helps improve space efficiency and query
   * evaluation efficiency. Note that the returned {@link CodecReader} is slow and should typically
   * be used in a call to {@link IndexWriter#addIndexes(CodecReader...)}.
   */
  public static CodecReader reorder(
      CodecReader reader,
      Directory tempDir,
      Terms terms,
      int minDocFreq,
      int minPartitionSize,
      int maxIters,
      ExecutorService executor)
      throws IOException {
    int[] newToOld =
        computePermutation(
            tempDir, reader.maxDoc(), terms, minDocFreq, minPartitionSize, maxIters, executor);
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
  private static int[] computePermutation(
      Directory dir,
      int maxDoc,
      Terms terms,
      int minDocFreq,
      int minPartitionSize,
      int maxIters,
      ExecutorService executor)
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

    ForwardIndex forwardIndex = null;
    IndexOutput postingsOutput = null;
    boolean success = false;
    try {
      postingsOutput = tempDir.createTempOutput("postings", "", IOContext.DEFAULT);
      int numTerms = writePostings(terms, minDocFreq, tempDir, postingsOutput);
      CodecUtil.writeFooter(postingsOutput);
      postingsOutput.close();
      forwardIndex =
          buildForwardIndex(tempDir, postingsOutput.getName(), maxDoc, numTerms, executor);
      tempDir.deleteFile(postingsOutput.getName());
      postingsOutput = null;

      int[] sortedDocs = new int[maxDoc];
      for (int i = 0; i < maxDoc; ++i) {
        sortedDocs[i] = i;
      }

      try (CloseableThreadLocal<RecursiveGraphBisection> threadLocal =
          new CloseableThreadLocal<>()) {
        Queue<Future<?>> futures = new ConcurrentLinkedQueue<>();
        final ForwardIndex finalForwardIndex = forwardIndex;
        final CountDownLatch latch = new CountDownLatch(1);
        executor.submit(
            () -> {
              RecursiveGraphBisection rgb =
                  new RecursiveGraphBisection(
                      minPartitionSize, maxIters, numTerms, finalForwardIndex, new float[maxDoc]);
              threadLocal.set(rgb);
              rgb.recurse(
                  new IntsRef(sortedDocs, 0, sortedDocs.length), executor, futures, threadLocal);
              latch.countDown();
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

  /**
   * Helper method to control recursion on {@link #computePermutation(Directory, int, Terms, int,
   * int, int, ExecutorService)}.
   */
  private void recurse(
      IntsRef docIDs,
      ExecutorService executor,
      Collection<Future<?>> futures,
      CloseableThreadLocal<RecursiveGraphBisection> threadLocal) {
    int leftSize = docIDs.length / 2;
    int rightSize = docIDs.length - leftSize;
    IntsRef left = new IntsRef(docIDs.ints, docIDs.offset, leftSize);
    IntsRef right = new IntsRef(docIDs.ints, docIDs.offset + leftSize, rightSize);

    if (left.length < minPartitionSize) {
      return;
    }

    leftDocFreqs.reset(left);
    rightDocFreqs.reset(right);

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

    recurse(left, executor, futures, threadLocal);
    futures.add(
        executor.submit(
            () -> {
              RecursiveGraphBisection rgb = threadLocal.get();
              if (rgb == null) {
                rgb = clone();
                threadLocal.set(rgb);
              }
              rgb.recurse(right, executor, futures, threadLocal);
            }));
  }

  /**
   * Shuffle doc IDs across both partitions so that each partition has lower gaps between
   * consecutive postings.
   */
  private static boolean shuffle(
      ForwardIndex forwardIndex,
      IntsRef left,
      IntsRef right,
      DocFreqs leftDocFreqs,
      DocFreqs rightDocFreqs,
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
      int docID, ForwardIndex forwardIndex, DocFreqs leftDocFreqs, DocFreqs rightDocFreqs)
      throws IOException {
    forwardIndex.seek(docID);
    double bias = 0;
    for (int termID = forwardIndex.nextTerm(); termID != -1L; termID = forwardIndex.nextTerm()) {
      // This uses the simpler estimator proposed by Mackenzie et al in "Tradeoff Options for
      // Bipartite Graph Partitioning"
      final int leftDocFreq = leftDocFreqs.docFreq(termID);
      final int rightDocFreq = rightDocFreqs.docFreq(termID);
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
      DocFreqs leftDocFreqs,
      DocFreqs rightDocFreqs)
      throws IOException {
    assert left < right;

    int leftDoc = docs[left];
    int rightDoc = docs[right];

    // Now update the cache, this makes things much faster than invalidating it and having to
    // recompute doc freqs on the next iteration.
    forwardIndex.seek(leftDoc);
    for (int term = forwardIndex.nextTerm(); term != -1; term = forwardIndex.nextTerm()) {
      leftDocFreqs.decrement(term);
      rightDocFreqs.increment(term);
    }

    forwardIndex.seek(rightDoc);
    for (int term = forwardIndex.nextTerm(); term != -1; term = forwardIndex.nextTerm()) {
      leftDocFreqs.increment(term);
      rightDocFreqs.decrement(term);
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
    if (i > 0) {
      // floorLog2 would be the exponent in the float representation of i
      int floorLog2 = 31 - Integer.numberOfLeadingZeros(i);
      // tableIndex would be the first 8 mantissa bits in the float representation of i, excluding
      // the implicit bit
      int tableIndex = i << (32 - floorLog2) >>> (32 - 8);
      // i = 1.tableIndex * 2 ^ floorLog2
      // log(i) = log2(1.tableIndex) + floorLog2
      return floorLog2 + LOG2_TABLE[tableIndex];
    } else if (i == 0) {
      return Float.NEGATIVE_INFINITY;
    } else {
      throw new IllegalArgumentException("Log of negative number: " + i);
    }
  }
}
