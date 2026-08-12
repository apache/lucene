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
package org.apache.lucene.index;

import java.util.Arrays;
import java.util.List;

/**
 * Common util methods for dealing with {@link IndexReader}s and {@link IndexReaderContext}s.
 *
 * @lucene.internal
 */
public final class ReaderUtil {

  private static final int[] EMPTY_INT_ARRAY = new int[0];

  private ReaderUtil() {} // no instance

  /**
   * Walks up the reader tree and return the given context's top level reader context, or in other
   * words the reader tree's root context.
   */
  public static IndexReaderContext getTopLevelContext(IndexReaderContext context) {
    while (context.parent != null) {
      context = context.parent;
    }
    return context;
  }

  /**
   * Returns index of the searcher/reader for document <code>n</code> in the array used to construct
   * this searcher/reader.
   */
  public static int subIndex(int n, int[] docStarts) {
    // find searcher/reader for doc n:
    int size = docStarts.length;
    int lo = 0; // search starts array
    int hi = size - 1; // for first element less than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = docStarts[mid];
      if (n < midValue) {
        hi = mid - 1;
      } else if (n > midValue) {
        lo = mid + 1;
      } else { // found a match
        while (mid + 1 < size && docStarts[mid + 1] == midValue) {
          mid++; // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }

  /**
   * Returns index of the searcher/reader for document <code>n</code> in the array used to construct
   * this searcher/reader.
   */
  public static int subIndex(int n, List<LeafReaderContext> leaves) {
    // find searcher/reader for doc n:
    int size = leaves.size();
    int lo = 0; // search starts array
    int hi = size - 1; // for first element less than n, return its index
    while (hi >= lo) {
      int mid = (lo + hi) >>> 1;
      int midValue = leaves.get(mid).docBase;
      if (n < midValue) {
        hi = mid - 1;
      } else if (n > midValue) {
        lo = mid + 1;
      } else { // found a match
        while (mid + 1 < size && leaves.get(mid + 1).docBase == midValue) {
          mid++; // scan to last match
        }
        return mid;
      }
    }
    return hi;
  }

  /**
   * Result of partitioning doc IDs by leaf, including the original input ordinals for
   * scatter/gather. {@code docIdsByLeaf[k]} holds the sorted global doc IDs that fall in leaf
   * {@code k}, and {@code ordinalsByLeaf[k][i]} is the index in the original {@code globalDocIds}
   * input array of the doc ID at {@code docIdsByLeaf[k][i]}.
   *
   * <p>Both arrays have the same shape: {@code ordinalsByLeaf[k].length == docIdsByLeaf[k].length}
   * for every leaf {@code k}.
   *
   * @param docIdsByLeaf per-leaf sorted global doc IDs; {@code docIdsByLeaf[k]} holds the doc IDs
   *     that fall in leaf {@code k} (empty if none)
   * @param ordinalsByLeaf per-leaf original input positions; {@code ordinalsByLeaf[k][i]} is the
   *     index in the original input array of the doc ID at {@code docIdsByLeaf[k][i]}
   */
  public record PartitionedHits(int[][] docIdsByLeaf, int[][] ordinalsByLeaf) {}

  /**
   * Partitions global doc IDs by leaf, tracking each doc ID's original position in the input array
   * so callers can reassemble per-leaf results back to input order (scatter/gather).
   *
   * <p>Doc IDs may be supplied in any order (e.g., ranking order); within each leaf the returned
   * doc IDs are sorted in ascending order. For every partitioned doc ID the result also records its
   * index in the original {@code globalDocIds} array, so callers can map per-leaf results back to
   * input order. Callers that do not need the ordinals can simply ignore {@link
   * PartitionedHits#ordinalsByLeaf()} and use {@link PartitionedHits#docIdsByLeaf()} — tracking the
   * ordinals is cheap enough (see the {@code PartitionByLeafBenchmark}) that a separate no-ordinals
   * method is not worth maintaining.
   *
   * <p>The input array is not mutated.
   *
   * @param globalDocIds global doc IDs in any order (e.g., ranking order)
   * @param leaves the index reader's leaves
   * @return per-leaf sorted doc IDs alongside per-leaf ordinals into the input array
   */
  public static PartitionedHits partitionByLeaf(
      int[] globalDocIds, List<LeafReaderContext> leaves) {
    int numLeaves = leaves.size();
    if (globalDocIds.length == 0) {
      int[][] docIdsByLeaf = new int[numLeaves][];
      int[][] ordinalsByLeaf = new int[numLeaves][];
      Arrays.fill(docIdsByLeaf, EMPTY_INT_ARRAY);
      Arrays.fill(ordinalsByLeaf, EMPTY_INT_ARRAY);
      return new PartitionedHits(docIdsByLeaf, ordinalsByLeaf);
    }

    // Pack (docId, ordinal) into a single long -- docId in the high 32 bits, the original input
    // position in the low 32 bits -- then sort as primitives. Sorting ascending orders by docId,
    // with the ordinal as a tiebreak (docIds are unique here, so the tiebreak never applies). This
    // relies on both values being non-negative (Lucene doc IDs and array indices), so the sign bit
    // is always clear and signed long order matches ascending docId order. Packing lets us use the
    // tuned primitive Arrays.sort(long[]) with no per-comparison callbacks and a single contiguous
    // array, which is faster than a comparator/IntroSorter over parallel int[]s.
    final long[] packed = new long[globalDocIds.length];
    for (int i = 0; i < packed.length; i++) {
      packed[i] = ((long) globalDocIds[i] << 32) | (i & 0xFFFFFFFFL);
    }
    Arrays.sort(packed);
    final int[] sortedDocIds = new int[packed.length];
    final int[] sortedOrdinals = new int[packed.length];
    for (int i = 0; i < packed.length; i++) {
      sortedDocIds[i] = (int) (packed[i] >>> 32);
      sortedOrdinals[i] = (int) packed[i];
    }

    // Partition the sorted doc IDs (and their parallel ordinals) into per-leaf slices, using a
    // binary search on each leaf's end boundary.
    int[][] docIdsByLeaf = new int[numLeaves][];
    int[][] ordinalsByLeaf = new int[numLeaves][];
    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < sortedDocIds.length; leafIdx++) {
      LeafReaderContext leaf = leaves.get(leafIdx);
      int leafEnd = leaf.docBase + leaf.reader().maxDoc();
      if (sortedDocIds[from] >= leafEnd) {
        docIdsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        ordinalsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(sortedDocIds, from, sortedDocIds.length, leafEnd);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      assert count > 0;
      docIdsByLeaf[leafIdx] = new int[count];
      ordinalsByLeaf[leafIdx] = new int[count];
      System.arraycopy(sortedDocIds, from, docIdsByLeaf[leafIdx], 0, count);
      System.arraycopy(sortedOrdinals, from, ordinalsByLeaf[leafIdx], 0, count);
      from = to;
    }

    Arrays.fill(docIdsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    Arrays.fill(ordinalsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return new PartitionedHits(docIdsByLeaf, ordinalsByLeaf);
  }
}
