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
import org.apache.lucene.util.IntroSorter;

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
   * Partitions global doc IDs by leaf. Doc IDs may be supplied in any order; the returned per-leaf
   * arrays are sorted in ascending docId order.
   *
   * <p>This is an optimized subset of {@link #partitionByLeafWithOrdinals(int[], List)} for callers
   * that only need the per-leaf grouping and do not need to map results back to the original input
   * order. It sorts with {@link Arrays#sort(int[])} and skips the extra bookkeeping required to
   * track input ordinals. Callers that need to reassemble per-leaf results into input order
   * (scatter/gather) should use {@link #partitionByLeafWithOrdinals(int[], List)} instead.
   *
   * <p>The input array is not mutated.
   *
   * @param globalDocIds global doc IDs in any order
   * @param leaves the index reader's leaves
   * @return array indexed by leaf ord, containing the (sorted) global doc IDs for that leaf (empty
   *     if no hits land in that leaf)
   */
  public static int[][] partitionByLeaf(int[] globalDocIds, List<LeafReaderContext> leaves) {
    int numLeaves = leaves.size();
    if (globalDocIds.length == 0) {
      int[][] result = new int[numLeaves][];
      Arrays.fill(result, EMPTY_INT_ARRAY);
      return result;
    }
    int[] sortedDocIds = globalDocIds.clone();
    Arrays.sort(sortedDocIds);
    return partitionSortedDocIds(sortedDocIds, leaves);
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
   * <p>This is the fuller-featured counterpart to {@link #partitionByLeaf(int[], List)}: it returns
   * the same per-leaf grouping and additionally records, for every partitioned doc ID, its index in
   * the original {@code globalDocIds} array. Tracking these ordinals carries a small amount of
   * extra work relative to {@link #partitionByLeaf(int[], List)}, so callers that do not need to
   * map results back to input order should prefer that method.
   *
   * <p>The input array is not mutated.
   *
   * @param globalDocIds global doc IDs in any order (e.g., ranking order)
   * @param leaves the index reader's leaves
   * @return per-leaf sorted doc IDs alongside per-leaf ordinals into the input array
   */
  public static PartitionedHits partitionByLeafWithOrdinals(
      int[] globalDocIds, List<LeafReaderContext> leaves) {
    int numLeaves = leaves.size();
    if (globalDocIds.length == 0) {
      int[][] docIdsByLeaf = new int[numLeaves][];
      int[][] ordinalsByLeaf = new int[numLeaves][];
      Arrays.fill(docIdsByLeaf, EMPTY_INT_ARRAY);
      Arrays.fill(ordinalsByLeaf, EMPTY_INT_ARRAY);
      return new PartitionedHits(docIdsByLeaf, ordinalsByLeaf);
    }

    // Sort doc IDs and ordinals as parallel arrays, so we keep the original positions while
    // moving doc IDs into ascending order. IntroSorter avoids the boxing/lambda overhead a
    // comparator-based Arrays.sort would incur on parallel int[]s.
    final int[] sortedDocIds = globalDocIds.clone();
    final int[] sortedOrdinals = new int[globalDocIds.length];
    for (int i = 0; i < sortedOrdinals.length; i++) {
      sortedOrdinals[i] = i;
    }
    new IntroSorter() {
      int pivot;

      @Override
      protected int compare(int i, int j) {
        return Integer.compare(sortedDocIds[i], sortedDocIds[j]);
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = tmp;
        tmp = sortedOrdinals[i];
        sortedOrdinals[i] = sortedOrdinals[j];
        sortedOrdinals[j] = tmp;
      }

      @Override
      protected void setPivot(int i) {
        pivot = sortedDocIds[i];
      }

      @Override
      protected int comparePivot(int j) {
        return Integer.compare(pivot, sortedDocIds[j]);
      }
    }.sort(0, sortedDocIds.length);

    return partitionSortedDocIdsWithOrdinals(sortedDocIds, sortedOrdinals, leaves);
  }

  /** Partitions an already-sorted array of doc IDs into per-leaf slices. */
  private static int[][] partitionSortedDocIds(int[] sortedDocIds, List<LeafReaderContext> leaves) {
    int numLeaves = leaves.size();
    int[][] result = new int[numLeaves][];

    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < sortedDocIds.length; leafIdx++) {
      LeafReaderContext leaf = leaves.get(leafIdx);
      int leafEnd = leaf.docBase + leaf.reader().maxDoc();
      if (sortedDocIds[from] >= leafEnd) {
        result[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(sortedDocIds, from, sortedDocIds.length, leafEnd);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      assert count > 0;
      result[leafIdx] = new int[count];
      System.arraycopy(sortedDocIds, from, result[leafIdx], 0, count);
      from = to;
    }

    Arrays.fill(result, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return result;
  }

  /**
   * Partitions sorted doc IDs and their parallel ordinals into per-leaf slices. {@code
   * sortedDocIds} and {@code sortedOrdinals} must have the same length and be in lockstep: {@code
   * sortedOrdinals[i]} is the original input index of {@code sortedDocIds[i]}.
   */
  private static PartitionedHits partitionSortedDocIdsWithOrdinals(
      int[] sortedDocIds, int[] sortedOrdinals, List<LeafReaderContext> leaves) {
    int numLeaves = leaves.size();
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
