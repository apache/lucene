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
   * PartitionedHits#ordinalsByLeaf()} and use {@link PartitionedHits#docIdsByLeaf()}.
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

    // Pack each (docId, ordinal) into a long: docId in the high 32 bits, ordinal in the low 32,
    // then sort with the primitive Arrays.sort(long[]). Both values are non-negative, so ascending
    // long order matches ascending docId order and the ordinal rides along.
    final long[] packed = new long[globalDocIds.length];
    for (int i = 0; i < packed.length; i++) {
      packed[i] = ((long) globalDocIds[i] << 32) | (i & 0xFFFFFFFFL);
    }
    Arrays.sort(packed);

    // Partition into per-leaf slices via binary search on each leaf's end boundary
    int[][] docIdsByLeaf = new int[numLeaves][];
    int[][] ordinalsByLeaf = new int[numLeaves][];
    int from = 0;
    int leafIdx = 0;
    for (; leafIdx < numLeaves && from < packed.length; leafIdx++) {
      LeafReaderContext leaf = leaves.get(leafIdx);
      long leafEndPacked = ((long) (leaf.docBase + leaf.reader().maxDoc())) << 32;
      if (packed[from] >= leafEndPacked) {
        docIdsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        ordinalsByLeaf[leafIdx] = EMPTY_INT_ARRAY;
        continue;
      }
      int to = Arrays.binarySearch(packed, from, packed.length, leafEndPacked);
      if (to < 0) {
        to = -to - 1;
      }
      int count = to - from;
      assert count > 0;
      int[] leafDocs = new int[count];
      int[] leafOrds = new int[count];
      for (int i = 0; i < count; i++) {
        long p = packed[from + i];
        leafDocs[i] = (int) (p >>> 32);
        leafOrds[i] = (int) p;
      }
      docIdsByLeaf[leafIdx] = leafDocs;
      ordinalsByLeaf[leafIdx] = leafOrds;
      from = to;
    }

    Arrays.fill(docIdsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    Arrays.fill(ordinalsByLeaf, leafIdx, numLeaves, EMPTY_INT_ARRAY);
    return new PartitionedHits(docIdsByLeaf, ordinalsByLeaf);
  }
}
