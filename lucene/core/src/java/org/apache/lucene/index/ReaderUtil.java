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
import org.apache.lucene.search.ScoreDoc;

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
   * Partitions global doc IDs from ScoreDoc array by leaf. Extracts doc IDs, sorts them, and
   * partitions across leaves.
   *
   * @param hits the ScoreDoc array (typically from TopDocs.scoreDocs)
   * @param leaves the index reader's leaves
   * @return array indexed by leaf ord, containing global doc IDs for that leaf (empty if no hits)
   */
  public static int[][] partitionByLeaf(ScoreDoc[] hits, List<LeafReaderContext> leaves) {
    int[] sortedDocIds = new int[hits.length];
    for (int i = 0; i < hits.length; i++) {
      sortedDocIds[i] = hits[i].doc;
    }
    Arrays.sort(sortedDocIds);
    int numLeaves = leaves.size();
    int[][] result = new int[numLeaves][];
    if (sortedDocIds.length == 0) {
      Arrays.fill(result, EMPTY_INT_ARRAY);
      return result;
    }
    int leafStart = 0;
    int leafIdx = 0;
    LeafReaderContext leaf = leaves.getFirst();
    int leafEnd = leaf.docBase + leaf.reader().maxDoc();
    for (int i = 0; i < sortedDocIds.length; i++) {
      int docId = sortedDocIds[i];
      while (docId >= leafEnd) {
        int count = i - leafStart;
        if (count == 0) {
          result[leafIdx] = EMPTY_INT_ARRAY;
        } else {
          result[leafIdx] = new int[count];
          System.arraycopy(sortedDocIds, leafStart, result[leafIdx], 0, count);
        }
        leafStart = i;
        leafIdx++;
        leaf = leaves.get(leafIdx);
        leafEnd = leaf.docBase + leaf.reader().maxDoc();
      }
    }
    // Handle remaining docIDs
    int count = sortedDocIds.length - leafStart;
    assert count > 0;
    result[leafIdx] = new int[count];
    System.arraycopy(sortedDocIds, leafStart, result[leafIdx], 0, count);
    // Fill remaining empty leaves
    Arrays.fill(result, leafIdx + 1, numLeaves, EMPTY_INT_ARRAY);
    return result;
  }
}
