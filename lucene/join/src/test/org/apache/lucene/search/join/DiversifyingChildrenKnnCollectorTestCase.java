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

package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;

/**
 * Base class for {@link DiversifyingNearestChildrenKnnCollector} tests. Provides shared helpers for
 * building the block-join parent {@link BitSet} used across test subclasses.
 */
abstract class DiversifyingChildrenKnnCollectorTestCase extends LuceneTestCase {

  /**
   * Builds a {@link BitSet} whose set bits are the parent doc ids in a contiguous block-join
   * layout: {@code [child_0 … child_{C-1}, parent_C]}, repeated {@code numParents} times.
   *
   * <p>Example with {@code childrenPerParent=3}: parent doc ids are 3, 7, 11, …
   */
  static BitSet parentBitSet(int numParents, int childrenPerParent) throws IOException {
    int[] parentDocIds = new int[numParents];
    for (int p = 1; p <= numParents; p++) {
      parentDocIds[p - 1] = p * (childrenPerParent + 1) - 1;
    }
    int totalDocs = numParents * (childrenPerParent + 1);
    return BitSet.of(
        new TestToParentJoinKnnResults.IntArrayDocIdSetIterator(parentDocIds, numParents),
        totalDocs + 1);
  }

  /**
   * Builds a docId-to-ordinal array for the block-join layout. Parent docs get -1 (no vector);
   * child docs get consecutive ordinals starting from 0.
   */
  static int[] buildDocToOrd(int numParents, int childrenPerParent) {
    int totalDocs = numParents * (childrenPerParent + 1);
    int[] docToOrd = new int[totalDocs];
    Arrays.fill(docToOrd, -1);
    int ord = 0;
    for (int d = 0; d < totalDocs; d++) {
      if ((d + 1) % (childrenPerParent + 1) != 0) {
        docToOrd[d] = ord++;
      }
    }
    return docToOrd;
  }

  static DiversifyingNearestChildrenKnnCollector makeCollector(
      int k, BitSet parents, int[] docToOrd) {
    return new DiversifyingNearestChildrenKnnCollector(
        k, Integer.MAX_VALUE, (KnnSearchStrategy) null, parents, docToOrd);
  }
}
