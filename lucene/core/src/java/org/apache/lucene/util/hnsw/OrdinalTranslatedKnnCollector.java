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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;

/**
 * Wraps a provided KnnCollector object, translating the provided vectorId ordinal to a documentId.
 * Sibling expansion is active only when the wrapped collector also implements {@link
 * DocSiblingExpansion}.
 */
public final class OrdinalTranslatedKnnCollector extends KnnCollector.Decorator {

  private final IntToIntFunction vectorOrdinalToDocId;

  public OrdinalTranslatedKnnCollector(
      KnnCollector collector, IntToIntFunction vectorOrdinalToDocId) {
    super(collector);
    this.vectorOrdinalToDocId = vectorOrdinalToDocId;
  }

  @Override
  public boolean collect(int vectorId, float similarity) {
    return super.collect(vectorOrdinalToDocId.apply(vectorId), similarity);
  }

  @Override
  public TopDocs topDocs() {
    TopDocs td = super.topDocs();
    return new TopDocs(
        new TotalHits(
            visitedCount(),
            this.earlyTerminated()
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO),
        td.scoreDocs);
  }

  // Needed since we could have a TopKnnCollector at this point
  public boolean isSiblingExpansionCollector() {
    return collector instanceof DocSiblingExpansion;
  }

  public int[] getSiblingOrdinals(int hnswNode, BitSet visitedHnswNodes, int[] siblingOrdinals) {
    DocSiblingExpansion docExpanderCollector = (DocSiblingExpansion) collector;
    int docId = vectorOrdinalToDocId.apply(hnswNode);
    // We do not check if parent is in heap since if we already seed A
    // - A was found and scored (parent added), but we reach the budget limit and were not able to
    // score B,
    //   we then found B through graph traversal. We want to visit it even if we already visited A.
    //   We do not visit siblings in score order.
    int[] siblingDocIds = docExpanderCollector.findSiblingDocIds(docId);
    if (siblingOrdinals.length < siblingDocIds.length) {
      siblingOrdinals = new int[siblingDocIds.length];
    }
    // siblingOrdinals is pre-allocated to siblingDocIds.length and Java initializes int arrays to 0
    // so this variable is necessary.
    // due to visited result we could have a partial array to return
    int count = 0;
    for (int sibDocId : siblingDocIds) {
      int sibOrd = docExpanderCollector.docIdToOrdinal(sibDocId);
      // sibOrd = -1: sibling has no vector for this field → no HNSW node, cannot be scored.
      //
      // !visitedHnswNodes: sibling was already reached via normal graph traversal:
      //   - B was scored via traversal but with score < minAcceptedSimilarity, so collect()
      //     was never called and the parent is not in the heap. Expansion from a different
      //     child A finds B already visited.
      if (sibOrd >= 0 && !visitedHnswNodes.get(sibOrd)) {
        siblingOrdinals[count++] = sibOrd;
      }
    }
    if (count == 0) {
      return new int[0];
    }
    return count < siblingOrdinals.length
        ? ArrayUtil.copyOfSubArray(siblingOrdinals, 0, count)
        : siblingOrdinals;
  }
}
