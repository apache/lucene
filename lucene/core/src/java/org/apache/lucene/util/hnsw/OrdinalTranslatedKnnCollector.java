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
 * This wrapper implements {@link ChildrenSiblingExpansion}; sibling expansion is active only when
 * the wrapped collector also implements {@link DocSiblingExpansion}.
 */
public final class OrdinalTranslatedKnnCollector extends KnnCollector.Decorator
    implements ChildrenSiblingExpansion {

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

  @Override
  public int[] getSiblingOrdinals(int hnswNode, BitSet visitedHnswNodes) {
    if (!(collector instanceof DocSiblingExpansion docExpander)) {
      return null;
    }
    int docId = vectorOrdinalToDocId.apply(hnswNode);
    int[] siblingDocIds = docExpander.findSiblingDocIds(docId);
    if (siblingDocIds == null) {
      return null;
    }
    int[] siblingOrdinals = new int[siblingDocIds.length];
    // siblingOrdinals is pre-allocated to siblingDocIds.length and Java initializes int arrays to 0
    // so this variable is necessary.
    int count = 0;
    for (int sibDocId : siblingDocIds) {
      int sibOrd = docExpander.docIdToOrdinal(sibDocId);
      //  sibOrd = -1 when a document has no vector for this field.
      //  Such a doc has no node in the HNSW graph and can't be scored, so it must be skipped.
      //  If a sibling was reached via normal graph traversal before sibling expansion triggered,
      // re-adding it would
      //  cause it to be scored twice. !visitedHnswNodes.get(sibOrd) filters those out.
      if (sibOrd >= 0 && !visitedHnswNodes.get(sibOrd)) {
        siblingOrdinals[count++] = sibOrd;
      }
    }
    return count > 0 ? ArrayUtil.copyOfSubArray(siblingOrdinals, 0, count) : null;
  }
}
