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

/**
 * Wraps a provided KnnCollector object, translating the provided vectorId ordinal to a documentId
 */
public final class OrdinalTranslatedKnnCollector implements KnnCollector {

  private final KnnCollector in;
  private final IntToIntFunction vectorOrdinalToDocId;

  public OrdinalTranslatedKnnCollector(KnnCollector in, IntToIntFunction vectorOrdinalToDocId) {
    this.in = in;
    this.vectorOrdinalToDocId = vectorOrdinalToDocId;
  }

  @Override
  public boolean earlyTerminated() {
    return in.earlyTerminated();
  }

  @Override
  public void incVisitedCount(int count) {
    in.incVisitedCount(count);
  }

  @Override
  public long visitedCount() {
    return in.visitedCount();
  }

  @Override
  public long visitLimit() {
    return in.visitLimit();
  }

  @Override
  public int k() {
    return in.k();
  }

  @Override
  public boolean collect(int vectorId, float similarity) {
    return in.collect(vectorOrdinalToDocId.apply(vectorId), similarity);
  }

  @Override
  public float minCompetitiveSimilarity() {
    return in.minCompetitiveSimilarity();
  }

  @Override
  public TopDocs topDocs() {
    TopDocs td = in.topDocs();
    return new TopDocs(
        new TotalHits(
            visitedCount(),
            this.earlyTerminated()
                ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                : TotalHits.Relation.EQUAL_TO),
        td.scoreDocs);
  }
}
