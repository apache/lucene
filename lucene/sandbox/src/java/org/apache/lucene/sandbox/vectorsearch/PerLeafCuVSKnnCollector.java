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
package org.apache.lucene.sandbox.vectorsearch;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/** KnnCollector for CuVS */
public class PerLeafCuVSKnnCollector implements KnnCollector {

  public List<ScoreDoc> scoreDocs;
  public int topK = 0;
  public int iTopK = topK; // TODO getter, no setter
  public int searchWidth = 1; // TODO getter, no setter
  public int results = 0;

  public PerLeafCuVSKnnCollector(int topK, int iTopK, int searchWidth) {
    super();
    this.topK = topK;
    this.iTopK = iTopK;
    this.searchWidth = searchWidth;
    scoreDocs = new ArrayList<ScoreDoc>();
  }

  @Override
  public boolean earlyTerminated() {
    // TODO: may need implementation
    return false;
  }

  @Override
  public void incVisitedCount(int count) {
    // TODO: may need implementation
  }

  @Override
  public long visitedCount() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public long visitLimit() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public int k() {
    return topK;
  }

  @Override
  @SuppressWarnings("cast")
  public boolean collect(int docId, float similarity) {
    scoreDocs.add(new ScoreDoc(docId, 1f / (float) (similarity)));
    return true;
  }

  @Override
  public float minCompetitiveSimilarity() {
    // TODO: may need implementation
    return 0;
  }

  @Override
  public TopDocs topDocs() {
    return new TopDocs(
        new TotalHits(scoreDocs.size(), TotalHits.Relation.EQUAL_TO),
        scoreDocs.toArray(new ScoreDoc[scoreDocs.size()]));
  }
}
