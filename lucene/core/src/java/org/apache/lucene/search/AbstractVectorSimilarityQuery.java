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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Search for all (approximate) vectors above a similarity score using the {@link
 * VectorSimilarityCollector}.
 *
 * @lucene.experimental
 */
abstract class AbstractVectorSimilarityQuery extends AbstractKnnVectorQuery {
  protected final float traversalSimilarity, resultSimilarity;

  /**
   * Abstract query for performing similarity-based vector searches.
   *
   * @param field a field that has been indexed as a vector field.
   * @param traversalSimilarity (lower) similarity score for graph traversal.
   * @param resultSimilarity (higher) similarity score for result collection.
   * @param filter a filter applied before the vector search.
   */
  public AbstractVectorSimilarityQuery(
      String field, float traversalSimilarity, float resultSimilarity, Query filter) {
    super(field, Integer.MAX_VALUE, filter);
    if (traversalSimilarity > resultSimilarity) {
      throw new IllegalArgumentException("traversalSimilarity should be <= resultSimilarity");
    }
    this.traversalSimilarity = traversalSimilarity;
    this.resultSimilarity = resultSimilarity;
  }

  @Override
  protected TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator)
      throws IOException {
    @SuppressWarnings("resource")
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return TopDocsCollector.EMPTY_TOPDOCS;
    }

    VectorScorer vectorScorer = createVectorScorer(context, fi);
    List<ScoreDoc> scoreDocList = new ArrayList<>();

    int doc;
    while ((doc = acceptIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      boolean advanced = vectorScorer.advanceExact(doc);
      assert advanced;

      float score = vectorScorer.score();
      if (score >= resultSimilarity) {
        scoreDocList.add(new ScoreDoc(doc, score));
      }
    }

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), TotalHits.Relation.EQUAL_TO);
    return new TopDocs(totalHits, scoreDocList.toArray(ScoreDoc[]::new));
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    long value = 0;
    TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
    List<ScoreDoc> scoreDocList = new ArrayList<>();

    // Segment-level results are not sorted (because we do not want to maintain the topK), just
    // concatenate them
    for (TopDocs topDocs : perLeafResults) {
      value += topDocs.totalHits.value;
      if (topDocs.totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
        relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
      }
      scoreDocList.addAll(List.of(topDocs.scoreDocs));
    }

    return new TopDocs(new TotalHits(value, relation), scoreDocList.toArray(ScoreDoc[]::new));
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o)
        && Float.compare(
                ((AbstractVectorSimilarityQuery) o).traversalSimilarity, traversalSimilarity)
            == 0
        && Float.compare(((AbstractVectorSimilarityQuery) o).resultSimilarity, resultSimilarity)
            == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), traversalSimilarity, resultSimilarity);
  }
}
