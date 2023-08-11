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
import java.util.Objects;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.HitQueue;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * kNN float vector query that joins matching children vector documents with their parent doc id.
 */
public class ToParentBlockJoinFloatKnnVectorQuery extends KnnFloatVectorQuery {
  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;

  private final BitSetProducer parentsFilter;
  private final Query childFilter;
  private final int k;
  private final float[] query;

  /**
   * Create a ToParentBlockJoinFloatVectorQuery.
   *
   * @param field the query field
   * @param query the vector query
   * @param childFilter the child filter
   * @param k how many parent documents to return given the matching children
   * @param parentsFilter Filter identifying the parent documents.
   */
  public ToParentBlockJoinFloatKnnVectorQuery(
      String field, float[] query, Query childFilter, int k, BitSetProducer parentsFilter) {
    super(field, query, k, childFilter);
    this.childFilter = childFilter;
    this.parentsFilter = parentsFilter;
    this.k = k;
    this.query = query;
  }

  @Override
  protected TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator)
      throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return NO_RESULTS;
    }
    if (fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    ParentBlockJoinFloatVectorScorer vectorScorer =
        new ParentBlockJoinFloatVectorScorer(
            context.reader().getFloatVectorValues(field),
            acceptIterator,
            parentBitSet,
            query,
            fi.getVectorSimilarityFunction());
    HitQueue queue = new HitQueue(k, true);
    ScoreDoc topDoc = queue.top();
    int doc;
    while ((doc = vectorScorer.nextParent()) != DocIdSetIterator.NO_MORE_DOCS) {
      float score = vectorScorer.score();
      if (score > topDoc.score) {
        topDoc.score = score;
        topDoc.doc = doc;
        topDoc = queue.updateTop();
      }
    }

    // Remove any remaining sentinel values
    while (queue.size() > 0 && queue.top().score < 0) {
      queue.pop();
    }

    ScoreDoc[] topScoreDocs = new ScoreDoc[queue.size()];
    for (int i = topScoreDocs.length - 1; i >= 0; i--) {
      topScoreDocs[i] = queue.pop();
    }

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), TotalHits.Relation.EQUAL_TO);
    return new TopDocs(totalHits, topScoreDocs);
  }

  @Override
  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit)
      throws IOException {
    BitSet parentBitSet = parentsFilter.getBitSet(context);
    KnnCollector collector = new ToParentJoinKnnCollector(k, visitedLimit, parentBitSet);
    context.reader().searchNearestVectors(field, query, collector, acceptDocs);
    return collector.topDocs();
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + ":" + this.field + "[" + query[0] + ",...][" + k + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ToParentBlockJoinFloatKnnVectorQuery that = (ToParentBlockJoinFloatKnnVectorQuery) o;
    return k == that.k
        && Objects.equals(parentsFilter, that.parentsFilter)
        && Objects.equals(childFilter, that.childFilter)
        && Arrays.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), parentsFilter, childFilter, k);
    result = 31 * result + Arrays.hashCode(query);
    return result;
  }

  private static class ParentBlockJoinFloatVectorScorer {
    private final float[] query;
    private final FloatVectorValues values;
    private final VectorSimilarityFunction similarity;
    private final DocIdSetIterator acceptedChildrenIterator;
    private final BitSet parentBitSet;
    private int currentParent = -1;
    private float currentScore = Float.NEGATIVE_INFINITY;

    protected ParentBlockJoinFloatVectorScorer(
        FloatVectorValues values,
        DocIdSetIterator acceptedChildrenIterator,
        BitSet parentBitSet,
        float[] query,
        VectorSimilarityFunction similarity) {
      this.query = query;
      this.values = values;
      this.similarity = similarity;
      this.acceptedChildrenIterator = acceptedChildrenIterator;
      this.parentBitSet = parentBitSet;
    }

    public int nextParent() throws IOException {
      int nextChild = acceptedChildrenIterator.docID();
      if (nextChild == -1) {
        nextChild = acceptedChildrenIterator.nextDoc();
      }
      if (nextChild == DocIdSetIterator.NO_MORE_DOCS) {
        currentParent = DocIdSetIterator.NO_MORE_DOCS;
        return currentParent;
      }
      currentScore = Float.NEGATIVE_INFINITY;
      currentParent = parentBitSet.nextSetBit(nextChild);
      do {
        values.advance(nextChild);
        currentScore = Math.max(currentScore, similarity.compare(query, values.vectorValue()));
      } while ((nextChild = acceptedChildrenIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS
          && nextChild < currentParent);
      return currentParent;
    }

    public float score() throws IOException {
      return currentScore;
    }
  }
}
