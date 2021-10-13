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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.VectorUtil;

/** The type Knn query. */
public class KnnQuery extends Query {

  private final String field;
  private final float[] vector;
  private final int topK;
  private final String text;

  /**
   * Instantiates a new Knn query.
   *
   * @param field the field
   * @param text the text
   * @param vector the vector
   * @param topK the top k
   */
  KnnQuery(String field, String text, float[] vector, int topK) {
    this.field = field;
    this.text = text;
    this.vector = vector;
    this.topK = topK;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new KnnWeight();
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj)
        && ((KnnQuery) obj).field.equals(field)
        && Arrays.equals(((KnnQuery) obj).vector, vector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, Arrays.hashCode(vector));
  }

  @Override
  public String toString(String field) {
    return "<vector:knn:" + field + "<" + text + ">[" + vector[0] + ",...]>";
  }

  @Override
  public void visit(QueryVisitor visitor) {}

  /** The type Knn weight. */
  class KnnWeight extends Weight {

    /** Instantiates a new Knn weight. */
    KnnWeight() {
      super(KnnQuery.this);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return new TopDocScorer(
          this,
          context
              .reader()
              .searchNearestVectors(field, vector, topK, context.reader().getLiveDocs()));
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      VectorValues vectors = context.reader().getVectorValues(field);
      vectors.advance(doc);
      float score = VectorUtil.dotProduct(vector, vectors.vectorValue());
      return Explanation.match(0, "" + getQuery() + " in " + doc + " score " + score);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return MatchesUtils.MATCH_WITH_NO_TERMS;
    }

    @Override
    public String toString() {
      return "weight(" + KnnQuery.this + ")";
    }
  }

  /** The type Top doc scorer. */
  static class TopDocScorer extends Scorer {

    private int upTo = -1;
    private final TopDocs topDocs;
    private final TopDocsIterator iterator;

    /**
     * Instantiates a new Top doc scorer.
     *
     * @param weight the weight
     * @param topDocs the top docs
     */
    TopDocScorer(Weight weight, TopDocs topDocs) {
      super(weight);
      this.topDocs = topDocs;
      iterator = new TopDocsIterator();
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    @Override
    public float score() {
      return topDocs.scoreDocs[upTo].score;
    }

    @Override
    public float getMaxScore(int upTo) {
      if (this.upTo < topDocs.scoreDocs.length - 1) {
        if (upTo >= topDocs.scoreDocs[this.upTo + 1].doc) {
          return topDocs.scoreDocs[this.upTo + 1].score;
        }
      }
      return score();
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    /** The type Top docs iterator. */
    class TopDocsIterator extends DocIdSetIterator {

      @Override
      public int docID() {
        if (upTo < 0) {
          return -1;
        }
        return topDocs.scoreDocs[upTo].doc;
      }

      @Override
      public int nextDoc() {
        if (++upTo >= topDocs.scoreDocs.length) {
          return NO_MORE_DOCS;
        }
        return docID();
      }

      @Override
      public int advance(int target) throws IOException {
        return slowAdvance(target);
      }

      @Override
      public long cost() {
        return topDocs.scoreDocs.length;
      }
    }
  }
}
