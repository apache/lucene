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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A {@link Query} that wraps a list of queries and verifies that they all produce the same results.
 */
public class VerifyingQuery extends Query {
  private List<Query> queries;

  /** Create a new VerifyingQuery out of a list of queries. */
  public VerifyingQuery(Query... queries) {
    this.queries = List.of(queries);
  }

  /** Create a {@link VerifyingQuery} out of an {@link IndexOrDocValuesQuery}. */
  public VerifyingQuery(IndexOrDocValuesQuery idxOrDvQuery) {
    this(idxOrDvQuery, idxOrDvQuery.getIndexQuery(), idxOrDvQuery.getRandomAccessQuery());
  }

  @Override
  public String toString(String field) {
    String str = "(";
    for (int i = 0; i < queries.size(); i++) {
      str += queries.get(i).toString(field);
      if (i != queries.size() - 1) {
        str += " | ";
      }
    }
    return str + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    for (Query query : queries) {
      query.visit(visitor);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight[] weights = new Weight[queries.size()];
    for (int i = 0; i < queries.size(); i++) {
      weights[i] = queries.get(i).createWeight(searcher, scoreMode, boost);
    }

    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) {
        return null;
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer[] scorers = new Scorer[weights.length];
        for (int i = 0; i < weights.length; i++) {
          scorers[i] = weights[i].scorer(context);
        }

        return new Scorer(this) {
          @Override
          public DocIdSetIterator iterator() {
            DocIdSetIterator[] iterators = new DocIdSetIterator[scorers.length];
            for (int i = 0; i < scorers.length; i++) {
              iterators[i] = scorers[i].iterator();
            }

            return new DocIdSetIterator() {
              @Override
              public int docID() {
                int docID = iterators[0].docID();
                for (int i = 1; i < iterators.length; i++) {
                  if (docID != iterators[i].docID()) {
                    throw new RuntimeException("docID mismatch");
                  }
                }
                return docID;
              }

              @Override
              public int nextDoc() throws IOException {
                int ret = iterators[0].nextDoc();
                for (int i = 1; i < iterators.length; i++) {
                  if (ret != iterators[i].nextDoc()) {
                    throw new RuntimeException("nextDoc mismatch");
                  }
                }
                return ret;
              }

              @Override
              public int advance(int target) throws IOException {
                int ret = iterators[0].advance(target);
                for (int i = 1; i < iterators.length; i++) {
                  if (ret != iterators[i].advance(target)) {
                    throw new RuntimeException("advance mismatch");
                  }
                }
                return ret;
              }

              @Override
              public long cost() {
                long cost = 0;
                for (DocIdSetIterator it : iterators) {
                  cost += it.cost();
                }
                return cost;
              }
            };
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            float maxScore = scorers[0].getMaxScore(upTo);
            for (int i = 1; i < scorers.length; i++) {
              if (maxScore != scorers[i].getMaxScore(upTo)) {
                throw new RuntimeException("getMaxScore mismatch");
              }
            }
            return maxScore;
          }

          @Override
          public float score() throws IOException {
            float score = scorers[0].score();
            for (int i = 1; i < scorers.length; i++) {
              if (score != scorers[i].score()) {
                throw new RuntimeException("score mismatch");
              }
            }
            return score;
          }

          @Override
          public int docID() {
            int docID = scorers[0].docID();
            for (int i = 1; i < scorers.length; i++) {
              if (docID != scorers[i].docID()) {
                throw new RuntimeException("docID mismatch");
              }
            }
            return docID;
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    VerifyingQuery that = (VerifyingQuery) obj;
    return this.queries.equals(that.queries);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    for (Query query : queries) {
      h = 31 * h + query.hashCode();
    }
    return h;
  }
}
