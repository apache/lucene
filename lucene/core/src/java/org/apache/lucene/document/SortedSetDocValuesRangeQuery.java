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
package org.apache.lucene.document;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

final class SortedSetDocValuesRangeQuery extends Query {

  private final String field;
  private final BytesRef lowerValue;
  private final BytesRef upperValue;
  private final boolean lowerInclusive;
  private final boolean upperInclusive;

  SortedSetDocValuesRangeQuery(
      String field,
      BytesRef lowerValue,
      BytesRef upperValue,
      boolean lowerInclusive,
      boolean upperInclusive) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
    this.lowerInclusive = lowerInclusive && lowerValue != null;
    this.upperInclusive = upperInclusive && upperValue != null;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedSetDocValuesRangeQuery that = (SortedSetDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && Objects.equals(lowerValue, that.lowerValue)
        && Objects.equals(upperValue, that.upperValue)
        && lowerInclusive == that.lowerInclusive
        && upperInclusive == that.upperInclusive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, lowerValue, upperValue, lowerInclusive, upperInclusive);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b.append(lowerInclusive ? "[" : "{")
        .append(lowerValue == null ? "*" : lowerValue)
        .append(" TO ")
        .append(upperValue == null ? "*" : upperValue)
        .append(upperInclusive ? "]" : "}")
        .toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (lowerValue == null && upperValue == null) {
      return new FieldExistsQuery(field);
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Weight weight = this;
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

        // implement ScorerSupplier, since we do some expensive stuff to make a scorer
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {

            final long minOrd;
            if (lowerValue == null) {
              minOrd = 0;
            } else {
              final long ord = values.lookupTerm(lowerValue);
              if (ord < 0) {
                minOrd = -1 - ord;
              } else if (lowerInclusive) {
                minOrd = ord;
              } else {
                minOrd = ord + 1;
              }
            }

            final long maxOrd;
            if (upperValue == null) {
              maxOrd = values.getValueCount() - 1;
            } else {
              final long ord = values.lookupTerm(upperValue);
              if (ord < 0) {
                maxOrd = -2 - ord;
              } else if (upperInclusive) {
                maxOrd = ord;
              } else {
                maxOrd = ord - 1;
              }
            }

            // no terms matched in this segment
            if (minOrd > maxOrd) {
              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.empty());
            }

            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            final TwoPhaseIterator iterator;
            if (singleton != null) {
              iterator =
                  new TwoPhaseIterator(singleton) {
                    @Override
                    public boolean matches() throws IOException {
                      final long ord = singleton.ordValue();
                      return ord >= minOrd && ord <= maxOrd;
                    }

                    @Override
                    public float matchCost() {
                      return 2; // 2 comparisons
                    }
                  };
            } else {
              iterator =
                  new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                      for (int i = 0; i < values.docValueCount(); i++) {
                        long ord = values.nextOrd();
                        if (ord < minOrd) {
                          continue;
                        }
                        // Values are sorted, so the first ord that is >= minOrd is our best
                        // candidate
                        return ord <= maxOrd;
                      }
                      return false; // all ords were < minOrd
                    }

                    @Override
                    public float matchCost() {
                      return 2; // 2 comparisons
                    }
                  };
            }
            return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
          }

          @Override
          public long cost() {
            return values.cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }
}
