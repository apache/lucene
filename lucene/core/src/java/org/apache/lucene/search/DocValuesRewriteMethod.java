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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.LongBitSet;

/**
 * Rewrites MultiTermQueries into a filter, using DocValues for term enumeration.
 *
 * <p>This can be used to perform these queries against an unindexed docvalues field.
 *
 * @lucene.experimental
 */
public final class DocValuesRewriteMethod extends MultiTermQuery.RewriteMethod {

  @Override
  public Query rewrite(IndexReader reader, MultiTermQuery query) {
    return new ConstantScoreQuery(new MultiTermQueryDocValuesWrapper(query));
  }

  static class MultiTermQueryDocValuesWrapper extends Query {

    protected final MultiTermQuery query;

    /** Wrap a {@link MultiTermQuery} as a Filter. */
    protected MultiTermQueryDocValuesWrapper(MultiTermQuery query) {
      this.query = query;
    }

    @Override
    public String toString(String field) {
      // query.toString should be ok for the filter, too, if the query boost is 1.0f
      return query.toString(field);
    }

    @Override
    public final boolean equals(final Object other) {
      return sameClassAs(other) && query.equals(((MultiTermQueryDocValuesWrapper) other).query);
    }

    @Override
    public final int hashCode() {
      return 31 * classHash() + query.hashCode();
    }

    /** Returns the field name for this query */
    public final String getField() {
      return query.getField();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      if (visitor.acceptField(query.getField())) {
        visitor.getSubVisitor(BooleanClause.Occur.FILTER, query);
      }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
          final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), query.field);
          return MatchesUtils.forField(
              query.field,
              () ->
                  DisjunctionMatchesIterator.fromTermsEnum(
                      context, doc, query, query.field, getTermsEnum(values)));
        }

        /**
         * Create a TermsEnum that provides the intersection of the query terms with the terms
         * present in the doc values.
         */
        private TermsEnum getTermsEnum(SortedSetDocValues values) throws IOException {
          return query.getTermsEnum(
              new Terms() {

                @Override
                public TermsEnum iterator() throws IOException {
                  return values.termsEnum();
                }

                @Override
                public long getSumTotalTermFreq() {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long getSumDocFreq() {
                  throw new UnsupportedOperationException();
                }

                @Override
                public int getDocCount() {
                  throw new UnsupportedOperationException();
                }

                @Override
                public long size() {
                  return -1;
                }

                @Override
                public boolean hasFreqs() {
                  return false;
                }

                @Override
                public boolean hasOffsets() {
                  return false;
                }

                @Override
                public boolean hasPositions() {
                  return false;
                }

                @Override
                public boolean hasPayloads() {
                  return false;
                }
              });
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          final SortedSetDocValues values = DocValues.getSortedSet(context.reader(), query.field);
          if (values.getValueCount() == 0) {
            return null; // no values/docs so nothing can match
          }

          final Weight weight = this;
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              // Create a TermsEnum that will provide the intersection of the terms specified in the
              // query with the values present in the doc values:
              TermsEnum termsEnum = getTermsEnum(values);
              assert termsEnum != null;

              if (termsEnum.next() == null) {
                // no matching terms
                return new ConstantScoreScorer(
                    weight, score(), scoreMode, DocIdSetIterator.empty());
              }

              // Create a bit set for the "term set" ordinals (these are the terms provided by the
              // query that are actually present in the doc values field). Cannot use FixedBitSet
              // because we require long index (ord):
              final LongBitSet termSet = new LongBitSet(values.getValueCount());
              long maxOrd = -1;
              do {
                long ord = termsEnum.ord();
                if (ord >= 0) {
                  assert ord > maxOrd;
                  maxOrd = ord;
                  termSet.set(ord);
                }
              } while (termsEnum.next() != null);

              // no terms matched in this segment
              if (maxOrd < 0) {
                return new ConstantScoreScorer(
                    weight, score(), scoreMode, DocIdSetIterator.empty());
              }

              final SortedDocValues singleton = DocValues.unwrapSingleton(values);
              final TwoPhaseIterator iterator;
              final long max = maxOrd;
              if (singleton != null) {
                iterator =
                    new TwoPhaseIterator(singleton) {
                      @Override
                      public boolean matches() throws IOException {
                        return termSet.get(singleton.ordValue());
                      }

                      @Override
                      public float matchCost() {
                        return 3; // lookup in a bitset
                      }
                    };
              } else {
                iterator =
                    new TwoPhaseIterator(values) {
                      @Override
                      public boolean matches() throws IOException {
                        for (int i = 0; i < values.docValueCount(); i++) {
                          long value = values.nextOrd();
                          if (value > max) {
                            return false; // values are sorted, terminate
                          } else if (termSet.get(value)) {
                            return true;
                          }
                        }
                        return false;
                      }

                      @Override
                      public float matchCost() {
                        return 3; // lookup in a bitset
                      }
                    };
              }

              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }

            @Override
            public long cost() {
              // We have no prior knowledge of how many docs might match for any given query term,
              // so we assume that all docs with a value could be a match:
              return values.cost();
            }
          };
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final ScorerSupplier scorerSupplier = scorerSupplier(context);
          if (scorerSupplier == null) {
            return null;
          }
          return scorerSupplier.get(Long.MAX_VALUE);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return DocValues.isCacheable(ctx, query.field);
        }
      };
    }
  }

  @Override
  public boolean equals(Object other) {
    return other != null && getClass() == other.getClass();
  }

  @Override
  public int hashCode() {
    return 641;
  }
}
