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
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

final class SortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;

  SortedNumericDocValuesRangeQuery(String field, long lowerValue, long upperValue) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    SortedNumericDocValuesRangeQuery that = (SortedNumericDocValuesRangeQuery) obj;
    return Objects.equals(field, that.field)
        && lowerValue == that.lowerValue
        && upperValue == that.upperValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, lowerValue, upperValue);
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
    return b.append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new FieldExistsQuery(field);
    }
    if (lowerValue > upperValue) {
      return new MatchNoDocsQuery();
    }
    return super.rewrite(indexSearcher);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }

        DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
        if (skipper != null) {
          if (skipper.minValue() > upperValue || skipper.maxValue() < lowerValue) {
            return null;
          }
        }

        SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        TwoPhaseIterator iterator;
        if (singleton != null) {
          iterator =
              new TwoPhaseIterator(singleton) {
                @Override
                public boolean matches() throws IOException {
                  final long value = singleton.longValue();
                  return value >= lowerValue && value <= upperValue;
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
                  for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                    final long value = values.nextValue();
                    if (value < lowerValue) {
                      continue;
                    }
                    // Values are sorted, so the first value that is >= lowerValue is our best
                    // candidate
                    return value <= upperValue;
                  }
                  return false; // all values were < lowerValue
                }

                @Override
                public float matchCost() {
                  return 2; // 2 comparisons
                }
              };
        }
        if (skipper != null) {
          iterator = new DocValuesRangeIterator(iterator, skipper, lowerValue, upperValue);
        }
        final var scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
        return new DefaultScorerSupplier(scorer);
      }
    };
  }
}
