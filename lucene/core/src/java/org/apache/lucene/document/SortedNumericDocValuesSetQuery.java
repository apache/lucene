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
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/** Similar to SortedNumericDocValuesRangeQuery but for a set */
final class SortedNumericDocValuesSetQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(SortedNumericDocValuesSetQuery.class);

  private final String field;
  private final LongHashSet numbers;

  SortedNumericDocValuesSetQuery(String field, long[] numbers) {
    this.field = Objects.requireNonNull(field);
    Arrays.sort(numbers);
    this.numbers = new LongHashSet(numbers);
  }

  @Override
  public boolean equals(Object other) {
    if (sameClassAs(other) == false) {
      return false;
    }
    SortedNumericDocValuesSetQuery that = (SortedNumericDocValuesSetQuery) other;
    return field.equals(that.field) && numbers.equals(that.numbers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), field, numbers);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String defaultField) {
    return new StringBuilder().append(field).append(": ").append(numbers.toString()).toString();
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES
        + RamUsageEstimator.sizeOfObject(field)
        + RamUsageEstimator.sizeOfObject(numbers);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (numbers.size() == 0) {
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
      public Scorer scorer(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        final TwoPhaseIterator iterator;
        if (singleton != null) {
          iterator =
              new TwoPhaseIterator(singleton) {
                @Override
                public boolean matches() throws IOException {
                  long value = singleton.longValue();
                  return value >= numbers.minValue
                      && value <= numbers.maxValue
                      && numbers.contains(value);
                }

                @Override
                public float matchCost() {
                  return 5; // 2 comparisions, possible lookup in the set
                }
              };
        } else {
          iterator =
              new TwoPhaseIterator(values) {
                @Override
                public boolean matches() throws IOException {
                  int count = values.docValueCount();
                  for (int i = 0; i < count; i++) {
                    final long value = values.nextValue();
                    if (value < numbers.minValue) {
                      continue;
                    } else if (value > numbers.maxValue) {
                      return false; // values are sorted, terminate
                    } else if (numbers.contains(value)) {
                      return true;
                    }
                  }
                  return false;
                }

                @Override
                public float matchCost() {
                  return 5; // 2 comparisons, possible lookup in the set
                }
              };
        }
        return new ConstantScoreScorer(this, score(), scoreMode, iterator);
      }
    };
  }
}
