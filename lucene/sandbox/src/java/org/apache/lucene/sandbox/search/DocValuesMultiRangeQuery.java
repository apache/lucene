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
package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** A few query builder for doc values multi range queries */
public final class DocValuesMultiRangeQuery {

  private DocValuesMultiRangeQuery() {}

  /** Representation of a single clause in a MultiRangeQuery */
  static final class Range {
    BytesRef lower;
    BytesRef upper;

    /** NB: One absolutely must copy ByteRefs before. */
    Range(BytesRef lowerValue, BytesRef upperValue) {
      this.lower = lowerValue;
      this.upper = upperValue;
    }

    // TODO test equals
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Range that = (Range) o;
      return lower.equals(that.lower) && upper.equals(that.upper);
    }

    @Override
    public int hashCode() {
      int result = lower.hashCode();
      result = 31 * result + upper.hashCode();
      return result;
    }
  }

  /**
   * Builder for creating a multi-range query for stabbing by SortedSet or Sorted fixed width field
   * values. Name highlights two key points:
   *
   * <ul>
   *   <li>treats multiple or single field value as a scalar for range matching (stabbing)
   *   <li>field values have fixed width
   * </ul>
   *
   * For example, it matches IPs in docvalues field by multiple IP ranges. For the single range it
   * behaves like {@link SortedSetDocValuesField#newSlowRangeQuery(String, BytesRef, BytesRef,
   * boolean, boolean)} with both true arguments
   */
  public static class SordedSetStabbingFixedBuilder
      implements BiConsumer<BytesRef, BytesRef>, Supplier<Query> {
    protected final String fieldName;
    protected final List<Range> clauses = new ArrayList<>();
    protected final int bytesPerDim;
    protected final ArrayUtil.ByteArrayComparator comparator;

    public SordedSetStabbingFixedBuilder(String fieldName, int bytesPerDim) {
      this.fieldName = Objects.requireNonNull(fieldName);
      if (bytesPerDim <= 0) {
        throw new IllegalArgumentException("bytesPerDim should be a valid value");
      }
      this.bytesPerDim = bytesPerDim;
      this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
    }

    // TODO support nulls as min,max boundaries ???
    public SordedSetStabbingFixedBuilder add(BytesRef lowerValue, BytesRef upperValue) {
      BytesRef lowRef = BytesRef.deepCopyOf(lowerValue);
      BytesRef upRef = BytesRef.deepCopyOf(upperValue);
      if (this.comparator.compare(lowRef.bytes, 0, upRef.bytes, 0) > 0) {
        // TODO let's just ignore so far.
        //  throw new IllegalArgumentException("lower must be <= upperValue");
      } else {
        clauses.add(new Range(lowRef, upRef));
      }
      return this;
    }

    public Query build() {
      if (clauses.isEmpty()) {
        return new MatchNoDocsQuery();
      }
      if (clauses.size() == 1) {
        Range theOnlyOne = clauses.getFirst();
        return SortedSetDocValuesField.newSlowRangeQuery(
            fieldName, theOnlyOne.lower, theOnlyOne.upper, true, true);
      }
      return createSortedSetDocValuesMultiRangeQuery();
    }

    SortedSetDocValuesMultiRangeQuery createSortedSetDocValuesMultiRangeQuery() {
      return new SortedSetDocValuesMultiRangeQuery(
          fieldName, clauses, this.bytesPerDim, comparator);
    }

    @Override
    public void accept(BytesRef bytesRef, BytesRef bytesRef2) {
      add(bytesRef, bytesRef2);
    }

    @Override
    public Query get() {
      return build();
    }
  }

  /**
   * Builder like {@link SordedSetStabbingFixedBuilder} but using log(ranges) lookup per doc value
   * instead of bitset check
   */
  public static class SordedSetStabbingFixedTreeBuilder extends SordedSetStabbingFixedBuilder {

    public SordedSetStabbingFixedTreeBuilder(String fieldName, int bytesPerDim) {
      super(fieldName, bytesPerDim);
    }

    @Override
    SortedSetDocValuesMultiRangeQuery createSortedSetDocValuesMultiRangeQuery() {
      return new SortedSetDocValuesMultiRangeQuery(
          fieldName, clauses, this.bytesPerDim, comparator) {
        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
            throws IOException {
          return new MultiRangeWeight(boost, scoreMode) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
              if (context.reader().getFieldInfos().fieldInfo(field) == null) {
                return null;
              }
              SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

              return new MultiRangeScorerSupplier(values, context) {
                @Override
                public Scorer get(long leadCost) throws IOException {
                  assert !rangeClauses.isEmpty() : "Builder should prevent it";
                  TreeSet<OrdRange> ordRanges =
                      new TreeSet<>((or1, or2) -> (int) (or1.lower - or2.lower));
                  bytesRangesToOrdRanges(this.values, ordRanges);
                  if (ordRanges.isEmpty()) {
                    return empty();
                  }
                  long minOrd = ordRanges.getFirst().lower, maxOrd = ordRanges.getLast().upper;

                  DocValuesSkipper skipper = this.context.reader().getDocValuesSkipper(field);

                  if (skipper != null
                      && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue())) {
                    return empty();
                  }

                  TwoPhaseIterator iterator;
                  SortedSetDocValues docValues = this.values;
                  iterator =
                      new TwoPhaseIterator(docValues) {
                        // TODO unwrap singleton?
                        @Override
                        public boolean matches() throws IOException {
                          for (int i = 0; i < docValues.docValueCount(); i++) {
                            long ord = docValues.nextOrd();
                            if (ord >= minOrd && ord <= maxOrd) {
                              // TODO reuse instance, lookup increasing
                              OrdRange lessOrEq = ordRanges.floor(new OrdRange(ord, ord));
                              if (lessOrEq != null && lessOrEq.upper >= ord) {
                                assert lessOrEq.lower <= ord;
                                return true;
                              }
                            }
                          }
                          return false;
                        }

                        @Override
                        public float matchCost() {
                          return 2; // 2 comparisons
                        }
                      };
                  //                        }
                  if (skipper != null) {
                    iterator =
                        new DocValuesRangeIterator(
                            iterator, skipper, minOrd, maxOrd, true);
                  }
                  return new ConstantScoreScorer(score(), scoreMode, iterator);
                }
              };
            }
          };
        }
      };
    }
  }
}
