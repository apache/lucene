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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;

/** A union multiple ranges over SortedSetDocValuesField */
public class SortedSetMultiRangeQuery extends Query {
  private final String field;
  private final int bytesPerDim;
  private final ArrayUtil.ByteArrayComparator comparator;
  private final List<MultiRangeQuery.RangeClause> rangeClauses;

  SortedSetMultiRangeQuery(
      String name,
      List<MultiRangeQuery.RangeClause> clauses,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    this.field = name;
    this.bytesPerDim = bytesPerDim;
    this.comparator = comparator;
    ArrayList<MultiRangeQuery.RangeClause> sortedClauses = new ArrayList<>(clauses);
    sortedClauses.sort(
            (o1, o2) -> {
              // if (result == 0) {
              //    return comparator.compare(o1.upperValue, 0, o2.upperValue, 0);
              // } else {
              return comparator.compare(o1.lowerValue, 0, o2.lowerValue, 0);
              // }
            });
    this.rangeClauses = sortedClauses;
  }

  /** Builder for creating a SortedSetMultiRangeQuery. */
  public static class Builder {
    private final String fieldName;
    private final List<MultiRangeQuery.RangeClause> clauses = new ArrayList<>();
    private final int bytesPerDim;
    private final ArrayUtil.ByteArrayComparator comparator;

    public Builder(String fieldName, int bytesPerDim) {
      this.fieldName = Objects.requireNonNull(fieldName);
      this.bytesPerDim = bytesPerDim; // TODO assrt positive
      this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
    }

    public Builder add(BytesRef lowerValue, BytesRef upperValue) {
      byte[] low = BytesRef.deepCopyOf(lowerValue).bytes;
      byte[] up = BytesRef.deepCopyOf(upperValue).bytes;
      if (this.comparator.compare(low, 0, up, 0) > 0) {
        throw new IllegalArgumentException("lowerValue must be <= upperValue");
      } else {
        clauses.add(new MultiRangeQuery.RangeClause(low, up));
      }
      return this;
    }

    public Query build() {
      if (clauses.isEmpty()) {
        return new MatchNoDocsQuery();
      }
      if (clauses.size() == 1) {
        return SortedSetDocValuesField.newSlowRangeQuery(
                fieldName,
            new BytesRef(clauses.getFirst().lowerValue),
            new BytesRef(clauses.getFirst().upperValue),
            true,
            true);
      }
      return new SortedSetMultiRangeQuery(fieldName, clauses, this.bytesPerDim, comparator);
    }
  }

  @Override
  public String toString(String fld) {
    return "SortedSetMultiRangeQuery{"
        + "field='"
        + fld
        + '\''
        + ", rangeClauses="
        + rangeClauses
        + // TODO better toString
        '}';
  }

  // what TODO with reverse ranges ???
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            assert !rangeClauses.isEmpty(): "Builder should prevent it";
            DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
            SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);
            TermsEnum termsEnum = values.termsEnum();
            LongBitSet matchingOrdsShifted = null;
            long minOrd = 0, maxOrd = values.getValueCount() - 1;
            long matchesAbove =
                values.getValueCount(); // it's last range goes to maxOrd, by default - no match
            long maxSeenOrd = values.getValueCount();
            TermsEnum.SeekStatus seekStatus = TermsEnum.SeekStatus.NOT_FOUND;
            for (int r = 0; r < rangeClauses.size(); r++) {
              MultiRangeQuery.RangeClause range = rangeClauses.get(r);
              long startingOrd;
              seekStatus = termsEnum.seekCeil(new BytesRef(range.lowerValue));
              if (matchingOrdsShifted == null) { // first iter
                if (seekStatus == TermsEnum.SeekStatus.END) {
                  return empty(); // no bitset yet, give up
                }
                minOrd = termsEnum.ord();
                if (skipper != null) {
                  minOrd = Math.max(minOrd, skipper.minValue());
                  maxOrd = Math.min(maxOrd, skipper.maxValue());
                }
                assert maxOrd >= minOrd;
                startingOrd = minOrd;
              } else {
                if (seekStatus == TermsEnum.SeekStatus.END) {
                  break; // ranges - we are done, terms are exhausted
                } else {
                  startingOrd = termsEnum.ord();
                }
              }
              byte[] upper = range.upperValue; // TODO ignore reverse ranges
              // looking for overlap
              for (int overlap = r + 1; overlap < rangeClauses.size(); overlap++, r++) {
                MultiRangeQuery.RangeClause mayOverlap = rangeClauses.get(overlap);
                assert comparator.compare(range.lowerValue, 0, mayOverlap.lowerValue, 0) <= 0
                    : "since they are sorted";
                // TODO it might be contiguous ranges, it's worth to check but I have no idea how
                if (comparator.compare(mayOverlap.lowerValue, 0, upper, 0) <= 0) {
                  // overlap, expand if needed
                  if (comparator.compare(upper, 0, mayOverlap.upperValue, 0) < 0) {
                    upper = mayOverlap.upperValue;
                  }
                  // continue; // skip overlapping rng
                } else {
                  break; // no r++
                }
              }//TODO copy Range here
              seekStatus = termsEnum.seekCeil(new BytesRef(upper));

              if (seekStatus == TermsEnum.SeekStatus.END) {
                maxSeenOrd = maxOrd; // perhaps it's worth to set for skipper
                matchesAbove = startingOrd;
                break; // no need to create bitset
              }
              maxSeenOrd =
                  seekStatus == TermsEnum.SeekStatus.FOUND
                      ? termsEnum.ord()
                      : termsEnum.ord() - 1; // floor

              if (matchingOrdsShifted == null) {
                matchingOrdsShifted = new LongBitSet(maxOrd + 1 - minOrd);
              }
              matchingOrdsShifted.set(
                  startingOrd - minOrd, maxSeenOrd - minOrd + 1); // up is exclusive
            }
            /// ranges are over, there might be no set!!
            TwoPhaseIterator iterator;
            long finalMatchesAbove = matchesAbove;
            LongBitSet finalMatchingOrdsShifted = matchingOrdsShifted;
            long finalMinOrd = minOrd;
            iterator =
                new TwoPhaseIterator(values) {
                  // TODO unwrap singleton?
                  @Override
                  public boolean matches() throws IOException {
                    for (int i = 0; i < values.docValueCount(); i++) {
                      long ord = values.nextOrd();
                      if (ord >= finalMinOrd
                          && ((finalMatchesAbove < values.getValueCount()
                                  && ord >= finalMatchesAbove)
                              || finalMatchingOrdsShifted.get(ord - finalMinOrd))) {
                        return true;
                      }
                    }
                    return false; // all ords were < minOrd
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
                      iterator,
                      skipper,
                      minOrd,
                      maxSeenOrd // values.getValueCount()
                      ,
                      matchingOrdsShifted != null);
            }
            return new ConstantScoreScorer(score(), scoreMode, iterator);
          }

          private ConstantScoreScorer empty() {
            return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
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

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SortedSetMultiRangeQuery that = (SortedSetMultiRangeQuery) o;
    return Objects.equals(field, that.field)
        && bytesPerDim == that.bytesPerDim
        && Objects.equals(rangeClauses, that.rangeClauses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, bytesPerDim, rangeClauses);
  }
}
