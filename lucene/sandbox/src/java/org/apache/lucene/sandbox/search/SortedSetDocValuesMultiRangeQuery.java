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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongBitSet;

/** A union multiple ranges over SortedSetDocValuesField */
class SortedSetDocValuesMultiRangeQuery extends Query {

  protected static final class OrdRange {
    final long lower;
    long upper;

    public OrdRange(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  protected final String fieldName;
  private final int bytesPerDim;
  protected final List<DocValuesMultiRangeQuery.Range> rangeClauses;

  SortedSetDocValuesMultiRangeQuery(
      String fieldName,
      List<DocValuesMultiRangeQuery.Range> clauses,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    this.fieldName = fieldName;
    this.bytesPerDim = bytesPerDim;
    ArrayList<DocValuesMultiRangeQuery.Range> sortedClauses = new ArrayList<>(clauses);
    sortedClauses.sort(
        (r, s) -> comparator.compare(r.lower.bytes, r.lower.offset, s.lower.bytes, s.lower.offset));
    this.rangeClauses = sortedClauses;
  }

  @Override
  public String toString(String fld) {
    return SortedSetDocValuesMultiRangeQuery.class.getSimpleName()
        + "{"
        + "field='"
        + fld
        + '\''
        + ", rangeClauses="
        + rangeClauses
        + // TODO better toString
        '}';
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new MultiRangeWeight(boost, scoreMode);
  }

  /**
   * Resolves ordinals for {@linkplain #rangeClauses}. Caveat: sometimes it updates ranges after
   * inserting
   *
   * @param values doc values to lookup ordinals
   * @param ordRanges destination collection for ord ranges
   */
  protected void createOrdRanges(SortedSetDocValues values, Collection<OrdRange> ordRanges)
      throws IOException {
    TermsEnum termsEnum = values.termsEnum();
    OrdRange previous = null;
    clauses:
    for (DocValuesMultiRangeQuery.Range range : rangeClauses) {
      TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(range.lower);
      long lowerOrd = -1;
      switch (seekStatus) {
        case TermsEnum.SeekStatus.END:
          break clauses;
        case FOUND, NOT_FOUND:
          lowerOrd = termsEnum.ord();
      }
      seekStatus = termsEnum.seekCeil(range.upper);
      long upperOrd = -1;
      switch (seekStatus) {
        case TermsEnum.SeekStatus.END:
          upperOrd = values.getValueCount() - 1;
          break;
        case FOUND:
          upperOrd = termsEnum.ord();
          break;
        case NOT_FOUND:
          if (termsEnum.ord() == 0) {
            continue; // this range is before values.
          }
          upperOrd = termsEnum.ord() - 1;
      }
      if (lowerOrd <= upperOrd) { // otherwise ignore
        if (previous != null) {
          if (previous.upper >= lowerOrd - 1) { // adjacent or overlap
            previous.upper =
                Math.max(upperOrd, previous.upper); // update one. which was yield. danger
            continue;
          }
        }
        ordRanges.add(previous = new OrdRange(lowerOrd, upperOrd));
      }
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(fieldName)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SortedSetDocValuesMultiRangeQuery that = (SortedSetDocValuesMultiRangeQuery) o;
    return Objects.equals(fieldName, that.fieldName)
        && bytesPerDim == that.bytesPerDim
        && Objects.equals(rangeClauses, that.rangeClauses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, bytesPerDim, rangeClauses);
  }

  protected class MultiRangeWeight extends ConstantScoreWeight {
    final ScoreMode scoreMode;

    public MultiRangeWeight(float boost, ScoreMode scoreMode) {
      super(SortedSetDocValuesMultiRangeQuery.this, boost);
      this.scoreMode = scoreMode;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
        return null;
      }
      SortedSetDocValues values = DocValues.getSortedSet(context.reader(), fieldName);

      return new MultiRangeScorerSupplier(values, context);
    }

    // TODO perhaps count() specification?

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, fieldName);
    }

    protected class MultiRangeScorerSupplier extends ScorerSupplier {
      final SortedSetDocValues values;
      protected final LeafReaderContext context;

      public MultiRangeScorerSupplier(SortedSetDocValues values, LeafReaderContext context) {
        this.values = values;
        this.context = context;
      }

      @Override
      public Scorer get(long leadCost) throws IOException {
        List<OrdRange> ordRanges = new ArrayList<>();
        createOrdRanges(values, ordRanges);
        if (ordRanges.isEmpty()) {
          return empty();
        }
        LongBitSet matchingOrdsShifted = null;
        long minOrd = ordRanges.getFirst().lower, maxOrd = ordRanges.getLast().upper;

        DocValuesSkipper skipper = context.reader().getDocValuesSkipper(fieldName);

        if (skipper != null && (minOrd > skipper.maxValue() || maxOrd < skipper.minValue())) {
          return empty();
        }

        if (ordRanges.size() > 1) {
          matchingOrdsShifted = new LongBitSet(maxOrd + 1 - minOrd);
          for (OrdRange range : ordRanges) {
            matchingOrdsShifted.set(
                range.lower - minOrd, range.upper - minOrd + 1); // up is exclusive
          }
        }
        TwoPhaseIterator iterator;
        LongBitSet finalMatchingOrdsShifted = matchingOrdsShifted;
        iterator =
            new TwoPhaseIterator(values) {
              // TODO unwrap singleton?
              @Override
              public boolean matches() throws IOException {
                for (int i = 0; i < values.docValueCount(); i++) {
                  long ord = values.nextOrd();
                  if (ord >= minOrd && ord <= maxOrd) {
                    if (finalMatchingOrdsShifted == null // singleton
                        || finalMatchingOrdsShifted.get(ord - minOrd)) {
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
                  iterator, skipper, minOrd, maxOrd, matchingOrdsShifted != null);
        }
        return new ConstantScoreScorer(score(), scoreMode, iterator);
      }

      protected ConstantScoreScorer empty() {
        return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty());
      }

      @Override
      public long cost() {
        return values.cost();
      }
    }
  }
}
