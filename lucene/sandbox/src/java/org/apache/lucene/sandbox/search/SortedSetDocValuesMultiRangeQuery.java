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
import java.util.function.Consumer;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.PriorityQueue;

/** A union multiple ranges over SortedSetDocValuesField */
class SortedSetDocValuesMultiRangeQuery extends Query {

  private static final class Edge {
    private final DocValuesMultiRangeQuery.Range range;
    private final boolean point;
    private final boolean upper;

    BytesRef getValue() {
      return upper ? range.upper : range.lower;
    }

    public Edge(DocValuesMultiRangeQuery.Range range, boolean upper) {
      this.range = range;
      this.upper = upper;
      this.point = false;
    }

    /** expecting only lower==upper i.e. point */
    public Edge(DocValuesMultiRangeQuery.Range range) {
      this.range = range;
      this.upper = false;
      this.point = true;
    }

    /** lower == upper */
    boolean isPoint() {
      return point;
    }
  }

  private static final class OrdRange {
    final long lower;
    long upper;

    public OrdRange(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  private final String field;
  private final int bytesPerDim;
  private final List<DocValuesMultiRangeQuery.Range> rangeClauses;

  SortedSetDocValuesMultiRangeQuery(
      String name,
      List<DocValuesMultiRangeQuery.Range> clauses,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    this.field = name;
    this.bytesPerDim = bytesPerDim;
    ArrayList<DocValuesMultiRangeQuery.Range> sortedClauses = new ArrayList<>();
    PriorityQueue<Edge> heap =
        new PriorityQueue<>(clauses.size() * 2) {
          @Override
          protected boolean lessThan(Edge a, Edge b) {
            return cmp(comparator, a.getValue(), b.getValue()) < 0;
          }
        };
    for (DocValuesMultiRangeQuery.Range r : clauses) {
      int cmp = cmp(comparator, r.lower, r.upper);
      if (cmp == 0) {
        heap.add(new Edge(r));
      } else {
        if (cmp < 0) {
          heap.add(new Edge(r, false));
          heap.add(new Edge(r, true));
        } // else drop reverse ranges
      }
    }
    int totalEdges = heap.size();
    int depth = 0;
    Consumer<DocValuesMultiRangeQuery.Range> outBoundSink = sortedClauses::add;
    Edge started = null;
    for (int i = 0; i < totalEdges; i++) {
      Edge smallest = heap.pop();
      if (depth == 0 && smallest.point) {
        if (i < totalEdges - 1 && heap.top().point) { // repeating same points
          if (cmp(comparator, smallest.getValue(), heap.top().getValue()) == 0) {
            continue;
          }
        }
        outBoundSink.accept(smallest.range);
      }
      if (!smallest.point) {
        if (!smallest.upper) {
          depth++;
          if (depth == 1) { // just started
            started = smallest;
          }
        } else {
          depth--;
          if (depth == 0) {
            outBoundSink.accept(
                started.range == smallest.range
                    ? smallest.range
                    : new DocValuesMultiRangeQuery.Range(started.getValue(), smallest.getValue()));
            started = null;
          }
        }
      }
    }
    this.rangeClauses = sortedClauses;
  }

  private static int cmp(ArrayUtil.ByteArrayComparator comparator, BytesRef a, BytesRef b) {
    return comparator.compare(a.bytes, a.offset, b.bytes, b.offset);
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

  // TODO how to handle reverse ranges ???
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        if (context.reader().getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);

        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            assert !rangeClauses.isEmpty() : "Builder should prevent it";
            // convert bytes ranges to ord ranges.
            List<OrdRange> ordRanges = new ArrayList<>();
            Consumer<OrdRange> yield = ordRanges::add;
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
                  if (previous.upper >= lowerOrd - 1) { // adjacent
                    previous.upper = upperOrd; // update one. which was yield. danger
                    continue;
                  }
                }
                yield.accept(previous = new OrdRange(lowerOrd, upperOrd));
              }
            }
            if (ordRanges.isEmpty()) {
              return empty();
            }
            LongBitSet matchingOrdsShifted = null;
            long minOrd = ordRanges.get(0).lower,
                maxOrd = ordRanges.get(ordRanges.size() - 1).upper;
            if (ordRanges.size() > 1) {
              matchingOrdsShifted = new LongBitSet(maxOrd + 1 - minOrd);
              for (OrdRange range : ordRanges) {
                matchingOrdsShifted.set(
                    range.lower - minOrd, range.upper - minOrd + 1); // up is exclusive
              }
            }
            DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
            /// ranges are over, there might be no set!!
            TwoPhaseIterator iterator;
            // long finalMatchesAbove = matchesAbove;
            LongBitSet finalMatchingOrdsShifted = matchingOrdsShifted;
            long finalMinOrd = minOrd;
            long finalMaxOrd = maxOrd;
            iterator =
                new TwoPhaseIterator(values) {
                  // TODO unwrap singleton?
                  @Override
                  public boolean matches() throws IOException {
                    for (int i = 0; i < values.docValueCount(); i++) {
                      long ord = values.nextOrd();
                      if (ord >= finalMinOrd && ord <= finalMaxOrd) {
                        if (finalMatchingOrdsShifted == null // singleton
                            || finalMatchingOrdsShifted.get(ord - finalMinOrd)) {
                          return true;
                        }
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
                      maxOrd // values.getValueCount()
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

      // TODO perhaps count() specification?

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
    SortedSetDocValuesMultiRangeQuery that = (SortedSetDocValuesMultiRangeQuery) o;
    return Objects.equals(field, that.field)
        && bytesPerDim == that.bytesPerDim
        && Objects.equals(rangeClauses, that.rangeClauses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, bytesPerDim, rangeClauses);
  }
}
