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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocValuesRangeIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * A union multiple ranges over SortedNumericDocValuesField
 *
 * @lucene.experimental
 */
public class SortedNumericDocValuesMultiRangeQuery extends Query {

  protected final String fieldName;
  protected final NavigableSet<DocValuesMultiRangeQuery.LongRange> sortedClauses;

  protected SortedNumericDocValuesMultiRangeQuery(
      String fieldName, List<DocValuesMultiRangeQuery.LongRange> clauses) {
    this.fieldName = fieldName;
    sortedClauses = resolveOverlaps(clauses);
  }

  private static final class Edge {
    private final DocValuesMultiRangeQuery.LongRange range;
    private final boolean point;
    private final boolean upper;

    private static Edge createPoint(DocValuesMultiRangeQuery.LongRange r) {
      return new Edge(r);
    }

    long getValue() {
      return upper ? range.upper : range.lower;
    }

    private Edge(DocValuesMultiRangeQuery.LongRange range, boolean upper) {
      this.range = range;
      this.upper = upper;
      this.point = false;
    }

    /** expecting Arrays.equals(lower.bytes,upper.bytes) i.e. point */
    private Edge(DocValuesMultiRangeQuery.LongRange range) {
      this.range = range;
      this.upper = false;
      this.point = true;
    }
  }

  /** Merges overlapping ranges. map.floor() doesn't work with overlaps */
  private static NavigableSet<DocValuesMultiRangeQuery.LongRange> resolveOverlaps(
      Collection<DocValuesMultiRangeQuery.LongRange> clauses) {
    NavigableSet<DocValuesMultiRangeQuery.LongRange> sortedClauses =
        new TreeSet<>(
            Comparator.comparing(r -> r.lower)
            // .thenComparing(r -> r.upper)// have to ignore upper boundary for .floor() lookups
            );
    List<Edge> clauseEdges = new ArrayList<>(clauses.size() * 2);

    for (DocValuesMultiRangeQuery.LongRange r : clauses) {
      long cmp = r.lower - r.upper;
      if (cmp == 0) {
        clauseEdges.add(Edge.createPoint(r));
      } else {
        if (cmp < 0) {
          clauseEdges.add(new Edge(r, false));
          clauseEdges.add(new Edge(r, true));
        } // else drop reverse ranges
      }
    }

    // sort by edge value, then points first
    clauseEdges.sort(
        Comparator.comparingLong(Edge::getValue)
            .thenComparing(e -> e.point, Comparator.reverseOrder()));

    int depth = 0;
    Edge started = null;
    for (int i = 0; i < clauseEdges.size(); i++) {
      Edge smallest = clauseEdges.get(i);
      if (depth == 0 && smallest.point) {
        if (i < clauseEdges.size() - 1) { // the point sits on the edge of the range
          if (smallest.getValue() == clauseEdges.get(i + 1).getValue()) {
            continue;
          }
        }
        sortedClauses.add(smallest.range);
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
            DocValuesMultiRangeQuery.LongRange range =
                started.range == smallest.range // no overlap case, the most often
                    ? smallest.range
                    : new DocValuesMultiRangeQuery.LongRange(
                        started.getValue(), smallest.getValue());
            boolean strictlyIncreasing = sortedClauses.add(range);
            assert strictlyIncreasing;
            started = null;
          }
        }
      }
    }
    return sortedClauses;
  }

  @Override
  public String toString(String fld) {
    return (Objects.equals(fieldName, fld) ? "" : fieldName + ":") + sortedClauses;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new MultiRangeWeight(boost, scoreMode);
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
    SortedNumericDocValuesMultiRangeQuery that = (SortedNumericDocValuesMultiRangeQuery) o;
    return Objects.equals(fieldName, that.fieldName)
        // && Objects.equals(sortedClauses, that.sortedClauses)
        && upperBoundWiseEquals(sortedClauses, that.sortedClauses);
  }

  /**
   * TreeSet.equals is ruled by {@linkplain Comparator} logic. This comparator have to be upper
   * bound agnostic to support floor() lookups. However, equals() should be upper bound sensitive
   * and here we ensure that.
   */
  private boolean upperBoundWiseEquals(
      NavigableSet<DocValuesMultiRangeQuery.LongRange> left,
      NavigableSet<DocValuesMultiRangeQuery.LongRange> right) {
    for (Iterator<DocValuesMultiRangeQuery.LongRange> li = left.iterator(), ri = right.iterator();
        li.hasNext() && ri.hasNext(); ) {
      if (!li.next().equals(ri.next()) || li.hasNext() != ri.hasNext()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, sortedClauses);
  }

  /** Weight for {@linkplain SortedNumericDocValuesMultiRangeQuery} */
  protected class MultiRangeWeight extends ConstantScoreWeight {
    final ScoreMode scoreMode;

    public MultiRangeWeight(float boost, ScoreMode scoreMode) {
      super(SortedNumericDocValuesMultiRangeQuery.this, boost);
      this.scoreMode = scoreMode;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (context.reader().getFieldInfos().fieldInfo(fieldName) == null) {
        return null;
      }
      long lowerValue = sortedClauses.getFirst().lower;
      long upperValue = sortedClauses.getLast().upper;
      int maxDoc = context.reader().maxDoc();
      DocValuesSkipper skipper = context.reader().getDocValuesSkipper(fieldName);
      if (skipper != null) {
        if (skipper.minValue() > upperValue || skipper.maxValue() < lowerValue) {
          return null;
        }
      }

      SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), fieldName);
      TwoPhaseIterator iterator;
      iterator =
          new TwoPhaseIterator(values) {
            final DocValuesMultiRangeQuery.LongRange lookupVal =
                new DocValuesMultiRangeQuery.LongRange(-Long.MAX_VALUE, -Long.MAX_VALUE);

            @Override
            public boolean matches() throws IOException {
              NavigableSet<DocValuesMultiRangeQuery.LongRange> rangeTree = sortedClauses;
              for (int i = 0, count = values.docValueCount(); i < count; ++i) {
                final long value = values.nextValue();
                if (value >= lowerValue && value <= upperValue) {
                  lookupVal.lower = value;
                  lookupVal.upper = value;
                  DocValuesMultiRangeQuery.LongRange lessOrEq = rangeTree.floor(lookupVal);
                  if (lessOrEq != null) {
                    if (lessOrEq.upper >= value) {
                      assert lessOrEq.lower <= value;
                      return true;
                    }
                    assert lessOrEq.upper < value
                        : "always true. prev range is over before the value";
                    // cut range tree for greater values, if we'll look up then
                    if (i < count - 1) {
                      rangeTree = rangeTree.tailSet(lessOrEq, false);
                    }
                  } // else
                  // lessOrEq == null - value before the first range
                }
              }
              return false; // all values were < lowerValue
            }

            @Override
            public float matchCost() {
              return sortedClauses.size();
            }
          };
      if (skipper != null) {
        iterator =
            new DocValuesRangeIterator(
                iterator, skipper, lowerValue, upperValue, sortedClauses.size() > 1);
      }
      return ConstantScoreScorerSupplier.fromIterator(
          TwoPhaseIterator.asDocIdSetIterator(iterator), score(), scoreMode, maxDoc);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return DocValues.isCacheable(ctx, fieldName);
    }
  }
}
