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
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * Bulk-scoring narrowing for a boolean whose FILTER aligns with the primary index sort.
 *
 * <p>When several FILTER clauses are optimizable, {@link BooleanQuery} passes only the first in
 * clause order as this query's {@code filter} argument; remaining FILTERs stay in the inner boolean
 * passed as {@code query}.
 */
final class FilteredOnPrimaryIndexSortFieldQuery extends Query {
  private final Query query;
  private final Query filter;
  private final PrimarySortAlignable primarySortAlignable;
  private final Query fallbackQuery;

  FilteredOnPrimaryIndexSortFieldQuery(Query query, Query filter, Query fallbackQuery) {
    this.query = Objects.requireNonNull(query);
    this.filter = Objects.requireNonNull(filter);
    this.primarySortAlignable =
        Objects.requireNonNull(
            filter instanceof PrimarySortAlignable psa ? psa : null,
            "filter must implement a primary-sort bulk-scoring alignment");
    this.fallbackQuery = Objects.requireNonNull(fallbackQuery);
  }

  static boolean canOptimize(Query filter, IndexSearcher searcher) throws IOException {
    PrimarySortAlignable alignment = filter instanceof PrimarySortAlignable psa ? psa : null;
    return alignment != null && alignment.canOptimize(searcher);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight queryWeight = searcher.createWeight(query, scoreMode, boost);
    final Weight fallbackWeight = searcher.createWeight(fallbackQuery, scoreMode, boost);

    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return fallbackWeight.explain(context, doc);
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return fallbackWeight.matches(context, doc);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        return fallbackWeight.count(context);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final DocIdRange narrowRange = primarySortAlignable.denseDocIdRangeOrNull(context);
        final ScorerSupplier fallbackSupplier = fallbackWeight.scorerSupplier(context);
        if (fallbackSupplier == null) {
          return null;
        }
        final ScorerSupplier querySupplier = queryWeight.scorerSupplier(context);
        if (querySupplier == null) {
          return fallbackSupplier;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            // Non-bulk path must use the full boolean: TopScorer uses Scorers, not BulkScorer.
            return fallbackSupplier.get(leadCost);
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            if (narrowRange == null) {
              return fallbackSupplier.bulkScorer();
            }
            if (narrowRange.isEmpty()) {
              return emptyBulkScorer();
            }
            return new RangeFilteredBulkScorer(querySupplier.bulkScorer(), narrowRange);
          }

          @Override
          public long cost() {
            if (narrowRange != null) {
              if (narrowRange.isEmpty()) {
                return 0L;
              }
              long span = (long) narrowRange.maxDoc() - narrowRange.minDoc();
              return Math.min(fallbackSupplier.cost(), span);
            }
            return fallbackSupplier.cost();
          }

          @Override
          public void setTopLevelScoringClause() {
            fallbackSupplier.setTopLevelScoringClause();
            querySupplier.setTopLevelScoringClause();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return queryWeight.isCacheable(ctx) && fallbackWeight.isCacheable(ctx);
      }
    };
  }

  private static BulkScorer emptyBulkScorer() {
    return new BulkScorer() {
      @Override
      public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }

  private static final class RangeFilteredBulkScorer extends BulkScorer {
    private final BulkScorer in;
    private final DocIdRange range;

    private RangeFilteredBulkScorer(BulkScorer in, DocIdRange range) {
      this.in = Objects.requireNonNull(in);
      this.range = Objects.requireNonNull(range);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      final int filteredMin = Math.max(min, range.minDoc());
      final int filteredMax = Math.min(max, range.maxDoc());
      if (filteredMin >= filteredMax) {
        return max <= range.minDoc() ? range.minDoc() : DocIdSetIterator.NO_MORE_DOCS;
      }

      final int next = in.score(collector, acceptDocs, filteredMin, filteredMax);
      if (range.maxDoc() <= max) {
        return DocIdSetIterator.NO_MORE_DOCS;
      }
      return next;
    }

    @Override
    public long cost() {
      return Math.min(in.cost(), range.maxDoc() - range.minDoc());
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    fallbackQuery.visit(visitor);
  }

  @Override
  public String toString(String field) {
    return "FilteredOnPrimaryIndexSortFieldQuery(query="
        + query.toString(field)
        + ", filter="
        + filter.toString(field)
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    FilteredOnPrimaryIndexSortFieldQuery that = (FilteredOnPrimaryIndexSortFieldQuery) obj;
    return query.equals(that.query)
        && filter.equals(that.filter)
        && fallbackQuery.equals(that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), query, filter, fallbackQuery);
  }
}
