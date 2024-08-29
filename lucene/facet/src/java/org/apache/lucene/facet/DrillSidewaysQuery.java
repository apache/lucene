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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorOwner;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/** Only purpose is to punch through and return a DrillSidewaysScorer */

// TODO change the way DrillSidewaysScorer is used, this query does not work
// with filter caching
class DrillSidewaysQuery extends Query {

  final Query baseQuery;

  final CollectorOwner<?, ?> drillDownCollectorOwner;
  final List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners;

  final Query[] drillDownQueries;

  final boolean scoreSubDocsAtOnce;

  /**
   * Construct a new {@code DrillSidewaysQuery} that will create new {@link FacetsCollector}s for
   * each {@link LeafReaderContext} using the provided {@link FacetsCollectorManager}s.
   */
  DrillSidewaysQuery(
      Query baseQuery,
      CollectorOwner<?, ?> drillDownCollectorOwner,
      List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners,
      Query[] drillDownQueries,
      boolean scoreSubDocsAtOnce) {
    this.baseQuery = Objects.requireNonNull(baseQuery);
    this.drillDownCollectorOwner = drillDownCollectorOwner;
    this.drillSidewaysCollectorOwners = drillSidewaysCollectorOwners;
    this.drillDownQueries = drillDownQueries;
    this.scoreSubDocsAtOnce = scoreSubDocsAtOnce;
  }

  @Override
  public String toString(String field) {
    return "DrillSidewaysQuery";
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query newQuery = baseQuery;
    while (true) {
      Query rewrittenQuery = newQuery.rewrite(indexSearcher);
      if (rewrittenQuery == newQuery) {
        break;
      }
      newQuery = rewrittenQuery;
    }
    if (newQuery == baseQuery) {
      return super.rewrite(indexSearcher);
    } else {
      return new DrillSidewaysQuery(
          newQuery,
          drillDownCollectorOwner,
          drillSidewaysCollectorOwners,
          drillDownQueries,
          scoreSubDocsAtOnce);
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight baseWeight = baseQuery.createWeight(searcher, scoreMode, boost);
    final Weight[] drillDowns = new Weight[drillDownQueries.length];
    for (int dim = 0; dim < drillDownQueries.length; dim++) {
      drillDowns[dim] =
          searcher.createWeight(
              searcher.rewrite(drillDownQueries[dim]), ScoreMode.COMPLETE_NO_SCORES, 1);
    }

    return new Weight(DrillSidewaysQuery.this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return baseWeight.explain(context, doc);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        ScorerSupplier baseScorerSupplier = baseWeight.scorerSupplier(context);

        int drillDownCount = drillDowns.length;

        Collector drillDownCollector;
        final LeafCollector drillDownLeafCollector;
        if (drillDownCollectorOwner != null) {
          drillDownCollector = drillDownCollectorOwner.newCollector();
          drillDownLeafCollector = drillDownCollector.getLeafCollector(context);
        } else {
          drillDownLeafCollector = null;
        }

        DrillSidewaysScorer.DocsAndCost[] dims =
            new DrillSidewaysScorer.DocsAndCost[drillDownCount];

        int nullCount = 0;
        for (int dim = 0; dim < dims.length; dim++) {
          Scorer scorer = drillDowns[dim].scorer(context);
          if (scorer == null) {
            nullCount++;
            scorer = new ConstantScoreScorer(0f, scoreMode, DocIdSetIterator.empty());
          }

          Collector sidewaysCollector = drillSidewaysCollectorOwners.get(dim).newCollector();

          dims[dim] =
              new DrillSidewaysScorer.DocsAndCost(
                  scorer, sidewaysCollector.getLeafCollector(context));
        }

        // If baseScorer is null or the dim nullCount > 1, then we have nothing to score. We return
        // a null scorer in this case, but we need to make sure #finish gets called on all facet
        // collectors since IndexSearcher won't handle this for us:
        if (baseScorerSupplier == null || nullCount > 1) {
          if (drillDownLeafCollector != null) {
            drillDownLeafCollector.finish();
          }
          for (DrillSidewaysScorer.DocsAndCost dim : dims) {
            dim.sidewaysLeafCollector.finish();
          }
          return null;
        }

        // Sort drill-downs by most restrictive first:
        Arrays.sort(dims, Comparator.comparingLong(o -> o.approximation.cost()));

        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            // We can only run as a top scorer:
            throw new UnsupportedOperationException();
          }

          @Override
          public BulkScorer bulkScorer() throws IOException {
            return new DrillSidewaysScorer(
                context,
                baseScorerSupplier.get(Long.MAX_VALUE),
                drillDownLeafCollector,
                dims,
                scoreSubDocsAtOnce);
          }

          @Override
          public long cost() {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // We can never cache DSQ instances. It's critical that the BulkScorer produced by this
        // Weight runs through the "normal" execution path so that it has access to an
        // "acceptDocs" instance that accurately reflects deleted docs. During caching,
        // "acceptDocs" is null so that caching over-matches (since the final BulkScorer would
        // account for deleted docs). The problem is that this BulkScorer has a side-effect of
        // populating the "sideways" FacetsCollectors, so it will use deleted docs in its
        // sideways counting if caching kicks in. See LUCENE-10060:
        return false;
      }
    };
  }

  // TODO: these should do "deeper" equals/hash on the 2-D drillDownTerms array

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hashCode(baseQuery);
    result = prime * result + Objects.hashCode(drillDownCollectorOwner);
    result = prime * result + Arrays.hashCode(drillDownQueries);
    result = prime * result + Objects.hashCode(drillSidewaysCollectorOwners);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(DrillSidewaysQuery other) {
    return Objects.equals(baseQuery, other.baseQuery)
        && Objects.equals(drillDownCollectorOwner, other.drillDownCollectorOwner)
        && Arrays.equals(drillDownQueries, other.drillDownQueries)
        && Objects.equals(drillSidewaysCollectorOwners, other.drillSidewaysCollectorOwners);
  }
}
