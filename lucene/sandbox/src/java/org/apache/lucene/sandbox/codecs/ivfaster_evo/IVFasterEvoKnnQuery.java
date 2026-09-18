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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link KnnFloatVectorQuery} that resolves a filter of several clauses with the filter's {@link
 * BulkScorer} rather than its iterator. For a dense conjunction Lucene's bulk scorer intersects the
 * clauses as bit sets, a window of documents at a time, where the iterator leap-frogs them document
 * by document. The resolved bit set is handed to the codec as is, so the codec reads the filter's
 * exact cardinality for free and widens its probes to match the filter's selectivity. A filter of
 * one clause has nothing to intersect and is left to the stock query, which adopts it as is.
 *
 * <p>The filter is kept here rather than in the superclass, which would resolve it through its
 * iterator, so {@link #getFilter()} returns null.
 *
 * @lucene.experimental
 */
public final class IVFasterEvoKnnQuery extends KnnFloatVectorQuery {
  private final Query denseFilter;
  private final Weight filterWeight; // only on the copy made by rewrite

  /**
   * Finds the {@code k} nearest vectors to {@code target} among the documents matching {@code
   * filter} (all documents if null), probing {@code numProbes} cells before filter scaling.
   */
  public IVFasterEvoKnnQuery(String field, float[] target, int k, Query filter, int numProbes) {
    super(field, target, k, null, new IVFasterEvoVectorsFormat.SearchStrategy(numProbes));
    this.denseFilter = filter;
    this.filterWeight = null;
  }

  private IVFasterEvoKnnQuery(IVFasterEvoKnnQuery query, Weight filterWeight) {
    super(query.field, query.target, query.k, null, query.searchStrategy);
    this.denseFilter = query.denseFilter;
    this.filterWeight = filterWeight;
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    if (denseFilter == null || filterWeight != null) {
      return super.rewrite(searcher);
    }
    Query required =
        new BooleanQuery.Builder()
            .add(denseFilter, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
    Query rewritten = searcher.rewrite(required);
    if (rewritten.getClass() == MatchNoDocsQuery.class) {
      return rewritten;
    }
    if ((rewritten instanceof BooleanQuery conjunction && conjunction.clauses().size() > 1)
        == false) {
      // A single clause has nothing to intersect, and the stock query already adopts its bit set
      // (or bulk-loads its postings) as is: re-collecting it document by document would only
      // rebuild what exists. On a field every document has, the field-exists clause above is
      // rewritten away, so a one-clause filter arrives here as one clause.
      return new KnnFloatVectorQuery(field, target, k, denseFilter, searchStrategy)
          .rewrite(searcher);
    }
    Weight weight = rewritten.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    return new IVFasterEvoKnnQuery(this, weight).rewrite(searcher);
  }

  @Override
  protected TopDocs approximateSearch(
      LeafReaderContext context,
      AcceptDocs liveDocs,
      int visitedLimit,
      KnnCollectorManager knnCollectorManager)
      throws IOException {
    if (filterWeight == null) {
      return super.approximateSearch(context, liveDocs, visitedLimit, knnCollectorManager);
    }
    BulkScorer scorer = filterWeight.bulkScorer(context);
    if (scorer == null) {
      return TopDocsCollector.EMPTY_TOPDOCS;
    }
    int maxDoc = context.reader().maxDoc();
    FixedBitSet accepted = new FixedBitSet(maxDoc);
    LeafCollector collector =
        new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) {
            accepted.set(doc);
          }
        };
    scorer.score(collector, liveDocs.bits(), 0, maxDoc);
    int cardinality = accepted.cardinality();
    if (cardinality <= k) {
      // Too few matches for an approximate search to beat scoring them all.
      return exactSearch(context, new BitSetIterator(accepted, cardinality), null);
    }
    // AcceptDocs adopts a BitSetIterator's bit set without copying it.
    AcceptDocs resolved =
        AcceptDocs.fromIteratorSupplier(
            () -> new BitSetIterator(accepted, cardinality), null, maxDoc);
    return super.approximateSearch(context, resolved, cardinality + 1, knnCollectorManager);
  }

  @Override
  public String toString(String field) {
    return "IVFasterEvoKnnQuery(" + super.toString(field) + ", filter=" + denseFilter + ")";
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other)
        && Objects.equals(denseFilter, ((IVFasterEvoKnnQuery) other).denseFilter);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hashCode(denseFilter);
  }
}
