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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.CollectorOwner;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Computes drill down and sideways counts for the provided {@link DrillDownQuery}. Drill sideways
 * counts include alternative values/aggregates for the drill-down dimensions so that a dimension
 * does not disappear after the user drills down into it.
 *
 * <p>Use one of the static search methods to do the search, and then get the hits and facet results
 * from the returned {@link DrillSidewaysResult}.
 *
 * <p>There is both a "standard" and "concurrent" implementation for drill sideways. The concurrent
 * approach is enabled by providing an {@code ExecutorService} to the ctor. The concurrent
 * implementation may be a little faster but does duplicate work (which grows linearly with the
 * number of drill down dimensions specified on the provided {@link DrillDownQuery}). The duplicate
 * work may impact the overall throughput of a system. The standard approach may be a little slower
 * but avoids duplicate computations and query processing. Note that both approaches are compatible
 * with concurrent searching across segments (i.e., if using an {@link IndexSearcher} constructed
 * with an {@code Executor}).
 *
 * <p><b>NOTE</b>: this allocates one {@link FacetsCollector} for each drill-down, plus one. If your
 * index has high number of facet labels then this will multiply your memory usage.
 *
 * @lucene.experimental
 */
public class DrillSideways {

  /** {@link IndexSearcher} passed to constructor. */
  protected final IndexSearcher searcher;

  /** {@link TaxonomyReader} passed to constructor. */
  protected final TaxonomyReader taxoReader;

  /** {@link SortedSetDocValuesReaderState} passed to constructor; can be null. */
  protected final SortedSetDocValuesReaderState state;

  /** {@link FacetsConfig} passed to constructor. */
  protected final FacetsConfig config;

  /** (optional) {@link ExecutorService} used for "concurrent" drill sideways if desired. */
  private final ExecutorService executor;

  /** Create a new {@code DrillSideways} instance. */
  public DrillSideways(IndexSearcher searcher, FacetsConfig config, TaxonomyReader taxoReader) {
    this(searcher, config, taxoReader, null);
  }

  /**
   * Create a new {@code DrillSideways} instance, assuming the categories were indexed with {@link
   * SortedSetDocValuesFacetField}.
   */
  public DrillSideways(
      IndexSearcher searcher, FacetsConfig config, SortedSetDocValuesReaderState state) {
    this(searcher, config, null, state);
  }

  /**
   * Create a new {@code DrillSideways} instance, where some dimensions were indexed with {@link
   * SortedSetDocValuesFacetField} and others were indexed with {@link FacetField}.
   */
  public DrillSideways(
      IndexSearcher searcher,
      FacetsConfig config,
      TaxonomyReader taxoReader,
      SortedSetDocValuesReaderState state) {
    this(searcher, config, taxoReader, state, null);
  }

  /**
   * Create a new {@code DrillSideways} instance, where some dimensions were indexed with {@link
   * SortedSetDocValuesFacetField} and others were indexed with {@link FacetField}.
   *
   * <p>Use this constructor to use the concurrent implementation
   */
  public DrillSideways(
      IndexSearcher searcher,
      FacetsConfig config,
      TaxonomyReader taxoReader,
      SortedSetDocValuesReaderState state,
      ExecutorService executor) {
    this.searcher = searcher;
    this.config = config;
    this.taxoReader = taxoReader;
    this.state = state;
    this.executor = executor;
  }

  /**
   * Subclass can override to customize drill down facets collector. Returning {@code null} is valid
   * if no drill down facet collection is needed.
   */
  protected FacetsCollectorManager createDrillDownFacetsCollectorManager() {
    return new FacetsCollectorManager();
  }

  /**
   * Subclass can override to customize drill sideways facets collector. This should not return
   * {@code null} as we assume drill sideways is being used to collect "sideways" hits:
   */
  protected FacetsCollectorManager createDrillSidewaysFacetsCollectorManager() {
    return new FacetsCollectorManager();
  }

  /** Subclass can override to customize per-dim Facets impl. */
  protected Facets buildFacetsResult(
      FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims)
      throws IOException {

    Facets drillDownFacets = null;
    Map<String, Facets> drillSidewaysFacets = new HashMap<>();

    if (taxoReader != null) {
      if (drillDowns != null) {
        drillDownFacets = new FastTaxonomyFacetCounts(taxoReader, config, drillDowns);
      }
      if (drillSideways != null) {
        for (int i = 0; i < drillSideways.length; i++) {
          drillSidewaysFacets.put(
              drillSidewaysDims[i],
              new FastTaxonomyFacetCounts(taxoReader, config, drillSideways[i]));
        }
      }
    } else {
      if (drillDowns != null) {
        drillDownFacets = new SortedSetDocValuesFacetCounts(state, drillDowns);
      }
      if (drillSideways != null) {
        for (int i = 0; i < drillSideways.length; i++) {
          drillSidewaysFacets.put(
              drillSidewaysDims[i], new SortedSetDocValuesFacetCounts(state, drillSideways[i]));
        }
      }
    }

    if (drillSidewaysFacets.isEmpty()) {
      return drillDownFacets;
    } else {
      return new MultiFacets(drillSidewaysFacets, drillDownFacets);
    }
  }

  /** Search, sorting by {@link Sort}, and computing drill down and sideways counts. */
  public DrillSidewaysResult search(
      DrillDownQuery query, Query filter, FieldDoc after, int topN, Sort sort, boolean doDocScores)
      throws IOException {
    if (filter != null) {
      query = new DrillDownQuery(config, filter, query);
    }
    if (sort != null) {
      int limit = searcher.getIndexReader().maxDoc();
      if (limit == 0) {
        limit = 1; // the collector does not alow numHits = 0
      }
      final int fTopN = Math.min(topN, limit);
      final boolean supportsConcurrency = searcher.getSlices().length > 1;
      final TopFieldCollectorManager collectorManager =
          new TopFieldCollectorManager(sort, fTopN, after, Integer.MAX_VALUE, supportsConcurrency);
      final ConcurrentDrillSidewaysResult<TopFieldDocs> r = search(query, collectorManager);
      TopFieldDocs topDocs = r.collectorResult;

      if (doDocScores) {
        TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, query);
      }
      return new DrillSidewaysResult(
          r.facets,
          r.collectorResult,
          r.drillDownFacetsCollector,
          r.drillSidewaysFacetsCollector,
          r.drillSidewaysDims);
    } else {
      return search(after, query, topN);
    }
  }

  /** Search, sorting by score, and computing drill down and sideways counts. */
  public DrillSidewaysResult search(DrillDownQuery query, int topN) throws IOException {
    return search(null, query, topN);
  }

  /** Search, sorting by score, and computing drill down and sideways counts. */
  public DrillSidewaysResult search(ScoreDoc after, DrillDownQuery query, int topN)
      throws IOException {
    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1; // the collector does not alow numHits = 0
    }
    final int fTopN = Math.min(topN, limit);
    final boolean supportsConcurrency = searcher.getSlices().length > 1;
    final TopScoreDocCollectorManager collectorManager =
        new TopScoreDocCollectorManager(fTopN, after, Integer.MAX_VALUE, supportsConcurrency);
    final ConcurrentDrillSidewaysResult<TopDocs> r = search(query, collectorManager);
    return new DrillSidewaysResult(
        r.facets,
        r.collectorResult,
        r.drillDownFacetsCollector,
        r.drillSidewaysFacetsCollector,
        r.drillSidewaysDims);
  }

  /**
   * Override this and return true if your collector (e.g., {@code ToParentBlockJoinCollector})
   * expects all sub-scorers to be positioned on the document being collected. This will cause some
   * performance loss; default is false.
   */
  protected boolean scoreSubDocsAtOnce() {
    return false;
  }

  /**
   * Result of a drill sideways search, including the {@link Facets} and {@link TopDocs}. The {@link
   * FacetsCollector}s for the drill down and drill sideways dimensions are also exposed for
   * advanced use-cases that need access to them as an alternative to accessing the {@code Facets}.
   */
  public static class DrillSidewaysResult {
    /** Combined drill down and sideways results. */
    public final Facets facets;

    /** Hits. */
    public final TopDocs hits;

    /**
     * FacetsCollector populated based on hits that match the full DrillDownQuery, treating all
     * drill down dimensions as required clauses. Useful for advanced use-cases that want to compute
     * Facets results separate from the provided Facets in this result.
     */
    public final FacetsCollector drillDownFacetsCollector;

    /**
     * FacetsCollectors populated for each drill sideways dimension. Each collector exposes the hits
     * that match on all DrillDownQuery dimensions, but treating their corresponding sideways
     * dimension as optional. This array provides a FacetsCollector for each drill down dimension
     * present in the original DrillDownQuery, and the associated dimension for each FacetsCollector
     * can be determined using the parallel {@link DrillSidewaysResult#drillSidewaysDims} array.
     * Useful for advanced use-cases that want to compute Facets results separate from the provided
     * Facets in this result.
     */
    public final FacetsCollector[] drillSidewaysFacetsCollector;

    /**
     * Dimensions that correspond to to the {@link DrillSidewaysResult#drillSidewaysFacetsCollector}
     */
    public final String[] drillSidewaysDims;

    /** Sole constructor. */
    public DrillSidewaysResult(
        Facets facets,
        TopDocs hits,
        FacetsCollector drillDownFacetsCollector,
        FacetsCollector[] drillSidewaysFacetsCollector,
        String[] drillSidewaysDims) {
      this.facets = facets;
      this.hits = hits;
      this.drillDownFacetsCollector = drillDownFacetsCollector;
      this.drillSidewaysFacetsCollector = drillSidewaysFacetsCollector;
      this.drillSidewaysDims = drillSidewaysDims;
    }
  }

  private static class CallableCollector implements Callable<Void> {
    private final IndexSearcher searcher;
    private final Query query;
    private final CollectorOwner<?, ?> collectorOwner;

    private CallableCollector(
        int pos, IndexSearcher searcher, Query query, CollectorOwner<?, ?> collectorOwner) {
      this.searcher = searcher;
      this.query = query;
      this.collectorOwner = collectorOwner;
    }

    @Override
    public Void call() throws Exception {
      // TODO: the difference is that we used to also call reduce in parallel, but not anymore.
      //  We can think about implementing reduce(executor) which allows parallelism? If doing it
      //  sequentially becomes a problem.
      searcher.search(query, collectorOwner);
      // TODO: we don't use the results - should we return null? In this case, we can
      // simplify/remove CallableResult class
      return null;
      // return new CallableResult(pos, collectorOwner);
    }
  }

  private DrillDownQuery getDrillDownQuery(
      final DrillDownQuery query, Query[] queries, final String excludedDimension) {
    final DrillDownQuery ddl = new DrillDownQuery(config, query.getBaseQuery());
    query
        .getDims()
        .forEach(
            (dim, pos) -> {
              if (!dim.equals(excludedDimension)) ddl.add(dim, queries[pos]);
            });
    return ddl.getDims().size() == queries.length ? null : ddl;
  }

  /** Runs a search, using a {@link CollectorManager} to gather and merge search results */
  @SuppressWarnings("unchecked")
  public <R> ConcurrentDrillSidewaysResult<R> search(
      final DrillDownQuery query, final CollectorManager<?, R> hitCollectorManager)
      throws IOException {
    // Main query
    FacetsCollectorManager drillDownFacetsCollectorManager =
        createDrillDownFacetsCollectorManager();
    final CollectorOwner<?, ?> mainCollectorOwner;
    if (drillDownFacetsCollectorManager != null) {
      // Make sure we populate a facet collector corresponding to the base query if desired:
      mainCollectorOwner =
          new CollectorOwner<>(
              new MultiCollectorManager(drillDownFacetsCollectorManager, hitCollectorManager));
    } else {
      mainCollectorOwner = new CollectorOwner<>(hitCollectorManager);
    }
    // Drill sideways dimensions
    final List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners;
    if (query.getDims().isEmpty() == false) {
      drillSidewaysCollectorOwners = new ArrayList<>(query.getDims().size());
      for (int i = 0; i < query.getDims().size(); i++) {
        drillSidewaysCollectorOwners.add(
            new CollectorOwner<>(createDrillSidewaysFacetsCollectorManager()));
      }
    } else {
      drillSidewaysCollectorOwners = null;
    }
    // Execute query
    if (executor != null) {
      searchConcurrently(query, mainCollectorOwner, drillSidewaysCollectorOwners);
    } else {
      searchSequentially(query, mainCollectorOwner, drillSidewaysCollectorOwners);
    }

    // Collect results
    final FacetsCollector facetsCollectorResult;
    final R hitCollectorResult;
    if (drillDownFacetsCollectorManager != null) {
      // drill down collected using MultiCollector
      // Extract the results:
      Object[] drillDownResult = (Object[]) mainCollectorOwner.getResult();
      facetsCollectorResult = (FacetsCollector) drillDownResult[0];
      hitCollectorResult = (R) drillDownResult[1];
    } else {
      facetsCollectorResult = null;
      hitCollectorResult = (R) mainCollectorOwner.getResult();
    }

    // Getting results for drill sideways dimensions (if any)
    final String[] drillSidewaysDims;
    final FacetsCollector[] drillSidewaysCollectors;
    if (query.getDims().isEmpty() == false) {
      drillSidewaysDims = query.getDims().keySet().toArray(new String[0]);
      int numDims = query.getDims().size();
      assert drillSidewaysCollectorOwners != null;
      assert drillSidewaysCollectorOwners.size() == numDims;
      drillSidewaysCollectors = new FacetsCollector[numDims];
      for (int dim = 0; dim < numDims; dim++) {
        drillSidewaysCollectors[dim] =
            (FacetsCollector) drillSidewaysCollectorOwners.get(dim).getResult();
      }
    } else {
      drillSidewaysDims = null;
      drillSidewaysCollectors = null;
    }

    return new ConcurrentDrillSidewaysResult<>(
        buildFacetsResult(facetsCollectorResult, drillSidewaysCollectors, drillSidewaysDims),
        null,
        hitCollectorResult,
        facetsCollectorResult,
        drillSidewaysCollectors,
        drillSidewaysDims);
  }

  /**
   * Search using DrillDownQuery with custom collectors. This method can be used with any {@link
   * CollectorOwner}s. It doesn't return anything because it is expected that you read results from
   * provided {@link CollectorOwner}s.
   *
   * <p>To read the results, run {@link CollectorOwner#getResult()} for drill down and all drill
   * sideways dimensions.
   *
   * <p>Note: use {@link Collections#unmodifiableList(List)} to wrap {@code
   * drillSidewaysCollectorOwners} to convince compiler that it is safe to use List here.
   *
   * <p>TODO: Class CollectorOwner was created so that we can ignore CollectorManager type C,
   * because we want each dimensions to be able to use their own types. Alternatively, we can use
   * typesafe heterogeneous container and provide CollectorManager type for each dimension to this
   * method? I do like CollectorOwner approach as it seems more intuitive?
   */
  public void search(
      final DrillDownQuery query,
      CollectorOwner<?, ?> drillDownCollectorOwner,
      List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners)
      throws IOException {
    if (drillDownCollectorOwner == null) {
      throw new IllegalArgumentException(
          "This search method requires client to provide drill down collector manager");
    }
    if (drillSidewaysCollectorOwners == null) {
      if (query.getDims().isEmpty() == false) {
        throw new IllegalArgumentException(
            "The query requires not null drillSidewaysCollectorOwners");
      }
    } else if (drillSidewaysCollectorOwners.size() != query.getDims().size()) {
      throw new IllegalArgumentException(
          "drillSidewaysCollectorOwners size must be equal to number of dimensions in the query.");
    }
    if (executor != null) {
      searchConcurrently(query, drillDownCollectorOwner, drillSidewaysCollectorOwners);
    } else {
      searchSequentially(query, drillDownCollectorOwner, drillSidewaysCollectorOwners);
    }

    // This method doesn't return results as each dimension might have its own result type.
    // But we call getResult to trigger results reducing, so that users don't have to worry about
    // it.
    // TODO: do we want to run reduce in parallel if executor is provided?
    drillDownCollectorOwner.getResult();
    if (drillSidewaysCollectorOwners != null) {
      for (CollectorOwner<?, ?> sidewaysOwner : drillSidewaysCollectorOwners) {
        sidewaysOwner.getResult();
      }
    }
  }

  private void searchSequentially(
      final DrillDownQuery query,
      final CollectorOwner<?, ?> drillDownCollectorOwner,
      final List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners)
      throws IOException {

    Map<String, Integer> drillDownDims = query.getDims();

    if (drillDownDims.isEmpty()) {
      // There are no drill-down dims, so there is no
      // drill-sideways to compute:
      searcher.search(query, drillDownCollectorOwner);
      return;
    }

    Query baseQuery = query.getBaseQuery();
    if (baseQuery == null) {
      // TODO: we could optimize this pure-browse case by
      // making a custom scorer instead:
      baseQuery = new MatchAllDocsQuery();
    }
    Query[] drillDownQueries = query.getDrillDownQueries();

    DrillSidewaysQuery dsq =
        new DrillSidewaysQuery(
            baseQuery,
            // drillDownCollectorOwner,
            // Don't pass drill down collector because drill down is collected by IndexSearcher
            // itself.
            // TODO: deprecate drillDown collection in DrillSidewaysQuery?
            null,
            drillSidewaysCollectorOwners,
            drillDownQueries,
            scoreSubDocsAtOnce());

    searcher.search(dsq, drillDownCollectorOwner);
  }

  private void searchConcurrently(
      final DrillDownQuery query,
      final CollectorOwner<?, ?> drillDownCollectorOwner,
      final List<CollectorOwner<?, ?>> drillSidewaysCollectorOwners)
      throws IOException {

    final Map<String, Integer> drillDownDims = query.getDims();
    final List<CallableCollector> callableCollectors = new ArrayList<>(drillDownDims.size() + 1);

    callableCollectors.add(new CallableCollector(-1, searcher, query, drillDownCollectorOwner));
    int i = 0;
    final Query[] filters = query.getDrillDownQueries();
    for (String dim : drillDownDims.keySet()) {
      callableCollectors.add(
          new CallableCollector(
              i,
              searcher,
              getDrillDownQuery(query, filters, dim),
              drillSidewaysCollectorOwners.get(i)));
      i++;
    }

    try {
      // Run the query pool
      final List<Future<Void>> futures = executor.invokeAll(callableCollectors);

      // Wait for results. We don't read the results as they are collected by CollectorOwners
      for (i = 0; i < futures.size(); i++) {
        futures.get(0).get();
      }
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Result of a concurrent drill sideways search, including the {@link Facets} and {@link TopDocs}.
   */
  public static class ConcurrentDrillSidewaysResult<R> extends DrillSidewaysResult {

    /** The merged search results */
    public final R collectorResult;

    /** Sole constructor. */
    ConcurrentDrillSidewaysResult(
        Facets facets,
        TopDocs hits,
        R collectorResult,
        FacetsCollector drillDownFacetsCollector,
        FacetsCollector[] drillSidewaysFacetsCollector,
        String[] drillSidewaysDims) {
      super(
          facets, hits, drillDownFacetsCollector, drillSidewaysFacetsCollector, drillSidewaysDims);
      this.collectorResult = collectorResult;
    }
  }
}
