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
import java.util.Collection;
import java.util.List;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.TotalHits;

/**
 * A {@link CollectorManager} implementation which produces FacetsCollector and produces a merged
 * FacetsCollector. This is used for concurrent FacetsCollection.
 */
public class FacetsCollectorManager implements CollectorManager<FacetsCollector, FacetsCollector> {

  private final boolean keepScores;

  /** Sole constructor. */
  public FacetsCollectorManager() {
    this(false);
  }

  /**
   * Creates a new collector manager that in turn creates {@link FacetsCollector} using the provided
   * {@code keepScores} flag. hits.
   */
  public FacetsCollectorManager(boolean keepScores) {
    this.keepScores = keepScores;
  }

  @Override
  public FacetsCollector newCollector() throws IOException {
    return new FacetsCollector(keepScores);
  }

  @Override
  public FacetsCollector reduce(Collection<FacetsCollector> collectors) throws IOException {
    if (collectors == null || collectors.size() == 0) {
      return new FacetsCollector();
    }
    if (collectors.size() == 1) {
      return collectors.iterator().next();
    }
    assert collectors.stream().allMatch(fc -> fc.getKeepScores() == keepScores);
    return new ReducedFacetsCollector(collectors, keepScores);
  }

  private static class ReducedFacetsCollector extends FacetsCollector {

    ReducedFacetsCollector(final Collection<FacetsCollector> facetsCollectors, boolean keepScores) {
      super(keepScores);
      final List<MatchingDocs> matchingDocs = this.getMatchingDocs();
      facetsCollectors.forEach(
          facetsCollector -> matchingDocs.addAll(facetsCollector.getMatchingDocs()));
    }
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult search(
      IndexSearcher searcher, Query q, int n, FacetsCollectorManager fcm) throws IOException {
    return doSearch(searcher, null, q, n, null, false, fcm);
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult search(
      IndexSearcher searcher, Query q, int n, Sort sort, FacetsCollectorManager fcm)
      throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return doSearch(searcher, null, q, n, sort, false, fcm);
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult search(
      IndexSearcher searcher,
      Query q,
      int n,
      Sort sort,
      boolean doDocScores,
      FacetsCollectorManager fcm)
      throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return doSearch(searcher, null, q, n, sort, doDocScores, fcm);
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult searchAfter(
      IndexSearcher searcher, ScoreDoc after, Query q, int n, FacetsCollectorManager fcm)
      throws IOException {
    return doSearch(searcher, after, q, n, null, false, fcm);
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult searchAfter(
      IndexSearcher searcher, ScoreDoc after, Query q, int n, Sort sort, FacetsCollectorManager fcm)
      throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return doSearch(searcher, after, q, n, sort, false, fcm);
  }

  /**
   * Utility method, to search and also populate a {@code FacetsCollector} with hits. The provided
   * {@code FacetsCollectorManager} will be used for creating/reducing {@code FacetsCollector}
   * instances.
   */
  public static FacetsResult searchAfter(
      IndexSearcher searcher,
      ScoreDoc after,
      Query q,
      int n,
      Sort sort,
      boolean doDocScores,
      FacetsCollectorManager fcm)
      throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return doSearch(searcher, after, q, n, sort, doDocScores, fcm);
  }

  private static FacetsResult doSearch(
      IndexSearcher searcher,
      ScoreDoc after,
      Query q,
      int n,
      Sort sort,
      boolean doDocScores,
      FacetsCollectorManager fcm)
      throws IOException {

    int limit = searcher.getIndexReader().maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    n = Math.min(n, limit);

    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException(
          "after.doc exceeds the number of documents in the reader: after.doc="
              + after.doc
              + " limit="
              + limit);
    }

    final TopDocs topDocs;
    final FacetsCollector facetsCollector;
    if (n == 0) {
      TotalHitCountCollectorManager hitCountCollectorManager = new TotalHitCountCollectorManager();
      MultiCollectorManager multiCollectorManager =
          new MultiCollectorManager(hitCountCollectorManager, fcm);
      Object[] result = searcher.search(q, multiCollectorManager);
      topDocs =
          new TopDocs(
              new TotalHits((Integer) result[0], TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
      facetsCollector = (FacetsCollector) result[1];
    } else {
      final MultiCollectorManager multiCollectorManager;
      if (sort != null) {
        if (after != null && !(after instanceof FieldDoc)) {
          // TODO: if we fix type safety of TopFieldDocs we can
          // remove this
          throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
        }
        TopFieldCollectorManager topFieldCollectorManager =
            new TopFieldCollectorManager(sort, n, (FieldDoc) after, Integer.MAX_VALUE, true);
        multiCollectorManager = new MultiCollectorManager(topFieldCollectorManager, fcm);
      } else {
        TopScoreDocCollectorManager topScoreDocCollectorManager =
            new TopScoreDocCollectorManager(n, after, Integer.MAX_VALUE, true);
        multiCollectorManager = new MultiCollectorManager(topScoreDocCollectorManager, fcm);
      }
      Object[] result = searcher.search(q, multiCollectorManager);
      topDocs = (TopDocs) result[0];
      if (doDocScores) {
        TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, q);
      }
      facetsCollector = (FacetsCollector) result[1];
    }
    return new FacetsResult(topDocs, facetsCollector);
  }

  /**
   * Holds results of a search run via static utility methods exposed by this class. Those include
   * {@link TopDocs} as well as facets result included in the returned {@link FacetsCollector}
   */
  public static class FacetsResult {
    private final TopDocs topDocs;
    private final FacetsCollector facetsCollector;

    /**
     * Create a new instance of this class.
     *
     * @param topDocs the top docs
     * @param facetsCollector the facets result included in a {@link FacetsCollector} instance
     */
    public FacetsResult(TopDocs topDocs, FacetsCollector facetsCollector) {
      this.topDocs = topDocs;
      this.facetsCollector = facetsCollector;
    }

    /**
     * Exposed the {@link TopDocs}
     *
     * @return the top docs returned from the search
     */
    public TopDocs topDocs() {
      return topDocs;
    }

    /**
     * Exposes the facet results
     *
     * @return the facet results via a reduced {@link FacetsCollector}
     */
    public FacetsCollector facetsCollector() {
      return facetsCollector;
    }
  }
}
