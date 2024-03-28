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
import java.util.Objects;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.Bits;

/**
 * Utility class with a single method for getting a DocIdSetIterator that skips deleted docs
 *
 * @lucene.experimental
 */
public final class FacetUtils {

  /** Do not instantiate this class */
  private FacetUtils() {}

  /**
   * Wrap the given DocIdSetIterator and liveDocs into another DocIdSetIterator that returns
   * non-deleted documents during iteration. This is useful for computing facet counts on match-all
   * style queries that need to exclude deleted documents.
   *
   * <p>{@link org.apache.lucene.search.ConjunctionUtils} could be better home for this method if we
   * can identify use cases outside facets module.
   *
   * <p>Making this class pkg-private unfortunately limits the visibility of this method to {@link
   * org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts} and {@link
   * org.apache.lucene.facet.sortedset.ConcurrentSortedSetDocValuesFacetCounts} classes as Java does
   * not allow pkg-private classes to be visible to sub-packages.
   *
   * @param it {@link DocIdSetIterator} being wrapped
   * @param liveDocs {@link Bits} containing set bits for non-deleted docs
   * @return wrapped iterator
   */
  public static DocIdSetIterator liveDocsDISI(DocIdSetIterator it, Bits liveDocs) {

    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return it.docID();
      }

      private int doNext(int doc) throws IOException {
        assert doc == it.docID();
        // Find next document that is not deleted until we exhaust all documents
        while (doc != NO_MORE_DOCS && liveDocs.get(doc) == false) {
          doc = it.nextDoc();
        }
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return doNext(it.nextDoc());
      }

      @Override
      public int advance(int target) throws IOException {
        return doNext(it.advance(target));
      }

      @Override
      public long cost() {
        return it.cost();
      }
    };
  }

  public static <T> Pair<TopDocs, T> search(IndexSearcher searcher, Query query, int numHits, CollectorManager<Collector, T> collectorManager) throws IOException {
    return (Pair<TopDocs, T>) doSearch(searcher, null, query, numHits, null, false, collectorManager);
  }

  public static <T> Pair<TopFieldDocs, T> search(IndexSearcher searcher, Query query, int numHits, Sort sort, CollectorManager<Collector, T> collectorManager)
          throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return (Pair<TopFieldDocs, T>) doSearch(searcher, null, query, numHits, sort, false, collectorManager);
  }

  public static <T> Pair<TopFieldDocs, T> search(IndexSearcher searcher, Query query, int numHits, Sort sort, boolean doDocScores, CollectorManager<Collector, T> collectorManager)
          throws IOException {
    if (sort == null) {
      throw new IllegalArgumentException("sort must not be null");
    }
    return (Pair<TopFieldDocs, T>) doSearch(searcher, null, query, numHits, sort, false, collectorManager);
  }

  private static <T> Pair<? extends TopDocs, T> doSearch(
          IndexSearcher searcher,
          ScoreDoc after,
          Query q,
          int n,
          Sort sort,
          boolean doDocScores,
          CollectorManager<Collector, T> collectorManager)
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

    TopDocs topDocs = null;
    Object[] results = null;
    if (n == 0) {
      MultiCollectorManager mcm = new MultiCollectorManager(new TotalHitCountCollectorManager(), collectorManager);
      results = searcher.search(q, mcm);
      // TotalHitCountCollectorManager's reduce method returns an Integer object
      assert results[0].getClass() == Integer.class;
      topDocs =
              new TopDocs(
                      new TotalHits((Integer) results[0], TotalHits.Relation.EQUAL_TO),
                      new ScoreDoc[0]);

    } else {
      CollectorManager hitsCollectorManager;
      if (sort != null) {
        if (after != null && !(after instanceof FieldDoc)) {
          // TODO: if we fix type safety of TopFieldDocs we can
          // remove this
          throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
        }

        hitsCollectorManager = new TopFieldCollectorManager(sort, n, (FieldDoc) after, Integer.MAX_VALUE);
      } else {
        hitsCollectorManager = new TopScoreDocCollectorManager(n, after, Integer.MAX_VALUE);
      }

      MultiCollectorManager mcm = new MultiCollectorManager(hitsCollectorManager, collectorManager);

      results = searcher.search(q, mcm);
      assert results[0].getClass() == TopDocs.class;
      topDocs = ((TopDocs) results[0]);
      if (doDocScores) {
        TopFieldCollector.populateScores(topDocs.scoreDocs, searcher, q);
      }

    }
    return new Pair(topDocs, results[1]);
  }


  /** Holds a single pair of two outputs. */
  public static class Pair<TopDocs, T> {
    public final TopDocs output1;
    public final T output2;

    // use newPair
    private Pair(TopDocs output1, T output2) {
      this.output1 = output1;
      this.output2 = output2;
    }
  }
}
