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

import org.apache.lucene.util.PriorityQueue;

/**
 * A base class for all collectors that return a {@link TopDocs} output. This collector allows easy
 * extension by providing a single constructor which accepts a {@link PriorityQueue} as well as
 * protected members for that priority queue and a counter of the number of total hits.<br>
 * Extending classes can override any of the methods to provide their own implementation, as well as
 * avoid the use of the priority queue entirely by passing null to {@link
 * #TopDocsCollector(PriorityQueue)}. In that case however, you might want to consider overriding
 * all methods, in order to avoid a NullPointerException.
 */
public abstract class TopDocsCollector<T extends ScoreDoc> implements Collector {

  /**
   * This is used in case topDocs() is called with illegal parameters, or there simply aren't
   * (enough) results.
   *
   * @lucene.internal
   */
  public static final TopDocs EMPTY_TOPDOCS =
      new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

  /**
   * The priority queue which holds the top documents. Note that different implementations of
   * PriorityQueue give different meaning to 'top documents'. HitQueue for example aggregates the
   * top scoring documents, while other PQ implementations may hold documents sorted by other
   * criteria.
   */
  protected final PriorityQueue<T> pq;

  /** The total number of documents that the collector encountered. */
  protected int totalHits;

  /** Whether {@link #totalHits} is exact or a lower bound. */
  protected TotalHits.Relation totalHitsRelation = TotalHits.Relation.EQUAL_TO;

  protected TopDocsCollector(PriorityQueue<T> pq) {
    this.pq = pq;
  }

  /**
   * Populates the results array with the ScoreDoc instances. This can be overridden in case a
   * different ScoreDoc type should be returned.
   */
  protected void populateResults(ScoreDoc[] results, int howMany) {
    for (int i = howMany - 1; i >= 0; i--) {
      results[i] = pq.pop();
    }
  }

  /**
   * Returns a {@link TopDocs} instance containing the given results. If <code>results</code> is
   * null it means there are no results to return, either because there were 0 calls to collect() or
   * because the arguments to topDocs were invalid.
   */
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    return results == null
        ? EMPTY_TOPDOCS
        : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  /** The total number of documents that matched this query. */
  public int getTotalHits() {
    return totalHits;
  }

  /** The number of valid PQ entries */
  protected int topDocsSize() {
    // In case pq was populated with sentinel values, there might be less
    // results than pq.size(). Therefore return all results until either
    // pq.size() or totalHits.
    return Math.min(totalHits, pq.size());
  }

  /** Returns the top docs that were collected by this collector. */
  public TopDocs topDocs() {
    // In case pq was populated with sentinel values, there might be less
    // results than pq.size(). Therefore return all results until either
    // pq.size() or totalHits.
    return topDocs(0, topDocsSize());
  }

  /**
   * Returns the documents in the range [start .. pq.size()) that were collected by this collector.
   * Note that if {@code start >= pq.size()}, an empty TopDocs is returned.<br>
   * This method is convenient to call if the application always asks for the last results, starting
   * from the last 'page'.<br>
   * <b>NOTE:</b> you cannot call this method more than once for each search execution. If you need
   * to call it more than once, passing each time a different <code>start</code>, you should call
   * {@link #topDocs()} and work with the returned {@link TopDocs} object, which will contain all
   * the results this search execution collected.
   */
  public TopDocs topDocs(int start) {
    // In case pq was populated with sentinel values, there might be less
    // results than pq.size(). Therefore return all results until either
    // pq.size() or totalHits.
    return topDocs(start, topDocsSize());
  }

  /**
   * Returns the documents in the range [start .. start+howMany) that were collected by this
   * collector. Note that if {@code start >= pq.size()}, an empty TopDocs is returned, and if
   * pq.size() - start &lt; howMany, then only the available documents in [start .. pq.size()) are
   * returned.<br>
   * This method is useful to call in case pagination of search results is allowed by the search
   * application, as well as it attempts to optimize the memory used by allocating only as much as
   * requested by howMany.<br>
   * <b>NOTE:</b> you cannot call this method more than once for each search execution. If you need
   * to call it more than once, passing each time a different range, you should call {@link
   * #topDocs()} and work with the returned {@link TopDocs} object, which will contain all the
   * results this search execution collected.
   */
  public TopDocs topDocs(int start, int howMany) {

    // In case pq was populated with sentinel values, there might be less
    // results than pq.size(). Therefore return all results until either
    // pq.size() or totalHits.
    int size = topDocsSize();

    if (howMany < 0) {
      throw new IllegalArgumentException(
          "Number of hits requested must be greater than 0 but value was " + howMany);
    }

    if (start < 0) {
      throw new IllegalArgumentException(
          "Expected value of starting position is between 0 and " + size + ", got " + start);
    }

    if (start >= size || howMany == 0) {
      return newTopDocs(null, start);
    }

    // We know that start < pqsize, so just fix howMany.
    howMany = Math.min(size - start, howMany);
    ScoreDoc[] results = new ScoreDoc[howMany];

    // Prune the least competitive hits until we reach the requested range.
    // Note that this loop will usually not be executed, since the common usage
    // should be that the caller asks for the last howMany results. However it's
    // needed here for completeness.
    pruneLeastCompetitiveHitsTo(start + howMany);

    // Get the requested results from pq.
    populateResults(results, howMany);

    return newTopDocs(results, start);
  }

  /**
   * Prune the least competitive hits until the number of candidates is less than or equal to {@code
   * keep}. This is typically called before {@link #populateResults} to ensure we are at right pos.
   */
  protected void pruneLeastCompetitiveHitsTo(int keep) {
    for (int i = pq.size() - keep; i > 0; i--) {
      pq.pop();
    }
  }
}
