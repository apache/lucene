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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/** Class used to match candidate queries selected by a Presearcher from a Monitor query index. */
public abstract class CandidateMatcher<T extends QueryMatch> {

  /** The searcher to run candidate queries against */
  protected final IndexSearcher searcher;

  private final Map<String, Exception> errors = new HashMap<>();
  private final List<MatchHolder<T>> matches;

  private long searchTime = System.nanoTime();

  private static class MatchHolder<T> {
    Map<String, T> matches = new HashMap<>();
  }

  /**
   * Creates a new CandidateMatcher for the supplied DocumentBatch
   *
   * @param searcher the IndexSearcher to run queries against
   */
  public CandidateMatcher(IndexSearcher searcher) {
    this.searcher = searcher;
    int docCount = searcher.getIndexReader().maxDoc();
    this.matches = new ArrayList<>(docCount);
    for (int i = 0; i < docCount; i++) {
      this.matches.add(new MatchHolder<>());
    }
  }

  /**
   * Runs the supplied query against this CandidateMatcher's set of documents, storing any resulting
   * match, and recording the query in the presearcher hits
   *
   * @param queryId the query id
   * @param matchQuery the query to run
   * @param metadata the query metadata
   * @throws IOException on IO errors
   */
  public abstract void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException;

  /**
   * Record a match
   *
   * @param match a QueryMatch object
   */
  protected final void addMatch(T match, int doc) {
    MatchHolder<T> docMatches = matches.get(doc);
    docMatches.matches.compute(
        match.getQueryId(),
        (_, oldValue) -> {
          if (oldValue != null) {
            return resolve(match, oldValue);
          }
          return match;
        });
  }

  /**
   * If two matches from the same query are found (for example, two branches of a disjunction),
   * combine them.
   *
   * @param match1 the first match found
   * @param match2 the second match found
   * @return a Match object that combines the two
   */
  public abstract T resolve(T match1, T match2);

  /** Called by the Monitor if running a query throws an Exception */
  public void reportError(String queryId, Exception e) {
    this.errors.put(queryId, e);
  }

  /**
   * @return the matches from this matcher
   */
  public final MultiMatchingQueries<T> finish(long buildTime, int queryCount) {
    return finish(buildTime, queryCount, List.of());
  }

  /**
   * Finishes this matcher and the other matchers, which searched the same documents, and adds their
   * matches and errors to this one. Matches for the same query are combined with {@link #resolve}.
   *
   * @param others matchers of the other slices of a concurrent search
   */
  final MultiMatchingQueries<T> finish(
      long buildTime, int queryCount, List<? extends CandidateMatcher<T>> others) {
    doFinish();
    // doFinish may replace the matches of this matcher, so add the others afterwards
    for (CandidateMatcher<T> other : others) {
      addAll(other.finish(0, 0));
    }
    this.searchTime =
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - searchTime, TimeUnit.NANOSECONDS);
    List<Map<String, T>> results = new ArrayList<>();
    for (MatchHolder<T> matchHolder : matches) {
      results.add(matchHolder.matches);
    }
    return new MultiMatchingQueries<>(
        results, errors, buildTime, searchTime, queryCount, matches.size());
  }

  /** Called when all monitoring of a batch of documents is complete */
  protected void doFinish() {}

  /**
   * Adds the matches and errors of a finished matcher's results over the same documents. Matches
   * for the same query are combined with {@link #resolve}.
   */
  void addAll(MultiMatchingQueries<T> other) {
    for (int doc = 0; doc < other.getBatchSize(); doc++) {
      for (T match : other.getMatches(doc)) {
        addMatch(match, doc);
      }
    }
    for (Map.Entry<String, Exception> error : other.getErrors().entrySet()) {
      reportError(error.getKey(), error.getValue());
    }
  }

  /** Copy all matches from another CandidateMatcher */
  protected void copyMatches(CandidateMatcher<T> other) {
    this.matches.clear();
    this.matches.addAll(other.matches);
  }
}
