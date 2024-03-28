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
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Query;

/**
 * Wrapper class for an existing candidate matcher exposing finish, matchQuery and reportError for
 * composition of matchers
 */
public class WrappedCandidateMatcher<T extends QueryMatch> {

  private CandidateMatcher<T> delegateTo;

  /**
   * Creates a new WrappedCandidateMatcher delegating to the supplied CandidateMatcher
   *
   * @param wrapThis the CandidateMatcher to which functions should be delegate
   */
  public WrappedCandidateMatcher(CandidateMatcher<T> wrapThis) {
    delegateTo = wrapThis;
  }

  /**
   * Runs the supplied query against the delegate CandidateMatcher's set of documents, storing any
   * resulting match, and recording the query in the presearcher hits
   *
   * @param queryId the query id
   * @param matchQuery the query to run
   * @param metadata the query metadata
   * @throws IOException on IO errors
   */
  public void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException {
    delegateTo.matchQuery(queryId, matchQuery, metadata);
  }

  /**
   * @return the matches from this matcher
   */
  public MultiMatchingQueries<T> finish(long buildTime, int queryCount) {
    return delegateTo.finish(buildTime, queryCount);
  }

  /**
   * Record a match into delegate
   *
   * @param match a QueryMatch object
   */
  protected void addMatch(T match, int doc) {
    delegateTo.addMatch(match, doc);
  }

  /**
   * If two matches from the same query are found (for example, two branches of a disjunction),
   * combine them using the delegate's resolve function.
   *
   * @param match1 the first match found
   * @param match2 the second match found
   * @return a Match object that combines the two
   */
  public T resolve(T match1, T match2) {
    return delegateTo.resolve(match1, match2);
  }

  /** If running a query throws an Exception, this function will add error into delegate */
  public void reportError(String queryId, Exception e) {
    delegateTo.reportError(queryId, e);
  }

  /** Called when all monitoring of a batch of documents is complete */
  protected void doFinish() {
    delegateTo.doFinish();
  }

  /** Copy all matches from another CandidateMatcher */
  protected void copyMatches(CandidateMatcher<T> other) {
    delegateTo.copyMatches(other);
  }

  /**
   * Build a MultiMatchingQueries
   *
   * @param matches the matches to include, mapping from queryId to match
   * @param errors any errors thrown while evaluating matches
   * @param queryBuildTime how long (in ns) it took to build the Presearcher query for the matcher
   *     run
   * @param searchTime how long (in ms) it took to run the selected queries
   * @param queriesRun the number of queries passed to this CandidateMatcher during the matcher run
   * @param batchSize the number of documents in the batch
   * @return a MultiMatchingQueries object with the results of matching a batch of Documents
   */
  public static <T1 extends QueryMatch> MultiMatchingQueries<T1> buildMultiMatchingQueries(
      List<Map<String, T1>> matches,
      Map<String, Exception> errors,
      long queryBuildTime,
      long searchTime,
      int queriesRun,
      int batchSize) {
    return new MultiMatchingQueries<>(
        matches, errors, queryBuildTime, searchTime, queriesRun, batchSize);
  }
}
