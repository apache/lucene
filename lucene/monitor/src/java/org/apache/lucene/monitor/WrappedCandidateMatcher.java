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

// Wrapper class for an existing candidate matcher exposing finish, matchQuery and reportError
// for composition of matchers
public class WrappedCandidateMatcher<T extends QueryMatch> {

  private CandidateMatcher<T> delegateTo;

  public WrappedCandidateMatcher(CandidateMatcher<T> wrapThis) {
    delegateTo = wrapThis;
  }

  public void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException {
    delegateTo.matchQuery(queryId, matchQuery, metadata);
  }

  public MultiMatchingQueries<T> finish(long buildTime, int queryCount) {
    return delegateTo.finish(buildTime, queryCount);
  }

  protected void addMatch(T match, int doc) {
    delegateTo.addMatch(match, doc);
  }

  public T resolve(T match1, T match2) {
    return delegateTo.resolve(match1, match2);
  }

  public void reportError(String queryId, Exception e) {
    delegateTo.reportError(queryId, e);
  }

  protected void doFinish() {
    delegateTo.doFinish();
  }

  protected void copyMatches(CandidateMatcher<T> other) {
    delegateTo.copyMatches(other);
  }

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
