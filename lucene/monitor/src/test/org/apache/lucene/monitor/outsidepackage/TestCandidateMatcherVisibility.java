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

package org.apache.lucene.monitor.outsidepackage;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.monitor.CandidateMatcher;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

public class TestCandidateMatcherVisibility {

  private CandidateMatcher<QueryMatch> newCandidateMatcher() {
    // Index and searcher for use in creating a matcher
    MemoryIndex index = new MemoryIndex();
    final IndexSearcher searcher = index.createSearcher();
    return QueryMatch.SIMPLE_MATCHER.createMatcher(searcher);
  }

  @Test
  public void testMatchQueryVisibleOutsidePackage() throws IOException {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.matchQuery("test", new TermQuery(new Term("test_field")), Collections.emptyMap());
  }

  @Test
  public void testReportErrorVisibleOutsidePackage() {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.reportError("test", new RuntimeException("test exception"));
  }

  @Test
  public void testFinishVisibleOutsidePackage() {
    CandidateMatcher<QueryMatch> matcher = newCandidateMatcher();
    // This should compile from outside org.apache.lucene.monitor package
    // (subpackage org.apache.lucene.monitor.outsidepackage cannot access package-private content
    // from org.apache.lucene.monitor)
    matcher.finish(0, 0);
  }
}
