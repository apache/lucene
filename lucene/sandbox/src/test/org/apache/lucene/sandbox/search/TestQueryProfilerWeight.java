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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestQueryProfilerWeight extends LuceneTestCase {

  private static final class FakeWeight extends Weight {
    FakeWeight(Query query) {
      super(query);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
      return Explanation.match(1, "fake_description");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return scorerSupplier(context).get(Long.MAX_VALUE);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) {
      Weight weight = this;
      return new ScorerSupplier() {
        private long cost = 0;

        @Override
        public Scorer get(long leadCost) {
          return new Scorer(weight) {
            @Override
            public DocIdSetIterator iterator() {
              return null;
            }

            @Override
            public float getMaxScore(int upTo) {
              return 42f;
            }

            @Override
            public float score() {
              return 0;
            }

            @Override
            public int docID() {
              return 0;
            }
          };
        }

        @Override
        public long cost() {
          return cost;
        }

        @Override
        public void setTopLevelScoringClause() {
          cost = 42;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) {
      return new Matches() {
        @Override
        public MatchesIterator getMatches(String field) {
          return new MatchesIterator() {
            @Override
            public boolean next() {
              return false;
            }

            @Override
            public int startPosition() {
              return 42;
            }

            @Override
            public int endPosition() {
              return 43;
            }

            @Override
            public int startOffset() {
              return 44;
            }

            @Override
            public int endOffset() {
              return 45;
            }

            @Override
            public MatchesIterator getSubMatches() {
              return null;
            }

            @Override
            public Query getQuery() {
              return parentQuery;
            }
          };
        }

        @Override
        public Collection<Matches> getSubMatches() {
          return Collections.emptyList();
        }

        @Override
        public Iterator<String> iterator() {
          return null;
        }
      };
    }
  }

  public void testPropagateMatches() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight fakeWeight = new FakeWeight(query);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight profileWeight = new QueryProfilerWeight(fakeWeight, profile);
    assertEquals(42, profileWeight.matches(null, 1).getMatches("some_field").startPosition());
  }

  public void testPropagateExplain() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight fakeWeight = new FakeWeight(query);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight profileWeight = new QueryProfilerWeight(fakeWeight, profile);
    assertEquals("fake_description", profileWeight.explain(null, 1).getDescription());
  }

  public void testPropagateScorer() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight fakeWeight = new FakeWeight(query);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight profileWeight = new QueryProfilerWeight(fakeWeight, profile);
    assertEquals(42f, profileWeight.scorer(null).getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
  }

  public void testPropagateTopLevelScoringClause() throws IOException {
    Query query = new MatchAllDocsQuery();
    Weight fakeWeight = new FakeWeight(query);
    QueryProfilerBreakdown profile = new QueryProfilerBreakdown();
    QueryProfilerWeight profileWeight = new QueryProfilerWeight(fakeWeight, profile);
    ScorerSupplier scorerSupplier = profileWeight.scorerSupplier(null);
    scorerSupplier.setTopLevelScoringClause();
    assertEquals(42, scorerSupplier.cost());
  }
}
