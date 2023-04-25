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
    public Scorer scorer(LeafReaderContext context) {
      return new Scorer(this) {
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
}
