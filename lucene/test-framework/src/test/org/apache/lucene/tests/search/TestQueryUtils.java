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
package org.apache.lucene.tests.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests that {@link QueryUtils#checkSkipTo} and {@link QueryUtils#checkFirstSkipTo} actually detect
 * broken scorer behaviour.
 */
public class TestQueryUtils extends LuceneTestCase {

  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(newTextField("f", "v", Field.Store.NO));
      iw.addDocument(doc);
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  // advance() always returns NO_MORE_DOCS — exercises assertTrue(more)

  public void testCheckSkipToDetectsBrokenAdvance() throws IOException {
    Query q = new BrokenAdvanceQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkSkipTo(q, searcher));
  }

  public void testCheckFirstSkipToDetectsBrokenAdvance() throws IOException {
    Query q = new BrokenAdvanceQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkFirstSkipTo(q, searcher));
  }

  // advance() skips one doc too far — exercises assertEquals(scorerDoc, doc)

  public void testCheckSkipToDetectsWrongDocFromAdvance() throws IOException {
    Query q = new SkipTooFarQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkSkipTo(q, searcher));
  }

  public void testCheckFirstSkipToDetectsWrongDocFromAdvance() throws IOException {
    Query q = new SkipTooFarQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkFirstSkipTo(q, searcher));
  }

  // score() returns different values on successive calls — exercises the score-stability assertions

  public void testCheckSkipToDetectsUnstableScore() throws IOException {
    Query q = new UnstableScoreQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkSkipTo(q, searcher));
  }

  public void testCheckFirstSkipToDetectsUnstableScore() throws IOException {
    Query q = new UnstableScoreQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkFirstSkipTo(q, searcher));
  }

  // score() differs depending on whether the doc was reached via advance() or nextDoc() —
  // exercises the score-consistency assertions between the two navigation paths

  public void testCheckSkipToDetectsScoreMismatch() throws IOException {
    Query q = new ScoreMismatchQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkSkipTo(q, searcher));
  }

  public void testCheckFirstSkipToDetectsScoreMismatch() throws IOException {
    Query q = new ScoreMismatchQuery(new TermQuery(new Term("f", "v")));
    expectThrows(AssertionError.class, () -> QueryUtils.checkFirstSkipTo(q, searcher));
  }

  // ---------------------------------------------------------------------------
  // Shared query/weight infrastructure
  // ---------------------------------------------------------------------------

  /**
   * Base class for queries that wrap a delegate and substitute a broken scorer. Subclasses provide
   * the scorer via {@link #wrapScorer}.
   */
  private abstract static class BrokenQuery extends Query {
    final Query delegate;

    BrokenQuery(Query delegate) {
      this.delegate = delegate;
    }

    abstract Scorer wrapScorer(Scorer inner);

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new FilterWeight(this, delegate.createWeight(searcher, scoreMode, boost)) {
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          Scorer inner = in.scorer(context);
          if (inner == null) return null;
          return new DefaultScorerSupplier(wrapScorer(inner));
        }
      };
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName() + "(" + delegate.toString(field) + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      delegate.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
      return sameClassAs(obj) && delegate.equals(((BrokenQuery) obj).delegate);
    }

    @Override
    public int hashCode() {
      return classHash() ^ delegate.hashCode();
    }
  }

  // ---------------------------------------------------------------------------
  // Broken query variants
  // ---------------------------------------------------------------------------

  /** advance() always returns NO_MORE_DOCS regardless of whether matches exist. */
  private static final class BrokenAdvanceQuery extends BrokenQuery {
    BrokenAdvanceQuery(Query delegate) {
      super(delegate);
    }

    @Override
    Scorer wrapScorer(Scorer inner) {
      DocIdSetIterator innerIter = inner.iterator();
      final int[] docId = {-1};
      DocIdSetIterator iter =
          new DocIdSetIterator() {
            @Override
            public int docID() {
              return docId[0];
            }

            @Override
            public int nextDoc() throws IOException {
              return docId[0] = innerIter.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
              return docId[0] = NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              return innerIter.cost();
            }
          };
      return scorerForIter(inner, iter);
    }
  }

  /**
   * advance() calls the real advance but then steps one doc further, so the scorer lands on the
   * wrong document.
   */
  private static final class SkipTooFarQuery extends BrokenQuery {
    SkipTooFarQuery(Query delegate) {
      super(delegate);
    }

    @Override
    Scorer wrapScorer(Scorer inner) {
      DocIdSetIterator innerIter = inner.iterator();
      final int[] docId = {-1};
      DocIdSetIterator iter =
          new DocIdSetIterator() {
            @Override
            public int docID() {
              return docId[0];
            }

            @Override
            public int nextDoc() throws IOException {
              return docId[0] = innerIter.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
              int doc = innerIter.advance(target);
              if (doc != NO_MORE_DOCS) {
                doc = innerIter.nextDoc(); // skip one too far
              }
              return docId[0] = doc;
            }

            @Override
            public long cost() {
              return innerIter.cost();
            }
          };
      return scorerForIter(inner, iter);
    }
  }

  /**
   * score() multiplies the real score by 2 on every even call, producing different values on
   * successive calls to score() for the same document.
   */
  private static final class UnstableScoreQuery extends BrokenQuery {
    UnstableScoreQuery(Query delegate) {
      super(delegate);
    }

    @Override
    Scorer wrapScorer(Scorer inner) {
      DocIdSetIterator innerIter = inner.iterator();
      final int[] docId = {-1};
      DocIdSetIterator iter =
          new DocIdSetIterator() {
            @Override
            public int docID() {
              return docId[0];
            }

            @Override
            public int nextDoc() throws IOException {
              return docId[0] = innerIter.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
              return docId[0] = innerIter.advance(target);
            }

            @Override
            public long cost() {
              return innerIter.cost();
            }
          };
      return new Scorer() {
        private int scoreCallCount = 0;

        @Override
        public float score() throws IOException {
          return inner.score() * (++scoreCallCount % 2 == 0 ? 2f : 1f);
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return inner.getMaxScore(upTo) * 2f;
        }

        @Override
        public int docID() {
          return iter.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
          return iter;
        }
      };
    }
  }

  /**
   * score() returns a value inflated by 100 when the current doc was reached via advance(), and the
   * real score when reached via nextDoc(). This makes the score differ between the two navigation
   * paths.
   */
  private static final class ScoreMismatchQuery extends BrokenQuery {
    ScoreMismatchQuery(Query delegate) {
      super(delegate);
    }

    @Override
    Scorer wrapScorer(Scorer inner) {
      DocIdSetIterator innerIter = inner.iterator();
      final int[] docId = {-1};
      final boolean[] lastWasAdvance = {false};
      DocIdSetIterator iter =
          new DocIdSetIterator() {
            @Override
            public int docID() {
              return docId[0];
            }

            @Override
            public int nextDoc() throws IOException {
              lastWasAdvance[0] = false;
              return docId[0] = innerIter.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
              lastWasAdvance[0] = true;
              return docId[0] = innerIter.advance(target);
            }

            @Override
            public long cost() {
              return innerIter.cost();
            }
          };
      return new Scorer() {
        @Override
        public float score() throws IOException {
          return lastWasAdvance[0] ? inner.score() + 100f : inner.score();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return inner.getMaxScore(upTo) + 100f;
        }

        @Override
        public int docID() {
          return iter.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
          return iter;
        }
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /** Wraps a scorer's scoring logic with a replacement iterator. */
  private static Scorer scorerForIter(Scorer inner, DocIdSetIterator iter) {
    return new Scorer() {
      @Override
      public float score() throws IOException {
        return inner.score();
      }

      @Override
      public float getMaxScore(int upTo) throws IOException {
        return inner.getMaxScore(upTo);
      }

      @Override
      public int docID() {
        return iter.docID();
      }

      @Override
      public DocIdSetIterator iterator() {
        return iter;
      }
    };
  }
}
