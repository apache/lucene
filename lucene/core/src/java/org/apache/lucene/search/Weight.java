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

import java.io.IOException;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * Expert: Calculate query weights and build query scorers.
 *
 * <p>The purpose of {@link Weight} is to ensure searching does not modify a {@link Query}, so that
 * a {@link Query} instance can be reused.
 *
 * <p>{@link IndexSearcher} dependent state of the query should reside in the {@link Weight}.
 *
 * <p>{@link org.apache.lucene.index.LeafReader} dependent state should reside in the {@link
 * Scorer}.
 *
 * <p>Since {@link Weight} creates {@link Scorer} instances for a given {@link
 * org.apache.lucene.index.LeafReaderContext} ({@link
 * #scorer(org.apache.lucene.index.LeafReaderContext)}) callers must maintain the relationship
 * between the searcher's top-level {@link IndexReaderContext} and the context used to create a
 * {@link Scorer}.
 *
 * <p>A <code>Weight</code> is used in the following way:
 *
 * <ol>
 *   <li>A <code>Weight</code> is constructed by a top-level query, given a <code>IndexSearcher
 *       </code> ({@link Query#createWeight(IndexSearcher, ScoreMode, float)}).
 *   <li>A <code>Scorer</code> is constructed by {@link
 *       #scorer(org.apache.lucene.index.LeafReaderContext)}.
 * </ol>
 *
 * @since 2.9
 */
public abstract class Weight implements SegmentCacheable {

  protected final Query parentQuery;

  /**
   * Sole constructor, typically invoked by sub-classes.
   *
   * @param query the parent query
   */
  protected Weight(Query query) {
    this.parentQuery = query;
  }

  /**
   * Returns {@link Matches} for a specific document, or {@code null} if the document does not match
   * the parent query
   *
   * <p>A query match that contains no position information (for example, a Point or DocValues
   * query) will return {@link MatchesUtils#MATCH_WITH_NO_TERMS}
   *
   * @param context the reader's context to create the {@link Matches} for
   * @param doc the document's id relative to the given context's reader
   * @lucene.experimental
   */
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    ScorerSupplier scorerSupplier = scorerSupplier(context);
    if (scorerSupplier == null) {
      return null;
    }
    Scorer scorer = scorerSupplier.get(1);
    final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
    if (twoPhase == null) {
      if (scorer.iterator().advance(doc) != doc) {
        return null;
      }
    } else {
      if (twoPhase.approximation().advance(doc) != doc || twoPhase.matches() == false) {
        return null;
      }
    }
    return MatchesUtils.MATCH_WITH_NO_TERMS;
  }

  /**
   * An explanation of the score computation for the named document.
   *
   * @param context the readers context to create the {@link Explanation} for.
   * @param doc the document's id relative to the given context's reader
   * @return an Explanation for the score
   * @throws IOException if an {@link IOException} occurs
   */
  public abstract Explanation explain(LeafReaderContext context, int doc) throws IOException;

  /** The query that this concerns. */
  public final Query getQuery() {
    return parentQuery;
  }

  /**
   * Returns a {@link Scorer} which can iterate in order over all matching documents and assign them
   * a score.
   *
   * <p><b>NOTE:</b> null can be returned if no documents will be scored by this query.
   *
   * <p><b>NOTE</b>: The returned {@link Scorer} does not have {@link LeafReader#getLiveDocs()}
   * applied, they need to be checked on top.
   *
   * @param context the {@link org.apache.lucene.index.LeafReaderContext} for which to return the
   *     {@link Scorer}.
   * @return a {@link Scorer} which scores documents in/out-of order.
   * @throws IOException if there is a low-level I/O error
   */
  public abstract Scorer scorer(LeafReaderContext context) throws IOException;

  /**
   * Optional method. Get a {@link ScorerSupplier}, which allows to know the cost of the {@link
   * Scorer} before building it. The default implementation calls {@link #scorer} and builds a
   * {@link ScorerSupplier} wrapper around it.
   *
   * @see #scorer
   */
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    final Scorer scorer = scorer(context);
    if (scorer == null) {
      return null;
    }
    return new ScorerSupplier() {
      @Override
      public Scorer get(long leadCost) {
        return scorer;
      }

      @Override
      public long cost() {
        return scorer.iterator().cost();
      }
    };
  }

  /**
   * Optional method, to return a {@link BulkScorer} to score the query and send hits to a {@link
   * Collector}. Only queries that have a different top-level approach need to override this; the
   * default implementation pulls a normal {@link Scorer} and iterates and collects the resulting
   * hits which are not marked as deleted.
   *
   * @param context the {@link org.apache.lucene.index.LeafReaderContext} for which to return the
   *     {@link Scorer}.
   * @return a {@link BulkScorer} which scores documents and passes them to a collector.
   * @throws IOException if there is a low-level I/O error
   */
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {

    ScorerSupplier scorerSupplier = scorerSupplier(context);
    if (scorerSupplier == null) {
      // No docs match
      return null;
    }

    scorerSupplier.setTopLevelScoringClause();

    return new DefaultBulkScorer(scorerSupplier.get(Long.MAX_VALUE));
  }

  /**
   * Counts the number of live documents that match a given {@link Weight#parentQuery} in a leaf.
   *
   * <p>The default implementation returns -1 for every query. This indicates that the count could
   * not be computed in sub-linear time.
   *
   * <p>Specific query classes should override it to provide other accurate sub-linear
   * implementations (that actually return the count). Look at {@link
   * MatchAllDocsQuery#createWeight(IndexSearcher, ScoreMode, float)} for an example
   *
   * <p>We use this property of the function to count hits in {@link IndexSearcher#count(Query)}.
   *
   * @param context the {@link org.apache.lucene.index.LeafReaderContext} for which to return the
   *     count.
   * @return integer count of the number of matches
   * @throws IOException if there is a low-level I/O error
   */
  public int count(LeafReaderContext context) throws IOException {
    return -1;
  }

  /**
   * Just wraps a Scorer and performs top scoring using it.
   *
   * @lucene.internal
   */
  protected static class DefaultBulkScorer extends BulkScorer {
    private final Scorer scorer;
    private final DocIdSetIterator iterator;
    private final TwoPhaseIterator twoPhase;

    /** Sole constructor. */
    public DefaultBulkScorer(Scorer scorer) {
      if (scorer == null) {
        throw new NullPointerException();
      }
      this.scorer = scorer;
      this.iterator = scorer.iterator();
      this.twoPhase = scorer.twoPhaseIterator();
    }

    @Override
    public long cost() {
      return iterator.cost();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      collector.setScorer(scorer);
      DocIdSetIterator scorerIterator = twoPhase == null ? iterator : twoPhase.approximation();
      DocIdSetIterator competitiveIterator = collector.competitiveIterator();

      if (competitiveIterator == null
          && scorerIterator.docID() == -1
          && min == 0
          && max == DocIdSetIterator.NO_MORE_DOCS) {
        scoreAll(collector, scorerIterator, twoPhase, acceptDocs);
        return DocIdSetIterator.NO_MORE_DOCS;
      } else {
        return scoreRange(
            collector, scorerIterator, twoPhase, competitiveIterator, acceptDocs, min, max);
      }
    }

    /**
     * Specialized method to bulk-score a range of hits; we separate this from {@link #scoreAll} to
     * help out hotspot. See <a
     * href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a>
     */
    static int scoreRange(
        LeafCollector collector,
        DocIdSetIterator iterator,
        TwoPhaseIterator twoPhase,
        DocIdSetIterator competitiveIterator,
        Bits acceptDocs,
        int min,
        int max)
        throws IOException {

      if (competitiveIterator != null) {
        if (competitiveIterator.docID() > min) {
          min = competitiveIterator.docID();
          // The competitive iterator may not match any docs in the range.
          min = Math.min(min, max);
        }
      }

      int doc = iterator.docID();
      if (doc < min) {
        if (doc == min - 1) {
          doc = iterator.nextDoc();
        } else {
          doc = iterator.advance(min);
        }
      }

      if (twoPhase == null && competitiveIterator == null) {
        // Optimize simple iterators with collectors that can't skip
        while (doc < max) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            collector.collect(doc);
          }
          doc = iterator.nextDoc();
        }
      } else {
        while (doc < max) {
          if (competitiveIterator != null) {
            assert competitiveIterator.docID() <= doc;
            if (competitiveIterator.docID() < doc) {
              competitiveIterator.advance(doc);
            }
            if (competitiveIterator.docID() != doc) {
              doc = iterator.advance(competitiveIterator.docID());
              continue;
            }
          }

          if ((acceptDocs == null || acceptDocs.get(doc))
              && (twoPhase == null || twoPhase.matches())) {
            collector.collect(doc);
          }
          doc = iterator.nextDoc();
        }
      }

      return doc;
    }

    /**
     * Specialized method to bulk-score all hits; we separate this from {@link #scoreRange} to help
     * out hotspot. See <a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487</a>
     */
    static void scoreAll(
        LeafCollector collector,
        DocIdSetIterator iterator,
        TwoPhaseIterator twoPhase,
        Bits acceptDocs)
        throws IOException {
      if (twoPhase == null) {
        for (int doc = iterator.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = iterator.nextDoc()) {
          if (acceptDocs == null || acceptDocs.get(doc)) {
            collector.collect(doc);
          }
        }
      } else {
        // The scorer has an approximation, so run the approximation first, then check acceptDocs,
        // then confirm
        for (int doc = iterator.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = iterator.nextDoc()) {
          if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
            collector.collect(doc);
          }
        }
      }
    }
  }
}
