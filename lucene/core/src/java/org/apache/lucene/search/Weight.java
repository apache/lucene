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
import java.util.Objects;
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
   * Optional method that delegates to scorerSupplier.
   *
   * <p>Returns a {@link Scorer} which can iterate in order over all matching documents and assign
   * them a score. A scorer for the same {@link LeafReaderContext} instance may be requested
   * multiple times as part of a single search call.
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
  public final Scorer scorer(LeafReaderContext context) throws IOException {
    ScorerSupplier scorerSupplier = scorerSupplier(context);
    if (scorerSupplier == null) {
      return null;
    }
    return scorerSupplier.get(Long.MAX_VALUE);
  }

  /**
   * Get a {@link ScorerSupplier}, which allows knowing the cost of the {@link Scorer} before
   * building it. A scorer supplier for the same {@link LeafReaderContext} instance may be requested
   * multiple times as part of a single search call.
   *
   * <p><strong>Note:</strong> It must return null if the scorer is null.
   *
   * @param context the leaf reader context
   * @return a {@link ScorerSupplier} providing the scorer, or null if scorer is null
   * @throws IOException if an IOException occurs
   * @see Scorer
   * @see DefaultScorerSupplier
   */
  public abstract ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException;

  /**
   * Helper method that delegates to {@link #scorerSupplier(LeafReaderContext)}. It is implemented
   * as
   *
   * <pre class="prettyprint">
   * ScorerSupplier scorerSupplier = scorerSupplier(context);
   * if (scorerSupplier == null) {
   *   // No docs match
   *   return null;
   * }
   *
   * scorerSupplier.setTopLevelScoringClause();
   * return scorerSupplier.bulkScorer();
   * </pre>
   *
   * A bulk scorer for the same {@link LeafReaderContext} instance may be requested multiple times
   * as part of a single search call.
   */
  public final BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    ScorerSupplier scorerSupplier = scorerSupplier(context);
    if (scorerSupplier == null) {
      // No docs match
      return null;
    }

    scorerSupplier.setTopLevelScoringClause();
    return scorerSupplier.bulkScorer();
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
   * A wrap for default scorer supplier.
   *
   * @lucene.internal
   */
  protected static final class DefaultScorerSupplier extends ScorerSupplier {
    private final Scorer scorer;

    public DefaultScorerSupplier(Scorer scorer) {
      this.scorer = Objects.requireNonNull(scorer, "Scorer must not be null");
    }

    @Override
    public Scorer get(long leadCost) throws IOException {
      return scorer;
    }

    @Override
    public long cost() {
      return scorer.iterator().cost();
    }
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
      this.scorer = Objects.requireNonNull(scorer);
      this.twoPhase = scorer.twoPhaseIterator();
      if (twoPhase == null) {
        this.iterator = scorer.iterator();
      } else {
        this.iterator = twoPhase.approximation();
      }
    }

    @Override
    public long cost() {
      return iterator.cost();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      collector.setScorer(scorer);
      DocIdSetIterator competitiveIterator = collector.competitiveIterator();

      if (competitiveIterator != null) {
        if (competitiveIterator.docID() > min) {
          min = competitiveIterator.docID();
          // The competitive iterator may not match any docs in the range.
          min = Math.min(min, max);
        }
      }

      if (iterator.docID() < min) {
        if (iterator.docID() == min - 1) {
          iterator.nextDoc();
        } else {
          iterator.advance(min);
        }
      }

      // These various specializations help save some null checks in a hot loop, but as importantly
      // if not more importantly, they help reduce the polymorphism of calls sites to nextDoc() and
      // collect() because only a subset of collectors produce a competitive iterator, and the set
      // of implementing classes for two-phase approximations is smaller than the set of doc id set
      // iterator implementations.
      if (twoPhase == null && competitiveIterator == null) {
        // Optimize simple iterators with collectors that can't skip
        scoreIterator(collector, acceptDocs, iterator, max);
      } else if (competitiveIterator == null) {
        scoreTwoPhaseIterator(collector, acceptDocs, iterator, twoPhase, max);
      } else if (twoPhase == null) {
        scoreCompetitiveIterator(collector, acceptDocs, iterator, competitiveIterator, max);
      } else {
        scoreTwoPhaseOrCompetitiveIterator(
            collector, acceptDocs, iterator, twoPhase, competitiveIterator, max);
      }

      return iterator.docID();
    }

    private static void scoreIterator(
        LeafCollector collector, Bits acceptDocs, DocIdSetIterator iterator, int max)
        throws IOException {
      for (int doc = iterator.docID(); doc < max; doc = iterator.nextDoc()) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          collector.collect(doc);
        }
      }
    }

    private static void scoreTwoPhaseIterator(
        LeafCollector collector,
        Bits acceptDocs,
        DocIdSetIterator iterator,
        TwoPhaseIterator twoPhase,
        int max)
        throws IOException {
      for (int doc = iterator.docID(); doc < max; doc = iterator.nextDoc()) {
        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
          collector.collect(doc);
        }
      }
    }

    private static void scoreCompetitiveIterator(
        LeafCollector collector,
        Bits acceptDocs,
        DocIdSetIterator iterator,
        DocIdSetIterator competitiveIterator,
        int max)
        throws IOException {
      for (int doc = iterator.docID(); doc < max; ) {
        assert competitiveIterator.docID() <= doc; // invariant
        if (competitiveIterator.docID() < doc) {
          int competitiveNext = competitiveIterator.advance(doc);
          if (competitiveNext != doc) {
            doc = iterator.advance(competitiveNext);
            continue;
          }
        }

        if ((acceptDocs == null || acceptDocs.get(doc))) {
          collector.collect(doc);
        }

        doc = iterator.nextDoc();
      }
    }

    private static void scoreTwoPhaseOrCompetitiveIterator(
        LeafCollector collector,
        Bits acceptDocs,
        DocIdSetIterator iterator,
        TwoPhaseIterator twoPhase,
        DocIdSetIterator competitiveIterator,
        int max)
        throws IOException {
      for (int doc = iterator.docID(); doc < max; ) {
        assert competitiveIterator.docID() <= doc; // invariant
        if (competitiveIterator.docID() < doc) {
          int competitiveNext = competitiveIterator.advance(doc);
          if (competitiveNext != doc) {
            doc = iterator.advance(competitiveNext);
            continue;
          }
        }

        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
          collector.collect(doc);
        }

        doc = iterator.nextDoc();
      }
    }
  }
}
