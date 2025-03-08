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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Weight.DefaultBulkScorer;
import org.apache.lucene.util.Bits;

final class BooleanScorerSupplier extends ScorerSupplier {
  private final Map<BooleanClause.Occur, Collection<ScorerSupplier>> subs;
  private final ScoreMode scoreMode;
  private final int minShouldMatch;
  private final int maxDoc;
  private long cost = -1;
  private boolean topLevelScoringClause;

  BooleanScorerSupplier(
      Weight weight,
      Map<Occur, Collection<ScorerSupplier>> subs,
      ScoreMode scoreMode,
      int minShouldMatch,
      int maxDoc) {
    if (minShouldMatch < 0) {
      throw new IllegalArgumentException(
          "minShouldMatch must be positive, but got: " + minShouldMatch);
    }
    if (minShouldMatch != 0 && minShouldMatch >= subs.get(Occur.SHOULD).size()) {
      throw new IllegalArgumentException(
          "minShouldMatch must be strictly less than the number of SHOULD clauses");
    }
    if (scoreMode.needsScores() == false
        && minShouldMatch == 0
        && subs.get(Occur.SHOULD).size() > 0
        && subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() > 0) {
      throw new IllegalArgumentException(
          "Cannot pass purely optional clauses if scores are not needed");
    }
    if (subs.get(Occur.SHOULD).size() + subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size()
        == 0) {
      throw new IllegalArgumentException("There should be at least one positive clause");
    }
    this.subs = subs;
    this.scoreMode = scoreMode;
    this.minShouldMatch = minShouldMatch;
    this.maxDoc = maxDoc;
  }

  private long computeCost() {
    OptionalLong minRequiredCost =
        Stream.concat(subs.get(Occur.MUST).stream(), subs.get(Occur.FILTER).stream())
            .mapToLong(ScorerSupplier::cost)
            .min();
    if (minRequiredCost.isPresent() && minShouldMatch == 0) {
      return minRequiredCost.getAsLong();
    } else {
      final Collection<ScorerSupplier> optionalScorers = subs.get(Occur.SHOULD);
      final long shouldCost =
          ScorerUtil.costWithMinShouldMatch(
              optionalScorers.stream().mapToLong(ScorerSupplier::cost),
              optionalScorers.size(),
              minShouldMatch);
      return Math.min(minRequiredCost.orElse(Long.MAX_VALUE), shouldCost);
    }
  }

  @Override
  public void setTopLevelScoringClause() {
    topLevelScoringClause = true;
    if (subs.get(Occur.SHOULD).size() + subs.get(Occur.MUST).size() == 1) {
      // If there is a single scoring clause, propagate the call.
      for (ScorerSupplier ss : subs.get(Occur.SHOULD)) {
        ss.setTopLevelScoringClause();
      }
      for (ScorerSupplier ss : subs.get(Occur.MUST)) {
        ss.setTopLevelScoringClause();
      }
    }
  }

  @Override
  public long cost() {
    if (cost == -1) {
      cost = computeCost();
    }
    return cost;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    Scorer scorer = getInternal(leadCost);
    if (scoreMode == ScoreMode.TOP_SCORES
        && subs.get(Occur.SHOULD).isEmpty()
        && subs.get(Occur.MUST).isEmpty()) {
      // no scoring clauses but scores are needed so we wrap the scorer in
      // a constant score in order to allow early termination
      return scorer.twoPhaseIterator() != null
          ? new ConstantScoreScorer(0f, scoreMode, scorer.twoPhaseIterator())
          : new ConstantScoreScorer(0f, scoreMode, scorer.iterator());
    }
    return scorer;
  }

  private Scorer getInternal(long leadCost) throws IOException {
    // three cases: conjunction, disjunction, or mix
    leadCost = Math.min(leadCost, cost());

    // pure conjunction
    if (subs.get(Occur.SHOULD).isEmpty()) {
      return excl(
          req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost, topLevelScoringClause),
          subs.get(Occur.MUST_NOT),
          leadCost);
    }

    // pure disjunction
    if (subs.get(Occur.FILTER).isEmpty() && subs.get(Occur.MUST).isEmpty()) {
      return excl(
          opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost, topLevelScoringClause),
          subs.get(Occur.MUST_NOT),
          leadCost);
    }

    // conjunction-disjunction mix:
    // we create the required and optional pieces, and then
    // combine the two: if minNrShouldMatch > 0, then it's a conjunction: because the
    // optional side must match. otherwise it's required + optional

    if (minShouldMatch > 0) {
      Scorer req =
          excl(
              req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost, false),
              subs.get(Occur.MUST_NOT),
              leadCost);
      Scorer opt = opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost, false);
      return new ConjunctionScorer(Arrays.asList(req, opt), Arrays.asList(req, opt));
    } else {
      assert scoreMode.needsScores();
      return new ReqOptSumScorer(
          excl(
              req(subs.get(Occur.FILTER), subs.get(Occur.MUST), leadCost, false),
              subs.get(Occur.MUST_NOT),
              leadCost),
          opt(subs.get(Occur.SHOULD), minShouldMatch, scoreMode, leadCost, false),
          scoreMode);
    }
  }

  @Override
  public BulkScorer bulkScorer() throws IOException {
    final BulkScorer bulkScorer = booleanScorer();
    if (bulkScorer != null) {
      // bulk scoring is applicable, use it
      return bulkScorer;
    } else {
      // use a Scorer-based impl (BS2)
      return super.bulkScorer();
    }
  }

  BulkScorer booleanScorer() throws IOException {
    final int numOptionalClauses = subs.get(Occur.SHOULD).size();
    final int numMustClauses = subs.get(Occur.MUST).size();
    final int numRequiredClauses = numMustClauses + subs.get(Occur.FILTER).size();

    BulkScorer positiveScorer;
    if (numRequiredClauses == 0) {
      // TODO: what is the right heuristic here?
      final long costThreshold;
      if (minShouldMatch <= 1) {
        // when all clauses are optional, use BooleanScorer aggressively
        // TODO: is there actually a threshold under which we should rather
        // use the regular scorer?
        costThreshold = -1;
      } else {
        // when a minimum number of clauses should match, BooleanScorer is
        // going to score all windows that have at least minNrShouldMatch
        // matches in the window. But there is no way to know if there is
        // an intersection (all clauses might match a different doc ID and
        // there will be no matches in the end) so we should only use
        // BooleanScorer if matches are very dense
        costThreshold = maxDoc / 3;
      }

      if (cost() < costThreshold) {
        return null;
      }

      positiveScorer = optionalBulkScorer();
    } else if (numMustClauses == 0 && numOptionalClauses > 1 && minShouldMatch >= 1) {
      positiveScorer = filteredOptionalBulkScorer();
    } else if (numRequiredClauses > 0 && numOptionalClauses == 0 && minShouldMatch == 0) {
      positiveScorer = requiredBulkScorer();
    } else {
      // TODO: there are some cases where BooleanScorer
      // would handle conjunctions faster than
      // BooleanScorer2...
      return null;
    }

    if (positiveScorer == null) {
      return null;
    }
    final long positiveScorerCost = positiveScorer.cost();

    List<Scorer> prohibited = new ArrayList<>();
    for (ScorerSupplier ss : subs.get(Occur.MUST_NOT)) {
      prohibited.add(ss.get(positiveScorerCost));
    }

    if (prohibited.isEmpty()) {
      return positiveScorer;
    } else {
      Scorer prohibitedScorer =
          prohibited.size() == 1
              ? prohibited.get(0)
              : new DisjunctionSumScorer(
                  prohibited, ScoreMode.COMPLETE_NO_SCORES, positiveScorerCost);
      return new ReqExclBulkScorer(positiveScorer, prohibitedScorer);
    }
  }

  static BulkScorer disableScoring(final BulkScorer scorer) {
    Objects.requireNonNull(scorer);
    return new BulkScorer() {

      @Override
      public int score(final LeafCollector collector, Bits acceptDocs, int min, int max)
          throws IOException {
        final LeafCollector noScoreCollector =
            new LeafCollector() {
              Score fake = new Score();

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                collector.setScorer(fake);
              }

              @Override
              public void collect(int doc) throws IOException {
                collector.collect(doc);
              }
            };
        return scorer.score(noScoreCollector, acceptDocs, min, max);
      }

      @Override
      public long cost() {
        return scorer.cost();
      }
    };
  }

  // Return a BulkScorer for the optional clauses only,
  // or null if it is not applicable
  // pkg-private for forcing use of BooleanScorer in tests
  BulkScorer optionalBulkScorer() throws IOException {
    if (subs.get(Occur.SHOULD).size() == 0) {
      return null;
    } else if (subs.get(Occur.SHOULD).size() == 1 && minShouldMatch <= 1) {
      return subs.get(Occur.SHOULD).iterator().next().bulkScorer();
    }

    if (scoreMode == ScoreMode.TOP_SCORES && minShouldMatch <= 1) {
      List<Scorer> optionalScorers = new ArrayList<>();
      for (ScorerSupplier ss : subs.get(Occur.SHOULD)) {
        optionalScorers.add(ss.get(Long.MAX_VALUE));
      }

      return new MaxScoreBulkScorer(maxDoc, optionalScorers, null);
    }

    List<Scorer> optional = new ArrayList<Scorer>();
    for (ScorerSupplier ss : subs.get(Occur.SHOULD)) {
      optional.add(ss.get(Long.MAX_VALUE));
    }

    return new BooleanScorer(optional, Math.max(1, minShouldMatch), scoreMode.needsScores());
  }

  BulkScorer filteredOptionalBulkScorer() throws IOException {
    if (subs.get(Occur.MUST).isEmpty() == false
        || subs.get(Occur.FILTER).isEmpty()
        || (scoreMode.needsScores() && scoreMode != ScoreMode.TOP_SCORES)
        || subs.get(Occur.SHOULD).size() <= 1
        || minShouldMatch != 1) {
      return null;
    }
    long cost = cost();
    List<Scorer> optionalScorers = new ArrayList<>();
    for (ScorerSupplier ss : subs.get(Occur.SHOULD)) {
      optionalScorers.add(ss.get(cost));
    }
    List<Scorer> filters = new ArrayList<>();
    for (ScorerSupplier ss : subs.get(Occur.FILTER)) {
      filters.add(ss.get(cost));
    }
    if (scoreMode == ScoreMode.TOP_SCORES) {
      Scorer filterScorer;
      if (filters.size() == 1) {
        filterScorer = filters.iterator().next();
      } else {
        filterScorer = new ConjunctionScorer(filters, Collections.emptySet());
      }
      return new MaxScoreBulkScorer(maxDoc, optionalScorers, filterScorer);
    } else {
      // In the beginning of this method, we exited early if the score mode is not either TOP_SCORES
      // or a score mode that doesn't need scores.
      assert scoreMode.needsScores() == false;
      filters.add(new DisjunctionSumScorer(optionalScorers, scoreMode, cost));

      if (filters.stream().map(Scorer::twoPhaseIterator).allMatch(Objects::isNull)
          && maxDoc >= DenseConjunctionBulkScorer.WINDOW_SIZE
          && cost >= maxDoc / DenseConjunctionBulkScorer.DENSITY_THRESHOLD_INVERSE) {
        return new DenseConjunctionBulkScorer(
            filters.stream().map(Scorer::iterator).toList(), maxDoc, 0f);
      }

      return new DefaultBulkScorer(new ConjunctionScorer(filters, Collections.emptyList()));
    }
  }

  // Return a BulkScorer for the required clauses only
  private BulkScorer requiredBulkScorer() throws IOException {
    if (subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() == 0) {
      // No required clauses at all.
      return null;
    } else if (subs.get(Occur.MUST).size() + subs.get(Occur.FILTER).size() == 1) {
      BulkScorer scorer;
      if (subs.get(Occur.MUST).isEmpty() == false) {
        scorer = subs.get(Occur.MUST).iterator().next().bulkScorer();
      } else {
        scorer = subs.get(Occur.FILTER).iterator().next().bulkScorer();
        if (scoreMode.needsScores()) {
          scorer = disableScoring(scorer);
        }
      }
      return scorer;
    }

    long leadCost =
        subs.get(Occur.MUST).stream().mapToLong(ScorerSupplier::cost).min().orElse(Long.MAX_VALUE);
    leadCost =
        subs.get(Occur.FILTER).stream().mapToLong(ScorerSupplier::cost).min().orElse(leadCost);

    List<Scorer> requiredNoScoring = new ArrayList<>();
    for (ScorerSupplier ss : subs.get(Occur.FILTER)) {
      requiredNoScoring.add(ss.get(leadCost));
    }
    List<Scorer> requiredScoring = new ArrayList<>();
    Collection<ScorerSupplier> requiredScoringSupplier = subs.get(Occur.MUST);
    for (ScorerSupplier ss : requiredScoringSupplier) {
      if (requiredScoringSupplier.size() == 1) {
        ss.setTopLevelScoringClause();
      }
      requiredScoring.add(ss.get(leadCost));
    }
    if (scoreMode == ScoreMode.TOP_SCORES
        && requiredScoring.size() > 1
        // Only specialize top-level conjunctions for clauses that don't have a two-phase iterator.
        && requiredNoScoring.stream().map(Scorer::twoPhaseIterator).allMatch(Objects::isNull)
        && requiredScoring.stream().map(Scorer::twoPhaseIterator).allMatch(Objects::isNull)) {
      // Turn all filters into scoring clauses with a score of zero, so that
      // BlockMaxConjunctionBulkScorer is applicable.
      for (Scorer filter : requiredNoScoring) {
        requiredScoring.add(new ConstantScoreScorer(0f, ScoreMode.COMPLETE, filter.iterator()));
      }
      return new BlockMaxConjunctionBulkScorer(maxDoc, requiredScoring);
    }
    if (scoreMode != ScoreMode.TOP_SCORES
        && requiredScoring.size() + requiredNoScoring.size() >= 2
        && requiredScoring.stream().map(Scorer::twoPhaseIterator).allMatch(Objects::isNull)
        && requiredNoScoring.stream().map(Scorer::twoPhaseIterator).allMatch(Objects::isNull)) {
      if (requiredScoring.isEmpty()
          && maxDoc >= DenseConjunctionBulkScorer.WINDOW_SIZE
          && leadCost >= maxDoc / DenseConjunctionBulkScorer.DENSITY_THRESHOLD_INVERSE) {
        return new DenseConjunctionBulkScorer(
            requiredNoScoring.stream().map(Scorer::iterator).toList(), maxDoc, 0f);
      } else {
        return new ConjunctionBulkScorer(requiredScoring, requiredNoScoring);
      }
    }
    if (scoreMode == ScoreMode.TOP_SCORES && requiredScoring.size() > 1) {
      requiredScoring = Collections.singletonList(new BlockMaxConjunctionScorer(requiredScoring));
    }
    Scorer conjunctionScorer;
    if (requiredNoScoring.size() + requiredScoring.size() == 1) {
      if (requiredScoring.size() == 1) {
        conjunctionScorer = requiredScoring.get(0);
      } else {
        conjunctionScorer = requiredNoScoring.get(0);
        if (scoreMode.needsScores()) {
          Scorer inner = conjunctionScorer;
          conjunctionScorer =
              new FilterScorer(inner) {
                @Override
                public float score() throws IOException {
                  return 0f;
                }

                @Override
                public float getMaxScore(int upTo) throws IOException {
                  return 0f;
                }
              };
        }
      }
    } else {
      List<Scorer> required = new ArrayList<>();
      required.addAll(requiredScoring);
      required.addAll(requiredNoScoring);
      conjunctionScorer = new ConjunctionScorer(required, requiredScoring);
    }
    return new DefaultBulkScorer(conjunctionScorer);
  }

  /**
   * Create a new scorer for the given required clauses. Note that {@code requiredScoring} is a
   * subset of {@code required} containing required clauses that should participate in scoring.
   */
  private Scorer req(
      Collection<ScorerSupplier> requiredNoScoring,
      Collection<ScorerSupplier> requiredScoring,
      long leadCost,
      boolean topLevelScoringClause)
      throws IOException {
    if (requiredNoScoring.size() + requiredScoring.size() == 1) {
      Scorer req =
          (requiredNoScoring.isEmpty() ? requiredScoring : requiredNoScoring)
              .iterator()
              .next()
              .get(leadCost);

      if (scoreMode.needsScores() == false) {
        return req;
      }

      if (requiredScoring.isEmpty()) {
        // Scores are needed but we only have a filter clause
        // BooleanWeight expects that calling score() is ok so we need to wrap
        // to prevent score() from being propagated
        return new FilterScorer(req) {
          @Override
          public float score() throws IOException {
            return 0f;
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            return 0f;
          }
        };
      }

      return req;
    } else {
      List<Scorer> requiredScorers = new ArrayList<>();
      List<Scorer> scoringScorers = new ArrayList<>();
      for (ScorerSupplier s : requiredNoScoring) {
        requiredScorers.add(s.get(leadCost));
      }
      for (ScorerSupplier s : requiredScoring) {
        Scorer scorer = s.get(leadCost);
        scoringScorers.add(scorer);
      }
      if (scoreMode == ScoreMode.TOP_SCORES && scoringScorers.size() > 1 && topLevelScoringClause) {
        Scorer blockMaxScorer = new BlockMaxConjunctionScorer(scoringScorers);
        if (requiredScorers.isEmpty()) {
          return blockMaxScorer;
        }
        scoringScorers = Collections.singletonList(blockMaxScorer);
      }
      requiredScorers.addAll(scoringScorers);
      return new ConjunctionScorer(requiredScorers, scoringScorers);
    }
  }

  private Scorer excl(Scorer main, Collection<ScorerSupplier> prohibited, long leadCost)
      throws IOException {
    if (prohibited.isEmpty()) {
      return main;
    } else {
      return new ReqExclScorer(
          main, opt(prohibited, 1, ScoreMode.COMPLETE_NO_SCORES, leadCost, false));
    }
  }

  private Scorer opt(
      Collection<ScorerSupplier> optional,
      int minShouldMatch,
      ScoreMode scoreMode,
      long leadCost,
      boolean topLevelScoringClause)
      throws IOException {
    if (optional.size() == 1) {
      return optional.iterator().next().get(leadCost);
    } else {
      final List<Scorer> optionalScorers = new ArrayList<>();
      for (ScorerSupplier scorer : optional) {
        optionalScorers.add(scorer.get(leadCost));
      }

      // Technically speaking, WANDScorer should be able to handle the following 3 conditions now
      // 1. Any ScoreMode (with scoring or not)
      // 2. Any minCompetitiveScore ( >= 0 )
      // 3. Any minShouldMatch ( >= 0 )
      //
      // However, as WANDScorer uses more complex algorithm and data structure, we would like to
      // still use DisjunctionSumScorer to handle exhaustive pure disjunctions, which may be faster
      if ((scoreMode == ScoreMode.TOP_SCORES && topLevelScoringClause) || minShouldMatch > 1) {
        return new WANDScorer(optionalScorers, minShouldMatch, scoreMode, leadCost);
      } else {
        return new DisjunctionSumScorer(optionalScorers, scoreMode, leadCost);
      }
    }
  }
}
