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
package org.apache.lucene.search.join;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.search.ScoreMode.COMPLETE;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * This query requires that you index children and parent docs as a single block, using the {@link
 * IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link IndexWriter#updateDocuments
 * IndexWriter.updateDocuments()} API. In each block, the child documents must appear first, ending
 * with the parent document. At search time you provide a Filter identifying the parents, however
 * this Filter must provide an {@link BitSet} per sub-reader.
 *
 * <p>Once the block index is built, use this query to wrap any sub-query matching only child docs
 * and join matches in that child document space up to the parent document space. You can then use
 * this Query as a clause with other queries in the parent document space.
 *
 * <p>See {@link ToChildBlockJoinQuery} if you need to join in the reverse order.
 *
 * <p>The child documents must be orthogonal to the parent documents: the wrapped child query must
 * never return a parent document.
 *
 * <p>See {@link org.apache.lucene.search.join} for an overview.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinQuery extends Query {

  private final BitSetProducer parentsFilter;
  private final Query childQuery;
  private final ScoreMode scoreMode;

  /**
   * Create a ToParentBlockJoinQuery.
   *
   * @param childQuery Query matching child documents.
   * @param parentsFilter Filter identifying the parent documents.
   * @param scoreMode How to aggregate multiple child scores into a single parent score.
   */
  public ToParentBlockJoinQuery(
      Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super();
    this.childQuery = childQuery;
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode weightScoreMode, float boost)
      throws IOException {
    ScoreMode childScoreMode = weightScoreMode.needsScores() ? scoreMode : ScoreMode.None;
    final Weight childWeight;
    if (childScoreMode == ScoreMode.None) {
      // we don't need to compute a score for the child query so we wrap
      // it under a constant score query that can early terminate if the
      // minimum score is greater than 0 and the total hits that match the
      // query is not requested.
      childWeight =
          searcher
              .rewrite(new ConstantScoreQuery(childQuery))
              .createWeight(searcher, weightScoreMode, 0f);
    } else {
      // if the score is needed and the score mode is not max, we force the collection mode to
      // COMPLETE because the child query cannot skip non-competitive documents.
      // weightScoreMode.needsScores() will always be true here, but keep the check to make the
      // logic clearer.
      childWeight =
          childQuery.createWeight(
              searcher,
              weightScoreMode.needsScores() && childScoreMode != ScoreMode.Max
                  ? COMPLETE
                  : weightScoreMode,
              boost);
    }
    return new BlockJoinWeight(this, childWeight, parentsFilter, childScoreMode);
  }

  /** Return our child query. */
  public Query getChildQuery() {
    return childQuery;
  }

  private static class BlockJoinWeight extends FilterWeight {
    private final BitSetProducer parentsFilter;
    private final ScoreMode scoreMode;

    public BlockJoinWeight(
        Query joinQuery, Weight childWeight, BitSetProducer parentsFilter, ScoreMode scoreMode) {
      super(joinQuery, childWeight);
      this.parentsFilter = parentsFilter;
      this.scoreMode = scoreMode;
    }

    // NOTE: acceptDocs applies (and is checked) only in the
    // parent document space
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      final ScorerSupplier childScorerSupplier = in.scorerSupplier(context);
      if (childScorerSupplier == null) {
        return null;
      }

      // NOTE: this does not take accept docs into account, the responsibility
      // to not match deleted docs is on the scorer
      final BitSet parents = parentsFilter.getBitSet(context);
      if (parents == null) {
        // No matches
        return null;
      }

      return new ScorerSupplier() {

        @Override
        public Scorer get(long leadCost) throws IOException {
          return new BlockJoinScorer(childScorerSupplier.get(leadCost), parents, scoreMode);
        }

        @Override
        public BulkScorer bulkScorer() throws IOException {
          if (scoreMode == ScoreMode.None) {
            // BlockJoinBulkScorer evaluates all child hits exhaustively, but when scoreMode is None
            // we only need to evaluate a single child doc per parent. In this case, use the default
            // bulk scorer instead, which uses BlockJoinScorer to iterate over child hits.
            // BlockJoinScorer is optimized to skip child hit evaluation when scoreMode is None.
            return super.bulkScorer();
          }
          return new BlockJoinBulkScorer(childScorerSupplier.bulkScorer(), parents, scoreMode);
        }

        @Override
        public long cost() {
          return childScorerSupplier.cost();
        }

        @Override
        public void setTopLevelScoringClause() throws IOException {
          if (scoreMode == ScoreMode.Max) {
            childScorerSupplier.setTopLevelScoringClause();
          }
        }
      };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      BlockJoinScorer scorer = (BlockJoinScorer) scorer(context);
      if (scorer != null && scorer.iterator().advance(doc) == doc) {
        return scorer.explain(context, in, scoreMode);
      }
      return Explanation.noMatch("Not a match");
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      // The default implementation would delegate to the joinQuery's Weight, which
      // matches on children.  We need to match on the parent instead
      Scorer scorer = scorer(context);
      if (scorer == null) {
        return null;
      }
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
  }

  private static class ParentApproximation extends DocIdSetIterator {

    private final DocIdSetIterator childApproximation;
    private final BitSet parentBits;
    private int doc = -1;

    ParentApproximation(DocIdSetIterator childApproximation, BitSet parentBits) {
      this.childApproximation = childApproximation;
      this.parentBits = parentBits;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      final int prevParent =
          target == 0 ? -1 : parentBits.prevSetBit(Math.min(target, parentBits.length()) - 1);

      int childDoc = childApproximation.docID();
      if (childDoc < prevParent) {
        childDoc = childApproximation.advance(prevParent);
      } else if (childDoc == -1) {
        childDoc = childApproximation.nextDoc();
      }

      if (childDoc == prevParent) {
        throw new IllegalStateException(
            "Child query must not match same docs with parent filter. "
                + "Combine them as must clauses (+) to find a problem doc. "
                + "docId="
                + childDoc
                + ", "
                + this.getClass());
      }

      if (childDoc >= parentBits.length()) {
        return doc = NO_MORE_DOCS;
      }
      return doc = parentBits.nextSetBit(childDoc);
    }

    @Override
    public long cost() {
      return childApproximation.cost();
    }
  }

  private static class ParentTwoPhase extends TwoPhaseIterator {

    private final ParentApproximation parentApproximation;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;

    ParentTwoPhase(ParentApproximation parentApproximation, TwoPhaseIterator childTwoPhase) {
      super(parentApproximation);
      this.parentApproximation = parentApproximation;
      this.childApproximation = childTwoPhase.approximation();
      this.childTwoPhase = childTwoPhase;
    }

    @Override
    public boolean matches() throws IOException {
      assert childApproximation.docID() < parentApproximation.docID();
      do {
        if (childTwoPhase.matches()) {
          return true;
        }
      } while (childApproximation.nextDoc() < parentApproximation.docID());
      return false;
    }

    @Override
    public float matchCost() {
      // TODO: how could we compute a match cost?
      return childTwoPhase.matchCost() + 10;
    }
  }

  private static class Score extends Scorable {
    private final ScoreMode scoreMode;
    private double score;
    private int freq;

    public Score(ScoreMode scoreMode) {
      this.scoreMode = scoreMode;
      this.score = 0;
      this.freq = 0;
    }

    public void reset(Scorable firstChildScorer) throws IOException {
      score = scoreMode == ScoreMode.None ? 0 : firstChildScorer.score();
      freq = 1;
    }

    public void addChildScore(Scorable childScorer) throws IOException {
      final float childScore = scoreMode == ScoreMode.None ? 0 : childScorer.score();
      freq++;
      switch (scoreMode) {
        case Total:
        case Avg:
          score += childScore;
          break;
        case Min:
          score = Math.min(score, childScore);
          break;
        case Max:
          score = Math.max(score, childScore);
          break;
        case None:
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    public float score() {
      assert freq > 0;
      double score = this.score;
      if (scoreMode == ScoreMode.Avg) {
        score /= freq;
      }
      return (float) score;
    }
  }

  static class BlockJoinScorer extends Scorer {
    private final Scorer childScorer;
    private final BitSet parentBits;
    private final ScoreMode scoreMode;
    private final DocIdSetIterator childApproximation;
    private final TwoPhaseIterator childTwoPhase;
    private final ParentApproximation parentApproximation;
    private final ParentTwoPhase parentTwoPhase;
    private final Score parentScore;

    public BlockJoinScorer(Scorer childScorer, BitSet parentBits, ScoreMode scoreMode) {
      // System.out.println("Q.init firstChildDoc=" + firstChildDoc);
      this.parentBits = parentBits;
      this.childScorer = childScorer;
      this.scoreMode = scoreMode;
      this.parentScore = new Score(scoreMode);
      childTwoPhase = childScorer.twoPhaseIterator();
      if (childTwoPhase == null) {
        childApproximation = childScorer.iterator();
        parentApproximation = new ParentApproximation(childApproximation, parentBits);
        parentTwoPhase = null;
      } else {
        childApproximation = childTwoPhase.approximation();
        parentApproximation = new ParentApproximation(childTwoPhase.approximation(), parentBits);
        parentTwoPhase = new ParentTwoPhase(parentApproximation, childTwoPhase);
      }
    }

    @Override
    public Collection<ChildScorable> getChildren() {
      return Collections.singleton(new ChildScorable(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public DocIdSetIterator iterator() {
      if (parentTwoPhase == null) {
        // the approximation is exact
        return parentApproximation;
      } else {
        return TwoPhaseIterator.asDocIdSetIterator(parentTwoPhase);
      }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return parentTwoPhase;
    }

    @Override
    public int docID() {
      return parentApproximation.docID();
    }

    @Override
    public float score() throws IOException {
      return scoreChildDocs();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      if (scoreMode == ScoreMode.None) {
        return childScorer.getMaxScore(upTo);
      }
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (scoreMode == ScoreMode.None || scoreMode == ScoreMode.Max) {
        childScorer.setMinCompetitiveScore(minScore);
      }
    }

    private float scoreChildDocs() throws IOException {
      if (childApproximation.docID() >= parentApproximation.docID()) {
        return parentScore.score();
      }

      float score = 0;
      if (scoreMode != ScoreMode.None) {
        parentScore.reset(childScorer);
        while (childApproximation.nextDoc() < parentApproximation.docID()) {
          if (childTwoPhase == null || childTwoPhase.matches()) {
            parentScore.addChildScore(childScorer);
          }
        }

        score = parentScore.score();
      }

      return score;
    }

    /*
     * This instance of Explanation requires three parameters, context, childWeight, and scoreMode.
     * The scoreMode parameter considers Avg, Total, Min, Max, and None.
     * */
    public Explanation explain(LeafReaderContext context, Weight childWeight, ScoreMode scoreMode)
        throws IOException {
      int prevParentDoc = parentBits.prevSetBit(parentApproximation.docID() - 1);
      int start =
          context.docBase + prevParentDoc + 1; // +1 b/c prevParentDoc is previous parent doc
      int end = context.docBase + parentApproximation.docID() - 1; // -1 b/c parentDoc is parent doc

      Explanation bestChild = null;
      Explanation worstChild = null;

      int matches = 0;
      for (int childDoc = start; childDoc <= end; childDoc++) {
        Explanation child = childWeight.explain(context, childDoc - context.docBase);
        if (child.isMatch()) {
          matches++;
          if (bestChild == null
              || child.getValue().doubleValue() > bestChild.getValue().doubleValue()) {
            bestChild = child;
          }
          if (worstChild == null
              || child.getValue().doubleValue() < worstChild.getValue().doubleValue()) {
            worstChild = child;
          }
        }
      }
      assert matches > 0 : "No matches should be handled before.";
      Explanation subExplain = scoreMode == ScoreMode.Min ? worstChild : bestChild;
      return Explanation.match(
          this.score(),
          formatScoreExplanation(matches, start, end, scoreMode),
          subExplain == null ? Collections.emptyList() : Collections.singleton(subExplain));
    }

    private String formatScoreExplanation(int matches, int start, int end, ScoreMode scoreMode) {
      return String.format(
          Locale.ROOT,
          "Score based on %d child docs in range from %d to %d, using score mode %s",
          matches,
          start,
          end,
          scoreMode);
    }
  }

  private abstract static class BatchAwareLeafCollector extends FilterLeafCollector {
    public BatchAwareLeafCollector(LeafCollector in) {
      super(in);
    }

    public void endBatch() throws IOException {}
  }

  private static class BlockJoinBulkScorer extends BulkScorer {
    private final BulkScorer childBulkScorer;
    private final ScoreMode scoreMode;
    private final BitSet parents;
    private final int parentsLength;

    public BlockJoinBulkScorer(BulkScorer childBulkScorer, BitSet parents, ScoreMode scoreMode) {
      this.childBulkScorer = childBulkScorer;
      this.scoreMode = scoreMode;
      this.parents = parents;
      this.parentsLength = parents.length();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      if (min == max) {
        return scoringCompleteCheck(max, max);
      }

      // Subtract one because max is exclusive w.r.t. score but inclusive w.r.t prevSetBit
      int lastParent = parents.prevSetBit(Math.min(parentsLength, max) - 1);
      int prevParent = min == 0 ? -1 : parents.prevSetBit(min - 1);
      if (lastParent == prevParent) {
        // No parent docs in this range.
        return scoringCompleteCheck(max, max);
      }

      BatchAwareLeafCollector wrappedCollector = wrapCollector(collector);
      childBulkScorer.score(wrappedCollector, acceptDocs, prevParent + 1, lastParent + 1);
      wrappedCollector.endBatch();

      return scoringCompleteCheck(lastParent + 1, max);
    }

    private int scoringCompleteCheck(int innerMax, int returnedMax) {
      // If we've scored the last parent in the bit set, return NO_MORE_DOCS to indicate we are done
      // scoring
      return innerMax >= parentsLength ? NO_MORE_DOCS : returnedMax;
    }

    @Override
    public long cost() {
      return childBulkScorer.cost();
    }

    private BatchAwareLeafCollector wrapCollector(LeafCollector collector) {
      return new BatchAwareLeafCollector(collector) {
        private final Score currentParentScore = new Score(scoreMode);
        private int currentParent = -1;
        private Scorable scorer = null;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          assert scorer != null;
          this.scorer = scorer;

          super.setScorer(
              new Scorable() {
                @Override
                public float score() {
                  return currentParentScore.score();
                }

                @Override
                public void setMinCompetitiveScore(float minScore) throws IOException {
                  if (scoreMode == ScoreMode.None || scoreMode == ScoreMode.Max) {
                    scorer.setMinCompetitiveScore(minScore);
                  }
                }
              });
        }

        @Override
        public void collect(int doc) throws IOException {
          if (doc > currentParent) {
            // Emit the current parent and setup scoring for the next parent
            if (currentParent >= 0) {
              in.collect(currentParent);
            }

            currentParent = parents.nextSetBit(doc);
            currentParentScore.reset(scorer);
          } else if (doc == currentParent) {
            throw new IllegalStateException(
                "Child query must not match same docs with parent filter. "
                    + "Combine them as must clauses (+) to find a problem doc. "
                    + "docId="
                    + doc
                    + ", "
                    + childBulkScorer.getClass());
          } else {
            currentParentScore.addChildScore(scorer);
          }
        }

        @Override
        public void endBatch() throws IOException {
          if (currentParent >= 0) {
            in.collect(currentParent);
          }
        }
      };
    }
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    final Query childRewrite = childQuery.rewrite(indexSearcher);
    if (childRewrite != childQuery) {
      return new ToParentBlockJoinQuery(childRewrite, parentsFilter, scoreMode);
    } else {
      return super.rewrite(indexSearcher);
    }
  }

  @Override
  public String toString(String field) {
    return "ToParentBlockJoinQuery (" + childQuery.toString() + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(ToParentBlockJoinQuery other) {
    return childQuery.equals(other.childQuery)
        && parentsFilter.equals(other.parentsFilter)
        && scoreMode == other.scoreMode;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + childQuery.hashCode();
    hash = prime * hash + scoreMode.hashCode();
    hash = prime * hash + parentsFilter.hashCode();
    return hash;
  }
}
