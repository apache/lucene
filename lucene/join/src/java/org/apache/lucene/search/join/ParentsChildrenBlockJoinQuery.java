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

import static org.apache.lucene.search.join.ToChildBlockJoinQuery.INVALID_QUERY_MESSAGE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;

/**
 * A query that returns the matching child documents for matching parent documents indexed together
 * in the same block. The provided parentQuery determines the parent documents of the returned
 * children documents. The provided childQuery determines which matching children documents are
 * being returned. childLimitPerParent is the maximum number of child documents to match per parent
 * document.
 *
 * @lucene.experimental
 */
public class ParentsChildrenBlockJoinQuery extends Query {

  private final BitSetProducer parentFilter;
  private final Query parentQuery;
  private final Query childQuery;
  private final int childLimitPerParent;
  private final BinaryOperator<Float> scoreCombiner;

  /** The default maximum number of child documents to match per parent document. */
  public static final int DEFAULT_CHILD_LIMIT_PER_PARENT = Integer.MAX_VALUE;

  /**
   * Create a ParentsChildrenBlockJoinQuery.
   *
   * @param parentFilter Filter identifying the parent documents.
   * @param parentQuery Query that matches parent documents.
   * @param childQuery Query that matches child documents.
   * @param childLimitPerParent The maximum number of child documents to match per parent.
   */
  public ParentsChildrenBlockJoinQuery(
      BitSetProducer parentFilter, Query parentQuery, Query childQuery, int childLimitPerParent) {
    this(parentFilter, parentQuery, childQuery, childLimitPerParent, Float::sum);
  }

  /**
   * Create a ParentsChildrenBlockJoinQuery with a custom score combiner.
   *
   * @param parentFilter Filter identifying the parent documents.
   * @param parentQuery Query that matches parent documents.
   * @param childQuery Query that matches child documents.
   * @param childLimitPerParent The maximum number of child documents to match per parent.
   * @param scoreCombiner Function to combine parent and child scores.
   */
  public ParentsChildrenBlockJoinQuery(
      BitSetProducer parentFilter,
      Query parentQuery,
      Query childQuery,
      int childLimitPerParent,
      BinaryOperator<Float> scoreCombiner) {
    super();
    if (childLimitPerParent <= 0) {
      throw new IllegalArgumentException(
          "childLimitPerParent must be > 0, got " + childLimitPerParent);
    }
    this.parentFilter = parentFilter;
    this.parentQuery = parentQuery;
    this.childQuery = childQuery;
    this.childLimitPerParent = childLimitPerParent;
    this.scoreCombiner = scoreCombiner;
  }

  /**
   * Create a ParentsChildrenBlockJoinQuery with DEFAULT_CHILD_LIMIT_PER_PARENT.
   *
   * @param parentFilter Filter identifying the parent documents.
   * @param parentQuery Query that matches parent documents.
   * @param childQuery Query that matches child documents.
   */
  public ParentsChildrenBlockJoinQuery(
      BitSetProducer parentFilter, Query parentQuery, Query childQuery) {
    this(parentFilter, parentQuery, childQuery, DEFAULT_CHILD_LIMIT_PER_PARENT);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
      throws IOException {
    return new ParentsChildrenBlockJoinWeight(
        this,
        parentFilter,
        parentQuery.createWeight(searcher, scoreMode, boost),
        childQuery.createWeight(searcher, scoreMode, boost),
        childLimitPerParent,
        scoreMode.needsScores(),
        scoreCombiner);
  }

  /** Return the parent query. */
  public Query getParentQuery() {
    return parentQuery;
  }

  /** Return the child query. */
  public Query getChildQuery() {
    return childQuery;
  }

  static class ParentsChildrenBlockJoinWeight extends Weight {
    private final BitSetProducer parentFilter;
    private final Weight parentWeight;
    private final Weight childWeight;
    private final int childLimitPerParent;
    private final boolean doScores;
    private final BinaryOperator<Float> scoreCombiner;
    private final Set<LeafReaderContext> seenContexts = new HashSet<>();

    public ParentsChildrenBlockJoinWeight(
        Query query,
        BitSetProducer parentFilter,
        Weight parentWeight,
        Weight childWeight,
        int childLimitPerParent,
        boolean doScores,
        BinaryOperator<Float> scoreCombiner) {
      super(query);
      this.parentFilter = parentFilter;
      this.parentWeight = parentWeight;
      this.childWeight = childWeight;
      this.childLimitPerParent = childLimitPerParent;
      this.doScores = doScores;
      this.scoreCombiner = scoreCombiner;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      ParentsChildrenBlockJoinScorer scorer = (ParentsChildrenBlockJoinScorer) scorer(context);
      if (scorer != null && scorer.iterator().advance(doc) == doc) {
        int parentDoc = scorer.getParentDoc();
        int childDoc = scorer.docID();

        return Explanation.match(
            scorer.score(),
            String.format(
                Locale.ROOT,
                "Score based on parent document %d and child document %d ",
                parentDoc + context.docBase,
                childDoc + context.docBase),
            parentWeight.explain(context, parentDoc),
            childWeight.explain(context, childDoc));
      }
      return Explanation.noMatch("Not a match");
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (!seenContexts.add(context)) {
        throw new IllegalStateException(
            "ParentsChildrenBlockJoinQuery does not support intraSegment concurrency. Context "
                + context
                + " was already seen.");
      }

      final BitSet parentBits = parentFilter.getBitSet(context);
      if (parentBits == null) {
        return null;
      }
      final ScorerSupplier parentScorerSupplier = parentWeight.scorerSupplier(context);
      final ScorerSupplier childScorerSupplier = childWeight.scorerSupplier(context);

      if (parentScorerSupplier == null || childScorerSupplier == null) {
        return null;
      }

      return new ScorerSupplier() {
        private long cost = -1;

        @Override
        public Scorer get(long leadCost) throws IOException {
          final Scorer parentScorer = parentScorerSupplier.get(leadCost);
          final Scorer childScorer = childScorerSupplier.get(leadCost);
          return new ParentsChildrenBlockJoinScorer(
              parentBits, parentScorer, childScorer, childLimitPerParent, doScores, scoreCombiner);
        }

        @Override
        public long cost() {
          if (cost == -1) {
            // Calculate cost based on parent and child costs
            // The cost should reflect the number of documents that will be visited
            long parentCost = parentScorerSupplier.cost();
            long childCost = childScorerSupplier.cost();
            // The actual cost depends on how many children per parent we'll visit
            cost = Math.min(parentCost * childLimitPerParent, childCost);
          }
          return cost;
        }

        @Override
        public void setTopLevelScoringClause() {
          // Propagate to both parent and child scorers
          parentScorerSupplier.setTopLevelScoringClause();
          childScorerSupplier.setTopLevelScoringClause();
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return parentWeight.isCacheable(ctx) && childWeight.isCacheable(ctx);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      Matches parentMatch = parentWeight.matches(context, doc);
      Matches childMatch = childWeight.matches(context, doc);

      if (parentMatch == null && childMatch == null) {
        // Neither matches
        return null;
      }
      // Combine non-null matches
      List<Matches> subMatches = new ArrayList<>();
      if (parentMatch != null) subMatches.add(parentMatch);
      if (childMatch != null) subMatches.add(childMatch);
      return MatchesUtils.fromSubMatches(subMatches);
    }
  }

  static class ParentsChildrenBlockJoinScorer extends Scorer {
    private final BitSet parentBits;
    private final Scorer parentScorer;
    private final DocIdSetIterator parentIt;
    private final Scorer childScorer;
    private final DocIdSetIterator childIt;
    private final int childLimitPerParent;
    private final boolean doScores;
    private final BinaryOperator<Float> scoreCombiner;
    private float parentScore;
    private float childScore;

    private int parentDoc = 0;
    private int childDoc = -1;
    private int childDocCount = 0;

    public ParentsChildrenBlockJoinScorer(
        BitSet parentBits,
        Scorer parentScorer,
        Scorer childScorer,
        int childLimitPerParent,
        boolean doScores,
        BinaryOperator<Float> scoreCombiner) {
      this.parentBits = parentBits;
      this.parentScorer = parentScorer;
      this.parentIt = parentScorer.iterator();
      this.childScorer = childScorer;
      this.childIt = childScorer.iterator();
      this.childLimitPerParent = childLimitPerParent;
      this.doScores = doScores;
      this.scoreCombiner = scoreCombiner;
    }

    @Override
    public Collection<ChildScorable> getChildren() {
      return Arrays.asList(
          new ChildScorable(parentScorer, "BLOCK_JOIN"),
          new ChildScorable(childScorer, "BLOCK_JOIN"));
    }

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        @Override
        public int docID() {
          return childDoc;
        }

        @Override
        public int nextDoc() throws IOException {
          if (childDocCount < childLimitPerParent) {
            childDoc = childIt.nextDoc();
          }

          // Need to move to the next parent if we have exhausted the current parent or child is out
          // of
          // the current parent block
          if (childDocCount >= childLimitPerParent || childDoc >= parentDoc) {
            childDocCount = 0;
            parentDoc = parentIt.nextDoc();
            if (parentDoc == 0) {
              // first parent doc has no children
              parentDoc = parentIt.nextDoc();
            }
            validateParentDoc();
          }

          // Adjust the parentIt and childIt so that they are in the same block
          alignParentAndChildIterator();

          if (exhausted()) {
            childDoc = parentDoc = NO_MORE_DOCS;
            return childDoc;
          }

          if (doScores) {
            childScore = childScorer.score();
            parentScore = parentScorer.score();
          }

          childDocCount++;
          return childDoc;
        }

        @Override
        public int advance(int target) throws IOException {
          if (target <= childDoc) {
            return childDoc;
          }

          childDoc = childIt.advance(target);
          if (childDocCount >= childLimitPerParent || childDoc >= parentDoc) {
            // need to move to the next parent block
            childDocCount = 0;
            if (childDoc <= parentDoc) {
              parentDoc = parentIt.nextDoc();
            } else {
              parentDoc = parentIt.advance(childDoc);
            }
            validateParentDoc();

            // Adjust the parentIt and childIt so that they are in the same block
            alignParentAndChildIterator();
          }

          if (exhausted()) {
            childDoc = parentDoc = NO_MORE_DOCS;
            return childDoc;
          }

          if (doScores) {
            childScore = childScorer.score();
            parentScore = parentScorer.score();
          }

          childDocCount++;
          return childDoc;
        }

        private void alignParentAndChildIterator() throws IOException {
          while (!exhausted()) {
            int firstChild = parentBits.prevSetBit(parentDoc - 1) + 1;
            if (childDoc >= firstChild && childDoc < parentDoc) {
              // order is correct , childDoc is within a valid parent block
              break;
            } else if (childDoc < firstChild) {
              // childDoc is before the current parent block, advance the child iterator
              childDoc = childIt.advance(firstChild);
            } else {
              // childDoc is after the current parent block, advance the parent iterator
              // when childDoc equals to parentDoc we skip to the next parent as well
              if (childDoc == parentDoc) {
                parentDoc = parentIt.nextDoc();
              } else {
                parentDoc = parentIt.advance(childDoc);
              }
              validateParentDoc();
            }
          }
        }

        @Override
        public long cost() {
          if (childLimitPerParent == DEFAULT_CHILD_LIMIT_PER_PARENT) {
            // When there's no limit, we'll visit all child documents for each parent
            return childIt.cost();
          } else {
            // When there's a limit, we'll visit at most childLimitPerParent child documents
            // for each parent that matches the parent query
            return Math.min(childIt.cost(), parentIt.cost() * childLimitPerParent);
          }
        }

        private boolean exhausted() {
          return childIt.docID() == NO_MORE_DOCS || parentIt.docID() == NO_MORE_DOCS;
        }
      };
    }

    /** Detect mis-use, where provided parent query in fact sometimes returns child documents. */
    private void validateParentDoc() {
      if (parentDoc != DocIdSetIterator.NO_MORE_DOCS && !parentBits.get(parentDoc)) {
        throw new IllegalStateException(INVALID_QUERY_MESSAGE + parentDoc);
      }
    }

    @Override
    public int docID() {
      return childDoc;
    }

    @Override
    public float score() throws IOException {
      if (doScores) {
        return scoreCombiner.apply(parentScore, childScore);
      }
      return 1.0f;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    int getParentDoc() {
      return parentDoc;
    }
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    final Query parentRewrite = parentQuery.rewrite(indexSearcher);
    final Query childRewrite = childQuery.rewrite(indexSearcher);
    if (parentRewrite != parentQuery || childRewrite != childQuery) {
      return new ParentsChildrenBlockJoinQuery(
          parentFilter, parentRewrite, childRewrite, childLimitPerParent);
    } else {
      return super.rewrite(indexSearcher);
    }
  }

  @Override
  public String toString(String field) {
    return "ParentsChildrenBlockJoinQuery(parentQuery="
        + parentQuery
        + ", childQuery="
        + childQuery
        + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(ParentsChildrenBlockJoinQuery other) {
    return parentFilter.equals(other.parentFilter)
        && parentQuery.equals(other.parentQuery)
        && childQuery.equals(other.childQuery)
        && childLimitPerParent == other.childLimitPerParent
        && scoreCombiner.equals(other.scoreCombiner);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int hash = classHash();
    hash = prime * hash + parentFilter.hashCode();
    hash = prime * hash + parentQuery.hashCode();
    hash = prime * hash + childQuery.hashCode();
    hash = prime * hash + childLimitPerParent;
    hash = prime * hash + scoreCombiner.hashCode();
    return hash;
  }
}
