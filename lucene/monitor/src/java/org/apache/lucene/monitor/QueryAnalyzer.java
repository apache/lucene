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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;

/**
 * Class to analyze and extract terms from a lucene query, to be used by a {@link Presearcher} in
 * indexing.
 */
class QueryAnalyzer {

  private final List<CustomQueryHandler> handlers;
  private final boolean isNegationOptimized;

  QueryAnalyzer(List<CustomQueryHandler> queryTreeBuilders) {
    this(queryTreeBuilders, false);
  }

  QueryAnalyzer(List<CustomQueryHandler> queryTreeBuilders, boolean isNegationOptimized) {
    this.handlers = queryTreeBuilders;
    this.isNegationOptimized = isNegationOptimized;
  }

  QueryAnalyzer() {
    this(List.of(), false);
  }

  /** The first non-null tree the custom handlers build for {@code query}, or null */
  private QueryTree handleCustomQuery(Query query, TermWeightor weightor, boolean negated) {
    for (CustomQueryHandler handler : handlers) {
      QueryTree tree =
          negated
              ? handler.handleNegatedQuery(query, weightor)
              : handler.handleQuery(query, weightor);
      if (tree != null) {
        return tree;
      }
    }
    return null;
  }

  /**
   * Create a {@link QueryTree} from a passed in Query or Filter
   *
   * @param luceneQuery the query to analyze
   * @return a QueryTree describing the analyzed query
   */
  QueryTree buildTree(Query luceneQuery, TermWeightor weightor) {
    QueryBuilder builder = new QueryBuilder();
    luceneQuery.visit(builder);
    return builder.build(weightor);
  }

  /** A clause of a {@link QueryBuilder}: a leaf of the tree, or a nested builder */
  private sealed interface Clause permits Leaf, QueryBuilder {}

  /** A leaf, built once the weightor is known */
  private record Leaf(Function<TermWeightor, QueryTree> tree) implements Clause {}

  /** Builds the tree for a group of clauses; under negation its operator flips (De Morgan) */
  private final class QueryBuilder extends QueryVisitor implements Clause {

    /** Whether the query this builder visits is negated */
    final boolean isNegated;

    /**
     * Whether this builds a disjunction: clauses combined with OR, or, by De Morgan, with AND under
     * negation
     */
    final boolean buildsDisjunction;

    final List<Clause> clauses = new ArrayList<>();
    final Set<Term> negatedTerms = new LinkedHashSet<>();

    /** The root builder: positive */
    QueryBuilder() {
      this(false, false);
    }

    private QueryBuilder(boolean isNegated, boolean combinesWithOr) {
      this.isNegated = isNegated;
      this.buildsDisjunction = combinesWithOr ^ isNegated;
    }

    /** A child for clauses combined with AND, in this builder's polarity */
    private QueryBuilder conjunction() {
      return addChild(new QueryBuilder(isNegated, false));
    }

    /** A child for clauses combined with OR, in this builder's polarity */
    private QueryBuilder disjunction() {
      return addChild(new QueryBuilder(isNegated, true));
    }

    /** A child for negated clauses: NOT a AND NOT b = NOT (a OR b), so OR with flipped polarity */
    private QueryBuilder negation() {
      return addChild(new QueryBuilder(!isNegated, true));
    }

    private QueryBuilder addChild(QueryBuilder child) {
      clauses.add(child);
      return child;
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (isNegated && !isExactUnderNegation(parent, occur)) {
        clauses.add(new Leaf(_ -> QueryTree.anyTerm("NOT " + parent)));
        return QueryVisitor.EMPTY_VISITOR;
      }
      if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
        return conjunction();
      }
      if (occur == BooleanClause.Occur.MUST_NOT) {
        // Lucene matches nothing for a pure negative query such as (-b). Negated, it matches every
        // document, which its clauses can't express (NOT (-b) is not b), so select the query for
        // every document. Positive, it is treated as *:* -b, which selects more documents than it
        // matches. Without negative disjuncts, it always selects the query for every document.
        if (parent instanceof BooleanQuery bq
            && isPureNegative(bq)
            && (!isNegationOptimized || isNegated)) {
          clauses.add(new Leaf(_ -> QueryTree.anyTerm("PURE NEGATIVE QUERY[" + parent + "]")));
          return QueryVisitor.EMPTY_VISITOR;
        }
        if (!isNegationOptimized) {
          return QueryVisitor.EMPTY_VISITOR;
        }
        return negation();
      }
      // It's a disjunction clause.  If the parent has MUST or FILTER clauses, we can
      // ignore it
      if (parent instanceof BooleanQuery bq && hasRequiredClauses(bq)) {
        return QueryVisitor.EMPTY_VISITOR;
      }
      return disjunction();
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      if (isNegated) {
        if (query instanceof TermQuery
            || query instanceof SynonymQuery
            || query instanceof TermInSetQuery
            || query instanceof PhraseQuery) {
          negatedTerms.addAll(Arrays.asList(terms));
        } else {
          clauses.add(new Leaf(_ -> QueryTree.anyTerm("NOT " + query)));
        }
        return;
      }
      for (Term term : terms) {
        clauses.add(new Leaf(w -> QueryTree.term(term, w)));
      }
    }

    @Override
    public void visitLeaf(Query query) {
      // Under negation only handleNegatedQuery is used: a handler's positive tree may select more
      // documents than the query matches, so negating it would be unsound
      clauses.add(
          new Leaf(
              w -> {
                QueryTree q = handleCustomQuery(query, w, isNegated);
                if (q == null) {
                  return QueryTree.anyTerm(isNegated ? "NOT " + query : query.toString());
                }
                return q;
              }));
    }

    /** Whether this combines its clauses with AND; a single clause counts as either */
    private boolean isConjunctive() {
      return !buildsDisjunction || clauses.size() + negatedTerms.size() == 1;
    }

    /**
     * Moves the negated terms of the conjunctions nested in this one into {@code into}, and removes
     * the builders left empty: (a AND NOT x) AND NOT y = a AND NOT (x OR y), one negative disjunct
     */
    private void mergeNestedNegatedConjuncts(QueryBuilder into) {
      Iterator<Clause> it = clauses.iterator();
      while (it.hasNext()) {
        if (it.next() instanceof QueryBuilder child && child.isConjunctive()) {
          into.negatedTerms.addAll(child.negatedTerms);
          child.negatedTerms.clear();
          child.mergeNestedNegatedConjuncts(into);
          if (child.clauses.isEmpty()) {
            it.remove();
          }
        }
      }
    }

    /** Builds the tree of this builder's clauses */
    QueryTree build(TermWeightor termWeightor) {
      if (isNegationOptimized && !buildsDisjunction) {
        mergeNestedNegatedConjuncts(this);
      }
      List<Function<TermWeightor, QueryTree>> all = new ArrayList<>();
      for (Clause clause : clauses) {
        Function<TermWeightor, QueryTree> tree =
            switch (clause) {
              case Leaf leaf -> leaf.tree();
              case QueryBuilder child -> child::build;
            };
        all.add(tree);
      }
      if (!negatedTerms.isEmpty()) {
        if (buildsDisjunction) {
          // NOT a OR NOT b: each term is a negative disjunct of its own
          for (Term term : negatedTerms) {
            all.add(w -> QueryTree.negativeDisjunct(Set.of(term), w));
          }
        } else {
          // NOT a AND NOT b: one negative disjunct, a document must contain none of the terms
          all.add(w -> QueryTree.negativeDisjunct(negatedTerms, w));
        }
      }
      if (all.isEmpty() && isNegationOptimized) {
        return QueryTree.anyTerm("NO CLAUSES");
      }
      if (buildsDisjunction) {
        return QueryTree.disjunction(all, termWeightor);
      }
      return QueryTree.conjunction(all, termWeightor);
    }
  }

  /** Whether NOT {@code parent} can be expressed exactly in terms of its {@code occur} clauses */
  private static boolean isExactUnderNegation(Query parent, BooleanClause.Occur occur) {
    if (parent instanceof BooleanQuery bq) {
      return bq.getMinimumNumberShouldMatch() == 0;
    }
    if (parent instanceof BoostQuery || parent instanceof ConstantScoreQuery) {
      return true;
    }
    if (parent instanceof SynonymQuery) {
      return occur == BooleanClause.Occur.SHOULD;
    }
    if (parent instanceof PhraseQuery pq) {
      return pq.getTerms().length == 1; // a one-term phrase is a term query
    }
    return false;
  }

  private static boolean isPureNegative(BooleanQuery bq) {
    return bq.clauses().stream().allMatch(c -> c.occur() == BooleanClause.Occur.MUST_NOT);
  }

  private static boolean hasRequiredClauses(BooleanQuery bq) {
    return bq.clauses().stream()
        .anyMatch(
            c -> c.occur() == BooleanClause.Occur.MUST || c.occur() == BooleanClause.Occur.FILTER);
  }
}
