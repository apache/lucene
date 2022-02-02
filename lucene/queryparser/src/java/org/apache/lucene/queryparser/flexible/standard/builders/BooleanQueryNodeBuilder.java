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
package org.apache.lucene.queryparser.flexible.standard.builders;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object. Every child in the
 * {@link BooleanQueryNode} object must be already tagged using {@link
 * QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object.
 *
 * <p>It takes in consideration if the child is a {@link ModifierQueryNode} to define the {@link
 * BooleanClause}.
 *
 * <p>This node builder can make an optional attempt to optimize sub-clauses of a Boolean node. This
 * may be useful to allow a larger number of sub-clauses than {@link
 * IndexSearcher#getMaxClauseCount()} by default permits and to make queries run faster. See {@link
 * #setMinClauseCountForDisjunctionOptimization(int)} for caveats of using such optimisations.
 */
public class BooleanQueryNodeBuilder implements StandardQueryBuilder {

  /** @see #setMinClauseCountForDisjunctionOptimization(int) */
  private int minClauseCountForDisjunctions = Integer.MAX_VALUE;

  @Override
  public BooleanQuery build(QueryNode queryNode) throws QueryNodeException {
    BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    List<QueryNode> children = booleanNode.getChildren();
    if (children == null || children.isEmpty()) {
      return builder.build();
    }

    ArrayList<BooleanClause> boolClauses = new ArrayList<>();
    for (QueryNode child : children) {
      Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (obj != null) {
        Query clause = (Query) obj;
        BooleanClause.Occur occur = getModifierValue(child);
        boolClauses.add(new BooleanClause(clause, occur));
      }
    }

    try {
      maybeOptimizeDisjunctions(boolClauses, builder);

      // All remaining clauses.
      boolClauses.forEach(builder::add);
      if (boolClauses.size() < minClauseCountForDisjunctions) {
        return builder.build();
      }

      return builder.build();
    } catch (IndexSearcher.TooManyClauses ex) {
      throw new QueryNodeException(
          new MessageImpl(
              QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES,
              IndexSearcher.getMaxClauseCount(),
              queryNode.toQueryString(new EscapeQuerySyntaxImpl())),
          ex);
    }
  }

  /**
   * Replaces multiple {@link TermQuery} disjunctions with a single {@link TermInSetQuery} clause.
   * This changes document scoring but is sometimes better than nothing (when the query would
   * otherwise break on {@link IndexSearcher#getMaxClauseCount()} or be prohibitively expensive to
   * execute.
   *
   * @see #setMinClauseCountForDisjunctionOptimization(int)
   */
  private void maybeOptimizeDisjunctions(
      ArrayList<BooleanClause> boolClauses, BooleanQuery.Builder builder) {
    Predicate<BooleanClause> candidate =
        clause ->
            clause.getQuery() instanceof TermQuery
                && clause.getOccur() == BooleanClause.Occur.SHOULD;

    // Fast check if we have a chance at all.
    if (boolClauses.size() < minClauseCountForDisjunctions
        || boolClauses.stream().filter(candidate).count() < minClauseCountForDisjunctions) {
      return;
    }

    // Group clauses by field and replace if they exceed the threshold.
    Set<BooleanClause> replaced = new HashSet<>();
    boolClauses.stream()
        .filter(candidate)
        .collect(Collectors.groupingBy(clause -> ((TermQuery) clause.getQuery()).getTerm().field()))
        .forEach(
            (field, fieldClauses) -> {
              if (fieldClauses.size() >= minClauseCountForDisjunctions) {
                List<BytesRef> terms =
                    fieldClauses.stream()
                        .map(clause -> ((TermQuery) clause.getQuery()).getTerm().bytes())
                        .collect(Collectors.toList());

                Query termInSet = new TermInSetQuery(field, terms);
                builder.add(termInSet, BooleanClause.Occur.SHOULD);
                replaced.addAll(fieldClauses);
              }
            });

    boolClauses.removeIf(replaced::contains);
  }

  private static BooleanClause.Occur getModifierValue(QueryNode node) {
    if (node instanceof ModifierQueryNode mNode) {
      return switch (mNode.getModifier()) {
        case MOD_REQ -> BooleanClause.Occur.MUST;
        case MOD_NOT -> BooleanClause.Occur.MUST_NOT;
        case MOD_NONE -> BooleanClause.Occur.SHOULD;
      };
    } else {
      return BooleanClause.Occur.SHOULD;
    }
  }

  /**
   * Sets the minimum number of boolean {@link BooleanClause.Occur#SHOULD} (disjunction) sub-clauses
   * before they are converted to a single {@link TermInSetQuery}. This allows parsing much larger
   * sets of clauses than ({@link IndexSearcher#getMaxClauseCount()}) would normally allow and it
   * can speed up queries significantly. However, <em>using this function will change document
   * scoring because {@link TermInSetQuery} is a constant-score query.</em>.
   *
   * @param minClauseCount Minimum subclause count for the optimization to kick in (inclusive).
   *     Typically, this is set to {@link IndexSearcher#getMaxClauseCount()}.
   */
  public void setMinClauseCountForDisjunctionOptimization(int minClauseCount) {
    this.minClauseCountForDisjunctions = minClauseCount;
  }
}
