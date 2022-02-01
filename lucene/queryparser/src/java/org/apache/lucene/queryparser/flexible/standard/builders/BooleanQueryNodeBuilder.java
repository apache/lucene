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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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

/**
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object. Every child in the
 * {@link BooleanQueryNode} object must be already tagged using {@link
 * QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object.
 *
 * <p>It takes in consideration if the children is a {@link ModifierQueryNode} to define the {@link
 * BooleanClause}.
 *
 * <p>If the set of children exceeds {@link #MIN_CLAUSES_TO_OPTIMIZE} then an attempt is made to
 * convert individual {@link TermQuery} sub-clauses into a single {@link TermInSetQuery}. This
 * allows a larger number of sub-clauses (than {@link IndexSearcher#getMaxClauseCount()}) and
 * typically runs much faster.
 */
public class BooleanQueryNodeBuilder implements StandardQueryBuilder {
  /**
   * Minimum number of Boolean sub-clauses required for the {@link TermInSetQuery} optimisation to
   * be attempted. This is computed dynamically and is a minimum of 128 or 25% of the current {@link
   * IndexSearcher#getMaxClauseCount()}.
   */
  public final int MIN_CLAUSES_TO_OPTIMIZE = Math.min(IndexSearcher.getMaxClauseCount() / 4, 128);

  @Override
  public BooleanQuery build(QueryNode queryNode) throws QueryNodeException {
    BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    List<QueryNode> children = booleanNode.getChildren();
    if (children == null || children.isEmpty()) {
      return builder.build();
    }

    List<BooleanClause> boolClauses = new ArrayList<>();
    for (QueryNode child : children) {
      Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
      if (obj != null) {
        Query clause = (Query) obj;
        BooleanClause.Occur occur = getModifierValue(child);
        boolClauses.add(new BooleanClause(clause, occur));
      }
    }

    try {
      if (boolClauses.size() < MIN_CLAUSES_TO_OPTIMIZE) {
        boolClauses.forEach(builder::add);
      } else {
        List<BooleanClause> termQueries = new ArrayList<>();
        for (BooleanClause clause : boolClauses) {
          if (clause.getQuery() instanceof TermQuery) {
            termQueries.add(clause);
          } else {
            builder.add(clause);
          }
        }

        termQueries.stream()
            .collect(
                Collectors.groupingBy(clause -> ((TermQuery) clause.getQuery()).getTerm().field()))
            .forEach(
                (field, fieldClauses) -> {
                  fieldClauses.stream()
                      .collect(Collectors.groupingBy(BooleanClause::getOccur))
                      .forEach(
                          (occur, occurClauses) -> {
                            List<BytesRef> terms =
                                occurClauses.stream()
                                    .map(
                                        clause -> ((TermQuery) clause.getQuery()).getTerm().bytes())
                                    .collect(Collectors.toList());
                            Query termInSet = new TermInSetQuery(field, terms);
                            builder.add(termInSet, occur);
                          });
                });
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

  private static BooleanClause.Occur getModifierValue(QueryNode node) {
    if (node instanceof ModifierQueryNode) {
      ModifierQueryNode mNode = ((ModifierQueryNode) node);
      switch (mNode.getModifier()) {
        case MOD_REQ:
          return BooleanClause.Occur.MUST;

        case MOD_NOT:
          return BooleanClause.Occur.MUST_NOT;

        case MOD_NONE:
          return BooleanClause.Occur.SHOULD;

        default:
          throw new RuntimeException("Unreachable.");
      }
    }

    return BooleanClause.Occur.SHOULD;
  }
}
