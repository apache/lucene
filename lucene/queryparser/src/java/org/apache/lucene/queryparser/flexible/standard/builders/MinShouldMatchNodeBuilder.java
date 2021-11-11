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

import java.util.List;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MinShouldMatchNode;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/** Builds a {@link BooleanQuery} from a {@link MinShouldMatchNode}. */
public class MinShouldMatchNodeBuilder implements QueryBuilder {
  @Override
  public Query build(QueryNode queryNode) {
    MinShouldMatchNode mmNode = (MinShouldMatchNode) queryNode;

    List<QueryNode> children = queryNode.getChildren();
    if (children.size() != 1) {
      throw new RuntimeException("Unexpected number of node children: " + children.size());
    }

    Query q = (Query) mmNode.groupQueryNode.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    BooleanQuery booleanQuery = (BooleanQuery) q;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.setMinimumNumberShouldMatch(mmNode.minShouldMatch);
    booleanQuery.clauses().forEach(builder::add);
    return builder.build();
  }
}
