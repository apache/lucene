package org.apache.lucene.queryparser.flexible.standard.builders;

import java.util.List;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MinShouldMatchNode;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

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
