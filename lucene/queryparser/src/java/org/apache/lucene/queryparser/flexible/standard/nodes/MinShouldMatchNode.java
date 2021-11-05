package org.apache.lucene.queryparser.flexible.standard.nodes;

import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

public class MinShouldMatchNode extends QueryNodeImpl {
  public final int minShouldMatch;
  public final GroupQueryNode groupQueryNode;

  public MinShouldMatchNode(int minShouldMatch, GroupQueryNode groupQueryNode) {
    this.minShouldMatch = minShouldMatch;
    this.groupQueryNode = groupQueryNode;

    this.setLeaf(false);
    this.allocate();
    add(groupQueryNode);
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return groupQueryNode.toQueryString(escapeSyntaxParser) + "@" + minShouldMatch;
  }
}
