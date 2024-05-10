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
package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.List;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 * A {@link TokenizedPhraseQueryNode} represents a node created by a code that
 * tokenizes/lemmatizes/analyzes.
 */
public class TokenizedPhraseQueryNode extends QueryNodeImpl implements FieldableNode {

  public TokenizedPhraseQueryNode() {
    setLeaf(false);
    allocate();
  }

  @Override
  public String toString() {
    List<QueryNode> children = getChildren();
    if (children == null || children.isEmpty()) return "<tokenizedphrase/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<tokenizedphrase>");
    for (QueryNode child : children) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</tokenizedphrase>");
    return sb.toString();
  }

  // This text representation is not re-parseable
  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    List<QueryNode> children = getChildren();
    if (children == null || children.isEmpty()) return "";
    StringBuilder sb = new StringBuilder();
    String filler = "";
    for (QueryNode child : children) {
      sb.append(filler).append(child.toQueryString(escapeSyntaxParser));
      filler = ",";
    }
    return "[TP[" + sb + "]]";
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    TokenizedPhraseQueryNode clone = (TokenizedPhraseQueryNode) super.cloneTree();

    // nothing to do

    return clone;
  }

  @Override
  public CharSequence getField() {
    List<QueryNode> children = getChildren();
    if (children != null) {
      for (QueryNode child : children) {
        if (child instanceof FieldableNode) {
          return ((FieldableNode) child).getField();
        }
      }
    }
    return null;
  }

  @Override
  public void setField(CharSequence fieldName) {
    List<QueryNode> children = getChildren();
    if (children != null) {
      for (QueryNode child : children) {
        if (child instanceof FieldableNode) {
          ((FieldableNode) child).setField(fieldName);
        }
      }
    }
  }
}
