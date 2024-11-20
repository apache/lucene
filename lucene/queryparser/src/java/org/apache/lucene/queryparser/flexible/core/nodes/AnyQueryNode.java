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

/** A {@link AnyQueryNode} represents an ANY operator performed on a list of nodes. */
public class AnyQueryNode extends AndQueryNode {
  private CharSequence field;
  private int minimumMatchingElements;

  /**
   * @param clauses - the query nodes to be or'ed
   */
  public AnyQueryNode(List<QueryNode> clauses, CharSequence field, int minimumMatchingElements) {
    super(clauses);
    this.field = field;
    this.minimumMatchingElements = minimumMatchingElements;

    if (clauses != null) {
      for (QueryNode clause : clauses) {
        if (clause instanceof FieldQueryNode) {
          ((FieldQueryNode) clause).toQueryStringIgnoreFields = true;
          ((FieldQueryNode) clause).setField(field);
        }
      }
    }
  }

  public int getMinimumMatchingElements() {
    return this.minimumMatchingElements;
  }

  /**
   * returns null if the field was not specified
   *
   * @return the field
   */
  public CharSequence getField() {
    return this.field;
  }

  /**
   * returns - null if the field was not specified
   *
   * @return the field as a String
   */
  public String getFieldAsString() {
    if (this.field == null) return null;
    else return this.field.toString();
  }

  /**
   * @param field - the field to set
   */
  public void setField(CharSequence field) {
    this.field = field;
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    AnyQueryNode clone = (AnyQueryNode) super.cloneTree();

    clone.field = this.field;
    clone.minimumMatchingElements = this.minimumMatchingElements;

    return clone;
  }

  @Override
  public String toString() {
    if (getChildren() == null || getChildren().isEmpty())
      return "<any field='"
          + this.field
          + "'  matchelements="
          + this.minimumMatchingElements
          + "/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<any field='")
        .append(this.field)
        .append("'  matchelements=")
        .append(this.minimumMatchingElements)
        .append('>');
    for (QueryNode clause : getChildren()) {
      sb.append("\n");
      sb.append(clause.toString());
    }
    sb.append("\n</any>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    String anySTR = "ANY " + this.minimumMatchingElements;

    StringBuilder sb = new StringBuilder();
    if (getChildren() == null || getChildren().isEmpty()) {
      // no children case
    } else {
      String filler = "";
      for (QueryNode clause : getChildren()) {
        sb.append(filler).append(clause.toQueryString(escapeSyntaxParser));
        filler = " ";
      }
    }

    if (isDefaultField(this.field)) {
      return "( " + sb + " ) " + anySTR;
    } else {
      return this.field + ":(( " + sb + " ) " + anySTR + ")";
    }
  }
}
