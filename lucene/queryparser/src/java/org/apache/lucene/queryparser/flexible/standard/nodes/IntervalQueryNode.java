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
package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.queryparser.flexible.standard.nodes.intervalfn.IntervalFunction;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.Query;

/** Node that represents an interval function. */
public class IntervalQueryNode extends QueryNodeImpl implements FieldableNode {
  private final IntervalFunction source;
  private String field;
  private Analyzer analyzer;

  public IntervalQueryNode(String field, IntervalFunction source) {
    this.field = field;
    this.source = Objects.requireNonNull(source);
  }

  public Query getQuery() {
    Objects.requireNonNull(field, "Field must not be null for interval queries.");
    Objects.requireNonNull(analyzer, "Analyzer must not be null for interval queries.");
    return new IntervalQuery(field, source.toIntervalSource(field, analyzer));
  }

  @Override
  public String toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return String.format(Locale.ROOT, "%s:%s", field, source);
  }

  @Override
  public String toString() {
    return toQueryString(new EscapeQuerySyntaxImpl());
  }

  @Override
  public CharSequence getField() {
    return field;
  }

  @Override
  public void setField(CharSequence fieldName) {
    this.field = Objects.requireNonNull(fieldName.toString());
  }

  @Override
  public IntervalQueryNode cloneTree() {
    return new IntervalQueryNode(field, source);
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer =
        Objects.requireNonNull(analyzer, "Analyzer must not be null for interval queries.");
  }
}
