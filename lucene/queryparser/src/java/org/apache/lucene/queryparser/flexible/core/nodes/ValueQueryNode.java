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

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/** This interface should be implemented by {@link QueryNode} that holds an arbitrary value. */
public interface ValueQueryNode<T extends Object> extends QueryNode {

  public void setValue(T value);

  public T getValue();

  /**
   * This method is used to get the value converted to {@link String} and escaped using the given
   * {@link EscapeQuerySyntax}. For example:
   *
   * <pre>
   * new FieldQueryNode("FIELD", "(literal parens)", 0, 0).getTermEscaped(escaper);
   * </pre>
   *
   * <p>returns
   *
   * <pre>
   * \(literal\ parens\)
   * </pre>
   *
   * @param escaper the {@link EscapeQuerySyntax} used to escape the value {@link String}
   * @return the value converted to {@link String} and escaped
   */
  public CharSequence getTermEscaped(EscapeQuerySyntax escaper);
}
