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
package org.apache.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.RegExp;

/**
 * A fast regular expression query based on the {@link org.apache.lucene.util.automaton} package.
 *
 * <ul>
 *   <li>Comparisons are <a href="http://tusker.org/regex/regex_benchmark.html">fast</a>
 *   <li>The term dictionary is enumerated in an intelligent way, to avoid comparisons. See {@link
 *       AutomatonQuery} for more details.
 * </ul>
 *
 * <p>The supported syntax is documented in the {@link RegExp} class. Note this might be different
 * than other regular expression implementations. For some alternatives with different syntax, look
 * under the sandbox.
 *
 * <p>Note this query can be slow, as it needs to iterate over many terms. In order to prevent
 * extremely slow RegexpQueries, a Regexp term should not start with the expression <code>.*</code>
 *
 * @see RegExp
 * @lucene.experimental
 */
public class RegexpQuery extends AutomatonQuery {

  /** A provider that provides no named automata */
  public static final AutomatonProvider DEFAULT_PROVIDER = _ -> null;

  /**
   * Constructs a query for terms matching <code>term</code>.
   *
   * <p>By default, all regular expression features are enabled.
   *
   * @param term regular expression.
   */
  public RegexpQuery(Term term) {
    this(term, RegExp.ALL);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   *
   * @param term regular expression.
   * @param flags optional RegExp features from {@link RegExp}
   */
  public RegexpQuery(Term term, int flags) {
    this(term, flags, 0);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   *
   * @param term regular expression.
   * @param syntaxFlags optional RegExp syntax features from {@link RegExp} automaton for the regexp
   *     can result in. Set higher to allow more complex queries and lower to prevent memory
   *     exhaustion.
   * @param matchFlags boolean 'or' of match behavior options such as case insensitivity
   */
  public RegexpQuery(Term term, int syntaxFlags, int matchFlags) {
    this(term, syntaxFlags, matchFlags, DEFAULT_PROVIDER, CONSTANT_SCORE_BLENDED_REWRITE);
  }

  /**
   * Constructs a query for terms matching <code>term</code>.
   *
   * @param term regular expression.
   * @param syntaxFlags optional RegExp features from {@link RegExp}
   * @param matchFlags boolean 'or' of match behavior options such as case insensitivity
   * @param provider custom AutomatonProvider for named automata
   * @param rewriteMethod the rewrite method to use to build the final query
   */
  public RegexpQuery(
      Term term,
      int syntaxFlags,
      int matchFlags,
      AutomatonProvider provider,
      RewriteMethod rewriteMethod) {
    super(
        term,
        new RegExp(term.text(), syntaxFlags, matchFlags).toAutomaton(provider),
        false,
        rewriteMethod);
  }

  /** Returns the regexp of this query wrapped in a Term. */
  public Term getRegexp() {
    return term;
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append('/');
    buffer.append(term.text());
    buffer.append('/');
    return buffer.toString();
  }
}
