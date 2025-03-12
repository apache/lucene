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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * A query that matches terms in a case-insensitive manner using regular expressions.
 *
 * <p>This query creates a {@link RegexpQuery} for each term with case-insensitive matching enabled.
 * For multiple terms, it builds a boolean query combining the individual regex queries.
 */
public class CaseInsensitiveTermInSetQuery extends Query {

  private final String field;
  private final Set<BytesRef> terms;

  /**
   * Creates a new {@link CaseInsensitiveTermInSetQuery} from the given collection of terms.
   *
   * @param field the field to match
   * @param terms the terms to match (case-insensitively)
   */
  public CaseInsensitiveTermInSetQuery(String field, Collection<BytesRef> terms) {
    this.field = Objects.requireNonNull(field, "field must not be null");
    // We create a copy to avoid modifying the provided collection
    this.terms = new HashSet<>(terms);
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":caseInsensitive(");

    boolean first = true;
    for (BytesRef term : terms) {
      if (!first) {
        builder.append(' ');
      }
      first = false;
      builder.append(Term.toString(term));
    }
    builder.append(')');

    return builder.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    if (terms.size() == 1) {
      visitor.consumeTerms(this, new Term(field, terms.iterator().next()));
    } else if (terms.size() > 1) {
      visitor.consumeTermsMatching(this, field, this::asByteRunAutomaton);
    }
  }
  
  private ByteRunAutomaton asByteRunAutomaton() {
    // Create regex for case-insensitive matching: term1|term2|term3...
    StringBuilder pattern = new StringBuilder();
    boolean first = true;
    
    for (BytesRef term : terms) {
      if (!first) {
        pattern.append('|');
      }
      first = false;
      pattern.append(term.utf8ToString());
    }
    
    // Create regex automaton with case-insensitive flag
    RegExp regexp = new RegExp(pattern.toString(), RegExp.NONE, RegExp.CASE_INSENSITIVE);
    Automaton automaton = regexp.toAutomaton();
    
    // Ensure the automaton is deterministic
    if (!automaton.isDeterministic()) {
      automaton = Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }
    
    return new ByteRunAutomaton(automaton);
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (terms.isEmpty()) {
      return new MatchNoDocsQuery("Empty terms set");
    }
    
    // For a single term, use a simple RegexpQuery
    if (terms.size() == 1) {
      return new RegexpQuery(
          new Term(field, terms.iterator().next().utf8ToString()), 
          RegExp.NONE, 
          RegExp.CASE_INSENSITIVE);
    }
    
    // For multiple terms, create a boolean query of regexps
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (BytesRef term : terms) {
      RegexpQuery regexpQuery = new RegexpQuery(
          new Term(field, term.utf8ToString()),
          RegExp.NONE,
          RegExp.CASE_INSENSITIVE);
      
      builder.add(regexpQuery, BooleanClause.Occur.SHOULD);
    }
    
    return builder.build();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(CaseInsensitiveTermInSetQuery other) {
    return field.equals(other.field) && terms.equals(other.terms);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Objects.hash(field, terms);
  }
}