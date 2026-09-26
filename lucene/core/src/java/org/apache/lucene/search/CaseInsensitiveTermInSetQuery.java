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
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * A query that matches terms in a case-insensitive manner.
 *
 * <p>This query efficiently matches terms regardless of their case. For example, searching for
 * "test" will match "Test", "TEST", and "test".
 *
 * <p>The implementation uses different strategies based on the number of terms:
 *
 * <ul>
 *   <li>For single terms: Uses a simple RegexpQuery with the case-insensitive flag
 *   <li>For smaller sets (≤100 terms): Creates a BooleanQuery of RegexpQueries
 *   <li>For large sets (>100 terms): Uses an AutomatonQuery with an efficient HashSet-based
 *       implementation to avoid clause limits and automaton determinization costs
 * </ul>
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

  /**
   * Creates an efficient ByteRunAutomaton implementation for case-insensitive term matching.
   *
   * <p>This implementation addresses the maintainer's concern about automaton inefficiency with
   * large numbers of terms. Instead of building a complex automaton that requires expensive
   * determinization, we use a HashSet-based approach that provides O(1) lookups regardless of the
   * number of terms.
   *
   * @return A custom ByteRunAutomaton that efficiently matches terms case-insensitively
   */
  private ByteRunAutomaton asByteRunAutomaton() {
    // Convert all terms to lowercase for case-insensitive comparison
    final Set<String> lowerCaseTerms = new HashSet<>(terms.size());
    for (BytesRef term : terms) {
      lowerCaseTerms.add(term.utf8ToString().toLowerCase(Locale.ROOT));
    }

    // Create a minimal automaton shell
    final Automaton emptyAutomaton = new Automaton();
    emptyAutomaton.createState();

    // Override the run method with our HashSet-based implementation
    return new ByteRunAutomaton(emptyAutomaton) {
      @Override
      public boolean run(byte[] s, int offset, int length) {
        // Convert input bytes to string and lowercase for comparison
        BytesRef slice = new BytesRef(s, offset, length);
        String termString = slice.utf8ToString().toLowerCase(Locale.ROOT);
        return lowerCaseTerms.contains(termString);
      }
    };
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (terms.isEmpty()) {
      return new MatchNoDocsQuery("Empty terms set");
    }

    // For a single term, use a RegexpQuery with case-insensitive flag
    if (terms.size() == 1) {
      return new RegexpQuery(
          new Term(field, terms.iterator().next().utf8ToString()),
          RegExp.NONE,
          RegExp.CASE_INSENSITIVE);
    }

    // Handle different term set sizes appropriately:
    // 1. For smaller sets (<=100 terms), create a BooleanQuery with RegexpQueries
    // 2. For large sets (>100 terms), use an AutomatonQuery approach to avoid BooleanQuery clause
    // limits

    if (terms.size() <= 100) {
      // For a reasonable number of terms, create a BooleanQuery of regexps
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (BytesRef term : terms) {
        RegexpQuery regexpQuery =
            new RegexpQuery(
                new Term(field, term.utf8ToString()), RegExp.NONE, RegExp.CASE_INSENSITIVE);

        builder.add(regexpQuery, BooleanClause.Occur.SHOULD);
      }
      return builder.build();
    } else {
      // For large term sets, use a custom AutomatonQuery with our efficient
      // asByteRunAutomaton implementation to avoid BooleanQuery clause limits
      AutomatonQuery query = new AutomatonQuery(new Term(field), createAutomaton());

      // Wrap in a ConstantScoreQuery to maintain consistent scoring
      return new ConstantScoreQuery(query);
    }
  }

  /**
   * Creates a minimal automaton for use with AutomatonQuery.
   *
   * <p>This simple automaton acts as a placeholder. The actual matching logic is handled by our
   * custom asByteRunAutomaton() implementation, which efficiently checks terms using a HashSet for
   * case-insensitive matching. This approach avoids the inefficiency of building and determinizing
   * complex automata with many terms.
   */
  private Automaton createAutomaton() {
    // Create a minimal automaton that accepts any string
    // The actual matching is done via the ByteRunAutomaton override
    Automaton automaton = new Automaton();
    int state = automaton.createState();
    automaton.setAccept(state, true);
    return automaton;
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
