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
import java.util.Set;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * A {@link TermInSetQuery} that matches terms in a case-insensitive manner.
 *
 * <p>This query performs case-insensitive term matching by using a regexp-based approach to match
 * all case variations of the given terms. For multi-term queries with many terms, this can be more
 * efficient than creating a large boolean query with many disjunctions.
 */
public class CaseInsensitiveTermInSetQuery extends MultiTermQuery {

  private final String field;
  private final Set<BytesRef> terms;

  /**
   * Creates a new {@link CaseInsensitiveTermInSetQuery} from the given collection of terms.
   *
   * @param field the field to match
   * @param terms the terms to match (case-insensitively)
   */
  public CaseInsensitiveTermInSetQuery(String field, Collection<BytesRef> terms) {
    super(field, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);
    this.field = field;
    // We create a copy to avoid modifying the provided collection
    this.terms = new HashSet<>(terms);
  }

  /**
   * Creates a new {@link CaseInsensitiveTermInSetQuery} from the given collection of terms with the
   * specified rewrite method.
   *
   * @param rewriteMethod the rewrite method to use
   * @param field the field to match
   * @param terms the terms to match (case-insensitively)
   */
  public CaseInsensitiveTermInSetQuery(
      RewriteMethod rewriteMethod, String field, Collection<BytesRef> terms) {
    super(field, rewriteMethod);
    this.field = field;
    this.terms = new HashSet<>(terms);
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return new CaseInsensitiveSetEnum(terms.iterator());
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder();
    builder.append(field);
    builder.append(":caseInsensitive("); // More descriptive name

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
    // Create a regular expression based automaton for case-insensitive matching
    StringBuilder regexBuilder = new StringBuilder();
    boolean first = true;
    for (BytesRef term : terms) {
      if (!first) {
        regexBuilder.append('|');
      }
      first = false;
      // Escape special regex chars and build case-insensitive pattern
      regexBuilder.append(regexEscape(term.utf8ToString()));
    }
    
    // Create a regular expression with case-insensitive flag
    RegExp regexp = new RegExp(regexBuilder.toString(), RegExp.ALL, 
                              RegExp.CASE_INSENSITIVE);
    Automaton automaton = regexp.toAutomaton();
    
    // Ensure the automaton is deterministic
    if (!automaton.isDeterministic()) {
      automaton = Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }
    
    return new ByteRunAutomaton(automaton, true);
  }

  private String regexEscape(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // Escape all special regex characters
      if ("[](){}.*+?^$\\|".indexOf(c) != -1) {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * A specialized TermsEnum implementation that performs case-insensitive matching against the
   * terms dictionary using a ByteRunAutomaton for proper Unicode case handling.
   */
  private class CaseInsensitiveSetEnum extends FilteredTermsEnum {
    private final ByteRunAutomaton automaton;

    CaseInsensitiveSetEnum(TermsEnum termsEnum) {
      super(termsEnum);
      this.automaton = asByteRunAutomaton();
      setInitialSeekTerm(new BytesRef(""));
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
      // Use the automaton for proper Unicode-aware case-insensitive matching
      if (automaton.run(term.bytes, term.offset, term.length)) {
        return AcceptStatus.YES;
      }
      return AcceptStatus.NO;
    }
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
    return 31 * classHash() + terms.hashCode();
  }
}