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
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunnable;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Transition;

/**
 * A {@link Query} that will match terms against a finite-state machine.
 *
 * <p>This query will match documents that contain terms accepted by a given finite-state machine.
 * The automaton can be constructed with the {@link org.apache.lucene.util.automaton} API.
 * Alternatively, it can be created from a regular expression with {@link RegexpQuery} or from the
 * standard Lucene wildcard syntax with {@link WildcardQuery}.
 *
 * <p>When the query is executed, it will enumerate the term dictionary in an intelligent way to
 * reduce the number of comparisons. For example: the regular expression of <code>[dl]og?</code>
 * will make approximately four comparisons: do, dog, lo, and log.
 *
 * @lucene.experimental
 */
public class AutomatonQuery extends MultiTermQuery implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(AutomatonQuery.class);

  /** the automaton to match index terms against */
  protected final Automaton automaton;

  protected final CompiledAutomaton compiled;

  /** term containing the field, and possibly some pattern structure */
  protected final Term term;

  protected final boolean automatonIsBinary;

  private final long ramBytesUsed; // cache

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   *
   * @param term Term containing field and possibly some pattern structure. The term text is
   *     ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a match.
   */
  public AutomatonQuery(final Term term, Automaton automaton) {
    this(term, automaton, false);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   *
   * @param term Term containing field and possibly some pattern structure. The term text is
   *     ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a match.
   * @param isBinary if true, this automaton is already binary and will not go through the
   *     UTF32ToUTF8 conversion
   */
  public AutomatonQuery(final Term term, Automaton automaton, boolean isBinary) {
    this(term, automaton, isBinary, CONSTANT_SCORE_BLENDED_REWRITE);
  }

  /**
   * Create a new AutomatonQuery from an {@link Automaton}.
   *
   * @param term Term containing field and possibly some pattern structure. The term text is
   *     ignored.
   * @param automaton Automaton to run, terms that are accepted are considered a match.
   * @param isBinary if true, this automaton is already binary and will not go through the
   *     UTF32ToUTF8 conversion
   * @param rewriteMethod the rewriteMethod to use to build the final query from the automaton
   */
  public AutomatonQuery(
      final Term term, Automaton automaton, boolean isBinary, RewriteMethod rewriteMethod) {
    super(term.field(), rewriteMethod);
    this.term = term;
    this.automaton = automaton;
    this.automatonIsBinary = isBinary;
    this.compiled = new CompiledAutomaton(automaton, false, true, isBinary);

    // compiled may already reference the same Automaton instance; only count its bytes once.
    long automatonBytes = compiled.sharesAutomaton(automaton) ? 0L : automaton.ramBytesUsed();
    this.ramBytesUsed =
        BASE_RAM_BYTES + term.ramBytesUsed() + automatonBytes + compiled.ramBytesUsed();
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    return compiled.getTermsEnum(terms);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + compiled.hashCode();
    result = prime * result + ((term == null) ? 0 : term.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    AutomatonQuery other = (AutomatonQuery) obj;
    if (!compiled.equals(other.compiled)) return false;
    if (term == null) {
      if (other.term != null) return false;
    } else if (!term.equals(other.term)) return false;
    return true;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(getClass().getSimpleName());
    buffer.append(" {");
    buffer.append('\n');
    buffer.append(automaton.toString());
    buffer.append("}");
    return buffer.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      compiled.visit(visitor, this, field);
    }
  }

  /** Returns the automaton used to create this query */
  public Automaton getAutomaton() {
    return automaton;
  }

  public CompiledAutomaton getCompiled() {
    return compiled;
  }

  /** Is this a binary (byte) oriented automaton. See the constructor. */
  public boolean isAutomatonBinary() {
    return automatonIsBinary;
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  private static final int TERM_LIMIT_FOR_COST = 16;

  @Override
  protected long innerEstimateCost(Terms terms) throws IOException {
    // Special cases of automata
    if (compiled.type == CompiledAutomaton.AUTOMATON_TYPE.NONE) {
      return 0;
    } else if (compiled.type == CompiledAutomaton.AUTOMATON_TYPE.ALL) {
      return terms.getSumDocFreq();
    } else if (compiled.type == CompiledAutomaton.AUTOMATON_TYPE.SINGLE) {
      TermsEnum t = terms.iterator();
      return t.seekExact(compiled.term) ? t.docFreq() : 0;
    }
    assert compiled.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL;
    // Special case: Few terms overall
    if (terms.size() < TERM_LIMIT_FOR_COST) {
      // Exhaustively sum cost of matching terms
      long cost = 0;
      ByteRunnable byteRunnable = compiled.getByteRunnable();
      TermsEnum t = terms.iterator();
      BytesRef term;
      while ((term = t.next()) != null) {
        if (byteRunnable.run(term.bytes, term.offset, term.length)) {
          cost += t.docFreq();
        }
      }
      return cost;
    }
    // General case. Find a covering range
    BytesRef lowerBound = getBound(5, false);
    BytesRef upperBound = getBound(5, true);
    incrementBytesRef(upperBound);
    TermsEnum t = terms.iterator();
    TermsEnum.SeekStatus seekStatus = t.seekCeil(lowerBound);
    if (seekStatus == TermsEnum.SeekStatus.END) {
      // Automaton range is lexicographically greater than all terms
      return 0;
    } else if (upperBound.length == 0) {
      // The upper bound is unbounded (usually a leading wildcard). Assume match-all.
      return terms.getSumDocFreq();
    } else if (t.term().compareTo(upperBound) >= 0) {
      // Automaton range is lexicographically lower than all terms
      return 0;
    }
    long cost = 0;
    for (int i = 0; i < TERM_LIMIT_FOR_COST; i++) {
      cost += t.docFreq();
      BytesRef currentTerm = t.next();
      if (currentTerm == null || currentTerm.compareTo(upperBound) >= 0) {
        // Ran out of matching terms. Return the current estimate.
        return cost;
      }
    }
    // Ran out of budget. Assume match all.
    return terms.getSumDocFreq();
  }

  private static void incrementBytesRef(BytesRef out) {
    int pos = out.offset + out.length - 1;
    boolean carry = true;
    while (carry && pos >= out.offset) {
      if (out.bytes[pos] != -1) {
        carry = false;
      }
      out.bytes[pos--]++;
    }
    if (carry) {
      out.length = 0; // Prefix was all 0xFF
    }
  }

  private void getAcceptedTermPrefix(
      int state,
      BinaryOperator<Transition> selector,
      ToIntFunction<Transition> transitionExtractor,
      IntsRef term,
      int pos,
      int length,
      Transition reusableTransition) {
    if (automaton.isAccept(state) || length == 0) {
      return;
    }
    int numTransitions = automaton.getNumTransitions(state);
    Transition selected = null;
    for (int i = 0; i < numTransitions; i++) {
      automaton.getTransition(state, i, reusableTransition);
      selected = selector.apply(selected, reusableTransition);
    }
    if (selected == null) {
      return;
    }
    term.ints[pos] = transitionExtractor.applyAsInt(selected);
    term.length++;
    getAcceptedTermPrefix(
        selected.dest,
        selector,
        transitionExtractor,
        term,
        pos + 1,
        length - 1,
        reusableTransition);
  }

  protected BytesRef getBound(int maxLength, boolean upper) {
    Transition transition = new Transition();
    IntsRef bound = new IntsRef(maxLength);
    if (upper) {
      getAcceptedTermPrefix(
          0,
          (a, b) -> a == null ? b : a.max > b.max ? a : b,
          a -> a.max,
          bound,
          0,
          maxLength,
          transition);
    } else {
      // lower
      getAcceptedTermPrefix(
          0,
          (a, b) -> a == null ? b : a.min < b.min ? a : b,
          a -> a.min,
          bound,
          0,
          maxLength,
          transition);
    }
    if (automatonIsBinary) {
      return StringHelper.intsRefToBytesRef(bound);
    }
    return new BytesRef(UnicodeUtil.newString(bound.ints, bound.offset, bound.length));
  }
}
