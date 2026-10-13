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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;

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
 * <p>If the compiled automaton matches no terms, all terms or a single term, then the built-in
 * term-based rewrite methods are bypassed in favor of a {@link MatchNoDocsQuery}, {@link
 * FieldExistsQuery} or {@link TermQuery} respectively, with the same scoring behavior.
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
  public AutomatonQuery(final Term term, Automaton automaton, boolean isBinary, RewriteMethod rewriteMethod) {
    var compiled = new CompiledAutomaton(automaton, false, true, isBinary);
    super(term.field(), specialCaseRewriteMethod(compiled, rewriteMethod));
    this.term = term;
    this.automaton = automaton;
    this.automatonIsBinary = isBinary;
    this.compiled = compiled;

    // compiled may already reference the same Automaton instance; only count its bytes once.
    long automatonBytes = compiled.sharesAutomaton(automaton) ? 0L : automaton.ramBytesUsed();
    this.ramBytesUsed =
        BASE_RAM_BYTES + term.ramBytesUsed() + automatonBytes + compiled.ramBytesUsed();
  }

  /**
   * Returns a rewrite method that bypasses term enumeration when the compiled automaton matches no
   * terms, all terms or a single term, if the given rewrite method is a built-in term-based one
   * whose scoring we can reproduce. Otherwise, returns the given rewrite method.
   */
  private static RewriteMethod specialCaseRewriteMethod(
      CompiledAutomaton compiled, RewriteMethod rewriteMethod) {
    if (rewriteMethod instanceof SpecialCaseRewrite specialCaseRewrite) {
      rewriteMethod = specialCaseRewrite.delegate;
    }
    boolean constantScore =
        rewriteMethod == CONSTANT_SCORE_BLENDED_REWRITE
            || rewriteMethod == CONSTANT_SCORE_REWRITE
            || rewriteMethod == CONSTANT_SCORE_BOOLEAN_REWRITE;
    // top-terms rewrites only visit a bounded number of terms, so they can't match ALL
    boolean topTermsConstantScore = rewriteMethod instanceof TopTermsBoostOnlyBooleanQueryRewrite;
    boolean scoring =
        rewriteMethod == SCORING_BOOLEAN_REWRITE
            || rewriteMethod instanceof TopTermsScoringBooleanQueryRewrite;
    boolean applies =
        switch (compiled.type) {
          case NONE, SINGLE -> constantScore || topTermsConstantScore || scoring;
          case ALL -> constantScore;
          case NORMAL -> false;
        };
    return applies ? new SpecialCaseRewrite(rewriteMethod) : rewriteMethod;
  }

  /**
   * Rewrites a query whose compiled automaton is NONE, ALL or SINGLE without enumerating terms.
   * Other queries are rewritten by the delegate.
   */
  private static final class SpecialCaseRewrite extends RewriteMethod {
    private final RewriteMethod delegate;
    private final boolean constantScore;

    SpecialCaseRewrite(RewriteMethod delegate) {
      this.delegate = delegate;
      this.constantScore =
          delegate != SCORING_BOOLEAN_REWRITE
              && delegate instanceof TopTermsScoringBooleanQueryRewrite == false;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher, MultiTermQuery query) throws IOException {
      // may be handed a query other than the one it was created for via getRewriteMethod()
      if (query instanceof AutomatonQuery == false) {
        return delegate.rewrite(indexSearcher, query);
      }
      CompiledAutomaton compiled = ((AutomatonQuery) query).compiled;
      String field = query.getField();
      return switch (compiled.type) {
        case NONE -> new MatchNoDocsQuery("automaton matches no terms");
        case ALL ->
            constantScore ? new FieldExistsQuery(field) : delegate.rewrite(indexSearcher, query);
        case SINGLE -> {
          Query termQuery = new TermQuery(new Term(field, compiled.term));
          yield constantScore ? new ConstantScoreQuery(termQuery) : termQuery;
        }
        case NORMAL -> delegate.rewrite(indexSearcher, query);
      };
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof SpecialCaseRewrite other && delegate.equals(other.delegate);
    }

    @Override
    public int hashCode() {
      return 31 * SpecialCaseRewrite.class.hashCode() + delegate.hashCode();
    }
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
}
