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
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * A Query that matches documents containing terms with a specified prefix. A PrefixQuery is built
 * by QueryParser for input like <code>app*</code>.
 *
 * <p>This query uses the {@link MultiTermQuery#CONSTANT_SCORE_BLENDED_REWRITE} rewrite method.
 */
public class PrefixQuery extends MultiTermQuery {
  private final Term term;
  private final BytesRef prefix;
  private final BytesRef limit;
  private final ByteRunAutomaton[] runAutomaton = new ByteRunAutomaton[1];

  /** Constructs a query for terms starting with <code>prefix</code>. */
  public PrefixQuery(Term prefix) {
    this(prefix, CONSTANT_SCORE_BLENDED_REWRITE);
  }

  /**
   * Constructs a query for terms starting with <code>prefix</code> using a defined RewriteMethod
   */
  public PrefixQuery(Term prefix, RewriteMethod rewriteMethod) {
    super(prefix.field(), rewriteMethod);
    this.term = prefix;
    BytesRef tmp = prefix.bytes();
    byte[] backing = new byte[tmp.length + UnicodeUtil.BIG_TERM.length];
    System.arraycopy(tmp.bytes, tmp.offset, backing, 0, tmp.length);
    System.arraycopy(
        UnicodeUtil.BIG_TERM.bytes, 0, backing, tmp.length, UnicodeUtil.BIG_TERM.length);
    this.prefix = new BytesRef(backing, 0, tmp.length);
    this.limit = new BytesRef(backing);
  }

  /** Build an automaton accepting all terms with the specified prefix. */
  public static Automaton toAutomaton(BytesRef prefix) {
    final int numStatesAndTransitions = prefix.length + 1;
    final Automaton automaton = new Automaton(numStatesAndTransitions, numStatesAndTransitions);
    int lastState = automaton.createState();
    for (int i = 0; i < prefix.length; i++) {
      int state = automaton.createState();
      automaton.addTransition(lastState, state, prefix.bytes[prefix.offset + i] & 0xff);
      lastState = state;
    }
    automaton.setAccept(lastState, true);
    automaton.addTransition(lastState, lastState, 0, 255);
    automaton.finishState();
    assert automaton.isDeterministic();
    return automaton;
  }

  @Override
  @SuppressWarnings("fallthrough")
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    final TermsEnum te = terms.iterator();
    final BytesRef start;
    switch (te.seekCeil(PrefixQuery.this.prefix)) {
      case FOUND:
        start = PrefixQuery.this.prefix;
        break;
      case NOT_FOUND:
        BytesRef term = te.term();
        if (StringHelper.startsWith(term, PrefixQuery.this.prefix)) {
          start = BytesRef.deepCopyOf(term);
          break;
        }
        // fallthrough
        // $CASES-OMITTED$
      default:
        return TermsEnum.EMPTY;
    }
    final TermState startState = te.termState();
    if (te.seekCeil(PrefixQuery.this.limit) == TermsEnum.SeekStatus.END) {
      te.seekExact(start, startState);
      return new DirectPrefixTailTermsEnum(te, start);
    } else {
      BytesRef limit = te.term();
      final int tdi = getThresholdDeterminantIdx(PrefixQuery.this.prefix, limit);
      final int determinant = Byte.toUnsignedInt(limit.bytes[limit.offset + tdi]);
      final int limitLength = limit.length;
      te.seekExact(start, startState);
      return new DirectPrefixTermsEnum(te, start, limitLength, tdi, determinant);
    }
  }

  /**
   * Find an index that differs between the prefix and limit. The particular index is arbitrary, but
   * there is guaranteed to be at least one determinant index. Once we have this index, it will
   * suffice to check this index only (regardless of how long the prefix or limit threshold term
   * is).
   */
  private static int getThresholdDeterminantIdx(BytesRef prefix, BytesRef limit) {
    for (int i = Math.min(prefix.length, limit.length) - 1; i >= 0; i--) {
      if (prefix.bytes[i] != limit.bytes[limit.offset + i]) {
        return i;
      }
    }
    throw new IllegalStateException("`limit` must not start with `prefix`");
  }

  private static final class DirectPrefixTermsEnum extends FilteredTermsEnum {
    private final BytesRef startTerm;
    private final int thresholdLength;
    private final int tdi;
    private final int determinant;

    public DirectPrefixTermsEnum(
        TermsEnum tenum,
        BytesRef startTerm,
        int limitLength,
        int thresholdDeterminantIdx,
        int determinant) {
      super(tenum);
      this.startTerm = startTerm;
      this.tdi = thresholdDeterminantIdx;
      this.thresholdLength = limitLength;
      this.determinant = determinant;
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) {
      if (currentTerm == null) {
        return startTerm;
      } else {
        return null;
      }
    }

    @Override
    protected AcceptStatus accept(BytesRef candidate) {
      if (thresholdLength == candidate.length
          && determinant == Byte.toUnsignedInt(candidate.bytes[candidate.offset + tdi])) {
        return AcceptStatus.NO_AND_SEEK;
      } else {
        return AcceptStatus.YES;
      }
    }
  }

  private static final class DirectPrefixTailTermsEnum extends FilteredTermsEnum {
    private final BytesRef startTerm;

    public DirectPrefixTailTermsEnum(TermsEnum tenum, BytesRef startTerm) {
      super(tenum);
      this.startTerm = startTerm;
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) {
      assert currentTerm == null;
      return startTerm;
    }

    @Override
    protected AcceptStatus accept(BytesRef candidate) {
      return AcceptStatus.YES;
    }
  }

  /** Returns the prefix of this query. */
  public Term getPrefix() {
    return term;
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!getField().equals(field)) {
      buffer.append(getField());
      buffer.append(':');
    }
    buffer.append(term.text());
    buffer.append('*');
    return buffer.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    // build lazily. There are many cases that do not actually use automaton, so we can
    // often avoid building it.
    if (visitor.acceptField(field)) {
      visitor.consumeTermsMatching(
          this,
          field,
          () -> {
            ByteRunAutomaton ret = runAutomaton[0];
            if (ret == null) {
              ret = new CompiledAutomaton(toAutomaton(this.prefix), false, true, true).runAutomaton;
              runAutomaton[0] = ret;
            }
            return ret;
          });
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + term.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    // super.equals() ensures we are the same class
    PrefixQuery other = (PrefixQuery) obj;
    if (!term.equals(other.term)) {
      return false;
    }
    return true;
  }
}
