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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.IOSupplier;

/**
 * Maintains a {@link IndexReader} {@link TermState} view over {@link IndexReader} instances
 * containing a single term. The {@link TermStates} doesn't track if the given {@link TermState}
 * objects are valid, neither if the {@link TermState} instances refer to the same terms in the
 * associated readers.
 *
 * @lucene.experimental
 */
public final class TermStates {

  private static final TermState EMPTY_TERMSTATE =
      new TermState() {
        @Override
        public void copyFrom(TermState other) {}
      };

  // Important: do NOT keep hard references to index readers
  private final Object topReaderContextIdentity;
  private final TermState[] states;
  private final Term term; // null if stats are to be used
  private int docFreq;
  private long totalTermFreq;

  // public static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  private TermStates(Term term, IndexReaderContext context) {
    assert context != null && context.isTopLevel;
    topReaderContextIdentity = context.identity;
    docFreq = 0;
    totalTermFreq = 0;
    states = new TermState[context.leaves().size()];
    this.term = term;
  }

  /** Creates an empty {@link TermStates} from a {@link IndexReaderContext} */
  public TermStates(IndexReaderContext context) {
    this(null, context);
  }

  /**
   * Expert: Return whether this {@link TermStates} was built for the given {@link
   * IndexReaderContext}. This is typically used for assertions.
   *
   * @lucene.internal
   */
  public boolean wasBuiltFor(IndexReaderContext context) {
    return topReaderContextIdentity == context.identity;
  }

  /** Creates a {@link TermStates} with an initial {@link TermState}, {@link IndexReader} pair. */
  public TermStates(
      IndexReaderContext context, TermState state, int ord, int docFreq, long totalTermFreq) {
    this(null, context);
    register(state, ord, docFreq, totalTermFreq);
  }

  private record PendingTermLookup(TermsEnum termsEnum, IOBooleanSupplier supplier) {}

  /**
   * Creates a {@link TermStates} from a top-level {@link IndexReaderContext} and the given {@link
   * Term}. This method will lookup the given term in all context's leaf readers and register each
   * of the readers containing the term in the returned {@link TermStates} using the leaf reader's
   * ordinal.
   *
   * <p>Note: the given context must be a top-level context.
   *
   * @param needsStats if {@code true} then all leaf contexts will be visited up-front to collect
   *     term statistics. Otherwise, the {@link TermState} objects will be built only when requested
   */
  public static TermStates build(IndexSearcher indexSearcher, Term term, boolean needsStats)
      throws IOException {
    IndexReaderContext context = indexSearcher.getTopReaderContext();
    assert context != null;
    final TermStates perReaderTermState = new TermStates(needsStats ? null : term, context);
    if (needsStats) {
      PendingTermLookup[] pendingTermLookups = new PendingTermLookup[0];
      for (LeafReaderContext ctx : context.leaves()) {
        Terms terms = Terms.getTerms(ctx.reader(), term.field());
        TermsEnum termsEnum = terms.iterator();
        // Schedule the I/O in the terms dictionary in the background.
        IOBooleanSupplier termExistsSupplier = termsEnum.prepareSeekExact(term.bytes());
        if (termExistsSupplier != null) {
          pendingTermLookups = ArrayUtil.grow(pendingTermLookups, ctx.ord + 1);
          pendingTermLookups[ctx.ord] = new PendingTermLookup(termsEnum, termExistsSupplier);
        }
      }
      for (int ord = 0; ord < pendingTermLookups.length; ++ord) {
        PendingTermLookup pendingTermLookup = pendingTermLookups[ord];
        if (pendingTermLookup != null && pendingTermLookup.supplier.get()) {
          TermsEnum termsEnum = pendingTermLookup.termsEnum();
          perReaderTermState.register(
              termsEnum.termState(), ord, termsEnum.docFreq(), termsEnum.totalTermFreq());
        }
      }
    }
    return perReaderTermState;
  }

  /** Clears the {@link TermStates} internal state and removes all registered {@link TermState}s */
  public void clear() {
    docFreq = 0;
    totalTermFreq = 0;
    Arrays.fill(states, null);
  }

  /**
   * Registers and associates a {@link TermState} with an leaf ordinal. The leaf ordinal should be
   * derived from a {@link IndexReaderContext}'s leaf ord.
   */
  public void register(
      TermState state, final int ord, final int docFreq, final long totalTermFreq) {
    register(state, ord);
    accumulateStatistics(docFreq, totalTermFreq);
  }

  /**
   * Expert: Registers and associates a {@link TermState} with an leaf ordinal. The leaf ordinal
   * should be derived from a {@link IndexReaderContext}'s leaf ord. On the contrary to {@link
   * #register(TermState, int, int, long)} this method does NOT update term statistics.
   */
  public void register(TermState state, final int ord) {
    assert state != null : "state must not be null";
    assert ord >= 0 && ord < states.length;
    assert states[ord] == null : "state for ord: " + ord + " already registered";
    states[ord] = state;
  }

  /** Expert: Accumulate term statistics. */
  public void accumulateStatistics(final int docFreq, final long totalTermFreq) {
    assert docFreq >= 0;
    assert totalTermFreq >= 0;
    assert docFreq <= totalTermFreq;
    this.docFreq += docFreq;
    this.totalTermFreq += totalTermFreq;
  }

  /**
   * Returns a {@link Supplier} for a {@link TermState} for the given {@link LeafReaderContext}.
   * This may return {@code null} if some cheap checks help figure out that this term doesn't exist
   * in this leaf. The {@link Supplier} may then also return {@code null} if the term doesn't exist.
   *
   * <p>Calling this method typically schedules some I/O in the background, so it is recommended to
   * retrieve {@link Supplier}s across all required terms first before calling {@link Supplier#get}
   * on all {@link Supplier}s so that the I/O for these terms can be performed in parallel.
   *
   * @param ctx the {@link LeafReaderContext} to get the {@link TermState} for.
   * @return a Supplier for a TermState.
   */
  public IOSupplier<TermState> get(LeafReaderContext ctx) throws IOException {
    assert ctx.ord >= 0 && ctx.ord < states.length;
    if (term == null) {
      if (states[ctx.ord] == null) {
        return null;
      } else {
        return () -> states[ctx.ord];
      }
    }
    if (this.states[ctx.ord] == null) {
      final Terms terms = ctx.reader().terms(term.field());
      if (terms == null) {
        this.states[ctx.ord] = EMPTY_TERMSTATE;
        return null;
      }
      final TermsEnum termsEnum = terms.iterator();
      IOBooleanSupplier termExistsSupplier = termsEnum.prepareSeekExact(term.bytes());
      if (termExistsSupplier == null) {
        this.states[ctx.ord] = EMPTY_TERMSTATE;
        return null;
      }
      return () -> {
        if (this.states[ctx.ord] == null) {
          TermState state = null;
          if (termExistsSupplier.get()) {
            state = termsEnum.termState();
            this.states[ctx.ord] = state;
          } else {
            this.states[ctx.ord] = EMPTY_TERMSTATE;
          }
        }
        TermState state = this.states[ctx.ord];
        if (state == EMPTY_TERMSTATE) {
          return null;
        }
        return state;
      };
    }
    TermState state = this.states[ctx.ord];
    if (state == EMPTY_TERMSTATE) {
      return null;
    }
    return () -> state;
  }

  /**
   * Returns the accumulated document frequency of all {@link TermState} instances passed to {@link
   * #register(TermState, int, int, long)}.
   *
   * @return the accumulated document frequency of all {@link TermState} instances passed to {@link
   *     #register(TermState, int, int, long)}.
   */
  public int docFreq() {
    if (term != null) {
      throw new IllegalStateException("Cannot call docFreq() when needsStats=false");
    }
    return docFreq;
  }

  /**
   * Returns the accumulated term frequency of all {@link TermState} instances passed to {@link
   * #register(TermState, int, int, long)}.
   *
   * @return the accumulated term frequency of all {@link TermState} instances passed to {@link
   *     #register(TermState, int, int, long)}.
   */
  public long totalTermFreq() {
    if (term != null) {
      throw new IllegalStateException("Cannot call totalTermFreq() when needsStats=false");
    }
    return totalTermFreq;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TermStates\n");
    for (TermState termState : states) {
      sb.append("  state=");
      sb.append(termState);
      sb.append('\n');
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TermStates && ((TermStates) obj).docFreq == docFreq;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(docFreq);
  }
}
