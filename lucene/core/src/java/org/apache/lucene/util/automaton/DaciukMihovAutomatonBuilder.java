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
package org.apache.lucene.util.automaton;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.UnicodeUtil;

/**
 * Builds a minimal, deterministic {@link Automaton} that accepts a set of strings. The algorithm
 * requires sorted input data, but is very fast (nearly linear with the input size).
 *
 * @see #build(Collection)
 * @see Automata#makeStringUnion(Collection)
 * @see Automata#makeBinaryStringUnion(Collection)
 * @see Automata#makeStringUnion(BytesRefIterator)
 * @see Automata#makeBinaryStringUnion(BytesRefIterator)
 * @deprecated Visibility of this class will be reduced in a future release. Users can access this
 *     functionality directly through {@link Automata#makeStringUnion(Collection)}
 */
@Deprecated
public final class DaciukMihovAutomatonBuilder {

  /**
   * This builder rejects terms that are more than 1k chars long since it then uses recursion based
   * on the length of the string, which might cause stack overflows.
   *
   * @deprecated See {@link Automata#MAX_STRING_UNION_TERM_LENGTH}
   */
  @Deprecated public static final int MAX_TERM_LENGTH = 1_000;

  /** The default constructor is private. Use static methods directly. */
  private DaciukMihovAutomatonBuilder() {
    super();
  }

  /** DFSA state with <code>char</code> labels on transitions. */
  private static final class State {

    /** An empty set of labels. */
    private static final int[] NO_LABELS = new int[0];

    /** An empty set of states. */
    private static final State[] NO_STATES = new State[0];

    /**
     * Labels of outgoing transitions. Indexed identically to {@link #states}. Labels must be sorted
     * lexicographically.
     */
    int[] labels = NO_LABELS;

    /** States reachable from outgoing transitions. Indexed identically to {@link #labels}. */
    State[] states = NO_STATES;

    /** <code>true</code> if this state corresponds to the end of at least one input sequence. */
    boolean is_final;

    /**
     * Returns the target state of a transition leaving this state and labeled with <code>label
     * </code>. If no such transition exists, returns <code>null</code>.
     */
    State getState(int label) {
      final int index = Arrays.binarySearch(labels, label);
      return index >= 0 ? states[index] : null;
    }

    /**
     * Two states are equal if:
     *
     * <ul>
     *   <li>they have an identical number of outgoing transitions, labeled with the same labels
     *   <li>corresponding outgoing transitions lead to the same states (to states with an identical
     *       right-language).
     * </ul>
     */
    @Override
    public boolean equals(Object obj) {
      final State other = (State) obj;
      return is_final == other.is_final
          && Arrays.equals(this.labels, other.labels)
          && referenceEquals(this.states, other.states);
    }

    /** Compute the hash code of the <i>current</i> status of this state. */
    @Override
    public int hashCode() {
      int hash = is_final ? 1 : 0;

      hash ^= hash * 31 + this.labels.length;
      for (int c : this.labels) hash ^= hash * 31 + c;

      /*
       * Compare the right-language of this state using reference-identity of
       * outgoing states. This is possible because states are interned (stored
       * in registry) and traversed in post-order, so any outgoing transitions
       * are already interned.
       */
      for (State s : this.states) {
        hash ^= System.identityHashCode(s);
      }

      return hash;
    }

    /** Return <code>true</code> if this state has any children (outgoing transitions). */
    boolean hasChildren() {
      return labels.length > 0;
    }

    /**
     * Create a new outgoing transition labeled <code>label</code> and return the newly created
     * target state for this transition.
     */
    State newState(int label) {
      assert Arrays.binarySearch(labels, label) < 0
          : "State already has transition labeled: " + label;

      labels = ArrayUtil.growExact(labels, labels.length + 1);
      states = ArrayUtil.growExact(states, states.length + 1);

      labels[labels.length - 1] = label;
      return states[states.length - 1] = new State();
    }

    /** Return the most recent transitions's target state. */
    State lastChild() {
      assert hasChildren() : "No outgoing transitions.";
      return states[states.length - 1];
    }

    /**
     * Return the associated state if the most recent transition is labeled with <code>label</code>.
     */
    State lastChild(int label) {
      final int index = labels.length - 1;
      State s = null;
      if (index >= 0 && labels[index] == label) {
        s = states[index];
      }
      assert s == getState(label);
      return s;
    }

    /** Replace the last added outgoing transition's target state with the given state. */
    void replaceLastChild(State state) {
      assert hasChildren() : "No outgoing transitions.";
      states[states.length - 1] = state;
    }

    /** Compare two lists of objects for reference-equality. */
    private static boolean referenceEquals(Object[] a1, Object[] a2) {
      if (a1.length != a2.length) {
        return false;
      }

      for (int i = 0; i < a1.length; i++) {
        if (a1[i] != a2[i]) {
          return false;
        }
      }

      return true;
    }
  }

  /** A "registry" for state interning. */
  private HashMap<State, State> stateRegistry = new HashMap<>();

  /** Root automaton state. */
  private final State root = new State();

  /** Used for input order checking (only through assertions right now) */
  private BytesRefBuilder previous;

  /** Copy <code>current</code> into an internal buffer. */
  private boolean setPrevious(BytesRef current) {
    if (previous == null) {
      previous = new BytesRefBuilder();
    }
    previous.copyBytes(current);
    return true;
  }

  /** Internal recursive traversal for conversion. */
  private static int convert(
      Automaton.Builder a, State s, IdentityHashMap<State, Integer> visited) {

    Integer converted = visited.get(s);
    if (converted != null) {
      return converted;
    }

    converted = a.createState();
    a.setAccept(converted, s.is_final);

    visited.put(s, converted);
    int i = 0;
    int[] labels = s.labels;
    for (DaciukMihovAutomatonBuilder.State target : s.states) {
      a.addTransition(converted, convert(a, target, visited), labels[i++]);
    }

    return converted;
  }

  /**
   * Called after adding all terms. Performs final minimization and converts to a standard {@link
   * Automaton} instance.
   */
  private Automaton completeAndConvert() {
    // Final minimization:
    if (this.stateRegistry == null) throw new IllegalStateException();
    if (root.hasChildren()) replaceOrRegister(root);
    stateRegistry = null;

    // Convert:
    Automaton.Builder a = new Automaton.Builder();
    convert(a, root, new IdentityHashMap<>());
    return a.finish();
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of {@link BytesRef} representing
   * strings in UTF-8. These strings must be binary-sorted.
   *
   * @deprecated Please see {@link Automata#makeStringUnion(Collection)} instead
   */
  @Deprecated
  public static Automaton build(Collection<BytesRef> input) {
    return build(input, false);
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of {@link BytesRef} representing
   * strings in UTF-8. These strings must be binary-sorted.
   */
  static Automaton build(Collection<BytesRef> input, boolean asBinary) {
    final DaciukMihovAutomatonBuilder builder = new DaciukMihovAutomatonBuilder();

    for (BytesRef b : input) {
      builder.add(b, asBinary);
    }

    return builder.completeAndConvert();
  }

  /**
   * Build a minimal, deterministic automaton from a sorted list of {@link BytesRef} representing
   * strings in UTF-8. These strings must be binary-sorted. Creates an {@link Automaton} with either
   * UTF-8 codepoints as transition labels or binary (compiled) transition labels based on {@code
   * asBinary}.
   */
  static Automaton build(BytesRefIterator input, boolean asBinary) throws IOException {
    final DaciukMihovAutomatonBuilder builder = new DaciukMihovAutomatonBuilder();

    for (BytesRef b = input.next(); b != null; b = input.next()) {
      builder.add(b, asBinary);
    }

    return builder.completeAndConvert();
  }

  private void add(BytesRef current, boolean asBinary) {
    if (current.length > Automata.MAX_STRING_UNION_TERM_LENGTH) {
      throw new IllegalArgumentException(
          "This builder doesn't allow terms that are larger than "
              + Automata.MAX_STRING_UNION_TERM_LENGTH
              + " characters, got "
              + current);
    }
    assert stateRegistry != null : "Automaton already built.";
    assert previous == null || previous.get().compareTo(current) <= 0
        : "Input must be in sorted UTF-8 order: " + previous.get() + " >= " + current;
    assert setPrevious(current);

    // Reusable codepoint information if we're building a non-binary based automaton
    UnicodeUtil.UTF8CodePoint codePoint = null;

    // Descend in the automaton (find matching prefix).
    byte[] bytes = current.bytes;
    int pos = current.offset, max = current.offset + current.length;
    State next, state = root;
    if (asBinary) {
      while (pos < max && (next = state.lastChild(bytes[pos] & 0xff)) != null) {
        state = next;
        pos++;
      }
    } else {
      while (pos < max) {
        codePoint = UnicodeUtil.codePointAt(bytes, pos, codePoint);
        next = state.lastChild(codePoint.codePoint);
        if (next == null) {
          break;
        }
        state = next;
        pos += codePoint.numBytes;
      }
    }

    if (state.hasChildren()) replaceOrRegister(state);

    // Add suffix
    if (asBinary) {
      while (pos < max) {
        state = state.newState(bytes[pos] & 0xff);
        pos++;
      }
    } else {
      while (pos < max) {
        codePoint = UnicodeUtil.codePointAt(bytes, pos, codePoint);
        state = state.newState(codePoint.codePoint);
        pos += codePoint.numBytes;
      }
    }
    state.is_final = true;
  }

  /**
   * Replace last child of <code>state</code> with an already registered state or stateRegistry the
   * last child state.
   */
  private void replaceOrRegister(State state) {
    final State child = state.lastChild();

    if (child.hasChildren()) replaceOrRegister(child);

    final State registered = stateRegistry.get(child);
    if (registered != null) {
      state.replaceLastChild(registered);
    } else {
      stateRegistry.put(child, child);
    }
  }
}
