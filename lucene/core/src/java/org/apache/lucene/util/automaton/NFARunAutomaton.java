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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.hppc.BitMixer;

/**
 * A RunAutomaton that does not require DFA. It will lazily determinize on-demand, memorizing the
 * generated DFA states that has been explored
 *
 * <p>implemented based on: https://swtch.com/~rsc/regexp/regexp1.html
 */
public class NFARunAutomaton implements ByteRunnable, TransitionAccessor {

  /** state ordinal of "no such state" */
  public static final int MISSING = -1;

  private static final int NOT_COMPUTED = -2;

  private final Automaton automaton;
  private final int[] points;
  private final Map<DState, Integer> dStateToOrd = new HashMap<>(); // could init lazily?
  private DState[] dStates;
  private final int alphabetSize;
  final int[] classmap; // map from char number to class

  private final Operations.PointTransitionSet transitionSet =
      new Operations.PointTransitionSet(); // reusable
  private final StateSet statesSet = new StateSet(5); // reusable

  /**
   * Constructor, assuming alphabet size is the whole Unicode code point space
   *
   * @param automaton incoming automaton, should be NFA, for DFA please use {@link RunAutomaton} for
   *     better efficiency
   */
  public NFARunAutomaton(Automaton automaton) {
    this(automaton, Character.MAX_CODE_POINT + 1);
  }

  /**
   * Constructor
   *
   * @param automaton incoming automaton, should be NFA, for DFA please use {@link RunAutomaton} *
   *     for better efficiency
   * @param alphabetSize alphabet size
   */
  public NFARunAutomaton(Automaton automaton, int alphabetSize) {
    this.automaton = automaton;
    points = automaton.getStartPoints();
    this.alphabetSize = alphabetSize;
    dStates = new DState[10];
    findDState(new DState(new int[] {0}));

    /*
     * Set alphabet table for optimal run performance.
     */
    classmap = new int[Math.min(256, alphabetSize)];
    int i = 0;
    for (int j = 0; j < classmap.length; j++) {
      if (i + 1 < points.length && j == points[i + 1]) {
        i++;
      }
      classmap[j] = i;
    }
  }

  /**
   * For a given state and an incoming character (codepoint), return the next state
   *
   * @param state incoming state, should either be 0 or some state that is returned previously by
   *     this function
   * @param c codepoint
   * @return the next state or {@link #MISSING} if the transition doesn't exist
   */
  @Override
  public int step(int state, int c) {
    assert dStates[state] != null;
    return step(dStates[state], c);
  }

  @Override
  public boolean isAccept(int state) {
    assert dStates[state] != null;
    return dStates[state].isAccept;
  }

  @Override
  public int getSize() {
    return dStates.length;
  }

  /**
   * Run through a given codepoint array, return accepted or not, should only be used in test
   *
   * @param s String represented by an int array
   * @return accept or not
   */
  boolean run(int[] s) {
    int p = 0;
    for (int c : s) {
      p = step(p, c);
      if (p == MISSING) return false;
    }
    return dStates[p].isAccept;
  }

  /**
   * From an existing DFA state, step to next DFA state given character c if the transition is
   * previously tried then this operation will just use the cached result, otherwise it will call
   * {@link DState#step(int)} to get the next state and cache the result
   */
  private int step(DState dState, int c) {
    int charClass = getCharClass(c);
    return dState.nextState(charClass);
  }

  /**
   * return the ordinal of given DFA state, generate a new ordinal if the given DFA state is a new
   * one
   */
  private int findDState(DState dState) {
    if (dState == null) {
      return MISSING;
    }
    int ord = dStateToOrd.getOrDefault(dState, -1);
    if (ord >= 0) {
      return ord;
    }
    ord = dStateToOrd.size();
    dStateToOrd.put(dState, ord);
    assert ord >= dStates.length || dStates[ord] == null;
    if (ord >= dStates.length) {
      dStates = ArrayUtil.grow(dStates, ord + 1);
    }
    dStates[ord] = dState;
    return ord;
  }

  /** Gets character class of given codepoint */
  final int getCharClass(int c) {
    assert c < alphabetSize;

    if (c < classmap.length) {
      return classmap[c];
    }

    // binary search
    int a = 0;
    int b = points.length;
    while (b - a > 1) {
      int d = (a + b) >>> 1;
      if (points[d] > c) b = d;
      else if (points[d] < c) a = d;
      else return d;
    }
    return a;
  }

  @Override
  public int initTransition(int state, Transition t) {
    t.source = state;
    t.transitionUpto = -1;
    return getNumTransitions(state);
  }

  @Override
  public void getNextTransition(Transition t) {
    assert t.transitionUpto < points.length - 1 && t.transitionUpto >= -1;
    while (dStates[t.source].transitions[++t.transitionUpto] == MISSING) {
      // this shouldn't throw AIOOBE as long as this function is only called
      // numTransitions times
    }
    assert dStates[t.source].transitions[t.transitionUpto] != NOT_COMPUTED;
    t.dest = dStates[t.source].transitions[t.transitionUpto];

    t.min = points[t.transitionUpto];
    if (t.transitionUpto == points.length - 1) {
      t.max = alphabetSize - 1;
    } else {
      t.max = points[t.transitionUpto + 1] - 1;
    }
  }

  @Override
  public int getNumTransitions(int state) {
    dStates[state].determinize();
    return dStates[state].outgoingTransitions;
  }

  @Override
  public void getTransition(int state, int index, Transition t) {
    dStates[state].determinize();
    int outgoingTransitions = -1;
    t.transitionUpto = -1;
    t.source = state;
    while (outgoingTransitions < index && t.transitionUpto < points.length - 1) {
      if (dStates[t.source].transitions[++t.transitionUpto] != MISSING) {
        outgoingTransitions++;
      }
    }
    assert outgoingTransitions == index;

    t.min = points[t.transitionUpto];
    if (t.transitionUpto == points.length - 1) {
      t.max = alphabetSize - 1;
    } else {
      t.max = points[t.transitionUpto + 1] - 1;
    }
  }

  private class DState {
    private final int[] nfaStates;
    // this field is lazily init'd when first time caller wants to add a new transition
    private int[] transitions;
    private final int hashCode;
    private final boolean isAccept;
    private final Transition stepTransition = new Transition();
    private Transition minimalTransition;
    private int computedTransitions;
    private int outgoingTransitions;

    private DState(int[] nfaStates) {
      assert nfaStates != null && nfaStates.length > 0;
      this.nfaStates = nfaStates;
      int hashCode = nfaStates.length;
      boolean isAccept = false;
      for (int s : nfaStates) {
        hashCode += BitMixer.mix(s);
        if (automaton.isAccept(s)) {
          isAccept = true;
        }
      }
      this.isAccept = isAccept;
      this.hashCode = hashCode;
    }

    private int nextState(int charClass) {
      initTransitions();
      assert charClass < transitions.length;
      if (transitions[charClass] == NOT_COMPUTED) {
        assignTransition(charClass, findDState(step(points[charClass])));
        // we could potentially update more than one char classes
        if (minimalTransition != null) {
          // to the left
          int cls = charClass;
          while (cls > 0 && points[--cls] >= minimalTransition.min) {
            assert transitions[cls] == NOT_COMPUTED || transitions[cls] == transitions[charClass];
            assignTransition(cls, transitions[charClass]);
          }
          // to the right
          cls = charClass;
          while (cls < points.length - 1 && points[++cls] <= minimalTransition.max) {
            assert transitions[cls] == NOT_COMPUTED || transitions[cls] == transitions[charClass];
            assignTransition(cls, transitions[charClass]);
          }
          minimalTransition = null;
        }
      }
      return transitions[charClass];
    }

    private void assignTransition(int charClass, int dest) {
      if (transitions[charClass] == NOT_COMPUTED) {
        computedTransitions++;
        transitions[charClass] = dest;
        if (transitions[charClass] != MISSING) {
          outgoingTransitions++;
        }
      }
    }

    /**
     * given a list of NFA states and a character c, compute the output list of NFA state which is
     * wrapped as a DFA state
     */
    private DState step(int c) {
      statesSet.reset(); // TODO: fork IntHashSet from hppc instead?
      int numTransitions;
      int left = -1, right = alphabetSize;
      for (int nfaState : nfaStates) {
        numTransitions = automaton.initTransition(nfaState, stepTransition);
        // TODO: binary search should be faster, since transitions are sorted
        for (int i = 0; i < numTransitions; i++) {
          automaton.getNextTransition(stepTransition);
          if (stepTransition.min <= c && stepTransition.max >= c) {
            statesSet.incr(stepTransition.dest);
            left = Math.max(stepTransition.min, left);
            right = Math.min(stepTransition.max, right);
          }
          if (stepTransition.max < c) {
            left = Math.max(stepTransition.max + 1, left);
          }
          if (stepTransition.min > c) {
            right = Math.min(stepTransition.min - 1, right);
            // transitions in automaton are sorted
            break;
          }
        }
      }
      if (statesSet.size() == 0) {
        return null;
      }
      minimalTransition = new Transition();
      minimalTransition.min = left;
      minimalTransition.max = right;
      return new DState(statesSet.getArray());
    }

    // determinize this state only
    private void determinize() {
      if (transitions != null && computedTransitions == transitions.length) {
        // already determinized
        return;
      }
      initTransitions();
      // Mostly forked from Operations.determinize
      transitionSet.reset();
      for (int nfaState : nfaStates) {
        int numTransitions = automaton.initTransition(nfaState, stepTransition);
        for (int i = 0; i < numTransitions; i++) {
          automaton.getNextTransition(stepTransition);
          transitionSet.add(stepTransition);
        }
      }
      if (transitionSet.count == 0) {
        // no outgoing transitions
        Arrays.fill(transitions, MISSING);
        computedTransitions = transitions.length;
        return;
      }

      transitionSet
          .sort(); // TODO: could use a PQ (heap) instead, since transitions for each state are
      // sorted
      statesSet.reset();
      int lastPoint = -1;
      int charClass = 0;
      for (int i = 0; i < transitionSet.count; i++) {
        final int point = transitionSet.points[i].point;
        if (statesSet.size() > 0) {
          assert lastPoint != -1;
          int ord = findDState(new DState(statesSet.getArray()));
          while (points[charClass] < lastPoint) {
            assignTransition(charClass++, MISSING);
          }
          assert points[charClass] == lastPoint;
          while (charClass < points.length && points[charClass] < point) {
            assert transitions[charClass] == NOT_COMPUTED || transitions[charClass] == ord;
            assignTransition(charClass++, ord);
          }
          assert (charClass == points.length && point == alphabetSize)
              || points[charClass] == point;
        }

        // process transitions that end on this point
        // (closes an overlapping interval)
        int[] transitions = transitionSet.points[i].ends.transitions;
        int limit = transitionSet.points[i].ends.next;
        for (int j = 0; j < limit; j += 3) {
          int dest = transitions[j];
          statesSet.decr(dest);
        }
        transitionSet.points[i].ends.next = 0;

        // process transitions that start on this point
        // (opens a new interval)
        transitions = transitionSet.points[i].starts.transitions;
        limit = transitionSet.points[i].starts.next;
        for (int j = 0; j < limit; j += 3) {
          int dest = transitions[j];
          statesSet.incr(dest);
        }

        lastPoint = point;
        transitionSet.points[i].starts.next = 0;
      }
      assert statesSet.size() == 0;
      assert computedTransitions
          >= charClass; // it's also possible that some transitions after the charClass has already
      // been explored
      // no more outgoing transitions, set rest of transition to MISSING
      assert charClass == transitions.length
          || transitions[charClass] == MISSING
          || transitions[charClass] == NOT_COMPUTED;
      Arrays.fill(transitions, charClass, transitions.length, MISSING);
      computedTransitions = transitions.length;
    }

    private void initTransitions() {
      if (transitions == null) {
        transitions = new int[points.length];
        Arrays.fill(transitions, NOT_COMPUTED);
      }
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DState dState = (DState) o;
      return hashCode == dState.hashCode && Arrays.equals(nfaStates, dState.nfaStates);
    }
  }
}
