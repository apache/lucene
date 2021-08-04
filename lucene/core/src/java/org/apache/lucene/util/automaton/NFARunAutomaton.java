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
 * A RunAutomaton that does not require DFA, it will determinize and memorize the generated DFA
 * state along with the run
 *
 * <p>implemented based on: https://swtch.com/~rsc/regexp/regexp1.html
 */
public class NFARunAutomaton {

  /** state ordinal of "no such state" */
  public static final int MISSING = -1;

  private static final int NOT_COMPUTED = -2;

  private final Automaton automaton;
  private final int[] points;
  private final Map<DState, Integer> dStateToOrd = new HashMap<>(); // could init lazily?
  private DState[] dStates;
  private final int alphabetSize;

  /**
   * Constructor, assuming alphabet size is the whole Unicode code point space
   *
   * @param automaton incoming automaton, should be NFA, for DFA please use {@link RunAutomaton} for
   *     better efficiency
   */
  public NFARunAutomaton(Automaton automaton) {
    this(automaton, Character.MAX_CODE_POINT);
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
  }

  /**
   * For a given state and an incoming character (codepoint), return the next state
   *
   * @param state incoming state, should either be 0 or some state that is returned previously by
   *     this function
   * @param c codepoint
   * @return the next state or {@link #MISSING} if the transition doesn't exist
   */
  public int step(int state, int c) {
    assert dStates[state] != null;
    return step(dStates[state], c);
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

  private class DState {
    private final int[] nfaStates;
    // this field is lazily init'd when first time caller wants to add a new transition
    private int[] transitions;
    private final int hashCode;
    private final boolean isAccept;
    private final Transition stepTransition = new Transition();

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
        transitions[charClass] = findDState(step(points[charClass]));
        // TODO: we could potentially update more than one char classes, but
        //       this isn't super easy, there're cases where the larger transition
        //       is accepted but smaller transition isn't, like [0,10] is accepted
        //       but [5,5] isn't, then we can only update [0,4] and [6,10]
      }
      return transitions[charClass];
    }

    /**
     * given a list of NFA states and a character c, compute the output list of NFA state which is
     * wrapped as a DFA state
     */
    private DState step(int c) {
      StateSet stateSet = new StateSet(5); // fork IntHashSet from hppc instead?
      int numTransitions;
      for (int nfaState : nfaStates) {
        numTransitions = automaton.initTransition(nfaState, stepTransition);
        for (int i = 0; i < numTransitions; i++) {
          automaton.getNextTransition(stepTransition);
          if (stepTransition.min <= c && stepTransition.max >= c) {
            stateSet.incr(stepTransition.dest);
          }
        }
      }
      if (stateSet.size() == 0) {
        return null;
      }
      return new DState(stateSet.getArray());
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
