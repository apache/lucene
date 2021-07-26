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
 */
public class NFARunAutomaton {

  private static final int NOT_COMPUTED = -2;
  private static final int MISSING = -1;

  private final Automaton automaton;
  private final int[] points;
  private final Map<DState, Integer> dStateToOrd = new HashMap<>(); // could init lazily?
  private DState[] dStates;
  private final int alphabetSize;

  public NFARunAutomaton(Automaton automaton) {
    this(automaton, Character.MAX_CODE_POINT);
  }

  public NFARunAutomaton(Automaton automaton, int alphabetSize) {
    this.automaton = automaton;
    points = automaton.getStartPoints();
    this.alphabetSize = alphabetSize;
    dStates = new DState[10];
    findDState(new DState(new int[] {0}));
  }

  public int step(int state, int c) {
    assert dStates[state] != null;
    return step(dStates[state], c);
  }

  public boolean run(int[] s) {
    int p = 0;
    for (int c : s) {
      p = step(p, c);
      if (p == MISSING) return false;
    }
    return dStates[p].isAccept;
  }

  private int step(DState dState, int c) {
    int charClass = getCharClass(c);
    if (dState.nextState(charClass) == NOT_COMPUTED) {
      // the next dfa state has not been computed yet
      dState.setNextState(charClass, findDState(step(dState.nfaStates, c)));
    }
    return dState.nextState(charClass);
  }

  private DState step(int[] nfaStates, int c) {
    Transition transition = new Transition();
    StateSet stateSet = new StateSet(5); // fork IntHashSet from hppc instead?
    int numTransitions;
    for (int nfaState : nfaStates) {
      numTransitions = automaton.initTransition(nfaState, transition);
      for (int i = 0; i < numTransitions; i++) {
        automaton.getNextTransition(transition);
        if (transition.min <= c && transition.max >= c) {
          stateSet.incr(transition.dest);
        }
      }
    }
    if (stateSet.size() == 0) {
      return null;
    }
    return new DState(stateSet.getArray());
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
    private int[] transitions;
    private final int hashCode;
    private final boolean isAccept;

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
      return transitions[charClass];
    }

    private void setNextState(int charClass, int nextState) {
      initTransitions();
      assert charClass < transitions.length;
      transitions[charClass] = nextState;
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
