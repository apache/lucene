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

import static org.apache.lucene.util.automaton.Operations.DEFAULT_DETERMINIZE_WORK_LIMIT;
import static org.apache.lucene.util.automaton.Operations.topoSortStates;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;

public class TestOperations extends LuceneTestCase {
  /** Test string union. */
  public void testStringUnion() {
    List<BytesRef> strings = new ArrayList<>();
    for (int i = RandomNumbers.randomIntBetween(random(), 0, 1000); --i >= 0; ) {
      strings.add(new BytesRef(TestUtil.randomUnicodeString(random())));
    }

    Collections.sort(strings);
    Automaton union = Automata.makeStringUnion(strings);
    assertTrue(union.isDeterministic());
    assertFalse(Operations.hasDeadStatesFromInitial(union));

    Automaton naiveUnion = naiveUnion(strings);
    assertTrue(naiveUnion.isDeterministic());
    assertFalse(Operations.hasDeadStatesFromInitial(naiveUnion));

    assertTrue(Operations.sameLanguage(union, naiveUnion));
  }

  private static Automaton naiveUnion(List<BytesRef> strings) {
    Automaton[] eachIndividual = new Automaton[strings.size()];
    int i = 0;
    for (BytesRef bref : strings) {
      eachIndividual[i++] = Automata.makeString(bref.utf8ToString());
    }
    return Operations.determinize(
        Operations.union(Arrays.asList(eachIndividual)), DEFAULT_DETERMINIZE_WORK_LIMIT);
  }

  /** Test concatenation with empty language returns empty */
  public void testEmptyLanguageConcatenate() {
    Automaton a = Automata.makeString("a");
    Automaton concat = Operations.concatenate(a, Automata.makeEmpty());
    assertTrue(Operations.isEmpty(concat));
  }

  /**
   * Test case for the topoSortStates method when the input Automaton contains a cycle. This test
   * case constructs an Automaton with two disjoint sets of states—one without a cycle and one with
   * a cycle. The topoSortStates method should detect the presence of a cycle and throw an
   * IllegalArgumentException.
   */
  public void testCycledAutomaton() {
    Automaton a = generateRandomAutomaton(true);
    IllegalArgumentException exc =
        expectThrows(IllegalArgumentException.class, () -> topoSortStates(a));
    assertTrue(exc.getMessage().contains("Input automaton has a cycle"));
  }

  public void testTopoSortStates() {
    Automaton a = generateRandomAutomaton(false);

    int[] sorted = topoSortStates(a);
    int[] stateMap = new int[a.getNumStates()];
    Arrays.fill(stateMap, -1);
    int order = 0;
    for (int state : sorted) {
      assertEquals(-1, stateMap[state]);
      stateMap[state] = (order++);
    }

    Transition transition = new Transition();
    for (int state : sorted) {
      int count = a.initTransition(state, transition);
      for (int i = 0; i < count; i++) {
        a.getNextTransition(transition);
        // ensure dest's order is higher than current state
        assertTrue(stateMap[transition.dest] > stateMap[state]);
      }
    }
  }

  /** Test optimization to concatenate() with empty String to an NFA */
  public void testEmptySingletonNFAConcatenate() {
    Automaton singleton = Automata.makeString("");
    Automaton expandedSingleton = singleton;
    // an NFA (two transitions for 't' from initial state)
    Automaton nfa = Operations.union(Automata.makeString("this"), Automata.makeString("three"));
    Automaton concat1 = Operations.concatenate(expandedSingleton, nfa);
    Automaton concat2 = Operations.concatenate(singleton, nfa);
    assertFalse(concat2.isDeterministic());
    assertTrue(
        Operations.sameLanguage(
            Operations.determinize(concat1, 100), Operations.determinize(concat2, 100)));
    assertTrue(
        Operations.sameLanguage(
            Operations.determinize(nfa, 100), Operations.determinize(concat1, 100)));
    assertTrue(
        Operations.sameLanguage(
            Operations.determinize(nfa, 100), Operations.determinize(concat2, 100)));
  }

  public void testGetRandomAcceptedString() throws Throwable {
    final int ITER1 = atLeast(100);
    final int ITER2 = atLeast(100);
    for (int i = 0; i < ITER1; i++) {

      final RegExp re = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
      // System.out.println("TEST i=" + i + " re=" + re);
      final Automaton a = Operations.determinize(re.toAutomaton(), DEFAULT_DETERMINIZE_WORK_LIMIT);
      assertFalse(Operations.isEmpty(a));

      final AutomatonTestUtil.RandomAcceptedStrings rx =
          new AutomatonTestUtil.RandomAcceptedStrings(a);
      for (int j = 0; j < ITER2; j++) {
        // System.out.println("TEST: j=" + j);
        int[] acc = null;
        try {
          acc = rx.getRandomAcceptedString(random());
          final String s = UnicodeUtil.newString(acc, 0, acc.length);
          // a.writeDot("adot");
          assertTrue(Operations.run(a, s));
        } catch (Throwable t) {
          System.out.println("regexp: " + re);
          if (acc != null) {
            System.out.println("fail acc re=" + re + " count=" + acc.length);
            for (int k = 0; k < acc.length; k++) {
              System.out.println("  " + Integer.toHexString(acc[k]));
            }
          }
          throw t;
        }
      }
    }
  }
  /** tests against the original brics implementation. */
  public void testIsFinite() {
    int num = atLeast(200);
    for (int i = 0; i < num; i++) {
      Automaton a = AutomatonTestUtil.randomAutomaton(random());
      assertEquals(AutomatonTestUtil.isFiniteSlow(a), Operations.isFinite(a));
    }
  }

  public void testIsFiniteEatsStack() {
    char[] chars = new char[50000];
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString1 = new String(chars);
    TestUtil.randomFixedLengthUnicodeString(random(), chars, 0, chars.length);
    String bigString2 = new String(chars);
    Automaton a =
        Operations.union(Automata.makeString(bigString1), Automata.makeString(bigString2));
    IllegalArgumentException exc =
        expectThrows(IllegalArgumentException.class, () -> Operations.isFinite(a));
    assertTrue(exc.getMessage().contains("input automaton is too large"));
  }

  /**
   * Returns the set of all accepted strings.
   *
   * <p>This method exist just to ease testing. For production code directly use {@link
   * FiniteStringsIterator} instead.
   *
   * @see FiniteStringsIterator
   */
  public static Set<IntsRef> getFiniteStrings(Automaton a) {
    return getFiniteStrings(new FiniteStringsIterator(a));
  }

  /**
   * Returns the set of accepted strings, up to at most <code>limit</code> strings.
   *
   * <p>This method exist just to ease testing. For production code directly use {@link
   * LimitedFiniteStringsIterator} instead.
   *
   * @see LimitedFiniteStringsIterator
   */
  public static Set<IntsRef> getFiniteStrings(Automaton a, int limit) {
    return getFiniteStrings(new LimitedFiniteStringsIterator(a, limit));
  }

  /** Get all finite strings of an iterator. */
  private static Set<IntsRef> getFiniteStrings(FiniteStringsIterator iterator) {
    Set<IntsRef> result = new HashSet<>();
    for (IntsRef finiteString; (finiteString = iterator.next()) != null; ) {
      result.add(IntsRef.deepCopyOf(finiteString));
    }

    return result;
  }

  /**
   * This method creates a random Automaton by generating states at multiple levels. At each level,
   * a random number of states are created, and transitions are added between the states of the
   * current and the previous level randomly, If the 'hasCycle' parameter is true, a transition is
   * added from the first state of the last level back to the initial state to create a cycle in the
   * Automaton..
   *
   * @param hasCycle if true, the generated Automaton will have a cycle; if false, it won't have a
   *     cycle.
   * @return a randomly generated Automaton instance.
   */
  private Automaton generateRandomAutomaton(boolean hasCycle) {
    Automaton a = new Automaton();
    List<Integer> lastLevelStates = new ArrayList<>();
    int initialState = a.createState();
    int maxLevel = TestUtil.nextInt(random(), 4, 9);
    lastLevelStates.add(initialState);

    for (int level = 1; level < maxLevel; level++) {
      int numStates = TestUtil.nextInt(random(), 3, 9);
      List<Integer> nextLevelStates = new ArrayList<>();

      for (int i = 0; i < numStates; i++) {
        int nextState = a.createState();
        nextLevelStates.add(nextState);
      }

      for (int lastState : lastLevelStates) {
        for (int nextState : nextLevelStates) {
          // if hasCycle is enabled, we will always add a transition, so we could make sure the
          // generated Automaton has a cycle.
          if (hasCycle || random().nextInt(7) >= 1) {
            a.addTransition(lastState, nextState, random().nextInt(10));
          }
        }
      }
      lastLevelStates = nextLevelStates;
    }

    if (hasCycle) {
      int lastState = lastLevelStates.get(0);
      a.addTransition(lastState, initialState, random().nextInt(10));
    }

    a.finishState();
    return a;
  }
}
