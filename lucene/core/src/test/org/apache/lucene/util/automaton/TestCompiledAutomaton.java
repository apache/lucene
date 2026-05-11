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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

public class TestCompiledAutomaton extends LuceneTestCase {

  private CompiledAutomaton build(int determinizeWorkLimit, String... strings) {
    final List<BytesRef> terms = new ArrayList<>();
    for (String s : strings) {
      terms.add(new BytesRef(s));
    }
    Collections.sort(terms);
    final Automaton a = Automata.makeStringUnion(terms);
    return new CompiledAutomaton(a, true, false, false);
  }

  private void testFloor(CompiledAutomaton c, String input, String expected) {
    final BytesRef b = new BytesRef(input);
    final BytesRef result = c.floor(b, new BytesRefBuilder());
    if (expected == null) {
      assertNull(result);
    } else {
      assertNotNull(result);
      assertEquals(
          "actual=" + result.utf8ToString() + " vs expected=" + expected + " (input=" + input + ")",
          result,
          new BytesRef(expected));
    }
  }

  private void testTerms(int determinizeWorkLimit, String[] terms) throws Exception {
    final CompiledAutomaton c = build(determinizeWorkLimit, terms);
    final BytesRef[] termBytes = new BytesRef[terms.length];
    for (int idx = 0; idx < terms.length; idx++) {
      termBytes[idx] = new BytesRef(terms[idx]);
    }
    Arrays.sort(termBytes);

    if (VERBOSE) {
      System.out.println("\nTEST: terms in unicode order");
      for (BytesRef t : termBytes) {
        System.out.println("  " + t.utf8ToString());
      }
      // System.out.println(c.utf8.toDot());
    }

    for (int iter = 0; iter < 100 * RANDOM_MULTIPLIER; iter++) {
      final String s =
          random().nextInt(10) == 1 ? terms[random().nextInt(terms.length)] : randomString();
      if (VERBOSE) {
        System.out.println("\nTEST: floor(" + s + ")");
      }
      int loc = Arrays.binarySearch(termBytes, new BytesRef(s));
      final String expected;
      if (loc >= 0) {
        expected = s;
      } else {
        // term doesn't exist
        loc = -(loc + 1);
        if (loc == 0) {
          expected = null;
        } else {
          expected = termBytes[loc - 1].utf8ToString();
        }
      }
      if (VERBOSE) {
        System.out.println("  expected=" + expected);
      }
      testFloor(c, s, expected);
    }
  }

  public void testRandom() throws Exception {
    final int numTerms = atLeast(400);
    final Set<String> terms = new HashSet<>();
    while (terms.size() != numTerms) {
      terms.add(randomString());
    }
    testTerms(numTerms * 100, terms.toArray(String[]::new));
  }

  private String randomString() {
    return TestUtil.randomRealisticUnicodeString(random());
  }

  public void testBasic() throws Exception {
    CompiledAutomaton c = build(Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, "fob", "foo", "goo");
    testFloor(c, "goo", "goo");
    testFloor(c, "ga", "foo");
    testFloor(c, "g", "foo");
    testFloor(c, "foc", "fob");
    testFloor(c, "foz", "foo");
    testFloor(c, "f", null);
    testFloor(c, "", null);
    testFloor(c, "aa", null);
    testFloor(c, "zzz", "goo");
  }

  // LUCENE-6367
  public void testBinaryAll() throws Exception {
    Automaton a = new Automaton();
    int state = a.createState();
    a.setAccept(state, true);
    a.addTransition(state, state, 0, 0xff);
    a.finishState();

    CompiledAutomaton ca = new CompiledAutomaton(a, false, true, true);
    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, ca.type);
  }

  // LUCENE-6367
  public void testUnicodeAll() throws Exception {
    Automaton a = new Automaton();
    int state = a.createState();
    a.setAccept(state, true);
    a.addTransition(state, state, 0, Character.MAX_CODE_POINT);
    a.finishState();

    CompiledAutomaton ca = new CompiledAutomaton(a, false, true, false);
    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, ca.type);
  }

  // LUCENE-6367
  public void testBinarySingleton() throws Exception {
    // This is just ascii so we can pretend it's binary:
    Automaton a = Automata.makeString("foobar");
    CompiledAutomaton ca = new CompiledAutomaton(a, true, true, true);
    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, ca.type);
  }

  // LUCENE-6367
  public void testUnicodeSingleton() throws Exception {
    Automaton a = Automata.makeString(TestUtil.randomRealisticUnicodeString(random()));
    CompiledAutomaton ca = new CompiledAutomaton(a, true, true, false);
    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, ca.type);
  }

  public void testNfaCompiledAutomatonEqualsAndHashCode() {
    Automaton nfa1 = makeSimpleNfa('a');
    Automaton nfa2 = makeSimpleNfa('a');
    assertNotSame(nfa1, nfa2);

    CompiledAutomaton ca1 = new CompiledAutomaton(nfa1, false, true, true);
    CompiledAutomaton ca2 = new CompiledAutomaton(nfa2, false, true, true);

    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, ca1.type);
    assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, ca2.type);
    assertNotNull(ca1.nfaRunAutomaton);
    assertNotNull(ca2.nfaRunAutomaton);

    assertEquals(ca1, ca2);
    assertEquals(ca1.hashCode(), ca2.hashCode());
  }

  public void testNfaCompiledAutomatonHashCodeIsStableForStructuralEquals() {
    CompiledAutomaton ca1 = new CompiledAutomaton(makeSimpleNfa('z'), false, true, true);
    CompiledAutomaton ca2 = new CompiledAutomaton(makeSimpleNfa('z'), false, true, true);
    CompiledAutomaton ca3 = new CompiledAutomaton(makeSimpleNfa('z'), false, true, true);

    assertEquals(ca1, ca2);
    assertEquals(ca2, ca3);
    assertEquals(ca1.hashCode(), ca2.hashCode());
    assertEquals(ca2.hashCode(), ca3.hashCode());
  }

  public void testNfaCompiledAutomatonNotEquals() {
    CompiledAutomaton ca1 = new CompiledAutomaton(makeSimpleNfa('a'), false, true, true);
    CompiledAutomaton ca2 = new CompiledAutomaton(makeSimpleNfa('b'), false, true, true);

    assertNotEquals(ca1, ca2);
  }

  public void testDfaCompiledAutomatonEqualsAndHashCode() {
    CompiledAutomaton ca1 =
        new CompiledAutomaton(makeDeterministicNormalAutomaton('a', 'b'), false, false, true);
    CompiledAutomaton ca2 =
        new CompiledAutomaton(makeDeterministicNormalAutomaton('a', 'b'), false, false, true);

    assertNotNull(ca1.runAutomaton);
    assertNotNull(ca2.runAutomaton);
    assertNull(ca1.nfaRunAutomaton);
    assertNull(ca2.nfaRunAutomaton);

    assertEquals(ca1, ca2);
    assertEquals(ca1.hashCode(), ca2.hashCode());
  }

  public void testDfaCompiledAutomatonNotEquals() {
    CompiledAutomaton ca1 =
        new CompiledAutomaton(makeDeterministicNormalAutomaton('a', 'b'), false, false, true);
    CompiledAutomaton ca2 =
        new CompiledAutomaton(makeDeterministicNormalAutomaton('a', 'c'), false, false, true);

    assertNotEquals(ca1, ca2);
  }

  public void testDfaAndNfaCompiledAutomataAreNotEqual() {
    CompiledAutomaton dfa =
        new CompiledAutomaton(makeDeterministicNormalAutomaton('a', 'b'), false, false, true);
    CompiledAutomaton nfa = new CompiledAutomaton(makeSimpleNfa('a'), false, true, true);

    assertNotNull(dfa.runAutomaton);
    assertNull(dfa.nfaRunAutomaton);
    assertNull(nfa.runAutomaton);
    assertNotNull(nfa.nfaRunAutomaton);
    assertNotEquals(dfa, nfa);
    assertNotEquals(nfa, dfa);
  }

  public void testVisitUsesMatchingCallbackForNfaAutomaton() {
    CompiledAutomaton ca = new CompiledAutomaton(makeSimpleNfa('a'), false, true, true);
    Query parent = new TermQuery(new Term("f", "x"));

    final boolean[] matchingCalled = new boolean[1];
    QueryVisitor visitor =
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            matchingCalled[0] = true;
            assertNotNull(automaton.get());
          }
        };

    ca.visit(visitor, parent, "f");
    assertTrue(matchingCalled[0]);
  }

  private static Automaton makeSimpleNfa(char ch) {
    return makeDeterministicNormalAutomaton(ch, ch);
  }

  private static Automaton makeDeterministicNormalAutomaton(char first, char second) {
    Automaton automaton = new Automaton();
    int start = automaton.createState();
    int accept1 = automaton.createState();
    int accept2 = automaton.createState();
    automaton.setAccept(accept1, true);
    automaton.setAccept(accept2, true);
    automaton.addTransition(start, accept1, first, first);
    automaton.addTransition(start, accept2, second, second);
    automaton.finishState();
    return automaton;
  }
}
