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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Util;

public class TestDaciukMihovAutomatonBuilder extends LuceneTestCase {

  public void testBasic() throws Exception {
    List<BytesRef> terms = basicTerms();
    Collections.sort(terms);

    Automaton a = build(terms, false);
    checkAutomaton(terms, a, false);
    checkMinimized(a);
  }

  public void testBasicBinary() throws Exception {
    List<BytesRef> terms = basicTerms();
    Collections.sort(terms);

    Automaton a = build(terms, true);
    checkAutomaton(terms, a, true);
    checkMinimized(a);
  }

  public void testRandomMinimized() throws Exception {
    int iters = RandomizedTest.isNightly() ? 20 : 5;
    for (int i = 0; i < iters; i++) {
      boolean buildBinary = random().nextBoolean();
      int size = RandomNumbers.randomIntBetween(random(), 2, 50);
      Set<BytesRef> terms = new HashSet<>();
      List<Automaton> automatonList = new ArrayList<>(size);
      for (int j = 0; j < size; j++) {
        if (buildBinary) {
          BytesRef t = TestUtil.randomBinaryTerm(random(), 8);
          terms.add(t);
          automatonList.add(Automata.makeBinary(t));
        } else {
          String s = TestUtil.randomRealisticUnicodeString(random(), 8);
          terms.add(newBytesRef(s));
          automatonList.add(Automata.makeString(s));
        }
      }
      List<BytesRef> sortedTerms = terms.stream().sorted().collect(Collectors.toList());

      Automaton expected =
          MinimizationOperations.minimize(
              Operations.union(automatonList), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
      Automaton actual = build(sortedTerms, buildBinary);
      assertSameAutomaton(expected, actual);
    }
  }

  public void testRandomUnicodeOnly() throws Exception {
    testRandom(false);
  }

  public void testRandomBinary() throws Exception {
    testRandom(true);
  }

  public void testLargeTerms() throws Exception {
    byte[] b10k = new byte[10_000];
    Arrays.fill(b10k, (byte) 'a');
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> build(Collections.singleton(new BytesRef(b10k)), false));
    assertTrue(
        e.getMessage()
            .startsWith(
                "This builder doesn't allow terms that are larger than "
                    + Automata.MAX_STRING_UNION_TERM_LENGTH
                    + " characters"));

    byte[] b1k = ArrayUtil.copyOfSubArray(b10k, 0, 1000);
    build(Collections.singleton(new BytesRef(b1k)), false); // no exception
  }

  private void testRandom(boolean allowBinary) throws Exception {
    int iters = RandomizedTest.isNightly() ? 50 : 10;
    for (int i = 0; i < iters; i++) {
      int size = RandomNumbers.randomIntBetween(random(), 500, 2_000);
      Set<BytesRef> terms = new HashSet<>(size);
      for (int j = 0; j < size; j++) {
        if (allowBinary && random().nextInt(10) < 2) {
          // Sometimes random bytes term that isn't necessarily valid unicode
          terms.add(newBytesRef(TestUtil.randomBinaryTerm(random())));
        } else {
          terms.add(newBytesRef(TestUtil.randomRealisticUnicodeString(random())));
        }
      }

      List<BytesRef> sorted = terms.stream().sorted().collect(Collectors.toList());
      Automaton a = build(sorted, allowBinary);
      checkAutomaton(sorted, a, allowBinary);
    }
  }

  private void checkAutomaton(List<BytesRef> expected, Automaton a, boolean isBinary) {
    CompiledAutomaton c =
        new CompiledAutomaton(a, true, false, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, isBinary);
    ByteRunAutomaton runAutomaton = c.runAutomaton;

    // Make sure every expected term is accepted
    for (BytesRef t : expected) {
      String readable = isBinary ? t.toString() : t.utf8ToString();
      assertTrue(
          readable + " should be found but wasn't", runAutomaton.run(t.bytes, t.offset, t.length));
    }

    // Make sure every term produced by the automaton is expected
    BytesRefBuilder scratch = new BytesRefBuilder();
    FiniteStringsIterator it = new FiniteStringsIterator(c.automaton);
    for (IntsRef r = it.next(); r != null; r = it.next()) {
      BytesRef t = Util.toBytesRef(r, scratch);
      assertTrue(expected.contains(t));
    }
  }

  private void checkMinimized(Automaton a) {
    Automaton minimized =
        MinimizationOperations.minimize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertSameAutomaton(minimized, a);
  }

  private static void assertSameAutomaton(Automaton a, Automaton b) {
    assertEquals(a.getNumStates(), b.getNumStates());
    assertEquals(a.getNumTransitions(), b.getNumTransitions());
    assertTrue(Operations.sameLanguage(a, b));
  }

  private List<BytesRef> basicTerms() {
    List<BytesRef> terms = new ArrayList<>();
    terms.add(newBytesRef("dog"));
    terms.add(newBytesRef("day"));
    terms.add(newBytesRef("dad"));
    terms.add(newBytesRef("cats"));
    terms.add(newBytesRef("cat"));
    return terms;
  }

  private Automaton build(Collection<BytesRef> terms, boolean asBinary) throws IOException {
    if (random().nextBoolean()) {
      return DaciukMihovAutomatonBuilder.build(terms, asBinary);
    } else {
      return DaciukMihovAutomatonBuilder.build(new TermIterator(terms), asBinary);
    }
  }

  private static final class TermIterator implements BytesRefIterator {
    private final Iterator<BytesRef> it;

    TermIterator(Collection<BytesRef> terms) {
      this.it = terms.iterator();
    }

    @Override
    public BytesRef next() throws IOException {
      if (it.hasNext() == false) {
        return null;
      }
      return it.next();
    }
  }
}
