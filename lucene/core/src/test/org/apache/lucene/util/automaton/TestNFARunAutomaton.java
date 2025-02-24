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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.IntsRef;
import org.junit.Assert;

public class TestNFARunAutomaton extends LuceneTestCase {

  private static final String FIELD = "field";

  public void testRamUsageEstimation() {
    RegExp regExp = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
    Automaton nfa = regExp.toAutomaton();
    NFARunAutomaton runAutomaton = new NFARunAutomaton(nfa);
    long estimation = runAutomaton.ramBytesUsed();
    long actual = RamUsageTester.ramUsed(runAutomaton);
    Assert.assertEquals((double) actual, (double) estimation, (double) actual * 0.3);
  }

  @SuppressWarnings("unused")
  public void testWithRandomRegex() {
    for (int i = 0; i < 100; i++) {
      RegExp regExp = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE);
      Automaton nfa = regExp.toAutomaton();
      if (nfa.isDeterministic()) {
        i--;
        continue;
      }
      Automaton dfa = Operations.determinize(nfa, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
      NFARunAutomaton candidate = new NFARunAutomaton(nfa);
      AutomatonTestUtil.RandomAcceptedStrings randomStringGen;
      try {
        randomStringGen = new AutomatonTestUtil.RandomAcceptedStrings(dfa);
      } catch (IllegalArgumentException e) {
        i--;
        continue; // sometimes the automaton accept nothing and throw this exception
      }

      for (int round = 0; round < 20; round++) {
        // test order of accepted strings and random (likely rejected) strings alternatively to make
        // sure caching system works correctly
        if (random().nextBoolean()) {
          testAcceptedString(regExp, randomStringGen, candidate, 10);
          testRandomString(regExp, dfa, candidate, 10);
        } else {
          testRandomString(regExp, dfa, candidate, 10);
          testAcceptedString(regExp, randomStringGen, candidate, 10);
        }
      }
    }
  }

  public void testRandomAccessTransition() {
    Automaton nfa = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE).toAutomaton();
    while (nfa.isDeterministic()) {
      nfa = new RegExp(AutomatonTestUtil.randomRegexp(random()), RegExp.NONE).toAutomaton();
    }
    NFARunAutomaton runAutomaton1, runAutomaton2;
    runAutomaton1 = new NFARunAutomaton(nfa);
    runAutomaton2 = new NFARunAutomaton(nfa);
    assertRandomAccessTransition(runAutomaton1, runAutomaton2, 0, new HashSet<>());
  }

  private void assertRandomAccessTransition(
      NFARunAutomaton automaton1, NFARunAutomaton automaton2, int state, Set<Integer> visited) {
    if (visited.contains(state)) {
      return;
    }
    visited.add(state);

    Transition t1 = new Transition();
    Transition t2 = new Transition();
    automaton1.initTransition(state, t1);
    if (random().nextBoolean()) {
      // init is not really necessary for t2
      automaton2.initTransition(state, t2);
    }
    int numStates = automaton2.getNumTransitions(state);
    for (int i = 0; i < numStates; i++) {
      automaton1.getNextTransition(t1);
      automaton2.getTransition(state, i, t2);
      assertEquals(t1.toString(), t2.toString());
      assertRandomAccessTransition(automaton1, automaton2, t1.dest, visited);
    }
  }

  public void testRandomAutomatonQuery() throws IOException {
    final int docNum = 50;
    final int automatonNum = 50;
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    Set<String> vocab = new HashSet<>();
    Set<String> perDocVocab = new HashSet<>();
    for (int i = 0; i < docNum; i++) {
      perDocVocab.clear();
      int termNum = random().nextInt(20) + 30;
      while (perDocVocab.size() < termNum) {
        String randomString;
        while ((randomString = TestUtil.randomUnicodeString(random())).length() == 0) {}
        perDocVocab.add(randomString);
        vocab.add(randomString);
      }
      Document document = new Document();
      document.add(
          newTextField(
              FIELD, perDocVocab.stream().reduce("", (s1, s2) -> s1 + " " + s2), Field.Store.NO));
      writer.addDocument(document);
    }
    writer.commit();
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    Set<String> foreignVocab = new HashSet<>();
    while (foreignVocab.size() < vocab.size()) {
      String randomString;
      while ((randomString = TestUtil.randomUnicodeString(random())).length() == 0) {}
      foreignVocab.add(randomString);
    }

    ArrayList<String> vocabList = new ArrayList<>(vocab);
    ArrayList<String> foreignVocabList = new ArrayList<>(foreignVocab);

    Set<String> perQueryVocab = new HashSet<>();

    for (int i = 0; i < automatonNum; i++) {
      perQueryVocab.clear();
      int termNum = random().nextInt(40) + 30;
      while (perQueryVocab.size() < termNum) {
        if (random().nextBoolean()) {
          perQueryVocab.add(vocabList.get(random().nextInt(vocabList.size())));
        } else {
          perQueryVocab.add(foreignVocabList.get(random().nextInt(foreignVocabList.size())));
        }
      }
      Automaton a = null;
      for (String term : perQueryVocab) {
        if (a == null) {
          a = Automata.makeString(term);
        } else {
          a = Operations.union(List.of(a, Automata.makeString(term)));
        }
      }
      if (a.isDeterministic()) {
        i--;
        continue;
      }
      AutomatonQuery dfaQuery =
          new AutomatonQuery(
              new Term(FIELD),
              Operations.determinize(a, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
      AutomatonQuery nfaQuery = new AutomatonQuery(new Term(FIELD), a);
      assertNotNull(nfaQuery.getCompiled().nfaRunAutomaton);
      assertEquals(searcher.count(dfaQuery), searcher.count(nfaQuery));
    }
    reader.close();
    writer.close();
    directory.close();
  }

  private void testAcceptedString(
      RegExp regExp,
      AutomatonTestUtil.RandomAcceptedStrings randomStringGen,
      NFARunAutomaton candidate,
      int repeat) {
    for (int n = 0; n < repeat; n++) {
      int[] acceptedString = randomStringGen.getRandomAcceptedString(random());
      assertTrue(
          "regExp: " + regExp + " testString: " + Arrays.toString(acceptedString),
          candidate.run(acceptedString));
    }
  }

  private void testRandomString(
      RegExp regExp, Automaton dfa, NFARunAutomaton candidate, int repeat) {
    for (int n = 0; n < repeat; n++) {
      int[] randomString =
          random().ints(random().nextInt(50), 0, Character.MAX_CODE_POINT).toArray();
      assertEquals(
          "regExp: " + regExp + " testString: " + Arrays.toString(randomString),
          Operations.run(dfa, new IntsRef(randomString, 0, randomString.length)),
          candidate.run(randomString));
    }
  }
}
