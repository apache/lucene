package org.apache.lucene.util.automaton;

import java.util.Arrays;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestNFARunAutomaton extends LuceneTestCase {

  public void testRandom() {
    for (int i = 0; i < 100; i++) {
      RegExp regExp = null;
      while (regExp == null) {
        try {
          regExp = new RegExp(AutomatonTestUtil.randomRegexp(random()));
        } catch (IllegalArgumentException e) {
          ignoreException(e);
        }
      }
      Automaton dfa = regExp.toAutomaton();
      NFARunAutomaton candidate = new NFARunAutomaton(regExp.toNFA());
      AutomatonTestUtil.RandomAcceptedStrings randomStringGen =
          new AutomatonTestUtil.RandomAcceptedStrings(dfa);

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

  private void ignoreException(Exception e) {
    // do nothing
  }
}
