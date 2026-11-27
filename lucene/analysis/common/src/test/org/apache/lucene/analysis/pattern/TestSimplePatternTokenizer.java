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
package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

public class TestSimplePatternTokenizer extends BaseTokenStreamTestCase {

  public void testGreedy() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("(foo)+");
    t.setReader(new StringReader("bar foofoo baz"));
    assertTokenStreamContents(t, new String[] {"foofoo"}, new int[] {4}, new int[] {10});
  }

  public void testBigLookahead() throws Exception {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      b.append('a');
    }
    b.append('b');
    Tokenizer t = new SimplePatternTokenizer(b.toString());

    b = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      b.append('a');
    }
    t.setReader(new StringReader(b.toString()));
    t.reset();
    assertFalse(t.incrementToken());
  }

  public void testOneToken() throws Exception {
    Tokenizer t = new SimplePatternTokenizer(".*");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    String s;
    do {
      s = TestUtil.randomUnicodeString(random());
    } while (s.isEmpty());
    t.setReader(new StringReader(s));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals(s, termAtt.toString());
  }

  public void testEmptyStringPatternNoMatch() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a*");
    t.setReader(new StringReader("bbb"));
    t.reset();
    assertFalse(t.incrementToken());
  }

  public void testEmptyStringPatternOneMatch() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a*");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    t.setReader(new StringReader("bbab"));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals("a", termAtt.toString());
    assertFalse(t.incrementToken());
  }

  public void testEndOffset() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a+");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = t.getAttribute(OffsetAttribute.class);
    t.setReader(new StringReader("aaabbb"));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals("aaa", termAtt.toString());
    assertFalse(t.incrementToken());
    t.end();
    assertEquals(6, offsetAtt.endOffset());
  }

  public void testFixedToken() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("aaaa");

    t.setReader(new StringReader("aaaaaaaaaaaaaaa"));
    assertTokenStreamContents(
        t, new String[] {"aaaa", "aaaa", "aaaa"}, new int[] {0, 4, 8}, new int[] {4, 8, 12});
  }

  public void testBasic() throws Exception {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // pattern        input                    output
      {":", "boo:and:foo", ": :"},
      {qpattern, "aaa 'bbb' 'ccc'", "'bbb' 'ccc'"},
    };

    for (String[] test : tests) {
      Tokenizer stream = new SimplePatternTokenizer(test[0]);
      stream.setReader(new StringReader(test[1]));
      String out = tsToString(stream);

      assertEquals("pattern: " + test[0] + " with input: " + test[1], test[2], out);
    }
  }

  public void testNotDeterminized() throws Exception {
    Automaton a = new Automaton();
    int start = a.createState();
    int mid1 = a.createState();
    int mid2 = a.createState();
    int end = a.createState();
    a.setAccept(end, true);
    a.addTransition(start, mid1, 'a', 'z');
    a.addTransition(start, mid2, 'a', 'z');
    a.addTransition(mid1, end, 'b');
    a.addTransition(mid2, end, 'b');
    a.finishState();
    assertFalse(a.isDeterministic());
    Tokenizer t = new SimplePatternTokenizer(a);
    t.setReader(new StringReader("xb ab c"));
    assertTokenStreamContents(t, new String[] {"xb", "ab"}, new int[] {0, 3}, new int[] {2, 5});
  }

  public void testNonDeterministicRegExp() throws Exception {
    assertFalse(new RegExp("ab|ac").toAutomaton().isDeterministic());
    Tokenizer t = new SimplePatternTokenizer("ab|ac");
    t.setReader(new StringReader("ab ac ad"));
    assertTokenStreamContents(t, new String[] {"ab", "ac"}, new int[] {0, 3}, new int[] {2, 5});
  }

  public void testNonDeterministicRegExpGreedy() throws Exception {
    assertFalse(new RegExp("[a-z]+|[a-z]+\\.[a-z]+").toAutomaton().isDeterministic());
    Tokenizer t = new SimplePatternTokenizer("[a-z]+|[a-z]+\\.[a-z]+");
    t.setReader(new StringReader("foo.bar baz"));
    assertTokenStreamContents(
        t, new String[] {"foo.bar", "baz"}, new int[] {0, 8}, new int[] {7, 11});
  }

  public void testTooComplexToDeterminize() throws Exception {
    String regexp = "(a|b)*a(a|b){20}";
    expectThrows(
        TooComplexToDeterminizeException.class,
        () ->
            Operations.determinize(
                new RegExp(regexp).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    String token = "a" + "b".repeat(20);
    Tokenizer t = new SimplePatternTokenizer(regexp);
    t.setReader(new StringReader("c " + token + " c"));
    assertTokenStreamContents(t, new String[] {token}, new int[] {2}, new int[] {23});
  }

  public void testRandomNFAMatchesDFA() throws Exception {
    checkRandomNFAMatchesDFA(random(), SimplePatternTokenizer::new);
  }

  /**
   * Checks that tokenizers built from random non-deterministic automata produce the same tokens as
   * tokenizers built from their determinized equivalents. Shared with {@link
   * TestSimplePatternSplitTokenizer}.
   */
  static void checkRandomNFAMatchesDFA(Random random, Function<Automaton, Tokenizer> newTokenizer)
      throws IOException {
    int iters = atLeast(random, 100);
    for (int iter = 0; iter < iters; iter++) {
      Automaton nfa = new RegExp(AutomatonTestUtil.randomRegexp(random), RegExp.NONE).toAutomaton();
      if (nfa.isDeterministic()) {
        iter--;
        continue;
      }
      Automaton dfa;
      AutomatonTestUtil.RandomAcceptedStrings acceptedStrings;
      try {
        dfa = Operations.determinize(nfa, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        acceptedStrings =
            Operations.isEmpty(dfa) ? null : new AutomatonTestUtil.RandomAcceptedStrings(dfa);
      } catch (TooComplexToDeterminizeException _) {
        iter--;
        continue;
      }
      // reuse the same NFA tokenizer for several inputs, to exercise its lazily built states
      Tokenizer nfaTokenizer = newTokenizer.apply(nfa);
      for (int round = 0; round < 10; round++) {
        StringBuilder input = new StringBuilder();
        int pieces = TestUtil.nextInt(random, 1, 5);
        for (int piece = 0; piece < pieces; piece++) {
          input.append(TestUtil.randomUnicodeString(random, 3));
          if (acceptedStrings != null) {
            int[] codePoints = acceptedStrings.getRandomAcceptedString(random);
            input.append(new String(codePoints, 0, codePoints.length));
          }
        }
        assertSameTokens(newTokenizer.apply(dfa), nfaTokenizer, input.toString());
      }
    }
  }

  private static void assertSameTokens(Tokenizer expected, Tokenizer actual, String input)
      throws IOException {
    CharTermAttribute termAtt = expected.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = expected.addAttribute(OffsetAttribute.class);
    List<String> terms = new ArrayList<>();
    List<Integer> startOffsets = new ArrayList<>();
    List<Integer> endOffsets = new ArrayList<>();
    expected.setReader(new StringReader(input));
    expected.reset();
    while (expected.incrementToken()) {
      terms.add(termAtt.toString());
      startOffsets.add(offsetAtt.startOffset());
      endOffsets.add(offsetAtt.endOffset());
    }
    expected.end();
    int finalOffset = offsetAtt.endOffset();
    expected.close();

    actual.setReader(new StringReader(input));
    assertTokenStreamContents(
        actual,
        terms.toArray(new String[0]),
        startOffsets.stream().mapToInt(Integer::intValue).toArray(),
        endOffsets.stream().mapToInt(Integer::intValue).toArray(),
        finalOffset);
  }

  public void testOffsetCorrection() throws Exception {
    final String INPUT = "G&uuml;nther G&uuml;nther is here";

    // create MappingCharFilter
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("&uuml;", "ü");
    NormalizeCharMap normMap = builder.build();
    CharFilter charStream = new MappingCharFilter(normMap, new StringReader(INPUT));

    // create SimplePatternTokenizer
    Tokenizer stream = new SimplePatternTokenizer("Günther");
    stream.setReader(charStream);
    assertTokenStreamContents(
        stream,
        new String[] {"Günther", "Günther"},
        new int[] {0, 13},
        new int[] {12, 25},
        INPUT.length());
  }

  /** TODO: rewrite tests not to use string comparison. */
  private static String tsToString(TokenStream in) throws IOException {
    StringBuilder out = new StringBuilder();
    CharTermAttribute termAtt = in.addAttribute(CharTermAttribute.class);
    // extra safety to enforce, that the state is not preserved and also
    // assign bogus values
    in.clearAttributes();
    termAtt.setEmpty().append("bogusTerm");
    in.reset();
    while (in.incrementToken()) {
      if (out.length() > 0) {
        out.append(' ');
      }
      out.append(termAtt);
      in.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
    }

    in.close();
    return out.toString();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new SimplePatternTokenizer("a");
            return new TokenStreamComponents(tokenizer);
          }
        };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();

    Analyzer b =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new SimplePatternTokenizer("a");
            return new TokenStreamComponents(tokenizer);
          }
        };
    checkRandomData(random(), b, 200 * RANDOM_MULTIPLIER);
    b.close();

    assertFalse(new RegExp("[a-z]+|[a-z]+\\.[a-z]+").toAutomaton().isDeterministic());
    Analyzer c =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new SimplePatternTokenizer("[a-z]+|[a-z]+\\.[a-z]+");
            return new TokenStreamComponents(tokenizer);
          }
        };
    checkRandomData(random(), c, 200 * RANDOM_MULTIPLIER);
    c.close();
  }

  public void testEndLookahead() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("(ab)+");
    t.setReader(new StringReader("aba"));
    assertTokenStreamContents(t, new String[] {"ab"}, new int[] {0}, new int[] {2}, 3);
  }
}
