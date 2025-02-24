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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;

/**
 * Simple unit tests for RegExp parsing.
 *
 * <p>For each type of node, test the toString() and parse tree, test the resulting automaton's
 * language, and test properties such as minimal, deterministic, no dead states
 */
public class TestRegExpParsing extends LuceneTestCase {

  public void testAnyChar() {
    RegExp re = new RegExp(".");
    assertEquals(".", re.toString());
    assertEquals("REGEXP_ANYCHAR\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeAnyChar();
    assertSameLanguage(expected, actual);
  }

  public void testAnyString() {
    RegExp re = new RegExp("@", RegExp.ANYSTRING);
    assertEquals("@", re.toString());
    assertEquals("REGEXP_ANYSTRING\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeAnyString();
    assertSameLanguage(expected, actual);
  }

  public void testChar() {
    RegExp re = new RegExp("c");
    assertEquals("\\c", re.toString());
    assertEquals("REGEXP_CHAR char=c\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('c');
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveChar() {
    RegExp re = new RegExp("c", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\\c", re.toString());
    assertEquals("REGEXP_CHAR char=c\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharSet(new int[] {'c', 'C'});
    assertSameLanguage(expected, actual);
  }

  // individual characters (only) inside a class are treated as case insensitive.
  public void testCaseInsensitiveClassChar() {
    RegExp re = new RegExp("[c]", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0043 U+0063] ends=[U+0043 U+0063]\n", re.toStringTree());
    AutomatonTestUtil.assertMinimalDFA(re.toAutomaton());
  }

  // ranges aren't treated as case-insensitive, but maybe ok with charclass
  // instead of adding range, expand it: iterate each codepoint, adding its alternatives
  public void testCaseInsensitiveClassRange() {
    RegExp re = new RegExp("[c-d]", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("REGEXP_CHAR_RANGE from=c to=d\n", re.toStringTree());
    AutomatonTestUtil.assertMinimalDFA(re.toAutomaton());
  }

  public void testCaseInsensitiveCharUpper() {
    RegExp re = new RegExp("C", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\\C", re.toString());
    assertEquals("REGEXP_CHAR char=C\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharSet(new int[] {'c', 'C'});
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveCharNotSensitive() {
    RegExp re = new RegExp("4", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\\4", re.toString());
    assertEquals("REGEXP_CHAR char=4\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('4');
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveCharNonAscii() {
    RegExp re = new RegExp("Ж", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\\Ж", re.toString());
    assertEquals("REGEXP_CHAR char=Ж\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharSet(new int[] {'Ж', 'ж'});
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveCharUnicode() {
    RegExp re = new RegExp("Ж", RegExp.NONE, RegExp.CASE_INSENSITIVE);
    assertEquals("\\Ж", re.toString());
    assertEquals("REGEXP_CHAR char=Ж\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharSet(new int[] {'Ж', 'ж'});
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveCharUnicodeSigma() {
    RegExp re = new RegExp("σ", RegExp.NONE, RegExp.CASE_INSENSITIVE);
    assertEquals("\\σ", re.toString());
    assertEquals("REGEXP_CHAR char=σ\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharSet(new int[] {'Σ', 'σ', 'ς'});
    assertSameLanguage(expected, actual);
  }

  public void testNegatedChar() {
    RegExp re = new RegExp("[^c]");
    // TODO: would be nice to emit negated class rather than this
    assertEquals("(.&~(\\c))", re.toString());
    assertEquals(
        String.join(
            "\n",
            "REGEXP_INTERSECTION",
            "  REGEXP_ANYCHAR",
            "  REGEXP_COMPLEMENT",
            "    REGEXP_CHAR char=c\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.union(
            List.of(
                Automata.makeCharRange(0, 'b'), Automata.makeCharRange('d', Integer.MAX_VALUE)));
    assertSameLanguage(expected, actual);
  }

  public void testNegatedClass() {
    RegExp re = new RegExp("[^c-da]");
    assertEquals(
        String.join(
            "\n",
            "REGEXP_INTERSECTION",
            "  REGEXP_ANYCHAR",
            "  REGEXP_COMPLEMENT",
            "    REGEXP_CHAR_CLASS starts=[U+0063 U+0061] ends=[U+0064 U+0061]\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);
  }

  public void testCharRange() {
    RegExp re = new RegExp("[b-d]");
    assertEquals("[\\b-\\d]", re.toString());
    assertEquals("REGEXP_CHAR_RANGE from=b to=d\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharRange('b', 'd');
    assertSameLanguage(expected, actual);
  }

  public void testNegatedCharRange() {
    RegExp re = new RegExp("[^b-d]");
    // TODO: would be nice to emit negated class rather than this
    assertEquals("(.&~([\\b-\\d]))", re.toString());
    assertEquals(
        String.join(
            "\n",
            "REGEXP_INTERSECTION",
            "  REGEXP_ANYCHAR",
            "  REGEXP_COMPLEMENT",
            "    REGEXP_CHAR_RANGE from=b to=d\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.union(
            List.of(
                Automata.makeCharRange(0, 'a'), Automata.makeCharRange('e', Integer.MAX_VALUE)));
    assertSameLanguage(expected, actual);
  }

  public void testIllegalCharRange() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("[z-a]");
        });
  }

  public void testCharClassDigit() {
    RegExp re = new RegExp("[\\d]");
    assertEquals("[\\0-\\9]", re.toString());
    assertEquals("REGEXP_CHAR_RANGE from=0 to=9\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeCharRange('0', '9');
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonDigit() {
    RegExp re = new RegExp("[\\D]");
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0000 U+003A] ends=[U+002F U+10FFFF]\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.minus(
            Automata.makeAnyChar(),
            Automata.makeCharRange('0', '9'),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertSameLanguage(expected, actual);
  }

  public void testCharClassWhitespace() {
    RegExp re = new RegExp("[\\s]");
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0009 U+000D U+0020] ends=[U+000A U+000D U+0020]\n",
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.union(
            List.of(
                Automata.makeChar(' '),
                Automata.makeChar('\n'),
                Automata.makeChar('\r'),
                Automata.makeChar('\t')));
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonWhitespace() {
    RegExp re = new RegExp("[\\S]");
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0000 U+000B U+000E U+0021] ends=[U+0008 U+000C U+001F U+10FFFF]\n",
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeAnyChar();
    expected =
        Operations.minus(
            expected, Automata.makeChar(' '), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeChar('\n'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeChar('\r'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeChar('\t'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertSameLanguage(expected, actual);
  }

  public void testCharClassWord() {
    RegExp re = new RegExp("[\\w]");
    assertEquals("[\\0-\\9\\A-\\Z\\_\\a-\\z]", re.toString());
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0030 U+0041 U+005F U+0061] ends=[U+0039 U+005A U+005F U+007A]\n",
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.union(
            List.of(
                Automata.makeCharRange('a', 'z'),
                Automata.makeCharRange('A', 'Z'),
                Automata.makeCharRange('0', '9'),
                Automata.makeChar('_')));
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonWord() {
    RegExp re = new RegExp("[\\W]");
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0000 U+003A U+005B U+0060 U+007B] ends=[U+002F U+0040 U+005E U+0060 U+10FFFF]\n",
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeAnyChar();
    expected =
        Operations.minus(
            expected, Automata.makeCharRange('a', 'z'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeCharRange('A', 'Z'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeCharRange('0', '9'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    expected =
        Operations.minus(
            expected, Automata.makeChar('_'), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertSameLanguage(expected, actual);
  }

  // char class with a couple of ranges, predefined,and individual chars
  public void testJumboCharClass() {
    RegExp re = new RegExp("[0-5a\\sbc-d]");
    assertEquals(
        "REGEXP_CHAR_CLASS starts=[U+0030 U+0061 U+0009 U+000D U+0020 U+0062 U+0063] ends=[U+0035 U+0061 U+000A U+000D U+0020 U+0062 U+0064]\n",
        re.toStringTree());
    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);
  }

  public void testTruncatedCharClass() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("[b-d");
        });
  }

  public void testBogusCharClass() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("[\\q]");
        });
  }

  public void testExcapedNotCharClass() {
    RegExp re = new RegExp("[\\?]");
    assertEquals("\\?", re.toString());
    assertEquals("REGEXP_CHAR char=?\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('?');
    assertSameLanguage(expected, actual);
  }

  public void testExcapedSlashNotCharClass() {
    RegExp re = new RegExp("[\\\\]");
    assertEquals("\\\\", re.toString());
    assertEquals("REGEXP_CHAR char=\\\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('\\');
    assertSameLanguage(expected, actual);
  }

  public void testEscapedDashCharClass() {
    RegExp re = new RegExp("[\\-]");
    assertEquals("REGEXP_CHAR char=-\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('-');
    assertSameLanguage(expected, actual);
  }

  public void testEmpty() {
    RegExp re = new RegExp("#", RegExp.EMPTY);
    assertEquals("#", re.toString());
    assertEquals("REGEXP_EMPTY\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeEmpty();
    assertSameLanguage(expected, actual);
  }

  public void testEmptyClass() {
    Exception expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new RegExp("[]");
            });
    assertEquals("expected ']' at position 2", expected.getMessage());
  }

  public void testEscapedInvalidClass() {
    Exception expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new RegExp("[\\]");
            });
    assertEquals("expected ']' at position 3", expected.getMessage());
  }

  public void testInterval() {
    RegExp re = new RegExp("<5-40>");
    assertEquals("<5-40>", re.toString());
    assertEquals("REGEXP_INTERVAL<5-40>\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    // TODO: numeric intervals are NFAs
    AutomatonTestUtil.assertCleanNFA(actual);

    Automaton expected = Automata.makeDecimalInterval(5, 40, 0);
    assertSameLanguage(expected, actual);
  }

  public void testBackwardsInterval() {
    RegExp re = new RegExp("<40-5>");
    assertEquals("<5-40>", re.toString());
    assertEquals("REGEXP_INTERVAL<5-40>\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    // TODO: numeric intervals are NFAs
    AutomatonTestUtil.assertCleanNFA(actual);

    Automaton expected = Automata.makeDecimalInterval(5, 40, 0);
    assertSameLanguage(expected, actual);
  }

  public void testTruncatedInterval() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("<1-");
        });
  }

  public void testTruncatedInterval2() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("<1");
        });
  }

  public void testEmptyInterval() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("<->");
        });
  }

  public void testOptional() {
    RegExp re = new RegExp("a?");
    assertEquals("(\\a)?", re.toString());
    assertEquals(String.join("\n", "REGEXP_OPTIONAL", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Operations.optional(Automata.makeChar('a'));
    assertSameLanguage(expected, actual);
  }

  public void testRepeat0() {
    RegExp re = new RegExp("a*");
    assertEquals("(\\a)*", re.toString());
    assertEquals(String.join("\n", "REGEXP_REPEAT", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Operations.repeat(Automata.makeChar('a'));
    assertSameLanguage(expected, actual);
  }

  public void testRepeat1() {
    RegExp re = new RegExp("a+");
    assertEquals("(\\a){1,}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MIN min=1", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    // not minimal, but close: minimal + 1
    assertEquals(3, actual.getNumStates());
    AutomatonTestUtil.assertCleanDFA(actual);

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 1);
    assertSameLanguage(expected, actual);
  }

  public void testRepeatN() {
    RegExp re = new RegExp("a{5}");
    assertEquals("(\\a){5,5}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MINMAX min=5 max=5", "  REGEXP_CHAR char=a\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 5, 5);
    assertSameLanguage(expected, actual);
  }

  public void testRepeatNPlus() {
    RegExp re = new RegExp("a{5,}");
    assertEquals("(\\a){5,}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MIN min=5", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    // not minimal, but close: minimal + 1
    assertEquals(7, actual.getNumStates());
    AutomatonTestUtil.assertCleanDFA(actual);

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 5);
    assertSameLanguage(expected, actual);
  }

  public void testRepeatMN() {
    RegExp re = new RegExp("a{5,8}");
    assertEquals("(\\a){5,8}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MINMAX min=5 max=8", "  REGEXP_CHAR char=a\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 5, 8);
    assertSameLanguage(expected, actual);
  }

  public void testTruncatedRepeat() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("a{5,8");
        });
  }

  public void testBogusRepeat() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("a{Z}");
        });
  }

  public void testString() {
    RegExp re = new RegExp("boo");
    assertEquals("\"boo\"", re.toString());
    assertEquals("REGEXP_STRING string=boo\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeString("boo");
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveString() {
    RegExp re = new RegExp("boo", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\"boo\"", re.toString());
    assertEquals("REGEXP_STRING string=boo\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton c1 = Operations.union(List.of(Automata.makeChar('b'), Automata.makeChar('B')));
    Automaton c2 = Operations.union(List.of(Automata.makeChar('o'), Automata.makeChar('O')));

    Automaton expected = Operations.concatenate(List.of(c1, c2, c2));
    assertSameLanguage(expected, actual);
  }

  public void testExplicitString() {
    RegExp re = new RegExp("\"boo\"");
    assertEquals("\"boo\"", re.toString());
    assertEquals("REGEXP_STRING string=boo\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeString("boo");
    assertSameLanguage(expected, actual);
  }

  public void testNotTerminatedString() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("\"boo");
        });
  }

  public void testConcatenation() {
    RegExp re = new RegExp("[b-c][e-f]");
    assertEquals("[\\b-\\c][\\e-\\f]", re.toString());
    assertEquals(
        String.join(
            "\n",
            "REGEXP_CONCATENATION",
            "  REGEXP_CHAR_RANGE from=b to=c",
            "  REGEXP_CHAR_RANGE from=e to=f\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.concatenate(
            List.of(Automata.makeCharRange('b', 'c'), Automata.makeCharRange('e', 'f')));
    assertSameLanguage(expected, actual);
  }

  public void testIntersection() {
    RegExp re = new RegExp("[b-f]&[e-f]");
    assertEquals("([\\b-\\f]&[\\e-\\f])", re.toString());
    assertEquals(
        String.join(
            "\n",
            "REGEXP_INTERSECTION",
            "  REGEXP_CHAR_RANGE from=b to=f",
            "  REGEXP_CHAR_RANGE from=e to=f\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.intersection(Automata.makeCharRange('b', 'f'), Automata.makeCharRange('e', 'f'));
    assertSameLanguage(expected, actual);
  }

  public void testTruncatedIntersection() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("a&");
        });
  }

  public void testTruncatedIntersectionParens() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("(a)&(");
        });
  }

  public void testUnion() {
    RegExp re = new RegExp("[b-c]|[e-f]");
    assertEquals("([\\b-\\c]|[\\e-\\f])", re.toString());
    assertEquals(
        String.join(
            "\n",
            "REGEXP_UNION",
            "  REGEXP_CHAR_RANGE from=b to=c",
            "  REGEXP_CHAR_RANGE from=e to=f\n"),
        re.toStringTree());

    Automaton actual = re.toAutomaton();
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected =
        Operations.union(
            List.of(Automata.makeCharRange('b', 'c'), Automata.makeCharRange('e', 'f')));
    assertSameLanguage(expected, actual);
  }

  public void testTruncatedUnion() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("a|");
        });
  }

  public void testTruncatedUnionParens() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("(a)|(");
        });
  }

  public void testAutomaton() {
    AutomatonProvider myProvider =
        new AutomatonProvider() {
          @Override
          public Automaton getAutomaton(String name) {
            return Automata.makeChar('z');
          }
        };
    RegExp re = new RegExp("<myletter>", RegExp.ALL);
    assertEquals("<myletter>", re.toString());
    assertEquals("REGEXP_AUTOMATON\n", re.toStringTree());
    assertEquals(Set.of("myletter"), re.getIdentifiers());

    Automaton actual = re.toAutomaton(myProvider);
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('z');
    assertSameLanguage(expected, actual);
  }

  public void testAutomatonMap() {
    RegExp re = new RegExp("<myletter>", RegExp.ALL);
    assertEquals("<myletter>", re.toString());
    assertEquals("REGEXP_AUTOMATON\n", re.toStringTree());
    assertEquals(Set.of("myletter"), re.getIdentifiers());

    Automaton actual = re.toAutomaton(Map.of("myletter", Automata.makeChar('z')));
    AutomatonTestUtil.assertMinimalDFA(actual);

    Automaton expected = Automata.makeChar('z');
    assertSameLanguage(expected, actual);
  }

  public void testAutomatonIOException() {
    AutomatonProvider myProvider =
        new AutomatonProvider() {
          @Override
          public Automaton getAutomaton(String name) throws IOException {
            throw new IOException("fake ioexception");
          }
        };
    RegExp re = new RegExp("<myletter>", RegExp.ALL);
    assertEquals("<myletter>", re.toString());
    assertEquals("REGEXP_AUTOMATON\n", re.toStringTree());
    assertEquals(Set.of("myletter"), re.getIdentifiers());

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          re.toAutomaton(myProvider);
        });
  }

  public void testAutomatonNotFound() {
    RegExp re = new RegExp("<bogus>", RegExp.ALL);
    assertEquals("<bogus>", re.toString());
    assertEquals("REGEXP_AUTOMATON\n", re.toStringTree());

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          re.toAutomaton(Map.of("myletter", Automata.makeChar('z')));
        });
  }

  public void testIllegalSyntaxFlags() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("bogus", Integer.MAX_VALUE);
        });
  }

  public void testIllegalMatchFlags() {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new RegExp("bogus", RegExp.ALL, 1);
        });
  }

  private void assertSameLanguage(Automaton expected, Automaton actual) {
    expected = Operations.determinize(expected, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    actual = Operations.determinize(actual, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    boolean result = AutomatonTestUtil.sameLanguage(expected, actual);
    if (result == false) {
      System.out.println(expected.toDot());
      System.out.println(actual.toDot());
    }
    assertTrue(result);
  }
}
