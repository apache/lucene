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

import org.apache.lucene.util.LuceneTestCase;

/**
 * Simple unit tests for RegExp parsing.
 *
 * <p>For each type of node, test the toString() and parse tree, test the resulting automaton's
 * language, and whether it is deterministic
 */
public class TestRegExpParsing extends LuceneTestCase {

  public void testAnyChar() {
    RegExp re = new RegExp(".");
    assertEquals(".", re.toString());
    assertEquals("REGEXP_ANYCHAR\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeAnyChar();
    assertSameLanguage(expected, actual);
  }

  public void testAnyString() {
    RegExp re = new RegExp("@", RegExp.ANYSTRING);
    assertEquals("@", re.toString());
    assertEquals("REGEXP_ANYSTRING\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeAnyString();
    assertSameLanguage(expected, actual);
  }

  public void testChar() {
    RegExp re = new RegExp("c");
    assertEquals("\\c", re.toString());
    assertEquals("REGEXP_CHAR char=c\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeChar('c');
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveChar() {
    RegExp re = new RegExp("c", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\\c", re.toString());
    assertEquals("REGEXP_CHAR char=c\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Operations.union(Automata.makeChar('c'), Automata.makeChar('C'));
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
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.union(
            Automata.makeCharRange(0, 'b'), Automata.makeCharRange('d', Integer.MAX_VALUE));
    assertSameLanguage(expected, actual);
  }

  public void testCharRange() {
    RegExp re = new RegExp("[b-d]");
    assertEquals("[\\b-\\d]", re.toString());
    assertEquals("REGEXP_CHAR_RANGE from=b to=d\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

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
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.union(
            Automata.makeCharRange(0, 'a'), Automata.makeCharRange('e', Integer.MAX_VALUE));
    assertSameLanguage(expected, actual);
  }

  public void testCharClassDigit() {
    RegExp re = new RegExp("[\\d]");
    assertEquals("\\d", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\d\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeCharRange('0', '9');
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonDigit() {
    RegExp re = new RegExp("[\\D]");
    assertEquals("\\D", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\D\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.minus(
            Automata.makeAnyChar(),
            Automata.makeCharRange('0', '9'),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    assertSameLanguage(expected, actual);
  }

  public void testCharClassWhitespace() {
    RegExp re = new RegExp("[\\s]");
    assertEquals("\\s", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\s\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeChar(' ');
    expected = Operations.union(expected, Automata.makeChar('\n'));
    expected = Operations.union(expected, Automata.makeChar('\r'));
    expected = Operations.union(expected, Automata.makeChar('\t'));
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonWhitespace() {
    RegExp re = new RegExp("[\\S]");
    assertEquals("\\S", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\S\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

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
    assertEquals("\\w", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\w\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeCharRange('a', 'z');
    expected = Operations.union(expected, Automata.makeCharRange('A', 'Z'));
    expected = Operations.union(expected, Automata.makeCharRange('0', '9'));
    expected = Operations.union(expected, Automata.makeChar('_'));
    assertSameLanguage(expected, actual);
  }

  public void testCharClassNonWord() {
    RegExp re = new RegExp("[\\W]");
    assertEquals("\\W", re.toString());
    assertEquals("REGEXP_PRE_CLASS class=\\W\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

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

  public void testEmpty() {
    RegExp re = new RegExp("#", RegExp.EMPTY);
    assertEquals("#", re.toString());
    assertEquals("REGEXP_EMPTY\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeEmpty();
    assertSameLanguage(expected, actual);
  }

  public void testInterval() {
    RegExp re = new RegExp("<5-40>");
    assertEquals("<5-40>", re.toString());
    assertEquals("REGEXP_INTERVAL<5-40>\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    // TODO: numeric intervals are NFAs

    Automaton expected = Automata.makeDecimalInterval(5, 40, 0);
    assertSameLanguage(expected, actual);
  }

  public void testOptional() {
    RegExp re = new RegExp("a?");
    assertEquals("(\\a)?", re.toString());
    assertEquals(String.join("\n", "REGEXP_OPTIONAL", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Operations.optional(Automata.makeChar('a'));
    assertSameLanguage(expected, actual);
  }

  public void testRepeat0() {
    RegExp re = new RegExp("a*");
    assertEquals("(\\a)*", re.toString());
    assertEquals(String.join("\n", "REGEXP_REPEAT", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Operations.repeat(Automata.makeChar('a'));
    assertSameLanguage(expected, actual);
  }

  public void testRepeat1() {
    RegExp re = new RegExp("a+");
    assertEquals("(\\a){1,}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MIN min=1", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

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
    assertTrue(actual.isDeterministic());

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 5, 5);
    assertSameLanguage(expected, actual);
  }

  public void testRepeatNPlus() {
    RegExp re = new RegExp("a{5,}");
    assertEquals("(\\a){5,}", re.toString());
    assertEquals(
        String.join("\n", "REGEXP_REPEAT_MIN min=5", "  REGEXP_CHAR char=a\n"), re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

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
    assertTrue(actual.isDeterministic());

    Automaton expected = Operations.repeat(Automata.makeChar('a'), 5, 8);
    assertSameLanguage(expected, actual);
  }

  public void testString() {
    RegExp re = new RegExp("boo");
    assertEquals("\"boo\"", re.toString());
    assertEquals("REGEXP_STRING string=boo\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeString("boo");
    assertSameLanguage(expected, actual);
  }

  public void testCaseInsensitiveString() {
    RegExp re = new RegExp("boo", RegExp.NONE, RegExp.ASCII_CASE_INSENSITIVE);
    assertEquals("\"boo\"", re.toString());
    assertEquals("REGEXP_STRING string=boo\n", re.toStringTree());

    Automaton actual = re.toAutomaton();
    assertTrue(actual.isDeterministic());

    Automaton c1 = Operations.union(Automata.makeChar('b'), Automata.makeChar('B'));
    Automaton c2 = Operations.union(Automata.makeChar('o'), Automata.makeChar('O'));

    Automaton expected = Operations.concatenate(c1, c2);
    expected = Operations.concatenate(expected, c2);
    assertSameLanguage(expected, actual);
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
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.concatenate(Automata.makeCharRange('b', 'c'), Automata.makeCharRange('e', 'f'));
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
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.intersection(Automata.makeCharRange('b', 'f'), Automata.makeCharRange('e', 'f'));
    assertSameLanguage(expected, actual);
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
    assertTrue(actual.isDeterministic());

    Automaton expected =
        Operations.union(Automata.makeCharRange('b', 'c'), Automata.makeCharRange('e', 'f'));
    assertSameLanguage(expected, actual);
  }

  public void testAutomaton() {
    AutomatonProvider myProvider =
        new AutomatonProvider() {
          @Override
          public Automaton getAutomaton(String name) {
            if (name.equals("myletter")) return Automata.makeChar('z');
            else return null;
          }
        };
    RegExp re = new RegExp("<myletter>", RegExp.ALL);
    assertEquals("<myletter>", re.toString());
    assertEquals("REGEXP_AUTOMATON\n", re.toStringTree());

    Automaton actual = re.toAutomaton(myProvider);
    assertTrue(actual.isDeterministic());

    Automaton expected = Automata.makeChar('z');
    assertSameLanguage(expected, actual);
  }

  private void assertSameLanguage(Automaton expected, Automaton actual) {
    expected = Operations.determinize(expected, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    actual = Operations.determinize(actual, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    boolean result = Operations.sameLanguage(expected, actual);
    if (result == false) {
      System.out.println(expected.toDot());
      System.out.println(actual.toDot());
    }
    assertTrue(result);
  }
}
