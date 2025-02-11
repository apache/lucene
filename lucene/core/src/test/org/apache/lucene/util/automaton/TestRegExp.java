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
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.BytesRef;

public class TestRegExp extends LuceneTestCase {

  /** Simple smoke test for regular expression. */
  public void testSmoke() {
    RegExp r = new RegExp("a(b+|c+)d");
    Automaton a = r.toAutomaton();
    assertTrue(a.isDeterministic());
    CharacterRunAutomaton run = new CharacterRunAutomaton(a);
    assertTrue(run.run("abbbbbd"));
    assertTrue(run.run("acd"));
    assertFalse(run.run("ad"));
  }

  public void testUnicodeAsciiInsensitiveFlags() {
    RegExp r;
    // ASCII behaves appropriately with different flags
    r = new RegExp("A");
    assertFalse(new CharacterRunAutomaton(r.toAutomaton()).run("a"));

    r = new RegExp("A", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("a"));

    r = new RegExp("A", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("a"));

    r = new RegExp("A", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE | RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("a"));

    // class 1 Unicode characters behaves appropriately with different flags
    r = new RegExp("Σ");
    assertFalse(new CharacterRunAutomaton(r.toAutomaton()).run("σ"));
    assertFalse(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("σ");
    assertFalse(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("Σ", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("σ"));
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("σ", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("Σ", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("σ"));
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("σ", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("Σ", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE | RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("σ"));
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    r = new RegExp("σ", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE | RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ς"));

    // class 2 Unicode characters behaves appropriately with different flags
    r = new RegExp("ῼ", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertTrue(new CharacterRunAutomaton(r.toAutomaton()).run("ῳ"));

    r = new RegExp("ῼ", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertFalse(
        new CharacterRunAutomaton(r.toAutomaton()).run("ῼ".toUpperCase(Locale.ROOT))); // "ΩΙ"

    // class 3 Unicode characters behaves appropriately with different flags
    r = new RegExp("ﬗ", RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertFalse(new CharacterRunAutomaton(r.toAutomaton()).run("ﬗ".toUpperCase(Locale.ROOT)));

    r = new RegExp(new String(Character.toChars(0x1C8A)), RegExp.ALL, RegExp.CASE_INSENSITIVE);
    assertTrue(
        new CharacterRunAutomaton(r.toAutomaton()).run(new String(Character.toChars(0x1C89))));
  }

  public void testRandomUnicodeInsensitiveMatchPatternParity() {
    HexFormat hexFormat = HexFormat.of().withUpperCase();
    int maxIters = 1000;
    List<Integer> reservedCharacters =
        Set.of(
                '.', '^', '$', '*', '+', '?', '(', ')', '[', '{', '\\', '|', '-', '"', '<', '>',
                '#', '@', '&', '~')
            .stream()
            .map(c -> (int) c)
            .toList();
    for (int i = 0; i < maxIters; i++) {
      int nextCode1 = random().nextInt(0, Character.MAX_CODE_POINT + 1);
      int nextCode2 = random().nextInt(0, Character.MAX_CODE_POINT + 1);

      // skip if we select a reserved character that blows up .^$*+?()[{\|-]"<
      if (reservedCharacters.contains(nextCode1)) {
        continue;
      }

      String pattern = new String(Character.toChars(nextCode1));
      String altString = new String(Character.toChars(nextCode2));

      Pattern javaRegex = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
      RegExp r = new RegExp(pattern, RegExp.ALL, RegExp.CASE_INSENSITIVE);
      CharacterRunAutomaton cra = new CharacterRunAutomaton(r.toAutomaton());

      // Pattern doesn't respect the Unicode spec so some things will not match
      if (javaRegex.matcher(altString).matches()) {
        // ... but if they do match then we must agree
        assertTrue(
            "Pattern and RegExp disagree on pattern: "
                + hexFormat.toHexDigits(nextCode1, 4)
                + " :text: "
                + hexFormat.toHexDigits(nextCode2, 4),
            cra.run(altString));
      }
    }
  }

  public void testUnicodeInsensitiveMatchPatternParity() throws IOException {
    // this ensures that if the Pattern class behavior were to change with a change to the Unicode
    // spec then we would pick it up.  It may help indicate in the future if we don't notice
    // that the spec has changed and Pattern picks up the change first
    for (int codepoint = 0; codepoint < Character.MAX_CODE_POINT + 1; codepoint++) {
      int[] caseInsensitiveAlternatives = CaseFolding.lookupAlternates(codepoint);
      if (caseInsensitiveAlternatives != null) {
        String pattern = new String(Character.toChars(codepoint));
        Pattern javaRegex =
            Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        RegExp r = new RegExp(pattern, RegExp.ALL, RegExp.CASE_INSENSITIVE);
        CharacterRunAutomaton cra = new CharacterRunAutomaton(r.toAutomaton());
        for (int i = 0; i < caseInsensitiveAlternatives.length; i++) {

          int alt = caseInsensitiveAlternatives[i];
          String altString = new String(Character.toChars(alt));

          // Pattern doesn't respect the Unicode spec so some things will not match
          if (javaRegex.matcher(altString).matches()) {
            // ... but if they do match then we must agree
            assertTrue(cra.run(altString));
          }
        }
      }
    }
  }

  // LUCENE-6046
  public void testRepeatWithEmptyString() throws Exception {
    Automaton a = new RegExp("[^y]*{1,2}").toAutomaton();
    // paranoia:
    assertTrue(a.toString().length() > 0);
  }

  public void testRepeatWithEmptyLanguage() throws Exception {
    Automaton a = new RegExp("#*").toAutomaton();
    // paranoia:
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#+").toAutomaton();
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#{2,10}").toAutomaton();
    assertTrue(a.toString().length() > 0);
    a = new RegExp("#?").toAutomaton();
    assertTrue(a.toString().length() > 0);
  }

  boolean caseSensitiveQuery = true;
  boolean unicodeCaseQuery = true;

  public void testCoreJavaParity() {
    // Generate random doc values and random regular expressions
    // and check for same matching behaviour as Java's Pattern class.
    for (int i = 0; i < 1000; i++) {
      caseSensitiveQuery = true;
      checkRandomExpression(randomDocValue(1 + random().nextInt(30), false));
    }

    for (int i = 0; i < 1000; i++) {
      caseSensitiveQuery = true;
      unicodeCaseQuery = true;
      checkRandomExpression(randomDocValue(1 + random().nextInt(30), true));
    }
  }

  public void testIllegalBackslashChars() {
    String illegalChars = "abcefghijklmnopqrtuvxyzABCEFGHIJKLMNOPQRTUVXYZ";
    for (int i = 0; i < illegalChars.length(); i++) {
      String illegalExpression = "\\" + illegalChars.charAt(i);
      IllegalArgumentException expected =
          expectThrows(
              IllegalArgumentException.class,
              () -> {
                new RegExp(illegalExpression);
              });
      assertTrue(expected.getMessage().contains("invalid character class"));
    }
  }

  public void testLegalBackslashChars() {
    String legalChars = "dDsSWw0123456789[]*&^$@!{}\\/";
    for (int i = 0; i < legalChars.length(); i++) {
      String legalExpression = "\\" + legalChars.charAt(i);
      new RegExp(legalExpression);
    }
  }

  public void testParseIllegalRepeatExp() {
    // out of order
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new RegExp("a{99,11}");
            });
    assertTrue(expected.getMessage().contains("out of order"));
  }

  static String randomDocValue(int minLength, boolean includeUnicode) {
    String charPalette = "AAAaaaBbbCccc123456 \t";
    if (includeUnicode) {
      charPalette += "Σσςῼῳ";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < minLength; i++) {
      sb.append(charPalette.charAt(randomInt(charPalette.length() - 1)));
    }
    return sb.toString();
  }

  private static int randomInt(int bound) {
    return bound == 0 ? 0 : random().nextInt(bound);
  }

  protected String checkRandomExpression(String docValue) {
    // Generate and test a random regular expression which should match the given docValue
    StringBuilder result = new StringBuilder();
    // Pick a part of the string to change
    int substitutionPoint = randomInt(docValue.length() - 1);
    int substitutionLength = 1 + randomInt(Math.min(10, docValue.length() - substitutionPoint));

    // Add any head to the result, unchanged
    if (substitutionPoint > 0) {
      result.append(docValue, 0, substitutionPoint);
    }

    // Modify the middle...
    String replacementPart =
        docValue.substring(substitutionPoint, substitutionPoint + substitutionLength);
    int mutation = random().nextInt(15);
    switch (mutation) {
      case 0:
        // OR with random alpha of same length
        result.append(
            "(" + replacementPart + "|d" + randomDocValue(replacementPart.length(), false) + ")");
        break;
      case 1:
        // OR with non-existant value
        result.append("(" + replacementPart + "|doesnotexist)");
        break;
      case 2:
        // OR with another randomised regex (used to create nested levels of expression).
        result.append("(" + checkRandomExpression(replacementPart) + "|doesnotexist)");
        break;
      case 3:
        // Star-replace all ab sequences.
        result.append(replacementPart.replace("ab", ".*"));
        break;
      case 4:
        // .-replace all b chars
        result.append(replacementPart.replace("b", "."));
        break;
      case 5:
        // length-limited stars {1,2}
        result.append(".{1," + replacementPart.length() + "}");
        break;
      case 6:
        // replace all chars with .
        result.append(".".repeat(replacementPart.length()));
        break;
      case 7:
        // OR with uppercase chars eg [aA] (many of these sorts of expression in the wild..
        char[] chars = replacementPart.toCharArray();
        for (char c : chars) {
          result.append("[" + c + Character.toUpperCase(c) + "]");
        }
        break;
      case 8:
        // NOT a character - replace all b's with "not a"
        result.append(replacementPart.replace("b", "[^a]"));
        break;
      case 9:
        // Make whole part repeatable 1 or more times
        result.append("(" + replacementPart + ")+");
        break;
      case 10:
        // Make whole part repeatable 0 or more times
        result.append("(" + replacementPart + ")?");
        break;
      case 11:
        // Make any digits replaced by character class
        result.append(replacementPart.replaceAll("\\d", "\\\\d"));
        break;
      case 12:
        // Make any whitespace chars replaced by not word class
        result.append(replacementPart.replaceAll("\\s", "\\\\W"));
        break;
      case 13:
        // Make any whitespace chars replace by whitespace class
        result.append(replacementPart.replaceAll("\\s", "\\\\s"));
        break;
      case 14:
        // Switch case of characters
        StringBuilder switchedCase = new StringBuilder();
        replacementPart
            .codePoints()
            .forEach(
                p -> {
                  int switchedP = p;
                  if (Character.isLowerCase(p)) {
                    switchedP = Character.toUpperCase(p);
                  } else {
                    switchedP = Character.toLowerCase(p);
                  }
                  switchedCase.appendCodePoint(switchedP);
                  if (p != switchedP) {
                    caseSensitiveQuery = false;
                  }
                });
        result.append(switchedCase.toString());
        break;
      default:
        break;
    }
    // add any remaining tail, unchanged
    if (substitutionPoint + substitutionLength <= docValue.length() - 1) {
      result.append(docValue.substring(substitutionPoint + substitutionLength));
    }

    String regexPattern = result.toString();
    // Assert our randomly generated regex actually matches the provided raw input using java's
    // expression matcher
    Pattern pattern =
        caseSensitiveQuery
            ? Pattern.compile(regexPattern)
            : Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    Matcher matcher = pattern.matcher(docValue);
    assertTrue(
        "Java regex " + regexPattern + " did not match doc value " + docValue, matcher.matches());

    int matchFlags =
        caseSensitiveQuery ? 0 : RegExp.ASCII_CASE_INSENSITIVE | RegExp.CASE_INSENSITIVE;
    RegExp regex = new RegExp(regexPattern, RegExp.ALL, matchFlags);
    Automaton automaton =
        Operations.determinize(regex.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
    BytesRef br = newBytesRef(docValue);
    assertTrue(
        "["
            + regexPattern
            + "]should match ["
            + docValue
            + "]"
            + substitutionPoint
            + "-"
            + substitutionLength
            + "/"
            + docValue.length(),
        bytesMatcher.run(br.bytes, br.offset, br.length));
    if (caseSensitiveQuery == false) {
      RegExp caseSensitiveRegex = new RegExp(regexPattern);
      Automaton csAutomaton = caseSensitiveRegex.toAutomaton();
      csAutomaton = Operations.determinize(csAutomaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
      ByteRunAutomaton csBytesMatcher = new ByteRunAutomaton(csAutomaton);
      assertFalse(
          "[" + regexPattern + "] with case sensitive setting should not match [" + docValue + "]",
          csBytesMatcher.run(br.bytes, br.offset, br.length));
    }
    return regexPattern;
  }

  public void testRegExpNoStackOverflow() {
    new RegExp("(a)|".repeat(50000) + "(a)");
  }

  /**
   * Tests the deprecate complement flag. Keep the simple test only, no random tests to let it cause
   * us pain.
   *
   * @deprecated Remove in Lucene 11
   */
  @Deprecated
  public void testDeprecatedComplement() {
    Automaton expected =
        Operations.complement(
            Automata.makeString("abcd"), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    Automaton actual = new RegExp("~(abcd)", RegExp.DEPRECATED_COMPLEMENT).toAutomaton();
    assertTrue(AutomatonTestUtil.sameLanguage(expected, actual));
  }
}
