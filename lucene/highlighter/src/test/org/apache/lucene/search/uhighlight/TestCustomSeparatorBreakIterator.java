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
package org.apache.lucene.search.uhighlight;

import static org.apache.lucene.search.uhighlight.TestWholeBreakIterator.assertSameBreaks;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.text.BreakIterator;
import java.util.Locale;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestCustomSeparatorBreakIterator extends LuceneTestCase {

  private static final Character[] SEPARATORS = new Character[] {' ', '\u0000', 8233};

  public void testBreakOnCustomSeparator() throws Exception {
    char separator = randomSeparator();
    BreakIterator bi = new CustomSeparatorBreakIterator(separator);
    String source =
        "this"
            + separator
            + "is"
            + separator
            + "the"
            + separator
            + "first"
            + separator
            + "sentence";
    bi.setText(source);
    assertEquals(0, bi.current());
    assertEquals(0, bi.first());
    assertEquals("this" + separator, source.substring(bi.current(), bi.next()));
    assertEquals("is" + separator, source.substring(bi.current(), bi.next()));
    assertEquals("the" + separator, source.substring(bi.current(), bi.next()));
    assertEquals("first" + separator, source.substring(bi.current(), bi.next()));
    assertEquals("sentence", source.substring(bi.current(), bi.next()));
    assertEquals(BreakIterator.DONE, bi.next());

    assertEquals(source.length(), bi.last());
    int current = bi.current();
    assertEquals("sentence", source.substring(bi.previous(), current));
    current = bi.current();
    assertEquals("first" + separator, source.substring(bi.previous(), current));
    current = bi.current();
    assertEquals("the" + separator, source.substring(bi.previous(), current));
    current = bi.current();
    assertEquals("is" + separator, source.substring(bi.previous(), current));
    current = bi.current();
    assertEquals("this" + separator, source.substring(bi.previous(), current));
    assertEquals(BreakIterator.DONE, bi.previous());
    assertEquals(0, bi.current());

    assertEquals(
        "this" + separator + "is" + separator + "the" + separator,
        source.substring(0, bi.following(9)));

    assertEquals("this" + separator + "is" + separator, source.substring(0, bi.preceding(9)));

    assertEquals(0, bi.first());
    assertEquals(
        "this" + separator + "is" + separator + "the" + separator, source.substring(0, bi.next(3)));
  }

  public void testSingleSentences() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("a", expected, actual);
    assertSameBreaks("ab", expected, actual);
    assertSameBreaks("abc", expected, actual);
    assertSameBreaks("", expected, actual);
  }

  public void testSliceEnd() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("a000", 0, 1, expected, actual);
    assertSameBreaks("ab000", 0, 1, expected, actual);
    assertSameBreaks("abc000", 0, 1, expected, actual);
    assertSameBreaks("000", 0, 0, expected, actual);
  }

  public void testSliceStart() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000a", 3, 1, expected, actual);
    assertSameBreaks("000ab", 3, 2, expected, actual);
    assertSameBreaks("000abc", 3, 3, expected, actual);
    assertSameBreaks("000", 3, 0, expected, actual);
  }

  public void testSliceMiddle() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000a000", 3, 1, expected, actual);
    assertSameBreaks("000ab000", 3, 2, expected, actual);
    assertSameBreaks("000abc000", 3, 3, expected, actual);
    assertSameBreaks("000000", 3, 0, expected, actual);
  }

  /** the current position must be ignored, initial position is always first() */
  public void testFirstPosition() throws Exception {
    BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
    BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
    assertSameBreaks("000ab000", 3, 2, 4, expected, actual);
  }

  private static char randomSeparator() {
    return RandomPicks.randomFrom(random(), SEPARATORS);
  }
}
