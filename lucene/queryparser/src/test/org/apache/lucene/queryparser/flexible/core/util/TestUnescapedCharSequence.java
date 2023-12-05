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
package org.apache.lucene.queryparser.flexible.core.util;

import java.util.Locale;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestUnescapedCharSequence extends LuceneTestCase {
  private static final char[] wildcardChars = {'*', '?'};

  @Test
  public void testToStringEscaped() {
    char[] chars = {'a', 'b', 'c', '\\', 'e'};
    boolean[] wasEscaped = {false, true, true, false, false};
    UnescapedCharSequence sequence = new UnescapedCharSequence(chars, wasEscaped, 0, chars.length);
    assertEquals("a\\b\\c\\\\e", sequence.toStringEscaped());
    assertFalse(sequence.wasEscaped(0));
    assertTrue(sequence.wasEscaped(1));
  }

  @Test
  public void testToStringEscapedWithEnabledChars() {
    char[] chars = {'a', 'b', 'c', '?', '*'};
    boolean[] wasEscaped = {true, true, true, true, true};
    UnescapedCharSequence sequence = new UnescapedCharSequence(chars, wasEscaped, 0, chars.length);
    assertEquals("abc\\?\\*", sequence.toStringEscaped(wildcardChars));
  }

  @Test
  public void testSubSequence() {
    UnescapedCharSequence sequence = new UnescapedCharSequence("abcdef");
    assertEquals("bc", sequence.subSequence(1, 3).toString());
  }

  @Test
  public void testToLowerCase() {
    UnescapedCharSequence sequence = new UnescapedCharSequence("ABC");
    assertEquals(
        "abc", UnescapedCharSequence.toLowerCase(sequence, Locale.getDefault()).toString());
    assertFalse(sequence.wasEscaped(0));
    assertFalse(sequence.wasEscaped(1));
    assertFalse(sequence.wasEscaped(2));
  }
}
