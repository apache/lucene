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
package org.apache.lucene.analysis.ja.completion;

import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.CharsRef;
import org.junit.Test;

public class TestKatakanaRomanizer extends LuceneTestCase {
  private final KatakanaRomanizer romanizer = KatakanaRomanizer.getInstance();

  @Test
  public void testRomanize() {
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("hasi"), new CharsRef("hashi")),
        romanizer.romanize(new CharsRef("ハシ")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("yuukyuu")), romanizer.romanize(new CharsRef("ユウキュウ")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("yakyuu")), romanizer.romanize(new CharsRef("ヤキュウ")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("toukyou")), romanizer.romanize(new CharsRef("トウキョウ")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("toーkyoー")), romanizer.romanize(new CharsRef("トーキョー")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("sakka")), romanizer.romanize(new CharsRef("サッカ")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("hyakkaten"), new CharsRef("hyakkatenn")),
        romanizer.romanize(new CharsRef("ヒャッカテン")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("voruteーru"), new CharsRef("vuxoruteーru")),
        romanizer.romanize(new CharsRef("ヴォルテール")));
  }

  @Test
  public void testRomanizeWithAlphabets() {
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("toukyout")), romanizer.romanize(new CharsRef("トウキョウt")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("kodakk")), romanizer.romanize(new CharsRef("コダッk")));
    assertCharsRefListEqualsUnordered(
        List.of(new CharsRef("syousy"), new CharsRef("shousy")),
        romanizer.romanize(new CharsRef("ショウsy")));
  }

  private static void assertCharsRefListEqualsUnordered(
      List<CharsRef> expected, List<CharsRef> actual) {
    assertEquals(expected.size(), actual.size());
    for (CharsRef ref : expected) {
      assertTrue(ref.toString() + " is not contained in " + actual, actual.contains(ref));
    }
  }
}
