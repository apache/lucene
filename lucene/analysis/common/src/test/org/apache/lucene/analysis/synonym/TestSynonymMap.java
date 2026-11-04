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
package org.apache.lucene.analysis.synonym;

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;

public class TestSynonymMap extends LuceneTestCase {

  public void testCompareCodePointsMatchesUTF8Order() {
    final int numStrings = atLeast(1000);
    BytesRef[] utf8 = new BytesRef[numStrings];
    CharsRef[] utf16 = new CharsRef[numStrings];

    for (int i = 0; i < numStrings; i++) {
      String s = TestUtil.randomUnicodeString(random());
      utf8[i] = new BytesRef(s);
      utf16[i] = new CharsRef(s);
    }

    Arrays.sort(utf8);
    Arrays.sort(utf16, SynonymMap.Builder::compareCodePoints);

    for (int i = 0; i < numStrings; i++) {
      assertEquals(utf8[i].utf8ToString(), utf16[i].toString());
    }
  }

  public void testSupplementaryCharsSortAfterHighBMP() {
    // U+FFFD is a single char above the surrogate range; U+10000 is a surrogate pair.
    CharsRef bmp = new CharsRef("\uFFFD");
    CharsRef supplementary = new CharsRef("\uD800\uDC00");
    assertTrue(SynonymMap.Builder.compareCodePoints(bmp, supplementary) < 0);
    assertTrue(SynonymMap.Builder.compareCodePoints(supplementary, bmp) > 0);
  }

  public void testPrefixAndEquality() {
    assertEquals(0, SynonymMap.Builder.compareCodePoints(new CharsRef("abc"), new CharsRef("abc")));
    assertTrue(SynonymMap.Builder.compareCodePoints(new CharsRef("ab"), new CharsRef("abc")) < 0);
    assertTrue(SynonymMap.Builder.compareCodePoints(new CharsRef("abc"), new CharsRef("ab")) > 0);
    assertEquals(0, SynonymMap.Builder.compareCodePoints(new CharsRef(""), new CharsRef("")));
  }

  public void testRespectsOffsetAndLength() {
    CharsRef a = new CharsRef("xx\uD800\uDC00yy".toCharArray(), 2, 2);
    CharsRef b = new CharsRef("zzz\uFFFDz".toCharArray(), 3, 1);
    assertTrue(SynonymMap.Builder.compareCodePoints(a, b) > 0);
  }
}
