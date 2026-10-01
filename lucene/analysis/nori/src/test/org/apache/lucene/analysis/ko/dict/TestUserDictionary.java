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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.analysis.ko.TestKoreanTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.RamUsageEstimator;

public class TestUserDictionary extends LuceneTestCase {
  public void testLookup() throws IOException {
    UserDictionary dictionary = TestKoreanTokenizer.readDict();
    String s = "세종";
    char[] sArray = s.toCharArray();
    List<Integer> wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(1, wordIds.size());
    assertNull(dictionary.getMorphAttributes().getMorphemes(wordIds.get(0), sArray, 0, s.length()));

    s = "세종시";
    sArray = s.toCharArray();
    wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(2, wordIds.size());
    assertNull(dictionary.getMorphAttributes().getMorphemes(wordIds.get(0), sArray, 0, s.length()));

    KoMorphData.Morpheme[] decompound =
        dictionary.getMorphAttributes().getMorphemes(wordIds.get(1), sArray, 0, s.length());
    assertNotNull(decompound);
    assertEquals(2, decompound.length);
    assertEquals(decompound[0].posTag(), POS.Tag.NNG);
    assertEquals(decompound[0].surfaceForm(), "세종");
    assertEquals(decompound[1].posTag(), POS.Tag.NNG);
    assertEquals(decompound[1].surfaceForm(), "시");

    s = "c++";
    sArray = s.toCharArray();
    wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(1, wordIds.size());
    assertNull(dictionary.getMorphAttributes().getMorphemes(wordIds.get(0), sArray, 0, s.length()));
  }

  public void testRead() {
    UserDictionary dictionary = TestKoreanTokenizer.readDict();
    assertNotNull(dictionary);
  }

  public void testRamBytesUsed() throws IOException {
    // The appended entry is a compound, so its segmentation is non-null; a simple noun is null.
    String entry = "세종";
    UserDictionary small = UserDictionary.open(new StringReader(entry));
    UserDictionary large = UserDictionary.open(new StringReader(entry + "\n세종시 세종 시"));
    assertTrue(large.ramBytesUsed() > small.ramBytesUsed());
    // The dictionary is just the FST plus the morphological data, so check the exact total.
    // TestTokenInfoFST checks the FST part, which this module cannot measure.
    assertEquals(
        RamUsageEstimator.shallowSizeOfInstance(UserDictionary.class)
            + large.getFST().ramBytesUsed()
            + large.getMorphAttributes().ramBytesUsed(),
        large.ramBytesUsed());
    // The morphological data is only arrays, so it can be measured directly.
    assertMorphAttributesRamBytesUsed(small);
    assertMorphAttributesRamBytesUsed(large);
  }

  private static void assertMorphAttributesRamBytesUsed(UserDictionary dictionary) {
    UserMorphData morphAtts = dictionary.getMorphAttributes();
    assertEquals(RamUsageTester.ramUsed(morphAtts), morphAtts.ramBytesUsed());
  }
}
