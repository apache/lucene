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
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Util;

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

  /**
   * User dictionary nouns must use the same connection context ids as plain common nouns (NNG) in
   * the system dictionary, both with and without a final consonant on the last syllable.
   */
  public void testContextIdsMatchSystemDictionaryNouns() throws IOException {
    TokenInfoDictionary systemDictionary = TokenInfoDictionary.getInstance();
    // "사람" ends with a final consonant, "친구" does not
    String[] nouns = {"사람", "친구"};
    UserDictionary userDictionary = UserDictionary.open(new StringReader(String.join("\n", nouns)));
    for (String noun : nouns) {
      int systemWordId = lookupCommonNounWordId(systemDictionary, noun);

      char[] chars = noun.toCharArray();
      List<Integer> userWordIds = userDictionary.lookup(chars, 0, chars.length);
      assertEquals(1, userWordIds.size());
      int userWordId = userWordIds.get(0);

      assertEquals(
          "left id of " + noun,
          systemDictionary.getLeftId(systemWordId),
          userDictionary.getLeftId(userWordId));
      assertEquals(
          "right id of " + noun,
          systemDictionary.getRightId(systemWordId),
          userDictionary.getRightId(userWordId));
    }
  }

  /** Returns the id of the single common noun (NNG morpheme) entry for the given surface form. */
  private static int lookupCommonNounWordId(TokenInfoDictionary dictionary, String surface)
      throws IOException {
    Long sourceId =
        Util.get(dictionary.getFST().getInternalFST(), Util.toUTF16(surface, new IntsRefBuilder()));
    assertNotNull(surface + " should be in the system dictionary", sourceId);
    IntsRef wordIds = new IntsRef();
    dictionary.lookupWordIds(sourceId.intValue(), wordIds);
    int commonNounWordId = -1;
    for (int i = 0; i < wordIds.length; i++) {
      int wordId = wordIds.ints[wordIds.offset + i];
      TokenInfoMorphData morphData = dictionary.getMorphAttributes();
      if (morphData.getPOSType(wordId) == POS.Type.MORPHEME
          && morphData.getLeftPOS(wordId) == POS.Tag.NNG) {
        assertEquals(surface + " should have a single common noun entry", -1, commonNounWordId);
        commonNounWordId = wordId;
      }
    }
    assertNotEquals(surface + " should have a common noun entry", -1, commonNounWordId);
    return commonNounWordId;
  }
}
