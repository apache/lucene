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
package org.apache.lucene.analysis.ja.dict;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.lucene.analysis.ja.TestJapaneseTokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestUserDictionary extends LuceneTestCase {

  @Test
  public void testLookup() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    String s = "関西国際空港に行った";
    List<int[]> dictionaryEntryResult = dictionary.lookup(s.toCharArray(), 0, s.length());
    // Length should be three 関西, 国際, 空港
    assertEquals(3, dictionaryEntryResult.size());

    // Test positions
    assertEquals(0, dictionaryEntryResult.get(0)[1]); // index of 関西
    assertEquals(2, dictionaryEntryResult.get(1)[1]); // index of 国際
    assertEquals(4, dictionaryEntryResult.get(2)[1]); // index of 空港

    // Test lengths
    assertEquals(2, dictionaryEntryResult.get(0)[2]); // length of 関西
    assertEquals(2, dictionaryEntryResult.get(1)[2]); // length of 国際
    assertEquals(2, dictionaryEntryResult.get(2)[2]); // length of 空港

    s = "関西国際空港と関西国際空港に行った";
    List<int[]> dictionaryEntryResult2 = dictionary.lookup(s.toCharArray(), 0, s.length());
    // Length should be six
    assertEquals(6, dictionaryEntryResult2.size());
  }

  @Test
  public void testReadings() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    List<int[]> result = dictionary.lookup("日本経済新聞".toCharArray(), 0, 6);
    assertEquals(3, result.size());
    int wordIdNihon = result.get(0)[0]; // wordId of 日本 in 日本経済新聞
    assertEquals("ニホン", dictionary.getReading(wordIdNihon, "日本".toCharArray(), 0, 2));

    result = dictionary.lookup("朝青龍".toCharArray(), 0, 3);
    assertEquals(1, result.size());
    int wordIdAsashoryu = result.get(0)[0]; // wordId for 朝青龍
    assertEquals("アサショウリュウ", dictionary.getReading(wordIdAsashoryu, "朝青龍".toCharArray(), 0, 3));
  }

  @Test
  public void testPartOfSpeech() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    List<int[]> result = dictionary.lookup("日本経済新聞".toCharArray(), 0, 6);
    assertEquals(3, result.size());
    int wordIdKeizai = result.get(1)[0]; // wordId of 経済 in 日本経済新聞
    assertEquals("カスタム名詞", dictionary.getPartOfSpeech(wordIdKeizai));
  }

  @Test
  public void testRead() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    assertNotNull(dictionary);
  }

  @Test
  public void testReadInvalid1() throws IOException {
    // the concatenated segment must be the same as the surface form
    String invalidEntry = "日経新聞,日本 経済 新聞,ニホン ケイザイ シンブン,カスタム名詞";
    RuntimeException e =
        expectThrows(
            RuntimeException.class,
            "RuntimeException should be thrown when passed an invalid dictionary entry.",
            () -> UserDictionary.open(new StringReader(invalidEntry)));
    assertTrue(e.getMessage().contains("does not match the surface form"));
  }

  @Test
  public void testReadInvalid2() throws IOException {
    // the concatenated segment must be the same as the surface form
    String invalidEntry = "日本経済新聞,日経 新聞,ニッケイ シンブン,カスタム名詞";
    RuntimeException e =
        expectThrows(
            RuntimeException.class,
            "RuntimeException should be thrown when passed an invalid dictionary entry.",
            () -> UserDictionary.open(new StringReader(invalidEntry)));
    assertTrue(e.getMessage().contains("does not match the surface form"));
  }

  @Test
  public void testSharp() throws IOException {
    String[] inputs = {"テスト#", "テスト#テスト"};
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();

    for (String input : inputs) {
      System.out.println(input);
      List<int[]> result = dictionary.lookup(input.toCharArray(), 0, input.length());
      assertEquals("カスタム名刺", dictionary.getPartOfSpeech(result.get(0)[0]));
    }
  }
}
