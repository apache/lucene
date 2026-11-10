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
package org.apache.lucene.analysis.th;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

/** Test case for {@link ThaiTokenizer}. */
public class TestThaiTokenizer extends BaseTokenStreamTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue(
        "JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
  }

  public void testDefaultSegmentation() throws IOException {
    Tokenizer tokenizer = new ThaiTokenizer();
    tokenizer.setReader(new StringReader("ภาษาไทย"));
    assertTokenStreamContents(tokenizer, new String[] {"ภาษา", "ไทย"});
  }

  public void testUserDictionary() throws IOException {
    CharArraySet userDict = new CharArraySet(List.of("พารากอน", "คนขับรถ"), false);
    Tokenizer tokenizer = new ThaiTokenizer(userDict);
    tokenizer.setReader(new StringReader("ไปพารากอนกัน"));
    assertTokenStreamContents(tokenizer, new String[] {"ไป", "พารากอน", "กัน"});
  }

  // empty string in a segmentation dictionary REALLY does not make sense...
  // but don't crash on it
  public void testUserDictionaryEmptyString() throws IOException {
    CharArraySet userDict = new CharArraySet(List.of("", "พารากอน"), false);
    Tokenizer tokenizer = new ThaiTokenizer(userDict);
    tokenizer.setReader(new StringReader("ไปพารากอนกัน"));
    assertTokenStreamContents(tokenizer, new String[] {"ไป", "พารากอน", "กัน"});
  }

  public void testMultipleUserDictionaryTermsAndOffsets() throws IOException {
    CharArraySet userDict = new CharArraySet(List.of("พารากอน", "คนขับรถ"), false);
    Tokenizer tokenizer = new ThaiTokenizer(userDict);
    tokenizer.setReader(new StringReader("เขาเป็นคนขับรถไปพารากอน"));
    assertTokenStreamContents(
        tokenizer,
        new String[] {"เขา", "เป็น", "คนขับรถ", "ไป", "พารากอน"},
        new int[] {0, 3, 7, 14, 16},
        new int[] {3, 7, 14, 16, 23});
  }

  public void testEmptyUserDictionary() throws IOException {
    CharArraySet userDict = new CharArraySet(0, false);
    Tokenizer tokenizer = new ThaiTokenizer(userDict);
    tokenizer.setReader(new StringReader("ภาษาไทย"));
    assertTokenStreamContents(tokenizer, new String[] {"ภาษา", "ไทย"});
  }

  public void testLongTextAcrossBufferBoundary() throws IOException {
    // Generate long Thai text (> 1024 chars) with spaces as clause boundaries
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 110; i++) {
      sb.append("ภาษาไทย "); // 9 chars per repetition, 110 * 9 = 990 chars
    }
    // Now at ~990 chars; add words spanning across index 1024
    sb.append("มหาวิทยาลัย "); // 12 chars -> total ~1002
    sb.append("กรุงเทพมหานคร "); // 14 chars -> total ~1016
    sb.append("ประเทศไทย"); // 9 chars -> total ~1025, crossing 1024 boundary

    Tokenizer tokenizer = new ThaiTokenizer();
    tokenizer.setReader(new StringReader(sb.toString()));
    tokenizer.reset();
    boolean foundCrossingWord = false;
    org.apache.lucene.analysis.tokenattributes.CharTermAttribute termAtt =
        tokenizer.addAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
    while (tokenizer.incrementToken()) {
      String term = termAtt.toString();
      if ("ประเทศ".equals(term)) {
        foundCrossingWord = true;
      }
      assertFalse("Word should not be truncated into fragment 'ประ'", "ประ".equals(term));
      assertFalse("Word should not be truncated into fragment 'เทศ'", "เทศ".equals(term));
    }
    tokenizer.end();
    tokenizer.close();
    assertTrue("Should find intact term 'ประเทศ'", foundCrossingWord);
  }

  public void testCustomBufferSize() throws IOException {
    // Tiny buffer size of 32 chars
    Tokenizer tokenizer = new ThaiTokenizer(32);
    tokenizer.setReader(new StringReader("การทดสอบ ภาษาไทย มหาวิทยาลัย"));
    assertTokenStreamContents(
        tokenizer, new String[] {"การ", "ทดสอบ", "ภาษา", "ไทย", "มหาวิทยาลัย"});
  }

  public void testFactoryWithBufferSize() throws IOException {
    java.util.Map<String, String> args = new java.util.HashMap<>();
    args.put("bufferSize", "2048");
    ThaiTokenizerFactory factory = new ThaiTokenizerFactory(args);
    Tokenizer tokenizer = factory.create();
    tokenizer.setReader(new StringReader("ภาษาไทย"));
    assertTokenStreamContents(tokenizer, new String[] {"ภาษา", "ไทย"});
  }
}
