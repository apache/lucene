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
}
