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
package org.apache.lucene.analysis.compound;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Simple tests to ensure the Dictionary compound filter factory is working. */
public class TestDictionaryCompoundWordTokenFilterFactory extends BaseTokenStreamFactoryTestCase {
  private static CharArraySet makeDictionary(String... dictionary) {
    return new CharArraySet(Arrays.asList(dictionary), true);
  }
  /** Ensure the filter actually decompounds text. */
  public void testDecompounding() throws Exception {
    Reader reader = new StringReader("I like to play softball");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer) stream).setReader(reader);
    stream =
        tokenFilterFactory("DictionaryCompoundWord", "dictionary", "compoundDictionary.txt")
            .create(stream);
    assertTokenStreamContents(
        stream, new String[] {"I", "like", "to", "play", "softball", "soft", "ball"});
  }

  /** Ensure subtoken are found in token and indexed zero * */
  public void testDecompounderSubmatches() throws Exception {
    CharArraySet dict = makeDictionary("ora", "orangen", "schoko", "schokolade");

    DictionaryCompoundWordTokenFilter tf =
        new DictionaryCompoundWordTokenFilter(
            whitespaceMockTokenizer("ich will orangenschokolade haben"),
            dict,
            CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE,
            false);
    assertTokenStreamContents(
        tf,
        new String[] {
          "ich", "will", "orangenschokolade", "ora", "orangen", "schoko", "schokolade", "haben"
        },
        new int[] {1, 1, 1, 0, 0, 0, 0, 1});
  }

  /** Ensure subtoken are found in token and only longest match is returned with same start * */
  public void testDecompounderSubmatchesOnlyLongestMatch() throws Exception {
    CharArraySet dict = makeDictionary("ora", "orangen", "schoko", "schokolade");

    DictionaryCompoundWordTokenFilter tf =
        new DictionaryCompoundWordTokenFilter(
            whitespaceMockTokenizer("ich will orangenschokolade haben"),
            dict,
            CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE,
            true);
    assertTokenStreamContents(
        tf,
        new String[] {"ich", "will", "orangenschokolade", "orangen", "schokolade", "haben"},
        new int[] {1, 1, 1, 0, 0, 1});
  }

  /** Ensure subtoken are found in token and only longest match is returned without same start * */
  public void testDecompounderPostSubmatchesOnlyLongestMatch() throws Exception {
    CharArraySet dict = makeDictionary("ngen", "orangen", "schoko", "schokolade");

    DictionaryCompoundWordTokenFilter tf =
        new DictionaryCompoundWordTokenFilter(
            whitespaceMockTokenizer("ich will orangenschokolade haben"),
            dict,
            CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
            CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE,
            true);
    assertTokenStreamContents(
        tf,
        new String[] {"ich", "will", "orangenschokolade", "orangen", "schokolade", "haben"},
        new int[] {1, 1, 1, 0, 0, 1});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "DictionaryCompoundWord",
                  "dictionary",
                  "compoundDictionary.txt",
                  "bogusArg",
                  "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
