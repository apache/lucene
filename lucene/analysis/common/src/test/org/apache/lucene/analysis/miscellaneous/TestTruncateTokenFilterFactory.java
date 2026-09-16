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
package org.apache.lucene.analysis.miscellaneous;

import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.Version;

/** Simple tests to ensure the simple truncation filter factory is working. */
public class TestTruncateTokenFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testCharTruncating() throws Exception {
    testCharTruncating(TruncateTokenFilterFactory.TRUNCATE_AFTER_CHARS_KEY);
    testCharTruncating(TruncateTokenFilterFactory.PREFIX_LENGTH_KEY);
    testCharTruncating(null);
  }

  private void testCharTruncating(String param) throws Exception {
    Reader reader = new StringReader("abcdefg 1234567 ABCDEFG abcde abc 12345 123 1234567 1234😃5");
    var tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream =
        (param == null
                ? tokenFilterFactory("truncate", Version.LUCENE_10_0_0)
                : tokenFilterFactory("Truncate", Version.LUCENE_10_0_0, param, "5"))
            .create(tokenizer);
    assertTokenStreamContents(
        stream,
        new String[] {
          "abcde",
          "12345",
          "ABCDE",
          "abcde",
          "abc",
          "12345",
          "123",
          "12345",
          "1234" + "😃".charAt(0)
        });
  }

  public void testCodePointTruncating() throws Exception {
    testCodePointTruncating(TruncateTokenFilterFactory.TRUNCATE_AFTER_CODEPOINTS_KEY);
    testCodePointTruncating(TruncateTokenFilterFactory.PREFIX_LENGTH_KEY);
    testCodePointTruncating(null);
  }

  public void testCodePointTruncating(String param) throws Exception {
    Reader reader =
        new StringReader(
            "abcdefg 1234567 ABCDEFG abcde abc 12345 123 1234😃5 1 😃 😃12345 😃😃 😃😃😃 😃😃😃😃 😃😃😃😃😃 😃😃😃😃😃😃");
    var tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(reader);
    TokenStream stream =
        (param == null
                ? tokenFilterFactory("truncate", Version.LATEST)
                : tokenFilterFactory("Truncate", Version.LATEST, param, "5"))
            .create(tokenizer);
    assertTokenStreamContents(
        stream,
        new String[] {
          "abcde",
          "12345",
          "ABCDE",
          "abcde",
          "abc",
          "12345",
          "123",
          "1234😃",
          "1",
          "😃",
          "😃1234",
          "😃😃",
          "😃😃😃",
          "😃😃😃😃",
          "😃😃😃😃😃",
          "😃😃😃😃😃"
        });
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "Truncate",
                  TruncateTokenFilterFactory.TRUNCATE_AFTER_CODEPOINTS_KEY,
                  "5",
                  "bogusArg",
                  "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameter(s):"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "Truncate",
                  TruncateTokenFilterFactory.TRUNCATE_AFTER_CODEPOINTS_KEY,
                  "5",
                  TruncateTokenFilterFactory.TRUNCATE_AFTER_CHARS_KEY,
                  "6");
            });
    assertTrue(expected.getMessage().contains("Can only give one of the following parameters:"));
  }

  /** Test that negative prefix length result in exception */
  public void testNonPositivePrefixLengthArgument() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "Truncate", TruncateTokenFilterFactory.TRUNCATE_AFTER_CODEPOINTS_KEY, "-5");
            });
    assertTrue(
        expected
            .getMessage()
            .contains(
                TruncateTokenFilterFactory.TRUNCATE_AFTER_CODEPOINTS_KEY
                    + " parameter must be a positive number: -5"));
  }
}
