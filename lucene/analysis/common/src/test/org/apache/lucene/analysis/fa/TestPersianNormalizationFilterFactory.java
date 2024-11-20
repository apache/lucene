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
package org.apache.lucene.analysis.fa;

import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;

/** Simple tests to ensure the Persian normalization factory is working. */
public class TestPersianNormalizationFilterFactory extends BaseTokenStreamFactoryTestCase {
  /** Ensure the filter actually normalizes persian text. */
  public void testNormalization() throws Exception {
    Reader reader = new StringReader("های");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("PersianNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] {"هاي"});
  }

  /** Test PersianStemFilterFactory */
  public void testStemmer() throws Exception {
    Reader reader = new StringReader("کتابها بهترین دوستان");
    Tokenizer tokenizer = whitespaceMockTokenizer(reader);
    TokenStream stream = tokenFilterFactory("PersianNormalization").create(tokenizer);
    stream = tokenFilterFactory("PersianStem").create(stream);
    assertTokenStreamContents(stream, new String[] {"كتاب", "به", "دوست"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("PersianNormalization", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("PersianStem", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
