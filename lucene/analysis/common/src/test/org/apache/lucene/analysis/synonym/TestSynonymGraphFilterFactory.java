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

import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.pattern.PatternTokenizerFactory;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.tests.util.StringMockResourceLoader;
import org.apache.lucene.util.Version;

public class TestSynonymGraphFilterFactory extends BaseTokenStreamFactoryTestCase {

  /** checks for synonyms of "GB" in synonyms.txt */
  private void checkSolrSynonyms(TokenFilterFactory factory) throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTrue(stream instanceof SynonymGraphFilter);
    assertTokenStreamContents(
        stream, new String[] {"gib", "gigabyte", "gigabytes", "GB"}, new int[] {1, 0, 0, 0});
  }

  /** checks for synonyms of "second" in synonyms-wordnet.txt */
  private void checkWordnetSynonyms(TokenFilterFactory factory) throws Exception {
    Reader reader = new StringReader("second");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTrue(stream instanceof SynonymGraphFilter);
    assertTokenStreamContents(stream, new String[] {"2nd", "two", "second"}, new int[] {1, 0, 0});
  }

  /** test that we can parse and use the solr syn file */
  public void testSynonyms() throws Exception {
    checkSolrSynonyms(tokenFilterFactory("SynonymGraph", "synonyms", "synonyms.txt"));
  }

  /** if the synonyms are completely empty, test that we still analyze correctly */
  public void testEmptySynonyms() throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream =
        tokenFilterFactory(
                "SynonymGraph",
                Version.LATEST,
                new StringMockResourceLoader(""), // empty file!
                "synonyms",
                "synonyms.txt")
            .create(stream);
    assertTokenStreamContents(stream, new String[] {"GB"});
  }

  public void testFormat() throws Exception {
    checkSolrSynonyms(
        tokenFilterFactory("SynonymGraph", "synonyms", "synonyms.txt", "format", "solr"));
    checkWordnetSynonyms(
        tokenFilterFactory(
            "SynonymGraph", "synonyms", "synonyms-wordnet.txt", "format", "wordnet"));
    // explicit class should work the same as the "solr" alias
    checkSolrSynonyms(
        tokenFilterFactory(
            "SynonymGraph",
            "synonyms",
            "synonyms.txt",
            "format",
            SolrSynonymParser.class.getName()));
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "SynonymGraph", "synonyms", "synonyms.txt", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  /** Test that analyzer and tokenizerFactory is both specified */
  public void testAnalyzer() throws Exception {
    final String analyzer = CJKAnalyzer.class.getName();
    final String tokenizerFactory = PatternTokenizerFactory.class.getName();
    TokenFilterFactory factory = null;

    factory = tokenFilterFactory("SynonymGraph", "synonyms", "synonyms2.txt", "analyzer", analyzer);
    assertNotNull(factory);

    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory(
                  "SynonymGraph",
                  "synonyms",
                  "synonyms.txt",
                  "analyzer",
                  analyzer,
                  "tokenizerFactory",
                  tokenizerFactory);
            });
    assertTrue(
        expected.getMessage().contains("Analyzer and TokenizerFactory can't be specified both"));
  }

  /** Test that we can parse TokenierFactory's arguments */
  public void testTokenizerFactoryArguments() throws Exception {
    final String clazz = PatternTokenizerFactory.class.getName();
    TokenFilterFactory factory = null;

    // simple arg form
    factory =
        tokenFilterFactory(
            "SynonymGraph",
            "synonyms",
            "synonyms.txt",
            "tokenizerFactory",
            clazz,
            "pattern",
            "(.*)",
            "group",
            "0");
    assertNotNull(factory);
    // prefix
    factory =
        tokenFilterFactory(
            "SynonymGraph",
            "synonyms",
            "synonyms.txt",
            "tokenizerFactory",
            clazz,
            "tokenizerFactory.pattern",
            "(.*)",
            "tokenizerFactory.group",
            "0");
    assertNotNull(factory);

    // sanity check that sub-PatternTokenizerFactory fails w/o pattern
    expectThrows(
        Exception.class,
        () -> {
          tokenFilterFactory("SynonymGraph", "synonyms", "synonyms.txt", "tokenizerFactory", clazz);
        });

    // sanity check that sub-PatternTokenizerFactory fails on unexpected
    expectThrows(
        Exception.class,
        () -> {
          tokenFilterFactory(
              "SynonymGraph",
              "synonyms",
              "synonyms.txt",
              "tokenizerFactory",
              clazz,
              "tokenizerFactory.pattern",
              "(.*)",
              "tokenizerFactory.bogusbogusbogus",
              "bogus",
              "tokenizerFactory.group",
              "0");
        });
  }
}
