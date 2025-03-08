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

package org.apache.lucene.analysis;

import static org.apache.lucene.analysis.Analyzer.PER_FIELD_REUSE_STRATEGY;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.Analyzer.ReuseStrategy;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestAnalyzerWrapper extends LuceneTestCase {

  public void testSourceDelegation() throws IOException {

    AtomicBoolean sourceCalled = new AtomicBoolean(false);

    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(
                _ -> {
                  sourceCalled.set(true);
                },
                new CannedTokenStream());
          }
        };

    Analyzer wrapper =
        new AnalyzerWrapper(analyzer.getReuseStrategy()) {
          @Override
          protected Analyzer getWrappedAnalyzer(String fieldName) {
            return analyzer;
          }

          @Override
          protected TokenStreamComponents wrapComponents(
              String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(
                components.getSource(), new LowerCaseFilter(components.getTokenStream()));
          }
        };

    try (TokenStream ts = wrapper.tokenStream("", "text")) {
      assert ts != null;
      assertTrue(sourceCalled.get());
    }
  }

  /**
   * Test that {@link AnalyzerWrapper.UnwrappingReuseStrategy} consults the wrapped analyzer's reuse
   * strategy if components can be reused or need to be updated.
   */
  public void testUnwrappingReuseStrategy() {
    AtomicBoolean reuse = new AtomicBoolean(true);

    final ReuseStrategy wrappedAnalyzerStrategy =
        new ReuseStrategy() {
          @Override
          public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
            if (reuse.get() == false) {
              return null;
            } else {
              return (TokenStreamComponents) getStoredValue(analyzer);
            }
          }

          @Override
          public void setReusableComponents(
              Analyzer analyzer, String fieldName, TokenStreamComponents components) {
            setStoredValue(analyzer, components);
          }
        };
    Analyzer wrappedAnalyzer =
        new Analyzer(wrappedAnalyzerStrategy) {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(_ -> {}, new CannedTokenStream());
          }
        };

    AnalyzerWrapper wrapperAnalyzer =
        new AnalyzerWrapper(PER_FIELD_REUSE_STRATEGY) {
          @Override
          protected Analyzer getWrappedAnalyzer(String fieldName) {
            return wrappedAnalyzer;
          }

          @Override
          protected TokenStreamComponents wrapComponents(
              String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(
                components.getSource(), new LowerCaseFilter(components.getTokenStream()));
          }
        };

    TokenStream ts = wrapperAnalyzer.tokenStream("", "text");
    TokenStream ts2 = wrapperAnalyzer.tokenStream("", "text");
    assertEquals(ts2, ts);

    reuse.set(false);
    TokenStream ts3 = wrapperAnalyzer.tokenStream("", "text");
    assertNotSame(ts3, ts2);
    TokenStream ts4 = wrapperAnalyzer.tokenStream("", "text");
    assertNotSame(ts4, ts3);

    reuse.set(true);
    TokenStream ts5 = wrapperAnalyzer.tokenStream("", "text");
    assertEquals(ts5, ts4);
    TokenStream ts6 = wrapperAnalyzer.tokenStream("", "text");
    assertEquals(ts6, ts5);
  }
}
