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
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKWidthFilterFactory;
import org.apache.lucene.tests.analysis.BaseTokenStreamFactoryTestCase;
import org.junit.Test;

public class TestJapaneseCompletionFilterFactory extends BaseTokenStreamFactoryTestCase {
  @Test
  public void testCompletion() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<>());
    TokenStream tokenStream = tokenizerFactory.create();
    ((Tokenizer) tokenStream).setReader(new StringReader("東京ｔ"));
    CJKWidthFilterFactory cjkWidthFactory = new CJKWidthFilterFactory(new HashMap<>());
    tokenStream = cjkWidthFactory.create(tokenStream);
    Map<String, String> map = new HashMap<>();
    map.put("mode", "QUERY");
    JapaneseCompletionFilterFactory filterFactory = new JapaneseCompletionFilterFactory(map);
    assertTokenStreamContents(filterFactory.create(tokenStream), new String[] {"東京t", "toukyout"});
  }

  /** Test that bogus arguments result in exception */
  @Test
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new JapaneseCompletionFilterFactory(
                  new HashMap<String, String>() {
                    {
                      put("bogusArg", "bogusValue");
                    }
                  });
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
