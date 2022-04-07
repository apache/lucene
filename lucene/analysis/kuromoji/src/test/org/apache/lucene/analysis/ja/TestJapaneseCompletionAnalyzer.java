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
import java.util.Random;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.junit.Test;

public class TestJapaneseCompletionAnalyzer extends BaseTokenStreamTestCase {

  @Test
  public void testCompletionDefault() throws IOException {
    // mode=INDEX (default)
    Analyzer analyzer = new JapaneseCompletionAnalyzer();
    assertAnalyzesTo(
        analyzer,
        "東京",
        new String[] {"東京", "toukyou"},
        new int[] {0, 0},
        new int[] {2, 2},
        new int[] {1, 0});
    analyzer.close();
  }

  @Test
  public void testCompletionQuery() throws IOException {
    // mode=QUERY
    Analyzer analyzer = new JapaneseCompletionAnalyzer(null, JapaneseCompletionFilter.Mode.QUERY);
    assertAnalyzesTo(
        analyzer,
        "東京ｔ",
        new String[] {"東京t", "toukyout"},
        new int[] {0, 0},
        new int[] {3, 3},
        new int[] {1, 0});
    analyzer.close();
  }

  /** blast random strings against the analyzer */
  @Test
  public void testRandom() throws IOException {
    Random random = random();
    final Analyzer a = new JapaneseCompletionAnalyzer();
    checkRandomData(random, a, atLeast(100));
    a.close();
  }

  /** blast some random large strings through the analyzer */
  @Test
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    final Analyzer a = new JapaneseCompletionAnalyzer();
    checkRandomData(random, a, 2 * RANDOM_MULTIPLIER, 8192);
    a.close();
  }
}
