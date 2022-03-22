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
package org.apache.lucene.analysis.kuromoji.tests;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;

public class TestJapaneseAnalyzer extends LuceneTestCase {

  public void testJapaneseAnalyzerWorksInModuleMode() throws IOException {
    Analyzer a = new JapaneseAnalyzer();
    assertNotNull(a);
    assertAnalyzesTo(
        a,
        "多くの学生が試験に落ちた。",
        new String[] {"多く", "学生", "試験", "落ちる"},
        new int[] {0, 3, 6, 9},
        new int[] {2, 5, 8, 11},
        new int[] {1, 2, 2, 2});
    a.close();
  }

  public void testWeAreModule() {
    Assert.assertTrue(this.getClass().getModule().isNamed());
  }

  public void testKuromojiIsAModule() {
    Assert.assertTrue(JapaneseAnalyzer.class.getModule().isNamed());
  }
}
