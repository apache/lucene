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
package org.apache.lucene.analysis.nori.tests;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;

public class TestKoreanAnalyzer extends LuceneTestCase {

  public void testKoreanAnalyzerWorksInModuleMode() throws IOException {
    Analyzer a = new KoreanAnalyzer();
    assertNotNull(a);
    assertAnalyzesTo(
        a,
        "한국은 대단한 나라입니다.",
        new String[] {"한국", "대단", "나라", "이"},
        new int[] {0, 4, 8, 10},
        new int[] {2, 6, 10, 13},
        new int[] {1, 2, 3, 1});
    a.close();
  }

  public void testWeAreModule() {
    Assert.assertTrue(this.getClass().getModule().isNamed());
  }

  public void testNoriIsAModule() {
    Assert.assertTrue(KoreanAnalyzer.class.getModule().isNamed());
  }
}
