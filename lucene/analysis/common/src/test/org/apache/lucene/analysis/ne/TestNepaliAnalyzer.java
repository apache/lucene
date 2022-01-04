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
package org.apache.lucene.analysis.ne;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

/** Tests the NepaliAnalyzer */
public class TestNepaliAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new NepaliAnalyzer().close();
  }

  /** test that snowball stemmer is hooked in correctly */
  public void testStemming() throws Exception {
    Analyzer a = new NepaliAnalyzer();
    // friend
    checkOneTerm(a, "मित्र", "मित्र");
    // friends
    checkOneTerm(a, "मित्रहरु", "मित्र");
    a.close();
  }

  public void testStopwords() throws Exception {
    Analyzer a = new NepaliAnalyzer();
    assertAnalyzesTo(
        a,
        "सबै व्यक्तिहरू जन्मजात स्वतन्त्र हुन् ती सबैको समान अधिकार र महत्व",
        new String[] {"व्यक्ति", "जन्मजात", "स्वतन्त्र", "सबै", "समान", "अधिकार", "महत्व"});
    a.close();
  }

  /** nepali has no case, but any latin-1 etc should be casefolded */
  public void testLowerCase() throws Exception {
    Analyzer a = new NepaliAnalyzer();
    checkOneTerm(a, "FIFA", "fifa");
    a.close();
  }

  public void testExclusionSet() throws Exception {
    CharArraySet exclusionSet = new CharArraySet(asSet("मित्रहरु"), false);
    Analyzer a = new NepaliAnalyzer(NepaliAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "मित्रहरु", "मित्रहरु");
    a.close();
  }

  /** test we fold digits to latin-1 */
  public void testDigits() throws Exception {
    NepaliAnalyzer a = new NepaliAnalyzer();
    checkOneTerm(a, "१२३४", "1234");
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new NepaliAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
