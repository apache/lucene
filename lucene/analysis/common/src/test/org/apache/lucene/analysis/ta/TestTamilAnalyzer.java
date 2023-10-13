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
package org.apache.lucene.analysis.ta;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

/** Tests the TamilAnalyzer */
public class TestTamilAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new TamilAnalyzer().close();
  }

  /** test that snowball stemmer is hooked in */
  public void testStemming() throws Exception {
    Analyzer a = new TamilAnalyzer();
    // friend
    checkOneTerm(a, "நண்பன்", "நண்");
    // friends
    checkOneTerm(a, "நண்பர்கள்", "நண்");
    a.close();
  }

  public void testExclusionSet() throws Exception {
    CharArraySet exclusionSet = new CharArraySet(asSet("நண்பர்கள்"), false);
    Analyzer a = new TamilAnalyzer(TamilAnalyzer.getDefaultStopSet(), exclusionSet);
    checkOneTerm(a, "நண்பர்கள்", "நண்பர்கள்");
    a.close();
  }

  /** test we fold digits to latin-1 */
  public void testDigits() throws Exception {
    TamilAnalyzer a = new TamilAnalyzer();
    checkOneTerm(a, "௧௨௩௪", "1234");
    a.close();
  }

  /** tamil doesn't have case, but test we case-fold any latin-1 etc */
  public void testLowerCase() throws Exception {
    TamilAnalyzer a = new TamilAnalyzer();
    checkOneTerm(a, "FIFA", "fifa");
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new TamilAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
