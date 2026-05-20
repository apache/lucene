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
package org.apache.lucene.analysis.nl;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;

/**
 * Test the Dutch Stem Filter, which only modifies the term text.
 *
 * <p>The code states that it uses the snowball algorithm, but tests reveal some differences.
 */
public class TestDutchAnalyzer extends BaseTokenStreamTestCase {

  public void testWithSnowballExamples() throws Exception {
    check("lichaamsziek", "lichaamsziek");
    check("lichamelijk", "lichamelijk");
    check("lichamelijke", "lichamelijk");
    check("lichamelijkheden", "lichamelijk");
    check("lichamen", "lichaam");
    check("lichere", "licher");
    check("licht", "licht");
    check("lichtbeeld", "lichtbeeld");
    check("lichtbruin", "lichtbruin");
    check("lichtdoorlatende", "lichtdoorlaat");
    check("lichte", "licht");
    check("lichten", "licht");
    check("lichtende", "licht");
    check("lichtenvoorde", "lichtenvoor");
    check("lichter", "lichter");
    check("lichtere", "lichter");
    check("lichters", "lichter");
    check("lichtgevoeligheid", "lichtvoel");
    check("lichtgewicht", "lichtwicht");
    check("lichtgrijs", "lichtgrijs");
    check("lichthoeveelheid", "lichthoeveel");
    check("lichtintensiteit", "lichtintens");
    check("lichtje", "licht");
    check("lichtjes", "licht");
    check("lichtkranten", "lichtkrant");
    check("lichtkring", "lichtkr");
    check("lichtkringen", "lichtkr");
    check("lichtregelsystemen", "lichtrelsysteem");
    check("lichtste", "licht");
    check("lichtstromende", "lichtstroom");
    check("lichtte", "licht");
    check("lichtten", "licht");
    check("lichttoetreding", "lichttoetreed");
    check("lichtverontreinigde", "lichtverontrein");
    check("lichtzinnige", "lichtzin");
    check("lid", "lid");
    check("lidia", "lidia");
    check("lidmaatschap", "lidmaatschap");
    check("lidstaten", "lidstaat");
    check("lidvereniging", "lidvereen");
    check("opgingen", "opg");
    check("opglanzing", "opglans");
    check("opglanzingen", "opglans");
    check("opglimlachten", "opglimlacht");
    check("opglimpen", "opglimp");
    check("opglimpende", "opglimp");
    check("opglimping", "opglimp");
    check("opglimpingen", "opglimp");
    check("opgraven", "opgraaf");
    check("opgrijnzen", "opgrijns");
    check("opgrijzende", "opgrijs");
    check("opgroeien", "opgroei");
    check("opgroeiende", "opgroeiend");
    check("opgroeiplaats", "opgroeiplaats");
    check("ophaal", "ophaal");
    check("ophaaldienst", "ophaaldienst");
    check("ophaalkosten", "ophaalkost");
    check("ophaalsystemen", "ophaalsysteem");
    check("ophaalt", "ophaalt");
    check("ophaaltruck", "ophaaltruck");
    check("ophalen", "ophaal");
    check("ophalend", "ophaal");
    check("ophalers", "ophaler");
    check("ophef", "ophef");
    check("opheldering", "opheldeer");
    check("ophemelde", "ophemel");
    check("ophemelen", "ophemeel");
    check("opheusden", "opheus");
    check("ophief", "ophief");
    check("ophield", "ophield");
    check("ophieven", "ophief");
    check("ophoepelt", "ophoepelt");
    check("ophoog", "ophoog");
    check("ophoogzand", "ophoogzand");
    check("ophopen", "ophoop");
    check("ophoping", "ophoop");
    check("ophouden", "ophoud");
  }

  public void testSnowballCorrectness() throws Exception {
    Analyzer a = new DutchAnalyzer();
    checkOneTerm(a, "opheffen", "ophef");
    checkOneTerm(a, "opheffende", "ophef");
    checkOneTerm(a, "opheffing", "ophef");
    a.close();
  }

  public void testReusableTokenStream() throws Exception {
    Analyzer a = new DutchAnalyzer();
    checkOneTerm(a, "lichaamsziek", "lichaamsziek");
    checkOneTerm(a, "lichamelijk", "lichamelijk");
    checkOneTerm(a, "lichamelijke", "lichamelijk");
    checkOneTerm(a, "lichamelijkheden", "lichamelijk");
    a.close();
  }

  public void testExclusionTableViaCtor() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("lichamelijk");
    DutchAnalyzer a = new DutchAnalyzer(CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "lichamelijk lichamelijke", new String[] {"lichamelijk", "lichamelijk"});
    a.close();

    a = new DutchAnalyzer(CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "lichamelijk lichamelijke", new String[] {"lichamelijk", "lichamelijk"});
    a.close();
  }

  /** check that the default stem overrides are used even if you use a non-default ctor. */
  public void testStemOverrides() throws IOException {
    DutchAnalyzer a = new DutchAnalyzer(CharArraySet.EMPTY_SET);
    checkOneTerm(a, "fiets", "fiets");
    a.close();
  }

  public void testEmptyStemDictionary() throws IOException {
    DutchAnalyzer a =
        new DutchAnalyzer(
            CharArraySet.EMPTY_SET, CharArraySet.EMPTY_SET, CharArrayMap.<String>emptyMap());
    checkOneTerm(a, "fiets", "fiet");
    a.close();
  }

  /** Test that stopwords are not case sensitive */
  public void testStopwordsCasing() throws IOException {
    DutchAnalyzer a = new DutchAnalyzer();
    assertAnalyzesTo(a, "Zelf", new String[] {});
    a.close();
  }

  private void check(final String input, final String expected) throws Exception {
    Analyzer analyzer = new DutchAnalyzer();
    checkOneTerm(analyzer, input, expected);
    analyzer.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new DutchAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
