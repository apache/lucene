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
package org.apache.lucene.analysis.sq;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.BytesRef;

/** Test the Albanian analyzer */
public class TestAlbanianAnalyzer extends BaseTokenStreamTestCase {

  /** This test fails with NPE when the stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new AlbanianAnalyzer().close();
  }

  public void testStopwords() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(a, "Çfarë është durimi?", new String[] {"dur"});
    // the stop list matches the spelling without diacritics as well
    assertAnalyzesTo(a, "Cfare eshte durimi?", new String[] {"dur"});
    a.close();
  }

  public void testCustomStopwords() throws IOException {
    Analyzer a = new AlbanianAnalyzer(CharArraySet.EMPTY_SET);
    assertAnalyzesTo(a, "Çfarë është durimi?", new String[] {"cfar", "esht", "dur"});
    a.close();
  }

  public void testReusableTokenStream() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(a, "durimtarët", new String[] {"dur"});
    assertAnalyzesTo(a, "durim", new String[] {"dur"});
    a.close();
  }

  /** Normalization runs before stemming: ë/ç folding and apostrophe removal */
  public void testNormalization() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(a, "Kur'ani Kur’anit kuran", new String[] {"kuran", "kuran", "kuran"});
    assertAnalyzesTo(a, "mëshirë meshire", new String[] {"meshir", "meshir"});
    a.close();
  }

  /** The query-time normalization chain folds the same way as the index-time chain. */
  public void testNormalize() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertEquals(new BytesRef("kurani"), a.normalize("dummy", "Kur'ani"));
    assertEquals(new BytesRef("eshte"), a.normalize("dummy", "Është"));
    a.close();
  }

  /**
   * Poetry of the Rilindja period (public domain): the opening of Naim Frashëri's "Bagëti e
   * Bujqësia" (1886) and the first stanza of Asdreni's "Himni i Flamurit" (1912), the Albanian
   * national anthem. The elided vowels written with apostrophes (malet', q'u, dit') are handled by
   * the normalizer.
   */
  public void testRilindjaPoetry() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(
        a,
        "O malet' e Shqipërisë e ju o lisat' e gjatë! Fushat e gjera me lule, q'u kam ndër mënt dit' e natë!",
        new String[] {
          "o", "mal", "shqipr", "o", "lis", "fush", "gjr", "lul", "ment", "dit", "nat"
        });
    assertAnalyzesTo(
        a,
        "Rreth flamurit të përbashkuar, me një dëshirë e një qëllim, të gjith' atij duke u betuar, të lidhim besën për shpëtim.",
        new String[] {"flam", "perbashk", "deshir", "qell", "bet", "lidh", "bes", "shpet"});
    a.close();
  }

  /**
   * Gheg (northern) writing, public domain: the first line of Gjergj Fishta's "Lahuta e Malcís"
   * (1937) and the best-known sentence of the Kanun of Lekë Dukagjini as codified by Shtjefën
   * Gjeçovi (1933). The circumflex vowels and the elided clitic are normalized; the stemmer treats
   * the Gheg endings like the standard ones.
   */
  public void testGhegText() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(a, "Ndihmo, Zot, si m'ke ndihmue!", new String[] {"ndihm", "zot", "ndihm"});
    assertAnalyzesTo(
        a,
        "Shpija e shqyptarit âsht e Zotit dhe e mikut.",
        new String[] {"shp", "shqyp", "zot", "mik"});
    a.close();
  }

  /**
   * Everyday Gheg as spoken and written in Kosovo and northern Albania: the Gheg forms of "to be"
   * and "to have" (âsht, kena), the dropped ë (shum, tash), the infinitive with "me" and the
   * participle in -ue, and the elided clitics.
   */
  public void testGhegEveryday() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(a, "A po vjen me shkue n'shpi?", new String[] {"shk", "shp"});
    assertAnalyzesTo(a, "Kena punue shum sot.", new String[] {"pun", "sot"});
    assertAnalyzesTo(a, "S'kam kohë me t'ndihmue.", new String[] {"koh", "ndihm"});
    assertAnalyzesTo(a, "Kjo âsht shpija e nânës.", new String[] {"shp", "nan"});
    assertAnalyzesTo(a, "Qysh je? Tash po vij.", new String[] {"vij"});
    assertAnalyzesTo(a, "Çka ke bâ? Hiq, kurrgjâ.", new String[] {"ba", "kurrg"});
    a.close();
  }

  /**
   * Legal prose: Article 1 of the Constitution of the Republic of Kosovo (2008) and Article 1 of
   * the Universal Declaration of Human Rights in Albanian.
   */
  public void testLegalText() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(
        a,
        "Republika e Kosovës është shtet i pavarur, sovran, demokratik, unik dhe i pandashëm.",
        new String[] {"republik", "kos", "shtet", "pavar", "sovran", "demokratik", "unik", "pand"});
    assertAnalyzesTo(
        a,
        "Të gjithë njerëzit lindin të lirë dhe të barabartë në dinjitet dhe në të drejta.",
        new String[] {"njerez", "lind", "lir", "barabart", "dinjitet", "drejt"});
    assertAnalyzesTo(
        a,
        "Ata kanë arsye dhe ndërgjegje dhe duhet të sillen ndaj njëri-tjetrit me frymë vëllazërimi.",
        new String[] {"arsy", "ndergjeg", "duh", "sill", "njr", "tjetr", "frym", "vellazer"});
    a.close();
  }

  /**
   * Encyclopedic prose: sentences from the Albanian Wikipedia articles "Gjuha shqipe", "Shqipëria",
   * "Naim Frashëri" and "Kushtetuta e Shqipërisë" (CC BY-SA).
   */
  public void testEncyclopedicText() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(
        a,
        "Gjuha shqipe është gjuhë dhe degë e veçantë e familjes indo-evropiane.",
        new String[] {"gjuh", "shqip", "gjuh", "deg", "vecant", "famil", "ind", "evropian"});
    assertAnalyzesTo(
        a,
        "Shqipja është gjuha zyrtare e Shqipërisë dhe Kosovës.",
        new String[] {"shqip", "gjuh", "zyr", "shqipr", "kos"});
    assertAnalyzesTo(
        a,
        "Shqipëria është një shtet në Europën Juglindore.",
        new String[] {"shqipr", "shtet", "europ", "juglindor"});
    assertAnalyzesTo(
        a,
        "Naim Frashëri ka qenë poeti më i madh i Rilindjes Kombëtare.",
        new String[] {"naim", "frashr", "poet", "madh", "rilind", "komb"});
    assertAnalyzesTo(
        a,
        "Kjo kushtetutë përkufizon Shqipërinë si republikë parlamentare, shtet unitar e të pandashëm.",
        new String[] {
          "kushtetut", "perkufiz", "shqipr", "republik", "parlam", "shtet", "uni", "pand"
        });
    assertAnalyzesTo(
        a,
        "Tradita e shqiptarëve për rregull dhe ligj është e vjetër.",
        new String[] {"trad", "shqip", "rreguj", "lig", "vjetr"});
    assertAnalyzesTo(
        a,
        "Kanuni i Lekë Dukagjinit u ka shërbyer shqiptarëve për ta ruajtur vetëqeverisjen e tyre.",
        new String[] {"kan", "lek", "dukag", "sherb", "shqip", "ruajt", "veteqevr"});
    assertAnalyzesTo(
        a,
        "Sipas Kanunit, vendime të rëndësishme merren nga kuvendet e pleqve.",
        new String[] {"kan", "vend", "rend", "merr", "kuvend", "plek"});
    a.close();
  }

  // spellchecker:ignore-next-line
  /** Religious prose: phrases from the Albanian Qur'an translation of Sherif Ahmeti. */
  public void testReligiousText() throws IOException {
    Analyzer a = new AlbanianAnalyzer();
    assertAnalyzesTo(
        a, "Kërkoni ndihmë me durim dhe me namaz", new String[] {"kerk", "ndihm", "dur", "namaz"});
    assertAnalyzesTo(
        a,
        "Me emrin e All-llahut, Mëshiruesit, Mëshirëbërësit!",
        new String[] {"emr", "all", "llah", "meshir", "meshirebr"});
    assertAnalyzesTo(a, "besimtarët besuan", new String[] {"bes", "bes"});
    assertAnalyzesTo(a, "Zoti i botëve", new String[] {"zot", "bot"});
    a.close();
  }

  public void testWithStemExclusionSet() throws IOException {
    CharArraySet set = new CharArraySet(1, true);
    set.add("durimi");
    Analyzer a = new AlbanianAnalyzer(CharArraySet.EMPTY_SET, set);
    assertAnalyzesTo(a, "durimi durim", new String[] {"durimi", "dur"});
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new AlbanianAnalyzer();
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
}
