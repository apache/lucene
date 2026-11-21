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

import static org.apache.lucene.tests.analysis.VocabularyAssert.assertVocabulary;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/**
 * Tests for {@link AlbanianLightStemFilter}. Inputs are given the way the filter sees them inside
 * {@link AlbanianAnalyzer}: lower-cased and with ë/ç already folded. The paradigms follow the
 * declension and conjugation tables of the English Wiktionary for the words in question.
 */
public class TestAlbanianLightStemFilter extends BaseTokenStreamTestCase {
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(source, new AlbanianLightStemFilter(source));
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }

  private void check(String stem, String... forms) throws IOException {
    for (String form : forms) {
      checkOneTerm(analyzer, form, stem);
    }
  }

  /**
   * Regression vocabulary: every inflected form of the Albanian lemmas that carry a declension or
   * conjugation table in the English Wiktionary, plus the vocabulary of a set of Albanian Wikipedia
   * articles and of a Qur'an translation, each with the stem this stemmer produced when the file
   * was generated. It pins the behaviour of the rules on tens of thousands of real words.
   */
  public void testVocabulary() throws IOException {
    assertVocabulary(analyzer, getDataPath("sqlighttestdata.zip"), "sqlight.txt");
  }

  /**
   * Masculine nouns: indefinite, definite nominative, accusative, dative, indefinite plural,
   * definite plural, plural dative, plural ablative.
   */
  public void testMasculineNouns() throws IOException {
    check("mal", "mal", "mali", "malin", "malit", "male", "malet", "maleve", "malesh");
    check("det", "det", "deti", "detin", "detit", "dete", "detet", "deteve", "detesh");
    check("vend", "vend", "vendi", "vendin", "vendit", "vende", "vendet", "vendeve", "vendesh");
    // "u" type (stem ends in h, k, g or a stressed vowel)
    check("allah", "allah", "allahu", "allahun", "allahut");
    check("zot", "zot", "zoti", "zotit", "zotin");
    // nouns in -im
    check(
        "bes",
        "besim",
        "besimi",
        "besimin",
        "besimit",
        "besime",
        "besimet",
        "besimeve",
        "besimesh");
  }

  /** Feminine nouns in -ë, -e, -i and -je. */
  public void testFeminineNouns() throws IOException {
    check("vajz", "vajze", "vajza", "vajzen", "vajzes", "vajzat", "vajzave", "vajzash");
    check("pun", "pune", "puna", "punen", "punes", "punet", "puneve", "punesh");
    check("gjuh", "gjuhe", "gjuha", "gjuhen", "gjuhes", "gjuhet", "gjuheve", "gjuhesh");
    check(
        "shkoll",
        "shkolle",
        "shkolla",
        "shkollen",
        "shkolles",
        "shkollat",
        "shkollave",
        "shkollash");
    check("rrug", "rruge", "rruga", "rrugen", "rruges", "rruget", "rrugeve");
    check("dit", "dite", "dita", "diten", "dites", "ditet", "diteve", "ditesh");
    check("lul", "lule", "lulja", "lulen", "lules", "lulet", "luleve", "lulesh");
    // -i nouns: the stacked endings all conflate
    check("shtep", "shtepi", "shtepia", "shtepine", "shtepise", "shtepite", "shtepive", "shtepish");
    check("lir", "liri", "liria", "lirine", "lirise", "lirite", "lirive", "lirish");
    check("shoqer", "shoqeri", "shoqeria", "shoqerine", "shoqerise", "shoqerite", "shoqerive");
    // -je nouns
    check("lut", "lutje", "lutja", "lutjen", "lutjes", "lutjet", "lutjeve", "lutjesh");
    check("fem", "femije", "femija", "femijen", "femijes", "femijet", "femijeve", "femijesh");
  }

  /**
   * Nouns whose final ë drops before an ending (libër / libri) get the same stem in both shapes.
   */
  public void testSyncope() throws IOException {
    check("libr", "liber", "libri", "librin", "librit", "libra", "librat", "librave", "librash");
    check("emr", "emer", "emri", "emrin", "emrit", "emra", "emrat", "emrave");
    check("zemr", "zemer", "zemra", "zemren", "zemres", "zemrat", "zemrave");
    check("qendr", "qender", "qendra", "qendren", "qendres", "qendrat");
    check("motr", "moter", "motra", "motren", "motres", "motrat");
    check("dimr", "dimer", "dimri", "dimrin", "dimrit");
    check("numr", "numer", "numri", "numrin", "numrat");
    check("vjetr", "vjeter", "vjetra", "vjetrit", "vjetrat");
    check("teatr", "teater", "teatri", "teatrin", "teatrit");
    check("ministr", "minister", "ministri", "ministrin", "ministrat");
    check("ajr", "ajer", "ajri", "ajrin", "ajrit");
    check("afr", "afer", "aferm", "aferme", "afermit");
    check("cakrr", "cakerr", "cakrri", "cakrrin");
    check("arkl", "arkel", "arkli", "arklin", "arkla");
    check("vegjl", "vegjel", "vegjlit", "vegjla");
    // roots in -er after z, r, l, n, s are left alone
    check("sheqer", "sheqer", "sheqeri", "sheqerin");
    check("vellazer", "vellazeri", "vellazerim", "vellazerimi");
  }

  /**
   * The less common plural classes: -inj (arabaxhi / arabaxhinj), -llarë (baba / baballarë), -ëra
   * (baltë / baltëra), and nouns in -ajë / -onjë whose j is part of the stem.
   */
  public void testOtherPlurals() throws IOException {
    check("arabaxh", "arabaxhi", "arabaxhinj", "arabaxhinjt", "arabaxhinjve", "arabaxhinjsh");
    check("bab", "baba", "babai", "baballare", "baballaret", "baballareve");
    check("balt", "balte", "balta", "balten", "baltera", "balterat", "balterave");
    check("zon", "zonje", "zonja", "zonjen", "zonjes", "zonjat", "zonjave");
    check("gjun", "gjunje", "gjunjet");
    // -enj plural (lumë / lumenj)
    check("lum", "lume", "lumi", "lumin", "lumenj", "lumenjte", "lumenjve", "lumenjsh");
  }

  /** Loanwords in -tor keep it: only the native agent suffix -ëtor is derivational. */
  public void testTorLoanwords() throws IOException {
    check("histor", "histori", "historia", "historine", "historise");
    check("historik", "historik", "historike", "historiket");
    check("sektor", "sektor", "sektori", "sektorin", "sektoret");
    check("motor", "motor", "motori", "motoret");
    check("doktor", "doktor", "doktori", "doktoret");
    check("pun", "punetor", "punetore", "punetoret");
    check("luft", "luftetor", "luftetoret", "lufta", "luften");
  }

  /** Stemming twice gives the same result as stemming once. */
  public void testIdempotent() throws IOException {
    for (String word :
        new String[] {
          "mesonjetoret",
          "afermit",
          "siperme",
          "lumenjve",
          "arbunjve",
          "medhenj",
          "biznesmeneve",
          "nepermjet",
          "popujve",
          "miqte"
        }) {
      String once = stem(word);
      checkOneTerm(analyzer, once, once);
    }
  }

  private static String stem(String word) {
    char[] buffer = word.toCharArray();
    int len = new AlbanianLightStemmer().stem(buffer, buffer.length);
    return new String(buffer, 0, len);
  }

  /**
   * Adjectives in -iv keep their stem in the feminine (masiv / masive); -i nouns still lose their
   * plural dative -ive (bashkësi / bashkësive).
   */
  public void testIvAdjectives() throws IOException {
    check("aktiv", "aktiv", "aktive", "aktivet", "aktivit");
    check("masiv", "masiv", "masive");
    check("pasiv", "pasiv", "pasive");
    check("intensiv", "intensiv", "intensive");
    check("bashk", "bashkesi", "bashkesia", "bashkesive");
    check("njes", "njesi", "njesia", "njesive");
    check("ushtr", "ushtri", "ushtria", "ushtrive");
  }

  /** Nouns in -ëm (bagëm / bagmi) and loanwords in consonant + m (film / filmi). */
  public void testEmNouns() throws IOException {
    check("bag", "bagem", "bagmi", "bagmit");
    check("fil", "film", "filmi", "filmat");
    check("rit", "ritem", "ritmi", "ritmin");
  }

  /** -së is a genitive only after i; after a consonant it is stem + ë. */
  public void testSeEnding() throws IOException {
    check("shtep", "shtepise");
    check("lir", "lirise");
    check("autobus", "autobus", "autobusi", "autobuse", "autobusat");
    check("baldos", "baldose", "baldosa");
    check("ananas", "ananas", "ananasi", "ananase");
    // the Gheg genitive -s needs a word of five letters; in rris and abis the s is the root
    check("rris", "rris");
    check("abis", "abis", "abisi");
    check("lir", "liris", "liri");
  }

  /**
   * Plurals formed by palatalization (k → q, g → gj, ll → j) conflate with the singular, including
   * their definite -të / -t and ablative -sh.
   */
  public void testPalatalPlurals() throws IOException {
    check("mik", "mik", "miku", "mikun", "mikut", "miq", "miqte", "miqt", "miqve", "miqsh");
    check("zog", "zog", "zogu", "zogun", "zogut", "zogj", "zogjte", "zogjve", "zogjsh");
    check(
        "popuj",
        "popull",
        "populli",
        "popullin",
        "popullit",
        "popuj",
        "popujt",
        "popujve",
        "popujsh");
    check("shok", "shok", "shoku", "shokun", "shoqe", "shoqja", "shoqen");
    // the endings -të -t -sh are not touched elsewhere
    check("drejt", "drejt", "drejte", "drejta");
    check("shenjt", "shenjte", "shenjta");
    check("lart", "lart", "larte", "larta");
    // consonant + q is palatalized too (ujk / ujq, peshk / peshq)
    check("ujk", "ujk", "ujku", "ujq", "ujqit", "ujqve");
    check("peshk", "peshk", "peshku", "peshq", "peshqit");
  }

  /**
   * Three characters that spell a digraph and a consonant (sht, shp, shq, zgj, ngj) are a bare
   * consonant cluster, not a stem: -et, -at and -it are not taken down to one, and a final q in one
   * is not a palatal plural. This keeps shtet (state), shtatë (seven) and shtoj (add) apart, shpatë
   * (sword) and shpëtim (rescue) off the Gheg shpi (house), zgjat (stretch) off zgjoj (wake), ngjit
   * (climb) off ngjaj (resemble), and shqetësoj (worry) off shkoj (go).
   */
  public void testConsonantClusters() throws IOException {
    check("shtet", "shtet", "shteti", "shtetin", "shtetet", "shteteve");
    check("shtat", "shtate", "shtatit");
    check("sht", "shtoj", "shtohet");
    check("shpat", "shpate", "shpata");
    check("shpet", "shpetim", "shpetoj", "shpetuar");
    check("shp", "shpi", "shpija");
    check("zgjat", "zgjat", "zgjatet", "zgjatur", "zgjatje");
    check("zgj", "zgjoj", "zgjohem", "zgjim");
    check("ngjit", "ngjit", "ngjitet", "ngjitur", "ngjitje");
    check("ngj", "ngjashem");
    check("dhjet", "dhjete", "dhjeta");
    check("shqet", "shqetesim", "shqetesimi", "shqetesoj", "shqetesuar", "shqetesohet");
    check("shq", "shquar", "shqisa");
    check("shk", "shkoj", "shkoi", "shkuar");
    // a vowel and a digraph is a root, and keeps its endings coming off
    check("udh", "udhe", "udha", "udhat");
    check("arr", "arre", "arrat");
    // so is a consonant and sh, which goes on in derivations
    check("fsh", "fshat", "fshati", "fshatar", "fshatare");
  }

  /**
   * -ut belongs to the -u declension (stems in k, g, h or a vowel); elsewhere it is the root, so
   * kushtetutë (constitution) stays off kusht (condition), valutë (currency) off valë (wave), and
   * këput (tear off) off qep (sew).
   */
  public void testUtEndings() throws IOException {
    check("mik", "mik", "miku", "mikut");
    check("zog", "zog", "zogu", "zogut");
    check("allah", "allah", "allahu", "allahut");
    check("dhe", "dhe", "dheu", "dheut");
    check("kushtetut", "kushtetute", "kushtetuta", "kushtetutes");
    check("kusht", "kusht", "kushti", "kushtet");
    check("institut", "institut", "instituti", "institutin");
    check("valut", "valute", "valuta", "valutes");
    check("val", "vale", "vala", "valet");
    check("keput", "keput", "keputur", "keputje");
    check("minut", "minute", "minuta", "minutat");
    check("hajdut", "hajdut", "hajduti", "hajdutet");
  }

  /**
   * After f the -sh endings are the optative's own -fsh- (punofsh, pafsha) or part of a root, so
   * kafshë (animal) keeps its sh and stays off kafe (coffee); kofshë (thigh) and rrafsh (plain)
   * likewise.
   */
  public void testShAfterF() throws IOException {
    check("kafsh", "kafshe", "kafsha", "kafshet", "kafshoj", "kafshim");
    check("kaf", "kafe", "kafeja");
    check("kofsh", "kofshe", "kofsha", "kofshet");
    check("rrafsh", "rrafsh", "rrafshi", "rrafshoj");
    check("pun", "punofsh", "punofsha", "punoj");
    check("hap", "hapsh", "hapsha", "hapshim", "hapim");
  }

  /**
   * Verbs in -ej (kthej, shërbej, përkthej): present plural, aorist, participle and the nouns
   * derived from them conflate.
   */
  public void testEjVerbs() throws IOException {
    check("kth", "kthej", "kthen", "kthejme", "kthejne", "ktheva", "ktheu", "kthyer", "kthim");
    check(
        "sherb", "sherbej", "sherben", "sherbejme", "sherbeva", "sherbyer", "sherbim", "sherbetor");
    check("perkth", "perkthej", "perkthyer", "perkthim");
  }

  /**
   * -ist is derivational: the agent noun joins its base (artist / art, kapitalist / kapital,
   * aktivist / aktiv), the feminine -iste joins the masculine, and the -is verbs' imperfect
   * (braktiste) joins their paradigm.
   */
  public void testIstNouns() throws IOException {
    check("art", "art", "arti", "artist", "artiste", "artistet");
    check("kapital", "kapital", "kapitali", "kapitalist", "kapitalistet");
    check("aktiv", "aktiv", "aktive", "aktivist", "aktiviste");
    check("brakt", "braktis", "braktiste", "braktisur", "braktisje");
  }

  /**
   * -enj is a plural only after m, ll, rr, dh or a vowel; after a single r it is part of an
   * adjective stem, so i shtrenjtë (expensive) stays apart from shtrat (bed) and shtroj (lay).
   */
  public void testEnjPlurals() throws IOException {
    check("shtrenjt", "shtrenjte", "shtrenjta", "shtrenjtit");
    check("shtr", "shtrat", "shtrati", "shtroj", "shtrese");
    check("perr", "perrua", "perrenj", "perrenjte");
  }

  /**
   * Conflations of unrelated words that a suffix stripper cannot avoid, pinned so that a change in
   * the rules shows up here: the pairs differ only in ë, which the normalizer folds (qendër /
   * qëndroj, mes / mësoj, derë / dërmoj), or one word ends like an inflection of the other (botë /
   * botoj, hekur / heq, beton / betoj, veri / verë), or the palatal plural rule meets an original q
   * (drekë / dreq).
   */
  public void testKnownConflations() throws IOException {
    check("qendr", "qender", "qendra", "qendroj", "qendrim");
    check("mes", "mes", "mesoj", "mesim");
    check("der", "dere", "dermoj");
    check("bot", "bote", "botoj", "botim");
    check("hek", "hekur", "heq");
    check("bet", "beton", "betoj", "beteje");
    check("ver", "veri", "vere");
    check("drek", "dreke", "dreq");
  }

  /**
   * Nouns whose stem ends in -itet (dinjitet, universitet) keep it; only the definite plural
   * -itetet loses an ending. Shorter words in -tet are stemmed like any other (qytet → qyt), which
   * keeps their paradigm together.
   */
  public void testItetNouns() throws IOException {
    check("dinjitet", "dinjitet", "dinjiteti", "dinjitetin", "dinjitetit");
    check(
        "universitet",
        "universitet",
        "universiteti",
        "universitetin",
        "universitetet",
        "universiteteve");
    // the universe keeps a key of its own; the adjective universitar loses its -tar and joins it
    check("univers", "univers", "universi", "universin", "universitar");
    check("qyt", "qytet", "qyteti", "qytetin", "qytetit", "qytete", "qytetet", "qyteteve");
    check("shtet", "shtet", "shteti", "shtetin", "shtetet", "shteteve");
    check("vit", "vit", "viti", "vitin", "vitit", "vite", "vitet", "viteve");
    check("prit", "pritet", "pritur", "prita");
  }

  /**
   * Place names in -ë/-a and -i/-ia conflate across their cases, including Kosovë / Kosova whose
   * stem happens to end like an aorist.
   */
  public void testPlaceNames() throws IOException {
    check("kos", "kosove", "kosova", "kosoven", "kosoves");
    check("kosovar", "kosovar", "kosovare", "kosovaret", "kosovareve");
    check("shqipr", "shqiperi", "shqiperia", "shqiperine", "shqiperise");
    check("tiran", "tirane", "tirana", "tiranen", "tiranes");
    check("prisht", "prishtine", "prishtina", "prishtinen", "prishtines");
  }

  /** Adjectives: -an/-ane, -antë, -ëm/-me, -shëm/-shme, -ur/-ura. */
  public void testAdjectives() throws IOException {
    check("evropian", "evropian", "evropiane", "evropianet");
    check("vecant", "vecante", "vecanta");
    check("mes", "mesem", "mesme", "mesmja");
    check("sot", "sot", "sotem", "sotme");
    check("lasht", "lashte", "lashtem", "lashta");
    check("dur", "durueshem", "durueshme");
    check("buk", "bukur", "bukura", "bukurat");
  }

  /** Nouns in -izëm drop the ë before the article (socializëm / socializmi). */
  public void testIzemNouns() throws IOException {
    check("socializm", "socializem", "socializmi", "socializmin", "socializmit");
    check("organizm", "organizem", "organizmi", "organizmat", "organizmave");
  }

  /**
   * Class 1 verbs (-oj): present, imperfect, aorist, participle, imperative, subjunctive and
   * optative.
   */
  public void testClassOneVerbs() throws IOException {
    check(
        "pun",
        "punoj",
        "punon",
        "punojme",
        "punoni",
        "punojne",
        "punoja",
        "punoje",
        "punonte",
        "punonim",
        "punonit",
        "punonin",
        "punova",
        "punove",
        "punoi",
        "punuam",
        "punuat",
        "punuan",
        "punuar",
        "puno",
        "punosh",
        "punofsha",
        "punofte",
        "punofshim",
        "punuaka",
        "punuakam",
        "punuakan",
        "punuake",
        "punuakemi",
        "punuakeni",
        "punuakesh",
        "punuakesha",
        "punuakeshe",
        "punuakeshim",
        "punuakeshin",
        "punuakeshit");
    check("mes", "mesoj", "meson", "mesojme", "mesuan", "mesuar", "meso", "mesosh");
    check("shk", "shkoj", "shkon", "shkova", "shkoi", "shkuan", "shkuar");
  }

  /** Class 1 verbs in -uaj (shkruaj, write) and the nouns derived from them. */
  public void testUajVerbs() throws IOException {
    check(
        "shkr",
        "shkruaj",
        "shkruan",
        "shkruajme",
        "shkruani",
        "shkruajne",
        "shkrova",
        "shkroi",
        "shkruar",
        "shkrim",
        "shkrimi",
        "shkrimet");
  }

  /**
   * Class 2 verbs (consonant stems): present, imperfect plural, aorist, participle, passive present
   * and imperfect, optative.
   */
  public void testClassTwoVerbs() throws IOException {
    check(
        "hap",
        "hap",
        "hapim",
        "hapin",
        "hapni",
        "hapsh",
        "hapnim",
        "hapnit",
        "hapnin",
        "hapa",
        "hape",
        "hapi",
        "hapur",
        "hapem",
        "hapesh",
        "hapet",
        "hapemi",
        "hapeni",
        "hapen",
        "hapej",
        "hapesha",
        "hapeshe",
        "hapeshim",
        "hapeshin",
        "hapeshit",
        "hapja",
        "hapje",
        "hapsha",
        "hapshim");
  }

  /** Passive voice of class 1 verbs (gëzohem, I rejoice) conflates with the active. */
  public void testPassive() throws IOException {
    check(
        "gez",
        "gezoj",
        "gezon",
        "gezuar",
        "gezohem",
        "gezohesh",
        "gezohet",
        "gezohemi",
        "gezoheni",
        "gezohen",
        "gezohej",
        "gezohesha",
        "gezoheshe",
        "gezoheshim",
        "gezoheshin",
        "gezoheshit",
        "gezohu",
        "gezohuni");
    check("pun", "punohet", "punohen", "punohej");
    check("lex", "lexoj", "lexo", "lexohet", "lexohen");
  }

  /** Derivation: the noun, the verb, the agent noun and the adjective of one root conflate. */
  public void testDerivation() throws IOException {
    // durim (patience), duroj (endure), durimtar (patient one), durueshëm (enduring)
    check(
        "dur",
        "durim",
        "durimi",
        "durimin",
        "durimtar",
        "durimtaret",
        "durimtareve",
        "duroj",
        "duroni",
        "duruan",
        "durueshem",
        "durueshme");
    // besim (faith), besoj (believe), besimtar (believer)
    check("bes", "besim", "besimi", "besimtar", "besimtaret", "beson", "besoj", "besuan", "besuar");
    // punë (work), punëtor (worker), punoj (work)
    check("pun", "pune", "punet", "punetor", "punetori", "punetoret", "punoj");
    // mëshirë (mercy), mëshirues (merciful), mëshiron (has mercy)
    check("meshir", "meshire", "meshira", "meshirues", "meshiruesit", "meshiron");
    // mësoj (learn/teach), mësim (lesson), mësues (teacher)
    check("mes", "mesim", "mesimi", "mesimet", "mesues", "mesuesi", "mesuesit", "mesuesve");
    // shqip (Albanian language), shqiptar (Albanian person)
    check(
        "shqip",
        "shqip",
        "shqipe",
        "shqipja",
        "shqiptar",
        "shqiptari",
        "shqiptare",
        "shqiptaret",
        "shqiptareve");
    // drejt (straight), e drejta (the right), drejtësi (justice)
    check("drejt", "drejt", "drejte", "drejta", "drejtesi", "drejtesia");
    // lirë (free), liri (freedom), lirisht (freely)
    check("lir", "lire", "liri", "liria", "lirisht");
  }

  /**
   * Gheg verb forms: the participle in -ue / -u / -un / -uem, the imperfect in -ojsha, and
   * consonant stems in -un, all conflate with the standard forms of the verb.
   */
  public void testGhegVerbs() throws IOException {
    check(
        "pun",
        "punue",
        "punu",
        "punun",
        "punuem",
        "punojsha",
        "punojshe",
        "punojshim",
        "punojshit",
        "punojshin",
        "punoj",
        "punuar",
        "punova");
    check("shk", "shkue", "shkoj", "shkova", "shkuar");
    check("hap", "hapun", "hapur", "hapsha", "hapshe");
    check("hangr", "hanger", "hangrun", "hangri");
    check("kqyr", "kqyr", "kqyrun", "kqyri");
    check("ardh", "ardhe", "ardhun", "ardhur");
    check("dal", "dale", "dalun", "dalur");
  }

  /** Gheg nouns: the genitive in -s after i (shpis, Shqypnís) joins the paradigm. */
  public void testGhegNouns() throws IOException {
    check("shp", "shpi", "shpija", "shpin", "shpis", "shpive");
    check("shqyp", "shqypni", "shqypnia", "shqypnis");
    check("nan", "nane", "nana", "nanes", "nanen");
    check("shqyp", "shqyptar", "shqyptari", "shqyptarit", "shqyptarve");
    check("lir", "liri", "liris", "liria", "lirise");
  }

  /** Loanwords in -o behave like class 1 imperatives and conflate with their inflected forms. */
  public void testWordsInO() throws IOException {
    check("rad", "radio", "radioja", "radion");
    check("fot", "foto", "fotoja", "foton");
  }

  /**
   * Words of three letters or fewer, and words that would be cut below three letters, are left
   * alone (apart from a final q, which is a palatal plural).
   */
  public void testShortWords() throws IOException {
    check("dhe", "dhe");
    check("uje", "uje");
    check("ata", "ata");
    check("ne", "ne");
    check("mik", "miq");
  }

  public void testKeyword() throws IOException {
    final CharArraySet exclusionSet = new CharArraySet(asSet("durimi"), false);
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            TokenStream sink = new SetKeywordMarkerFilter(source, exclusionSet);
            return new TokenStreamComponents(source, new AlbanianLightStemFilter(sink));
          }
        };
    checkOneTerm(a, "durimi", "durimi");
    a.close();
  }

  public void testEmptyTerm() throws IOException {
    Analyzer a =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new KeywordTokenizer();
            return new TokenStreamComponents(tokenizer, new AlbanianLightStemFilter(tokenizer));
          }
        };
    checkOneTerm(a, "", "");
    a.close();
  }
}
