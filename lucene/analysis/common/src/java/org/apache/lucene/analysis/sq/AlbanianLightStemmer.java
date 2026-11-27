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

import static org.apache.lucene.analysis.util.StemmerUtil.endsWith;

/**
 * Light stemmer for Albanian.
 *
 * <p>A suffix-stripping stemmer in the style of the UniNE light stemmers (see {@link
 * org.apache.lucene.analysis.hu.HungarianLightStemFilter}), built on the longest-match suffix
 * removal approach that the published Albanian stemmers use:
 *
 * <ul>
 *   <li>Nikitas N. Karanikolas, <i>Bootstrapping the Albanian Information Retrieval</i>, 4th Balkan
 *       Conference in Informatics, 2009 (longest-match suffix removal with a minimum remaining stem
 *       length, repeated so that a second suffix can be removed).
 *   <li>Jetmir Sadiku and Marenglen Biba, <i>A Novel Stemming Algorithm for Albanian in a Data
 *       Mining Approach for Document Classification</i>, 2012 (JStem: rule based suffix and prefix
 *       removal; its rules do not cover plural and gender formation).
 * </ul>
 *
 * <p>The suffix inventory is the regular inflection of Standard Albanian as described in reference
 * grammars (Newmark, Hubbard and Prifti, <i>Standard Albanian: A Reference Grammar for
 * Students</i>, 1982) and as tabulated for several hundred lemmas in the English Wiktionary: the
 * definite article and case endings of nouns (<code>-i -u -a -in -un -ën -it -ut -ës -së -në -t -ve
 * -sh</code>), the regular plural endings (<code>-ë -e -a -ra</code>), the feminine adjective
 * ending, and the endings of the regular verb classes in the active and the passive voice (present,
 * imperfect, aorist, subjunctive, optative, participle). The Gheg (northern) forms of the same
 * categories are covered where they differ: the participle in <code>-ue / -u / -un</code> (<code>
 * punue</code>, <code>hapun</code>), the imperfect in <code>-ojsha</code> and the genitive in
 * <code>-s</code> (<code>shpis</code>). Derivational suffixes that are very productive in retrieval
 * (<code>-tar -tor -ist -ues -shëm -shme -isht -im -je</code>) are removed in a second step, so
 * that for example <code>durim</code> (patience), <code>duroj</code> (I endure), <code>durimtar
 * </code> (patient one) and <code>durueshëm</code> (enduring) conflate.
 *
 * <p>Input is expected to be lower-cased and normalized by {@link AlbanianNormalizer} (<code>ë
 * </code>→<code>e</code>, <code>ç</code>→<code>c</code>), so every suffix is listed in its folded
 * form. Suffixes are only removed while at least three characters of stem remain. Removal is
 * repeated until no suffix matches, which is what makes the inflected forms of a paradigm conflate
 * even when more than one ending is stacked (<code>shtëpi / shtëpia / shtëpinë / shtëpive</code>).
 * A final step undoes two regular stem alternations: the <code>ë</code> that is dropped before an
 * ending (<code>libër / libri</code>, <code>i mesëm / e mesme</code>, <code>socializëm /
 * socializmi</code>) and the palatalized plural (<code>mik / miq</code>, <code>zog / zogj</code>,
 * <code>popull / popuj</code>). Irregular plurals with vowel changes (<code>njeri / njerëz</code>,
 * <code>natë / net</code>), suppletive verbs (<code>shoh / pashë</code>, <code>them / tha</code>)
 * and the admirative of consonant-stem verbs (<code>hapka</code>) are not handled; this is a light
 * stemmer. Like every suffix stripper it also conflates some unrelated words whose forms differ
 * only in a suffix or in <code>ë</code>: <code>qendër / qëndroj</code>, <code>botë / botoj</code>,
 * <code>mes / mësoj</code>, <code>drekë / dreq</code>, <code>hekur / heq</code>, <code>beton /
 * betoj</code>. Those are pinned in the tests. The stemmer is idempotent.
 */
public class AlbanianLightStemmer {

  /**
   * Suffixes removed by the stemmer are only removed when at least this many characters remain
   * (Karanikolas' "at least remain letters" condition).
   */
  private static final int MINIMUM_STEM_LENGTH = 3;

  /**
   * Inflectional endings, longest first. Nominal endings (definite article, case, number) and
   * verbal endings (person, tense, mood, voice, participle) share one table because after folding
   * they overlap heavily and the stemmer cannot tell parts of speech apart. Some entries need
   * context; see {@link #isInflection}.
   */
  private static final String[] INFLECTIONS = {
    // class 1 admirative imperfect plural (punuakëshim)
    "uakeshim",
    "uakeshin",
    "uakeshit",
    // passive imperfect plural of class 1 verbs (punoheshim); admirative imperfect singular
    "oheshim",
    "oheshin",
    "oheshit",
    "uakesha",
    "uakeshe",
    // optative plural; passive imperfect singular; passive imperative plural (punohuni);
    // admirative 2nd singular (punuakësh)
    "ofshim",
    "ofshin",
    "ofshit",
    "ohesha",
    "oheshe",
    "ohuni",
    "uakesh",
    // class 1 admirative plural (punuakemi); Gheg imperfect plural (punojshim)
    "uakemi",
    "uakeni",
    "ojshim",
    "ojshit",
    "ojshin",
    // optative singular; Gheg imperfect singular (punojsha); -uaj verbs 1st/3rd plural
    // (shkruajmë); passive present plural; -je nouns plural dative; class 2 passive
    // imperfect plural; admirative; -enj plural
    "ofsha",
    "ofshe",
    "ofshi",
    "ojsha",
    "ojshe",
    "uajme",
    "uajne",
    "ohemi",
    "oheni",
    "jeve",
    "uani",
    "uakam",
    "uakan",
    "enjve",
    "enjsh",
    "eshim",
    "eshin",
    "eshit",
    "ohesh",
    // plurals in -inj (arabaxhinj), -llarë (baballarë), -ëra (baltëra) and -ajë nouns
    // with their plural endings
    "injve",
    "injsh",
    "erave",
    "erash",
    "llare",
    "jave",
    "jash",
    // class 1 imperfect and 1st/3rd plural present; -ej verbs 1st/3rd plural (kthejmë);
    // optative base; passive present; class 2 passive imperfect; -je nouns plural ablative
    "onim",
    "onin",
    "onit",
    "onte",
    "ojme",
    "ojne",
    "ejme",
    "ejne",
    "shim",
    "shin",
    "shit",
    "ofte",
    "ofsh",
    "ohem",
    "ohet",
    "ohen",
    "ohej",
    "esha",
    "eshe",
    "jesh",
    // plural dative/ablative; accusative of -i nouns (shtëpinë); plural in -ëra; -ej verbs
    // aorist (ktheva) and participle (kthyer); -je nouns; -uaj verbs; class 1 aorist,
    // participle, imperfect, 2nd plural, subjunctive, optative; class 2 imperfect plural;
    // optative; participle
    "injt",
    "llar",
    "erat",
    "uaka",
    "uake",
    "enjt",
    "eve",
    "ave",
    "ive",
    "ine",
    "esh",
    "ash",
    "ish",
    "era",
    "eva",
    "yer",
    "jes",
    "jet",
    "jen",
    "jat",
    "inj",
    "enj",
    "uaj",
    "uam",
    "uat",
    "uan",
    "uar",
    "ova",
    "ove",
    "oje",
    "oja",
    "oni",
    "osh",
    "ohu",
    "nim",
    "nin",
    "nit",
    "oft",
    "sha",
    "she",
    "shi",
    // definite singular and plural, accusative, genitive/dative, plural dative;
    // class 1 present and aorist (incl. the -ov- aorist stem); participle; -je nouns;
    // passive; 2nd plural -ni and 2nd singular subjunctive -sh of consonant stems;
    // palatal plurals' definite -të; Gheg genitive -s (shpis)
    "et",
    "it",
    "ut",
    "at",
    "in",
    "un",
    "en",
    "es",
    "ve",
    "on",
    "oj",
    "oi",
    "ov",
    "ur",
    "je",
    "ja",
    "em",
    "ej",
    "se",
    "te",
    "sh",
    "ni",
    "t",
    "s",
    // indefinite plural, definite nominative, feminine, imperative of class 1 (puno)
    "i",
    "u",
    "a",
    "e",
    "o",
  };

  /**
   * Productive derivational suffixes, longest first. Removed one at a time, each followed by
   * another round of inflection removal.
   */
  private static final String[] DERIVATIONS = {
    // "shm" is what is left of the feminine "-shme" once the inflection step has
    // taken its final vowel. "-ist" (artist, komunist) also covers the feminine "-iste"
    // and the imperfect of the -is verbs (arratiste), once "-e" is gone.
    "shme", "shem", "isht", "shm", "ues", "tar", "tor", "ist", "im", "je",
  };

  /**
   * Stem an input buffer of Albanian text.
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int stem(char[] s, int len) {
    if (len < 3) { // do not touch
      return len;
    }

    // Normalizing can expose another ending (mësonjëtore → mësonj → mëson → mës), so the
    // whole thing repeats until nothing changes; that also makes the stemmer idempotent.
    int before;
    do {
      before = len;
      if (len >= 4) { // three-letter words carry no removable ending, but "miq" still normalizes
        int unstripped;
        do {
          unstripped = len;
          len = removeInflections(s, len);
          len = removeLongest(s, len, DERIVATIONS, false);
        } while (len < unstripped);
      }
      len = normalize(s, len);
    } while (len < before);

    return len;
  }

  /** Removes inflectional endings until none matches. */
  private static int removeInflections(char[] s, int len) {
    int before;
    do {
      before = len;
      len = removeLongest(s, len, INFLECTIONS, true);
    } while (len < before);
    return len;
  }

  /**
   * Removes the first (longest) suffix of {@code suffixes} that ends the buffer and leaves at least
   * {@link #MINIMUM_STEM_LENGTH} characters.
   */
  private static int removeLongest(char[] s, int len, String[] suffixes, boolean inflection) {
    for (String suffix : suffixes) {
      if (len - suffix.length() >= MINIMUM_STEM_LENGTH
          && endsWith(s, len, suffix)
          && (inflection ? isInflection(s, len, suffix) : isDerivation(s, len, suffix))) {
        return len - suffix.length();
      }
    }
    return len;
  }

  /**
   * <code>-ist</code> is stripped wherever it occurs; it is the agent suffix of loanwords (<code>
   * artist</code>, <code>komunist</code>, <code>kapitalist</code>), and in the <code>-is</code>
   * verbs' imperfect (<code>arratiste</code>) it is the stem plus <code>-te</code>, which the
   * inflection step cannot separate because <code>-ste</code> is also the feminine of those nouns.
   * The agent suffix is <code>-ëtor</code> (<code>punëtor</code>, <code>luftëtor</code>); a <code>
   * -tor</code> after any other letter is a loanword's own ending (<code>sektor</code>, <code>motor
   * </code>, <code>doktor</code>, <code>histori</code> → <code>histor</code>) and stays.
   */
  private static boolean isDerivation(char[] s, int len, String suffix) {
    return !suffix.equals("tor") || s[len - 4] == 'e';
  }

  /**
   * Some endings are only an inflection in a given context:
   *
   * <ul>
   *   <li><code>-et</code> after <code>-itet</code> in a long word is part of the word (<code>
   *       dinjitet</code>, <code>universitet</code>), unless the word ends in <code>-itetet</code>,
   *       the definite plural of such a noun.
   *   <li><code>-ve</code> after a vowel is the end of a stem (<code>Kosovë</code>); the plural
   *       dative after a vowel is covered by <code>-eve -ave -ive</code>.
   *   <li><code>-së</code>, and its Gheg form <code>-s</code>, is a genitive only after <code>i
   *       </code> (<code>shtëpisë</code>, <code>shpis</code>, <code>Shqypnís</code>); elsewhere it
   *       is stem + <code>-e</code> (<code>autobusë</code>, <code>baldosë</code>) or the word
   *       itself. In a four-letter word the <code>s</code> is the root (<code>rris</code>, <code>
   *       abis</code>, <code>kris</code>).
   *   <li><code>-et</code>, <code>-at</code> and <code>-it</code> are not taken off a word whose
   *       remainder would be a bare consonant cluster (<code>shtet</code>, <code>shtatë</code>,
   *       <code>shpatë</code>, <code>zgjat</code>, <code>ngjit</code>, <code>dhjetë</code>): three
   *       characters that spell two consonants are not a stem, and the <code>t</code> belongs to
   *       the root. Without this <code>shtet</code> (state), <code>shtatë</code> (seven) and <code>
   *       shtoj</code> (add) share a key, as do <code>zgjat</code> (stretch) and <code>zgjoj
   *       </code> (wake).
   *   <li><code>-ive</code> after <code>t</code>, or after <code>s</code> that does not follow
   *       <code>e</code>, is the feminine of an <code>-iv</code> adjective (<code>aktive</code>,
   *       <code>masive</code>), not the plural dative of an <code>-i</code> noun (<code>shtëpive
   *       </code>, <code>bashkësive</code>).
   *   <li>The class 2 imperfect plural <code>-nim -nit -nin</code>, the 2nd person <code>-ni
   *       </code> and <code>-sh</code>, and the optative <code>-sha -she -shi -shim -shin -shit
   *       </code> follow a consonant (<code>hapnim</code>, <code>hapni</code>, <code>hapsh</code>,
   *       <code>hapsha</code>); after a vowel they are part of the word (<code>Kuranit</code>,
   *       <code>zakoni</code>, <code>afishe</code>). After <code>f</code> they are either the
   *       optative's own <code>-fsh-</code> (<code>punofsh</code>, <code>pafsha</code>) or part of
   *       a root (<code>kafshë</code>, <code>kofshë</code>, <code>rrafsh</code>), so <code>kafshë
   *       </code> (animal) stays off <code>kafe</code>.
   *   <li><code>-ut</code> is the dative of the <code>-u</code> declension, which is limited to
   *       stems in <code>k g h</code> and vowels (<code>mikut</code>, <code>zogut</code>, <code>
   *       krahut</code>, <code>dheut</code>); elsewhere it is the root (<code>institut</code>,
   *       <code>kushtetutë</code>, <code>valutë</code>, <code>këput</code>, <code>minutë</code>).
   *   <li><code>-em</code> is not removed from <code>-izëm</code> nouns; the final step normalizes
   *       them instead.
   *   <li><code>-të</code> and <code>-t</code> are plural endings only after a palatalized plural
   *       stem (<code>miqtë</code>, <code>zogjtë</code>, <code>popujt</code>); elsewhere they
   *       belong to the word (<code>e drejtë</code>, <code>i shenjtë</code>).
   *   <li><code>-enj</code> is a plural after <code>m ll rr dh</code> or a vowel (<code>lumenj
   *       </code>, <code>budallenj</code>, <code>përrenj</code>, <code>mëdhenj</code>, <code>thonj
   *       </code>); after a single <code>r</code> it is the end of an adjective stem (<code>i
   *       shtrenjtë</code>), which would otherwise fall together with <code>shtrat</code> and
   *       <code>shtroj</code>.
   * </ul>
   */
  private static boolean isInflection(char[] s, int len, String suffix) {
    int stemLen = len - suffix.length();
    char before = s[stemLen - 1];

    switch (suffix) {
      case "et":
        if (isConsonantCluster(s, stemLen)) {
          return false;
        }
        return !(len >= 8 && endsWith(s, len, "itet")) || endsWith(s, len, "itetet");
      case "at":
      case "it":
        return !isConsonantCluster(s, stemLen);
      case "ve":
        return !isVowel(before);
      case "se":
        return before == 'i';
      case "s":
        return before == 'i' && len >= 5;
      case "ive":
        return before != 't' && !(before == 's' && s[stemLen - 2] != 'e');
      case "nim":
      case "nin":
      case "nit":
      case "ni":
        return !isVowel(before);
      case "sh":
      case "sha":
      case "she":
      case "shi":
      case "shim":
      case "shin":
      case "shit":
        return !isVowel(before) && before != 'f';
      case "ut":
        return before == 'k' || before == 'g' || before == 'h' || isVowel(before);
      case "em":
        return !endsWith(s, len, "izem");
      case "te":
      case "t":
        return before == 'q' || endsWith(s, stemLen, "gj") || endsWith(s, stemLen, "uj");
      case "enj":
      case "enjt":
      case "enjve":
      case "enjsh":
        return !(before == 'r' && s[stemLen - 2] != 'r');
      default:
        return true;
    }
  }

  /**
   * Undoes the regular stem alternations so that the uninflected form matches the stem of the
   * inflected forms:
   *
   * <ul>
   *   <li><code>ë</code> syncope before the article: <code>libër / libri</code>, <code>emër / emri
   *       </code>, <code>ajër / ajri</code>, <code>arkël / arkli</code> → <code>libr emr ajr arkl
   *       </code>. (Loanwords with a real <code>-er</code>, like <code>lider</code> or <code>hotel
   *       </code>, lose their <code>e</code> too, consistently across their forms.)
   *   <li><code>-ëm / -me</code> adjectives and <code>-ëm</code> nouns: <code>i mesëm / e mesme
   *       </code> → <code>mes</code>, <code>bagëm / bagmi</code> → <code>bag</code>; <code>-izëm /
   *       -izmi</code> nouns: <code>socializëm</code> → <code>socializm</code>.
   *   <li>palatalized plurals: <code>miq</code> → <code>mik</code>, <code>zogj</code> → <code>zog
   *       </code>, <code>arbunj</code> → <code>arbun</code>; for <code>-ull</code> nouns the
   *       singular is mapped onto the plural stem instead (<code>popull</code> → <code>popuj
   *       </code>) because the buffer cannot grow. A bare consonant cluster ending in <code>q
   *       </code> (<code>shq-</code>, left of <code>shqisë</code> and <code>shquar</code>) is a
   *       truncated root, not a plural, and keeps its <code>q</code>; mapping it to <code>shk
   *       </code> would merge those words with <code>shkoj</code>.
   * </ul>
   */
  private static int normalize(char[] s, int len) {
    // -ëm first, so that the -ër it may uncover (i afërm → afër) is syncopated too
    if (len >= 4 && s[len - 1] == 'm' && dropsFinalM(s[len - 2])) {
      len--;
    }

    if (len >= 4
        && (endsWith(s, len, "er") || endsWith(s, len, "el"))
        && takesSyncope(s[len - 3])) {
      s[len - 2] = s[len - 1];
      len--;
    } else if (len >= 5 && endsWith(s, len, "err") && takesSyncope(s[len - 4])) {
      s[len - 3] = 'r';
      len--;
    }

    if (len >= 6 && endsWith(s, len, "izem")) {
      s[len - 2] = 'm';
      len--;
    }

    if (len >= 4 && (endsWith(s, len, "gj") || endsWith(s, len, "nj"))) {
      len--;
    } else if (len >= 3 && s[len - 1] == 'q' && !isConsonantCluster(s, len)) {
      s[len - 1] = 'k';
    } else if (len >= 5 && endsWith(s, len, "ull")) {
      s[len - 3] = 'u';
      s[len - 2] = 'j';
      len--;
    }

    return len;
  }

  /**
   * The consonants after which a final <code>-ër</code> / <code>-ël</code> drops its <code>ë
   * </code> before an ending (<code>libër, qendër, gjarpër, emër, motër, ajër, arkël, vegjël
   * </code>). After <code>z r l n s</code> a final <code>-er</code> is a root (<code>vëllazëri,
   * sheqer</code>) and is left alone.
   */
  private static boolean takesSyncope(char c) {
    switch (c) {
      case 'b':
      case 'd':
      case 'f':
      case 'g':
      case 'h':
      case 'j':
      case 'k':
      case 'm':
      case 'p':
      case 't':
      case 'v':
        return true;
      default:
        return false;
    }
  }

  /**
   * The consonants after which a final <code>m</code> is the <code>-ëm</code> of an adjective or
   * noun whose <code>ë</code> has been dropped (<code>mesëm/mesme, sotëm, bagëm/bagmi, film/filmi
   * </code>). <code>z</code> is excluded so that <code>-izëm</code> nouns keep their <code>m
   * </code>.
   */
  private static boolean dropsFinalM(char c) {
    switch (c) {
      case 'b':
      case 'd':
      case 'g':
      case 'k':
      case 'l':
      case 'n':
      case 'p':
      case 'r':
      case 's':
      case 't':
        return true;
      default:
        return false;
    }
  }

  /**
   * Three characters that spell a digraph and a consonant (<code>sht shp shq dhj</code>) or a
   * consonant and <code>gj</code> (<code>zgj ngj</code>) are a consonant cluster, not a stem. A
   * vowel and a digraph (<code>udh</code>, <code>arr</code>, <code>eth</code>) is a real root, and
   * so is a consonant and <code>sh</code> (<code>fsh-</code>), which goes on in derivations (<code>
   * fshat</code>, <code>fshatar</code>).
   */
  private static boolean isConsonantCluster(char[] s, int len) {
    if (len != 3) {
      return false;
    }
    if (isDigraph(s[0], s[1])) {
      return !isVowel(s[2]);
    }
    return s[1] == 'g' && s[2] == 'j' && !isVowel(s[0]);
  }

  /** The consonant digraphs of the Albanian alphabet, which count as one letter. */
  private static boolean isDigraph(char a, char b) {
    switch (b) {
      case 'h':
        return a == 'd' || a == 's' || a == 't' || a == 'x' || a == 'z';
      case 'j':
        return a == 'g' || a == 'n';
      case 'l':
        return a == 'l';
      case 'r':
        return a == 'r';
      default:
        return false;
    }
  }

  private static boolean isVowel(char c) {
    switch (c) {
      case 'a':
      case 'e':
      case 'i':
      case 'o':
      case 'u':
      case 'y':
        return true;
      default:
        return false;
    }
  }
}
