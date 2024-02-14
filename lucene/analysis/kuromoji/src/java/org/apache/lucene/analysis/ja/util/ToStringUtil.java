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
package org.apache.lucene.analysis.ja.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Utility class for english translations of morphological data, used only for debugging. */
public class ToStringUtil {
  // a translation map for parts of speech, only used for reflectWith
  private static final Map<String, String> posTranslations;

  static {
    Map<String, String> translations = new HashMap<>();
    translations.put("名詞", "noun");
    translations.put("名詞-一般", "noun-common");
    translations.put("名詞-固有名詞", "noun-proper");
    translations.put("名詞-固有名詞-一般", "noun-proper-misc");
    translations.put("名詞-固有名詞-人名", "noun-proper-person");
    translations.put("名詞-固有名詞-人名-一般", "noun-proper-person-misc");
    translations.put("名詞-固有名詞-人名-姓", "noun-proper-person-surname");
    translations.put("名詞-固有名詞-人名-名", "noun-proper-person-given_name");
    translations.put("名詞-固有名詞-組織", "noun-proper-organization");
    translations.put("名詞-固有名詞-地域", "noun-proper-place");
    translations.put("名詞-固有名詞-地域-一般", "noun-proper-place-misc");
    translations.put("名詞-固有名詞-地域-国", "noun-proper-place-country");
    translations.put("名詞-代名詞", "noun-pronoun");
    translations.put("名詞-代名詞-一般", "noun-pronoun-misc");
    translations.put("名詞-代名詞-縮約", "noun-pronoun-contraction");
    translations.put("名詞-副詞可能", "noun-adverbial");
    translations.put("名詞-サ変接続", "noun-verbal");
    translations.put("名詞-形容動詞語幹", "noun-adjective-base");
    translations.put("名詞-数", "noun-numeric");
    translations.put("名詞-非自立", "noun-affix");
    translations.put("名詞-非自立-一般", "noun-affix-misc");
    translations.put("名詞-非自立-副詞可能", "noun-affix-adverbial");
    translations.put("名詞-非自立-助動詞語幹", "noun-affix-aux");
    translations.put("名詞-非自立-形容動詞語幹", "noun-affix-adjective-base");
    translations.put("名詞-特殊", "noun-special");
    translations.put("名詞-特殊-助動詞語幹", "noun-special-aux");
    translations.put("名詞-接尾", "noun-suffix");
    translations.put("名詞-接尾-一般", "noun-suffix-misc");
    translations.put("名詞-接尾-人名", "noun-suffix-person");
    translations.put("名詞-接尾-地域", "noun-suffix-place");
    translations.put("名詞-接尾-サ変接続", "noun-suffix-verbal");
    translations.put("名詞-接尾-助動詞語幹", "noun-suffix-aux");
    translations.put("名詞-接尾-形容動詞語幹", "noun-suffix-adjective-base");
    translations.put("名詞-接尾-副詞可能", "noun-suffix-adverbial");
    translations.put("名詞-接尾-助数詞", "noun-suffix-classifier");
    translations.put("名詞-接尾-特殊", "noun-suffix-special");
    translations.put("名詞-接続詞的", "noun-suffix-conjunctive");
    translations.put("名詞-動詞非自立的", "noun-verbal_aux");
    translations.put("名詞-引用文字列", "noun-quotation");
    translations.put("名詞-ナイ形容詞語幹", "noun-nai_adjective");
    translations.put("接頭詞", "prefix");
    translations.put("接頭詞-名詞接続", "prefix-nominal");
    translations.put("接頭詞-動詞接続", "prefix-verbal");
    translations.put("接頭詞-形容詞接続", "prefix-adjectival");
    translations.put("接頭詞-数接続", "prefix-numerical");
    translations.put("動詞", "verb");
    translations.put("動詞-自立", "verb-main");
    translations.put("動詞-非自立", "verb-auxiliary");
    translations.put("動詞-接尾", "verb-suffix");
    translations.put("形容詞", "adjective");
    translations.put("形容詞-自立", "adjective-main");
    translations.put("形容詞-非自立", "adjective-auxiliary");
    translations.put("形容詞-接尾", "adjective-suffix");
    translations.put("副詞", "adverb");
    translations.put("副詞-一般", "adverb-misc");
    translations.put("副詞-助詞類接続", "adverb-particle_conjunction");
    translations.put("連体詞", "adnominal");
    translations.put("接続詞", "conjunction");
    translations.put("助詞", "particle");
    translations.put("助詞-格助詞", "particle-case");
    translations.put("助詞-格助詞-一般", "particle-case-misc");
    translations.put("助詞-格助詞-引用", "particle-case-quote");
    translations.put("助詞-格助詞-連語", "particle-case-compound");
    translations.put("助詞-接続助詞", "particle-conjunctive");
    translations.put("助詞-係助詞", "particle-dependency");
    translations.put("助詞-副助詞", "particle-adverbial");
    translations.put("助詞-間投助詞", "particle-interjective");
    translations.put("助詞-並立助詞", "particle-coordinate");
    translations.put("助詞-終助詞", "particle-final");
    translations.put("助詞-副助詞／並立助詞／終助詞", "particle-adverbial/conjunctive/final");
    translations.put("助詞-連体化", "particle-adnominalizer");
    translations.put("助詞-副詞化", "particle-adnominalizer");
    translations.put("助詞-特殊", "particle-special");
    translations.put("助動詞", "auxiliary-verb");
    translations.put("感動詞", "interjection");
    translations.put("記号", "symbol");
    translations.put("記号-一般", "symbol-misc");
    translations.put("記号-句点", "symbol-period");
    translations.put("記号-読点", "symbol-comma");
    translations.put("記号-空白", "symbol-space");
    translations.put("記号-括弧開", "symbol-open_bracket");
    translations.put("記号-括弧閉", "symbol-close_bracket");
    translations.put("記号-アルファベット", "symbol-alphabetic");
    translations.put("その他", "other");
    translations.put("その他-間投", "other-interjection");
    translations.put("フィラー", "filler");
    translations.put("非言語音", "non-verbal");
    translations.put("語断片", "fragment");
    translations.put("未知語", "unknown");
    posTranslations = Collections.unmodifiableMap(translations);
  }

  /** Get the english form of a POS tag */
  public static String getPOSTranslation(String s) {
    return posTranslations.get(s);
  }

  // a translation map for inflection types, only used for reflectWith
  private static final Map<String, String> inflTypeTranslations;

  static {
    Map<String, String> translations = new HashMap<>();
    translations.put("*", "*");
    translations.put("形容詞・アウオ段", "adj-group-a-o-u");
    translations.put("形容詞・イ段", "adj-group-i");
    translations.put("形容詞・イイ", "adj-group-ii");
    translations.put("不変化型", "non-inflectional");
    translations.put("特殊・タ", "special-da");
    translations.put("特殊・ダ", "special-ta");
    translations.put("文語・ゴトシ", "classical-gotoshi");
    translations.put("特殊・ジャ", "special-ja");
    translations.put("特殊・ナイ", "special-nai");
    translations.put("五段・ラ行特殊", "5-row-cons-r-special");
    translations.put("特殊・ヌ", "special-nu");
    translations.put("文語・キ", "classical-ki");
    translations.put("特殊・タイ", "special-tai");
    translations.put("文語・ベシ", "classical-beshi");
    translations.put("特殊・ヤ", "special-ya");
    translations.put("文語・マジ", "classical-maji");
    translations.put("下二・タ行", "2-row-lower-cons-t");
    translations.put("特殊・デス", "special-desu");
    translations.put("特殊・マス", "special-masu");
    translations.put("五段・ラ行アル", "5-row-aru");
    translations.put("文語・ナリ", "classical-nari");
    translations.put("文語・リ", "classical-ri");
    translations.put("文語・ケリ", "classical-keri");
    translations.put("文語・ル", "classical-ru");
    translations.put("五段・カ行イ音便", "5-row-cons-k-i-onbin");
    translations.put("五段・サ行", "5-row-cons-s");
    translations.put("一段", "1-row");
    translations.put("五段・ワ行促音便", "5-row-cons-w-cons-onbin");
    translations.put("五段・マ行", "5-row-cons-m");
    translations.put("五段・タ行", "5-row-cons-t");
    translations.put("五段・ラ行", "5-row-cons-r");
    translations.put("サ変・−スル", "irregular-suffix-suru");
    translations.put("五段・ガ行", "5-row-cons-g");
    translations.put("サ変・−ズル", "irregular-suffix-zuru");
    translations.put("五段・バ行", "5-row-cons-b");
    translations.put("五段・ワ行ウ音便", "5-row-cons-w-u-onbin");
    translations.put("下二・ダ行", "2-row-lower-cons-d");
    translations.put("五段・カ行促音便ユク", "5-row-cons-k-cons-onbin-yuku");
    translations.put("上二・ダ行", "2-row-upper-cons-d");
    translations.put("五段・カ行促音便", "5-row-cons-k-cons-onbin");
    translations.put("一段・得ル", "1-row-eru");
    translations.put("四段・タ行", "4-row-cons-t");
    translations.put("五段・ナ行", "5-row-cons-n");
    translations.put("下二・ハ行", "2-row-lower-cons-h");
    translations.put("四段・ハ行", "4-row-cons-h");
    translations.put("四段・バ行", "4-row-cons-b");
    translations.put("サ変・スル", "irregular-suru");
    translations.put("上二・ハ行", "2-row-upper-cons-h");
    translations.put("下二・マ行", "2-row-lower-cons-m");
    translations.put("四段・サ行", "4-row-cons-s");
    translations.put("下二・ガ行", "2-row-lower-cons-g");
    translations.put("カ変・来ル", "kuru-kanji");
    translations.put("一段・クレル", "1-row-kureru");
    translations.put("下二・得", "2-row-lower-u");
    translations.put("カ変・クル", "kuru-kana");
    translations.put("ラ変", "irregular-cons-r");
    translations.put("下二・カ行", "2-row-lower-cons-k");
    inflTypeTranslations = Collections.unmodifiableMap(translations);
  }

  /** Get the english form of inflection type */
  public static String getInflectionTypeTranslation(String s) {
    return inflTypeTranslations.get(s);
  }

  // a translation map for inflection forms, only used for reflectWith
  private static final Map<String, String> inflFormTranslations;

  static {
    Map<String, String> translations = new HashMap<>();
    translations.put("*", "*");
    translations.put("基本形", "base");
    translations.put("文語基本形", "classical-base");
    translations.put("未然ヌ接続", "imperfective-nu-connection");
    translations.put("未然ウ接続", "imperfective-u-connection");
    translations.put("連用タ接続", "conjunctive-ta-connection");
    translations.put("連用テ接続", "conjunctive-te-connection");
    translations.put("連用ゴザイ接続", "conjunctive-gozai-connection");
    translations.put("体言接続", "uninflected-connection");
    translations.put("仮定形", "subjunctive");
    translations.put("命令ｅ", "imperative-e");
    translations.put("仮定縮約１", "conditional-contracted-1");
    translations.put("仮定縮約２", "conditional-contracted-2");
    translations.put("ガル接続", "garu-connection");
    translations.put("未然形", "imperfective");
    translations.put("連用形", "conjunctive");
    translations.put("音便基本形", "onbin-base");
    translations.put("連用デ接続", "conjunctive-de-connection");
    translations.put("未然特殊", "imperfective-special");
    translations.put("命令ｉ", "imperative-i");
    translations.put("連用ニ接続", "conjunctive-ni-connection");
    translations.put("命令ｙｏ", "imperative-yo");
    translations.put("体言接続特殊", "adnominal-special");
    translations.put("命令ｒｏ", "imperative-ro");
    translations.put("体言接続特殊２", "uninflected-special-connection-2");
    translations.put("未然レル接続", "imperfective-reru-connection");
    translations.put("現代基本形", "modern-base");
    translations.put("基本形-促音便", "base-onbin"); // not sure about this
    inflFormTranslations = Collections.unmodifiableMap(translations);
  }

  /** Get the english form of inflected form */
  public static String getInflectedFormTranslation(String s) {
    return inflFormTranslations.get(s);
  }

  /** Romanize katakana with modified hepburn */
  public static String getRomanization(String s) {
    StringBuilder out = new StringBuilder();
    try {
      getRomanization(out, s);
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
    return out.toString();
  }

  /** Romanize katakana with modified hepburn */
  // TODO: now that this is used by readingsfilter and not just for
  // debugging, fix this to really be a scheme that works best with IMEs
  public static void getRomanization(Appendable builder, CharSequence s) throws IOException {
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      // maximum lookahead: 3
      char ch = s.charAt(i);
      char ch2 = (i < len - 1) ? s.charAt(i + 1) : 0;
      char ch3 = (i < len - 2) ? s.charAt(i + 2) : 0;

      main:
      switch (ch) {
        case 'ッ':
          switch (ch2) {
            case 'カ':
            case 'キ':
            case 'ク':
            case 'ケ':
            case 'コ':
              builder.append('k');
              break main;
            case 'サ':
            case 'シ':
            case 'ス':
            case 'セ':
            case 'ソ':
              builder.append('s');
              break main;
            case 'タ':
            case 'チ':
            case 'ツ':
            case 'テ':
            case 'ト':
              builder.append('t');
              break main;
            case 'パ':
            case 'ピ':
            case 'プ':
            case 'ペ':
            case 'ポ':
              builder.append('p');
              break main;
          }
          break;
        case 'ア':
          builder.append('a');
          break;
        case 'イ':
          if (ch2 == 'ィ') {
            builder.append("yi");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("ye");
            i++;
          } else {
            builder.append('i');
          }
          break;
        case 'ウ':
          switch (ch2) {
            case 'ァ':
              builder.append("wa");
              i++;
              break;
            case 'ィ':
              builder.append("wi");
              i++;
              break;
            case 'ゥ':
              builder.append("wu");
              i++;
              break;
            case 'ェ':
              builder.append("we");
              i++;
              break;
            case 'ォ':
              builder.append("wo");
              i++;
              break;
            case 'ュ':
              builder.append("wyu");
              i++;
              break;
            default:
              builder.append('u');
              break;
          }
          break;
        case 'エ':
          builder.append('e');
          break;
        case 'オ':
          if (ch2 == 'ウ') {
            builder.append('ō');
            i++;
          } else {
            builder.append('o');
          }
          break;
        case 'カ':
          builder.append("ka");
          break;
        case 'キ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("kyō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("kyū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("kya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("kyo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("kyu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("kye");
            i++;
          } else {
            builder.append("ki");
          }
          break;
        case 'ク':
          switch (ch2) {
            case 'ァ':
              builder.append("kwa");
              i++;
              break;
            case 'ィ':
              builder.append("kwi");
              i++;
              break;
            case 'ェ':
              builder.append("kwe");
              i++;
              break;
            case 'ォ':
              builder.append("kwo");
              i++;
              break;
            case 'ヮ':
              builder.append("kwa");
              i++;
              break;
            default:
              builder.append("ku");
              break;
          }
          break;
        case 'ケ':
          builder.append("ke");
          break;
        case 'コ':
          if (ch2 == 'ウ') {
            builder.append("kō");
            i++;
          } else {
            builder.append("ko");
          }
          break;
        case 'サ':
          builder.append("sa");
          break;
        case 'シ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("shō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("shū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("sha");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("sho");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("shu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("she");
            i++;
          } else {
            builder.append("shi");
          }
          break;
        case 'ス':
          if (ch2 == 'ィ') {
            builder.append("si");
            i++;
          } else {
            builder.append("su");
          }
          break;
        case 'セ':
          builder.append("se");
          break;
        case 'ソ':
          if (ch2 == 'ウ') {
            builder.append("sō");
            i++;
          } else {
            builder.append("so");
          }
          break;
        case 'タ':
          builder.append("ta");
          break;
        case 'チ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("chō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("chū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("cha");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("cho");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("chu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("che");
            i++;
          } else {
            builder.append("chi");
          }
          break;
        case 'ツ':
          if (ch2 == 'ァ') {
            builder.append("tsa");
            i++;
          } else if (ch2 == 'ィ') {
            builder.append("tsi");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("tse");
            i++;
          } else if (ch2 == 'ォ') {
            builder.append("tso");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("tsyu");
            i++;
          } else {
            builder.append("tsu");
          }
          break;
        case 'テ':
          if (ch2 == 'ィ') {
            builder.append("ti");
            i++;
          } else if (ch2 == 'ゥ') {
            builder.append("tu");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("tyu");
            i++;
          } else {
            builder.append("te");
          }
          break;
        case 'ト':
          if (ch2 == 'ウ') {
            builder.append("tō");
            i++;
          } else if (ch2 == 'ゥ') {
            builder.append("tu");
            i++;
          } else {
            builder.append("to");
          }
          break;
        case 'ナ':
          builder.append("na");
          break;
        case 'ニ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("nyō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("nyū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("nya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("nyo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("nyu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("nye");
            i++;
          } else {
            builder.append("ni");
          }
          break;
        case 'ヌ':
          builder.append("nu");
          break;
        case 'ネ':
          builder.append("ne");
          break;
        case 'ノ':
          if (ch2 == 'ウ') {
            builder.append("nō");
            i++;
          } else {
            builder.append("no");
          }
          break;
        case 'ハ':
          builder.append("ha");
          break;
        case 'ヒ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("hyō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("hyū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("hya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("hyo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("hyu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("hye");
            i++;
          } else {
            builder.append("hi");
          }
          break;
        case 'フ':
          if (ch2 == 'ャ') {
            builder.append("fya");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("fyu");
            i++;
          } else if (ch2 == 'ィ' && ch3 == 'ェ') {
            builder.append("fye");
            i += 2;
          } else if (ch2 == 'ョ') {
            builder.append("fyo");
            i++;
          } else if (ch2 == 'ァ') {
            builder.append("fa");
            i++;
          } else if (ch2 == 'ィ') {
            builder.append("fi");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("fe");
            i++;
          } else if (ch2 == 'ォ') {
            builder.append("fo");
            i++;
          } else {
            builder.append("fu");
          }
          break;
        case 'ヘ':
          builder.append("he");
          break;
        case 'ホ':
          if (ch2 == 'ウ') {
            builder.append("hō");
            i++;
          } else if (ch2 == 'ゥ') {
            builder.append("hu");
            i++;
          } else {
            builder.append("ho");
          }
          break;
        case 'マ':
          builder.append("ma");
          break;
        case 'ミ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("myō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("myū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("mya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("myo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("myu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("mye");
            i++;
          } else {
            builder.append("mi");
          }
          break;
        case 'ム':
          builder.append("mu");
          break;
        case 'メ':
          builder.append("me");
          break;
        case 'モ':
          if (ch2 == 'ウ') {
            builder.append("mō");
            i++;
          } else {
            builder.append("mo");
          }
          break;
        case 'ヤ':
          builder.append("ya");
          break;
        case 'ユ':
          builder.append("yu");
          break;
        case 'ヨ':
          if (ch2 == 'ウ') {
            builder.append("yō");
            i++;
          } else {
            builder.append("yo");
          }
          break;
        case 'ラ':
          if (ch2 == '゜') {
            builder.append("la");
            i++;
          } else {
            builder.append("ra");
          }
          break;
        case 'リ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("ryō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("ryū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("rya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("ryo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("ryu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("rye");
            i++;
          } else if (ch2 == '゜') {
            builder.append("li");
            i++;
          } else {
            builder.append("ri");
          }
          break;
        case 'ル':
          if (ch2 == '゜') {
            builder.append("lu");
            i++;
          } else {
            builder.append("ru");
          }
          break;
        case 'レ':
          if (ch2 == '゜') {
            builder.append("le");
            i++;
          } else {
            builder.append("re");
          }
          break;
        case 'ロ':
          if (ch2 == 'ウ') {
            builder.append("rō");
            i++;
          } else if (ch2 == '゜') {
            builder.append("lo");
            i++;
          } else {
            builder.append("ro");
          }
          break;
        case 'ワ':
          builder.append("wa");
          break;
        case 'ヰ':
          builder.append("i");
          break;
        case 'ヱ':
          builder.append("e");
          break;
        case 'ヲ':
          builder.append("o");
          break;
        case 'ン':
          switch (ch2) {
            case 'バ':
            case 'ビ':
            case 'ブ':
            case 'ベ':
            case 'ボ':
            case 'パ':
            case 'ピ':
            case 'プ':
            case 'ペ':
            case 'ポ':
            case 'マ':
            case 'ミ':
            case 'ム':
            case 'メ':
            case 'モ':
              builder.append('m');
              break main;
            case 'ヤ':
            case 'ユ':
            case 'ヨ':
            case 'ア':
            case 'イ':
            case 'ウ':
            case 'エ':
            case 'オ':
              builder.append("n'");
              break main;
            default:
              builder.append("n");
              break main;
          }
        case 'ガ':
          builder.append("ga");
          break;
        case 'ギ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("gyō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("gyū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("gya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("gyo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("gyu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("gye");
            i++;
          } else {
            builder.append("gi");
          }
          break;
        case 'グ':
          switch (ch2) {
            case 'ァ':
              builder.append("gwa");
              i++;
              break;
            case 'ィ':
              builder.append("gwi");
              i++;
              break;
            case 'ェ':
              builder.append("gwe");
              i++;
              break;
            case 'ォ':
              builder.append("gwo");
              i++;
              break;
            case 'ヮ':
              builder.append("gwa");
              i++;
              break;
            default:
              builder.append("gu");
              break;
          }
          break;
        case 'ゲ':
          builder.append("ge");
          break;
        case 'ゴ':
          if (ch2 == 'ウ') {
            builder.append("gō");
            i++;
          } else {
            builder.append("go");
          }
          break;
        case 'ザ':
          builder.append("za");
          break;
        case 'ジ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("jō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("jū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("ja");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("jo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("ju");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("je");
            i++;
          } else {
            builder.append("ji");
          }
          break;
        case 'ズ':
          if (ch2 == 'ィ') {
            builder.append("zi");
            i++;
          } else {
            builder.append("zu");
          }
          break;
        case 'ゼ':
          builder.append("ze");
          break;
        case 'ゾ':
          if (ch2 == 'ウ') {
            builder.append("zō");
            i++;
          } else {
            builder.append("zo");
          }
          break;
        case 'ダ':
          builder.append("da");
          break;
        case 'ヂ':
          // TODO: investigate all this
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("jō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("jū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("ja");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("jo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("ju");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("je");
            i++;
          } else {
            builder.append("ji");
          }
          break;
        case 'ヅ':
          builder.append("zu");
          break;
        case 'デ':
          if (ch2 == 'ィ') {
            builder.append("di");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("dyu");
            i++;
          } else {
            builder.append("de");
          }
          break;
        case 'ド':
          if (ch2 == 'ウ') {
            builder.append("dō");
            i++;
          } else if (ch2 == 'ゥ') {
            builder.append("du");
            i++;
          } else {
            builder.append("do");
          }
          break;
        case 'バ':
          builder.append("ba");
          break;
        case 'ビ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("byō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("byū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("bya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("byo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("byu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("bye");
            i++;
          } else {
            builder.append("bi");
          }
          break;
        case 'ブ':
          builder.append("bu");
          break;
        case 'ベ':
          builder.append("be");
          break;
        case 'ボ':
          if (ch2 == 'ウ') {
            builder.append("bō");
            i++;
          } else {
            builder.append("bo");
          }
          break;
        case 'パ':
          builder.append("pa");
          break;
        case 'ピ':
          if (ch2 == 'ョ' && ch3 == 'ウ') {
            builder.append("pyō");
            i += 2;
          } else if (ch2 == 'ュ' && ch3 == 'ウ') {
            builder.append("pyū");
            i += 2;
          } else if (ch2 == 'ャ') {
            builder.append("pya");
            i++;
          } else if (ch2 == 'ョ') {
            builder.append("pyo");
            i++;
          } else if (ch2 == 'ュ') {
            builder.append("pyu");
            i++;
          } else if (ch2 == 'ェ') {
            builder.append("pye");
            i++;
          } else {
            builder.append("pi");
          }
          break;
        case 'プ':
          builder.append("pu");
          break;
        case 'ペ':
          builder.append("pe");
          break;
        case 'ポ':
          if (ch2 == 'ウ') {
            builder.append("pō");
            i++;
          } else {
            builder.append("po");
          }
          break;
        case 'ヷ':
          builder.append("va");
          break;
        case 'ヸ':
          builder.append("vi");
          break;
        case 'ヹ':
          builder.append("ve");
          break;
        case 'ヺ':
          builder.append("vo");
          break;
        case 'ヴ':
          if (ch2 == 'ィ' && ch3 == 'ェ') {
            builder.append("vye");
            i += 2;
          } else {
            builder.append('v');
          }
          break;
        case 'ァ':
          builder.append('a');
          break;
        case 'ィ':
          builder.append('i');
          break;
        case 'ゥ':
          builder.append('u');
          break;
        case 'ェ':
          builder.append('e');
          break;
        case 'ォ':
          builder.append('o');
          break;
        case 'ヮ':
          builder.append("wa");
          break;
        case 'ャ':
          builder.append("ya");
          break;
        case 'ュ':
          builder.append("yu");
          break;
        case 'ョ':
          builder.append("yo");
          break;
        case 'ー':
          break;
        default:
          builder.append(ch);
      }
    }
  }
}
