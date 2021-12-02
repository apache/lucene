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
package org.apache.lucene.analysis.es;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.analysis.CharArraySet;

/**
 * Plural Stemmer for Spanish
 *
 * <p>This stemmer implements the rules described in:
 * <i>http://www.wikilengua.org/index.php/Plural_(formación)</i>
 */
public class SpanishPluralStemmer {

  private static final CharArraySet invariants;
  private static final CharArraySet specialCases;

  private static final List<String> invariantsList =
      Arrays.asList(
          "abrebotellas",
          "abrecartas",
          "abrelatas",
          "afueras",
          "albatros",
          "albricias",
          "aledaños",
          "alexis",
          "alicates",
          "analisis",
          "andurriales",
          "antitesis",
          "añicos",
          "apendicitis",
          "apocalipsis",
          "arcoiris",
          "aries",
          "bilis",
          "boletus",
          "boris",
          "brindis",
          "cactus",
          "canutas",
          "caries",
          "cascanueces",
          "cascarrabias",
          "ciempies",
          "cifosis",
          "cortaplumas",
          "corpus",
          "cosmos",
          "cosquillas",
          "creces",
          "crisis",
          "cuatrocientas",
          "cuatrocientos",
          "cuelgacapas",
          "cuentacuentos",
          "cuentapasos",
          "cumpleaños",
          "doscientas",
          "doscientos",
          "dosis",
          "enseres",
          "entonces",
          "esponsales",
          "estatus",
          "exequias",
          "fauces",
          "forceps",
          "fotosintesis",
          "gafas",
          "gafotas",
          "gargaras",
          "gris",
          "honorarios",
          "ictus",
          "jueves",
          "lapsus",
          "lavacoches",
          "lavaplatos",
          "limpiabotas",
          "lunes",
          "maitines",
          "martes",
          "mondadientes",
          "novecientas",
          "novecientos",
          "nupcias",
          "ochocientas",
          "ochocientos",
          "pais",
          "paris",
          "parabrisas",
          "paracaidas",
          "parachoques",
          "paraguas",
          "pararrayos",
          "pisapapeles",
          "piscis",
          "portaaviones",
          "portamaletas",
          "portamantas",
          "quinientas",
          "quinientos",
          "quitamanchas",
          "recogepelotas",
          "rictus",
          "rompeolas",
          "sacacorchos",
          "sacapuntas",
          "saltamontes",
          "salvavidas",
          "seis",
          "seiscientas",
          "seiscientos",
          "setecientas",
          "setecientos",
          "sintesis",
          "tenis",
          "tifus",
          "trabalenguas",
          "vacaciones",
          "venus",
          "versus",
          "viacrucis",
          "virus",
          "viveres",
          "volandas");

  static {
    final CharArraySet invariantSet = new CharArraySet(invariantsList, true);
    invariants = CharArraySet.unmodifiableSet(invariantSet);
    assert invariants.size() == invariantsList.size();
    final List<String> specialCasesList =
        Arrays.asList(
            "yoes",
            "noes",
            "sies",
            "clubes",
            "faralaes",
            "albalaes",
            "itemes",
            "albumes",
            "sandwiches",
            "relojes",
            "bojes",
            "contrarreloj",
            "carcajes");
    final CharArraySet sepecialSet = new CharArraySet(specialCasesList, true);
    specialCases = CharArraySet.unmodifiableSet(sepecialSet);
  }

  public int stem(char s[], int len) {
    if (len < 4) return len; // plural have at least 4 letters (ases,eses,etc.)
    removeAccents(s, len);
    if (invariant(s, len)) return len;
    if (special(s, len)) return len - 2;
    switch (s[len - 1]) {
      case 's':
        if (!isVowel(s[len - 2])) { // no vocals, singular words ending with consonant
          return len - 1;
        }
        if ((s[len - 4] == 'q'
            || (s[len - 4] == 'g')
                && s[len - 3] == 'u'
                && (s[len - 2] == 'i' || s[len - 2] == 'e'))) { // maniquis,caquis, parques
          return len - 1;
        }
        if (isVowel(s[len - 4])
            && (s[len - 3] == 'r')
            && s[len - 2] == 'e') { // escaneres, alfileres, amores, cables
          return len - 2;
        }
        if (isVowel(s[len - 4])
            && (s[len - 3] == 'd' || s[len - 3] == 'l' || s[len - 3] == 'n' || s[len - 3] == 'x')
            && s[len - 2] == 'e') { // abades, comerciales, faxes,  relojes,
          return len - 2;
        }
        if ((s[len - 3] == 'y' || s[len - 3] == 'u') && s[len - 2] == 'e') { // bambues,leyes
          return len - 2;
        }
        if ((s[len - 4] == 'u'
                || s[len - 4] == 'l'
                || s[len - 4] == 'r'
                || s[len - 4] == 't'
                || s[len - 4] == 'n')
            && (s[len - 3] == 'i')
            && s[len - 2] == 'e') { // jabalies,israelies, maniquies
          return len - 2;
        }
        if ((s[len - 3] == 's' && s[len - 2] == 'e')) { // reses
          return len - 2;
        }
        if (isVowel(s[len - 3]) && s[len - 2] == 'i') { // jerseis
          s[len - 2] = 'y';
          return len - 1;
        }
        if (s[len - 3] == 'd' && s[len - 2] == 'i') { // brandis
          s[len - 2] = 'y';
          return len - 1;
        }
        if (s[len - 2] == 'e' && s[len - 3] == 'c') { // voces-->voz
          s[len - 3] = 'z';
          return len - 2;
        }
        if (isVowel(s[len - 2])) // remove last 's': jabalís, casas, coches, etc.
        {
          return len - 1;
        }
        break;
    }
    return len;
  }

  private boolean isVowel(char c) {
    boolean res = false;
    if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') {
      res = true;
    }
    return res;
  }

  private boolean invariant(char[] s, int len) {
    return invariants.contains(s, 0, len);
  }

  private boolean special(char[] s, int len) {
    return specialCases.contains(s, 0, len);
  }

  private void removeAccents(char[] s, int len) {
    for (int i = 0; i < len; i++) {
      switch (s[i]) {
        case 'à':
        case 'á':
        case 'â':
        case 'ä':
          s[i] = 'a';
          break;
        case 'ò':
        case 'ó':
        case 'ô':
        case 'ö':
          s[i] = 'o';
          break;
        case 'è':
        case 'é':
        case 'ê':
        case 'ë':
          s[i] = 'e';
          break;
        case 'ù':
        case 'ú':
        case 'û':
        case 'ü':
          s[i] = 'u';
          break;
        case 'ì':
        case 'í':
        case 'î':
        case 'ï':
          s[i] = 'i';
          break;
      }
    }
  }
}
