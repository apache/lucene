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
package org.apache.lucene.analysis.miscellaneous;

import java.util.EnumSet;
import java.util.Set;
import org.apache.lucene.analysis.util.StemmerUtil;

/**
 * This Normalizer does the heavy lifting for a set of Scandinavian normalization filters,
 * normalizing use of the interchangeable Scandinavian characters æÆäÄöÖøØ and folded variants (aa,
 * ao, ae, oe and oo) by transforming them to åÅæÆøØ.
 *
 * @since 9.0
 * @lucene.internal
 */
public final class ScandinavianNormalizer {

  /**
   * Create the instance, while choosing which foldings to apply. This may differ between Norwegian,
   * Danish and Swedish.
   *
   * @param foldings a Set of Foldings to apply (i.e. AE, OE, AA, AO, OO)
   */
  public ScandinavianNormalizer(Set<Foldings> foldings) {
    this.foldings = foldings;
  }

  /** List of possible foldings that can be used when configuring the filter */
  public enum Foldings {
    AA,
    AO,
    AE,
    OE,
    OO
  }

  private final Set<Foldings> foldings;

  public static final Set<Foldings> ALL_FOLDINGS = EnumSet.allOf(Foldings.class);

  static final char AA = '\u00C5'; // Å
  static final char aa = '\u00E5'; // å
  static final char AE = '\u00C6'; // Æ
  static final char ae = '\u00E6'; // æ
  static final char AE_se = '\u00C4'; // Ä
  static final char ae_se = '\u00E4'; // ä
  static final char OE = '\u00D8'; // Ø
  static final char oe = '\u00F8'; // ø
  static final char OE_se = '\u00D6'; // Ö
  static final char oe_se = '\u00F6'; // ö

  /**
   * Takes the original buffer and length as input. Modifies the buffer in-place and returns new
   * length
   *
   * @return new length
   */
  public int processToken(char[] buffer, int length) {
    int i;
    for (i = 0; i < length; i++) {

      if (buffer[i] == ae_se) {
        buffer[i] = ae;

      } else if (buffer[i] == AE_se) {
        buffer[i] = AE;

      } else if (buffer[i] == oe_se) {
        buffer[i] = oe;

      } else if (buffer[i] == OE_se) {
        buffer[i] = OE;

      } else if (length - 1 > i) {

        if (buffer[i] == 'a'
            && (foldings.contains(Foldings.AA) && (buffer[i + 1] == 'a' || buffer[i + 1] == 'A')
                || foldings.contains(Foldings.AO)
                    && (buffer[i + 1] == 'o' || buffer[i + 1] == 'O'))) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = aa;

        } else if (buffer[i] == 'A'
            && (foldings.contains(Foldings.AA) && (buffer[i + 1] == 'a' || buffer[i + 1] == 'A')
                || foldings.contains(Foldings.AO)
                    && (buffer[i + 1] == 'o' || buffer[i + 1] == 'O'))) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = AA;

        } else if (buffer[i] == 'a'
            && foldings.contains(Foldings.AE)
            && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = ae;

        } else if (buffer[i] == 'A'
            && foldings.contains(Foldings.AE)
            && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = AE;

        } else if (buffer[i] == 'o'
            && (foldings.contains(Foldings.OE) && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')
                || foldings.contains(Foldings.OO)
                    && (buffer[i + 1] == 'o' || buffer[i + 1] == 'O'))) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = oe;

        } else if (buffer[i] == 'O'
            && (foldings.contains(Foldings.OE) && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')
                || foldings.contains(Foldings.OO)
                    && (buffer[i + 1] == 'o' || buffer[i + 1] == 'O'))) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = OE;
        }
      }
    }
    return length;
  }
}
