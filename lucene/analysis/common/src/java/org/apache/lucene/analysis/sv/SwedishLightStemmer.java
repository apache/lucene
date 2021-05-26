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
package org.apache.lucene.analysis.sv;

import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Light Stemmer for Swedish.
 *
 * <p>This stemmer implements the algorithm described in: <i>Report on CLEF-2003 Monolingual
 * Tracks</i> Jacques Savoy
 *
 * This algorithm is updated based on code located at: http://members.unine.ch/jacques.savoy/clef/
 * Full copyright for that code can be found in LICENSE.txt
 */
public class SwedishLightStemmer {

  public int stem(char s[], int len) {
    if (len > 4 && s[len - 1] == 's') len--;

    if (len > 7 && (endsWith(s, len, "elser") || endsWith(s, len, "heten"))) return len - 5;

    if (len > 6
        && (endsWith(s, len, "arne")
            || endsWith(s, len, "erna")
            || endsWith(s, len, "ande")
            || endsWith(s, len, "else")
            || endsWith(s, len, "aste")
            || endsWith(s, len, "orna")
            || endsWith(s, len, "aren"))) return len - 4;

    if (len > 5 && (endsWith(s, len, "are") || endsWith(s, len, "ast") || endsWith(s, len, "het")))
      return len - 3;

    if (len > 4
        && (endsWith(s, len, "ar")
            || endsWith(s, len, "er")
            || endsWith(s, len, "or")
            || endsWith(s, len, "en")
            || endsWith(s, len, "at")
            || endsWith(s, len, "te")
            || endsWith(s, len, "et"))) return len - 2;

    if (len > 3)
      switch (s[len - 1]) {
        case 't':
        case 'a':
        case 'e':
        case 'n':
          return len - 1;
      }

    return len;
  }
}
