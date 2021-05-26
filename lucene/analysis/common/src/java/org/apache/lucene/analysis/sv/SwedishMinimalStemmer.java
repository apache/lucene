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

import static org.apache.lucene.analysis.util.StemmerUtil.endsWith;

/**
 * Minimal Stemmer for Swedish.
 *
 * This algorithm is derived from code located at: http://members.unine.ch/jacques.savoy/clef/
 * Full copyright for that code can be found in LICENSE.txt
 *
 * @since 9.0.0
 */
public class SwedishMinimalStemmer {

  public int stem(char s[], int len) {
    if (len > 4 && s[len - 1] == 's') len--;

    if (len > 6
        && (endsWith(s, len, "arne")
            || endsWith(s, len, "erna")
            || endsWith(s, len, "arna")
            || endsWith(s, len, "orna")
            || endsWith(s, len, "aren"))) return len - 4;

    if (len > 5 && (endsWith(s, len, "are"))) return len - 3;

    if (len > 4
        && (endsWith(s, len, "ar")
            || endsWith(s, len, "at")
            || endsWith(s, len, "er")
            || endsWith(s, len, "et")
            || endsWith(s, len, "or")
            || endsWith(s, len, "en"))) return len - 2;

    if (len > 3)
      switch (s[len - 1]) {
        case 'a':
        case 'e':
        case 'n':
          return len - 1;
      }

    return len;
  }
}
