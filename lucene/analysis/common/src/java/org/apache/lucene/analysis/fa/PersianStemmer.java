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
package org.apache.lucene.analysis.fa;

import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Stemmer for Persian.
 *
 * <p>Stemming is done in-place for efficiency, operating on a termbuffer.
 *
 * <p>Stemming is defined as:
 *
 * <ul>
 *   <li>Removal of attached definite article, conjunction, and prepositions.
 *   <li>Stemming of common suffixes.
 * </ul>
 */
public class PersianStemmer {
    public static final char ALEF = '\u0627';
    public static final char HEH = '\u0647';
    public static final char TEH = '\u062A';
    public static final char REH = '\u0631';
    public static final char NOON = '\u0646';
    public static final char YEH = '\u064A';
    public static final char ZWNJ = '\u200c';

    public static final char[][] suffixes = {
            ("" + ALEF + TEH).toCharArray(),
            ("" + ALEF + NOON).toCharArray(),
            ("" + TEH + REH + YEH + NOON).toCharArray(),
            ("" + TEH + REH).toCharArray(),
            ("" + YEH + YEH).toCharArray(),
            ("" + YEH).toCharArray(),
            ("" + HEH + ALEF).toCharArray(),
            ("" + ZWNJ).toCharArray(),
    };

    /**
     * Stem an input buffer of Persian text.
     *
     * @param s input buffer
     * @param len length of input buffer
     * @return length of input buffer after normalization
     */
    public int stem(char[] s, int len) {
        len = stemSuffix(s, len);

        return len;
    }

    /**
     * Stem suffix(es) off an Persian word.
     *
     * @param s input buffer
     * @param len length of input buffer
     * @return new length of input buffer after stemming
     */
    public int stemSuffix(char[] s, int len) {
        for (int i = 0; i < suffixes.length; i++)
            if (endsWithCheckLength(s, len, suffixes[i]))
                len = deleteN(s, len - suffixes[i].length, len, suffixes[i].length);
        return len;
    }

    /**
     * Returns true if the suffix matches and can be stemmed
     *
     * @param s input buffer
     * @param len length of input buffer
     * @param suffix suffix to check
     * @return true if the suffix matches and can be stemmed
     */
    boolean endsWithCheckLength(char[] s, int len, char[] suffix) {
        if (len < suffix.length + 2) { // all suffixes require at least 2 characters after stemming
            return false;
        } else {
            for (int i = 0; i < suffix.length; i++) {
                if (s[len - suffix.length + i] != suffix[i]) {
                    return false;
                }
            }

            return true;
        }
    }
}
