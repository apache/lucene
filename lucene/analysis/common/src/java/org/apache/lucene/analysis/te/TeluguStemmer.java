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
package org.apache.lucene.analysis.te;

import static org.apache.lucene.analysis.util.StemmerUtil.endsWith;

/**
 * Stemmer for Telugu.
 *
 * <p>The algorithm is based on the report in: <i>Natural Language Processing in an Indian Language
 * (Telugu)-I: Verb Phrase Analysis</i> P Sengupta and B B Chaudhuri
 *
 * TODO - see here https://github.com/nirupampratap/lda_te/blob/master/tepy.py
 * @author vinod@vinodsingh.com - WIP
 * @since 9.0.0
 */
public class TeluguStemmer {
  public int stem(char buffer[], int len) {

    // 2
    if ((len > 3)
        && (endsWith(buffer, len, "డు")
            || endsWith(buffer, len, "ము")
            || endsWith(buffer, len, "వు")
            || endsWith(buffer, len, "లు")
            || endsWith(buffer, len, "ని")
            || endsWith(buffer, len, "ను")
            || endsWith(buffer, len, "చే")
            || endsWith(buffer, len, "కై")
            || endsWith(buffer, len, "లో")
            || endsWith(buffer, len, "కి"))) return len - 2;

    // 1
    if ((len > 2)
        && (endsWith(buffer, len, "ి")
            || endsWith(buffer, len, "ీ")
            || endsWith(buffer, len, "ు")
            || endsWith(buffer, len, "ూ")
            || endsWith(buffer, len, "ె")
            || endsWith(buffer, len, "ే")
            || endsWith(buffer, len, "ొ")
            || endsWith(buffer, len, "ో")
            || endsWith(buffer, len, "ా"))) return len - 1;

    return len;
  }
}
