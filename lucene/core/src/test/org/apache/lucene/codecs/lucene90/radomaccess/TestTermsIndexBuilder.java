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

package org.apache.lucene.codecs.lucene90.radomaccess;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestTermsIndexBuilder extends LuceneTestCase {

    public void testBasics() throws IOException {
        String[] test_terms = {
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "j",
        };

        Map<String, Integer> termsToType = new HashMap<>();
        Map<String, Integer> termsToOrd = new HashMap<>();
        Map<Integer, Integer> typeCounters = new HashMap<>();

        for (String term : test_terms) {
            int termType = random().nextInt(TermType.NUM_TOTAL_TYPES);
            termsToType.put(term, termType);
            int ord = typeCounters.getOrDefault(termType, -1) + 1;
            typeCounters.put(termType, ord);
            termsToOrd.put(term, ord);
        }

        TermsIndexBuilder builder = new TermsIndexBuilder();
        for (String term : test_terms) {
            BytesRef termBytes = new BytesRef(term);
            builder.addTerm(termBytes, TermType.fromId(termsToType.get(term)));
        }
        TermsIndex termsIndex = builder.build();

        FST<Long> fst = termsIndex.fst();

        for (String term : test_terms) {
            BytesRef termBytes = new BytesRef(term);
            long encoded = Util.get(fst, termBytes);

            assertEquals(1L, encoded & 0b1L);
            assertEquals((long) termsToType.get(term), (encoded & 0b1110L) >> 1);
            assertEquals((long) termsToOrd.get(term), encoded >> 4);
        }

    }

}