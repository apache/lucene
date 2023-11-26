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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestTermsIndexBuilder extends LuceneTestCase {

  public void testBasics() throws IOException {
    String[] test_terms = {
      "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
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

    byte[] metaBytes = new byte[4096];
    byte[] dataBytes = new byte[4096];
    DataOutput metaOut = new ByteArrayDataOutput(metaBytes);
    DataOutput dataOutput = new ByteArrayDataOutput(dataBytes);

    termsIndex.serialize(metaOut, dataOutput);

    TermsIndexPrimitive termsIndexPrimitive =
        TermsIndexPrimitive.deserialize(
            new ByteArrayDataInput(metaBytes), new ByteArrayDataInput(dataBytes), false);

    for (String term : test_terms) {
      BytesRef termBytes = new BytesRef(term);
      TermsIndex.TypeAndOrd typeAndOrd = termsIndexPrimitive.getTerm(termBytes);

      assertEquals(termsToType.get(term).intValue(), typeAndOrd.termType().getId());
      assertEquals((long) termsToOrd.get(term), typeAndOrd.ord());
    }
  }
}
