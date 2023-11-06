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

import java.util.stream.LongStream;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTermStateCodecComponent extends LuceneTestCase {

  public void testGetBitWidth() {
    int expectedMaxBits = random().nextInt(31) + 1;
    int bitMask = 0xFFFFFFFF >>> (32 - expectedMaxBits);
    int highestBit = (bitMask >>> 1) + 1;

    IntBlockTermState[] termStates =
        random()
            .ints(256)
            .mapToObj(
                docFreq -> {
                  var x = new IntBlockTermState();
                  x.docFreq = (docFreq & bitMask) | highestBit;
                  return x;
                })
            .toArray(IntBlockTermState[]::new);

    byte bitWidth =
        TermStateCodecComponent.getBitWidth(termStates, TermStateCodecComponent.DocFreq.INSTANCE);
    assertEquals(expectedMaxBits, bitWidth);
  }

  public void testGetBitWidthWithIncreasingValues() {
    long baseValue = random().nextLong(Long.MAX_VALUE >> 1);
    int expectedMaxBits = random().nextInt(63) + 1;
    long bitMask = 0xFFFFFFFF_FFFFFFFFL >>> (64 - expectedMaxBits);
    long highestBit = (bitMask >>> 1) + 1;

    var randomLongs =
        random()
            .longs(256, 0, Long.MAX_VALUE - baseValue)
            .map(x -> baseValue + ((x & bitMask) | highestBit))
            .sorted();

    IntBlockTermState[] termStates =
        LongStream.concat(LongStream.of(baseValue), randomLongs)
            .mapToObj(
                docStartFP -> {
                  var x = new IntBlockTermState();
                  x.docStartFP = docStartFP;
                  return x;
                })
            .toArray(IntBlockTermState[]::new);

    byte bitWidth =
        TermStateCodecComponent.getBitWidth(
            termStates, TermStateCodecComponent.DocStartFP.INSTANCE);
    assertEquals(expectedMaxBits, bitWidth);
  }
}
