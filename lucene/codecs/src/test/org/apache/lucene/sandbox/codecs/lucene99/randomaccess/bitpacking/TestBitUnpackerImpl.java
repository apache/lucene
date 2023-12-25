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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess.bitpacking;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestBitUnpackerImpl extends LuceneTestCase {

  public void testUnpackBasics() {
    byte[] bytes = new byte[] {0x21, 0x43, 0x65, (byte) 0x87, (byte) 0xA9};
    BytesRef bytesRef = new BytesRef(bytes);

    for (int i = 1; i <= 10; i++) {
      long val = BitUnpackerImpl.INSTANCE.unpack(bytesRef, (i - 1) * 4, 4);
      assertEquals((long) i, val);
    }
  }

  public void testRandom() {
    ValueAndBitWidth[] expected = ValueAndBitWidth.getRandomArray(random(), 1000);

    BitPerBytePacker referencePacker = new BitPerBytePacker();
    for (var x : expected) {
      referencePacker.add(x.value(), x.bitWidth());
    }

    BytesRef bytes = new BytesRef(referencePacker.getCompactBytes());
    int startBitIndex = 0;
    for (var x : expected) {
      long unpacked = BitUnpackerImpl.INSTANCE.unpack(bytes, startBitIndex, x.bitWidth());
      startBitIndex += x.bitWidth();
      assertEquals(x.value(), unpacked);
    }
  }
}
