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

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBitPackerImpl extends LuceneTestCase {

  public void testBasic() {
    FixedSizeByteArrayBitPacker fixedSizeByteArrayBitPacker = new FixedSizeByteArrayBitPacker(5);
    for (int i = 1; i <= 10; i++) {
      fixedSizeByteArrayBitPacker.add(i, 4);
    }
    fixedSizeByteArrayBitPacker.flush();

    byte[] expectedBytes = new byte[] {0x21, 0x43, 0x65, (byte) 0x87, (byte) 0xA9};
    assertArrayEquals(expectedBytes, fixedSizeByteArrayBitPacker.getBytes());
  }

  public void testRandom() {
    ValueAndBitWidth[] randomInputs = ValueAndBitWidth.getRandomArray(random(), 1000);
    int totalNumberBits = Arrays.stream(randomInputs).mapToInt(ValueAndBitWidth::bitWidth).sum();

    BitPerBytePacker referencePacker = new BitPerBytePacker();
    int capacity = totalNumberBits / 8 + (totalNumberBits % 8 == 0 ? 0 : 1);
    FixedSizeByteArrayBitPacker fixedSizeByteArrayBitPacker =
        new FixedSizeByteArrayBitPacker(capacity);

    for (ValueAndBitWidth x : randomInputs) {
      referencePacker.add(x.value(), x.bitWidth());
      fixedSizeByteArrayBitPacker.add(x.value(), x.bitWidth());
    }
    referencePacker.flush();
    fixedSizeByteArrayBitPacker.flush();
    assertArrayEquals(referencePacker.getCompactBytes(), fixedSizeByteArrayBitPacker.getBytes());
  }
}
