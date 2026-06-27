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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.util.Optional;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTurboQuantEncoding extends LuceneTestCase {

  public void testEnumValues() {
    assertEquals(4, TurboQuantEncoding.values().length);
    assertEquals(2, TurboQuantEncoding.BITS_2.bitsPerCoordinate);
    assertEquals(3, TurboQuantEncoding.BITS_3.bitsPerCoordinate);
    assertEquals(4, TurboQuantEncoding.BITS_4.bitsPerCoordinate);
    assertEquals(8, TurboQuantEncoding.BITS_8.bitsPerCoordinate);
  }

  public void testWireNumberRoundTrip() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      Optional<TurboQuantEncoding> decoded = TurboQuantEncoding.fromWireNumber(enc.getWireNumber());
      assertTrue(decoded.isPresent());
      assertEquals(enc, decoded.get());
    }
  }

  public void testWireNumberUnknown() {
    assertFalse(TurboQuantEncoding.fromWireNumber(99).isPresent());
    assertFalse(TurboQuantEncoding.fromWireNumber(-1).isPresent());
  }

  public void testGetPackedByteLengthBits4() {
    // d=4096, b=4: 4096*4/8 = 2048
    assertEquals(2048, TurboQuantEncoding.BITS_4.getPackedByteLength(4096));
    // d=768, b=4: 768*4/8 = 384
    assertEquals(384, TurboQuantEncoding.BITS_4.getPackedByteLength(768));
  }

  public void testGetPackedByteLengthBits2() {
    // d=4096, b=2: 4096*2/8 = 1024
    assertEquals(1024, TurboQuantEncoding.BITS_2.getPackedByteLength(4096));
    // d=32, b=2: 32*2/8 = 8
    assertEquals(8, TurboQuantEncoding.BITS_2.getPackedByteLength(32));
  }

  public void testGetPackedByteLengthBits3() {
    // d=8, b=3: 8*3/8 = 3
    assertEquals(3, TurboQuantEncoding.BITS_3.getPackedByteLength(8));
    // d=768, b=3: 768*3/8 = 288
    assertEquals(288, TurboQuantEncoding.BITS_3.getPackedByteLength(768));
  }

  public void testGetPackedByteLengthBits8() {
    // d=4096, b=8: 4096 bytes
    assertEquals(4096, TurboQuantEncoding.BITS_8.getPackedByteLength(4096));
  }

  public void testGetDiscreteDimensions() {
    // b=4, d=4096: 4096*4=16384 bits, already byte-aligned → 4096
    assertEquals(4096, TurboQuantEncoding.BITS_4.getDiscreteDimensions(4096));
    // b=2, d=32: 32*2=64 bits = 8 bytes → 32
    assertEquals(32, TurboQuantEncoding.BITS_2.getDiscreteDimensions(32));
    // b=3, d=8: 8*3=24 bits = 3 bytes → 8
    assertEquals(8, TurboQuantEncoding.BITS_3.getDiscreteDimensions(8));
    // b=3, d=1: 1*3=3 bits, rounded to 8 bits → 8/3 = 2 (rounded down)
    // Actually (3+7)/8*8/3 = 8/3 = 2
    assertEquals(2, TurboQuantEncoding.BITS_3.getDiscreteDimensions(1));
  }
}
