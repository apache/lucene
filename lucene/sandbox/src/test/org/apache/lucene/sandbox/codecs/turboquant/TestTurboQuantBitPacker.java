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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestTurboQuantBitPacker extends LuceneTestCase {

  public void testRoundTripAllEncodings() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      int maxVal = (1 << b) - 1;
      for (int d : new int[] {32, 768, 4096}) {
        byte[] indices = new byte[d];
        java.util.Random rng = new java.util.Random(d * 31L + b);
        for (int i = 0; i < d; i++) {
          indices[i] = (byte) rng.nextInt(maxVal + 1);
        }

        int packedLen = enc.getPackedByteLength(d);
        byte[] packed = new byte[packedLen];
        TurboQuantBitPacker.pack(indices, d, b, packed);

        byte[] unpacked = new byte[d];
        TurboQuantBitPacker.unpack(packed, b, d, unpacked);

        for (int i = 0; i < d; i++) {
          assertEquals(
              "b=" + b + " d=" + d + " index " + i, indices[i] & 0xFF, unpacked[i] & 0xFF);
        }
      }
    }
  }

  public void testAllZeros() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      int d = 128;
      byte[] indices = new byte[d]; // all zeros
      byte[] packed = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(indices, d, b, packed);
      byte[] unpacked = new byte[d];
      TurboQuantBitPacker.unpack(packed, b, d, unpacked);
      for (int i = 0; i < d; i++) {
        assertEquals(0, unpacked[i]);
      }
    }
  }

  public void testAllMax() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      int maxVal = (1 << b) - 1;
      int d = 128;
      byte[] indices = new byte[d];
      for (int i = 0; i < d; i++) indices[i] = (byte) maxVal;

      byte[] packed = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(indices, d, b, packed);
      byte[] unpacked = new byte[d];
      TurboQuantBitPacker.unpack(packed, b, d, unpacked);
      for (int i = 0; i < d; i++) {
        assertEquals("b=" + b + " index " + i, maxVal, unpacked[i] & 0xFF);
      }
    }
  }

  public void testAlternatingPattern() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      int maxVal = (1 << b) - 1;
      int d = 256;
      byte[] indices = new byte[d];
      for (int i = 0; i < d; i++) {
        indices[i] = (byte) (i % 2 == 0 ? 0 : maxVal);
      }

      byte[] packed = new byte[enc.getPackedByteLength(d)];
      TurboQuantBitPacker.pack(indices, d, b, packed);
      byte[] unpacked = new byte[d];
      TurboQuantBitPacker.unpack(packed, b, d, unpacked);
      for (int i = 0; i < d; i++) {
        assertEquals(indices[i] & 0xFF, unpacked[i] & 0xFF);
      }
    }
  }

  public void testOutputLengthMatchesEncoding() {
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      for (int d : new int[] {32, 768, 4096, 16384}) {
        int expected = enc.getPackedByteLength(d);
        byte[] indices = new byte[d];
        byte[] packed = new byte[expected];
        // Should not throw — output buffer is exactly the right size
        TurboQuantBitPacker.pack(indices, d, enc.bitsPerCoordinate, packed);
      }
    }
  }

  public void testEdgeCaseMinDimension() {
    // d=1 for each encoding
    for (TurboQuantEncoding enc : TurboQuantEncoding.values()) {
      int b = enc.bitsPerCoordinate;
      byte[] indices = new byte[] {(byte) ((1 << b) - 1)};
      byte[] packed = new byte[enc.getPackedByteLength(1)];
      TurboQuantBitPacker.pack(indices, 1, b, packed);
      byte[] unpacked = new byte[1];
      TurboQuantBitPacker.unpack(packed, b, 1, unpacked);
      assertEquals(indices[0] & 0xFF, unpacked[0] & 0xFF);
    }
  }
}
