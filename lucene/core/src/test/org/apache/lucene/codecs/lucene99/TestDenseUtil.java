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
package org.apache.lucene.codecs.lucene99;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDenseUtil extends LuceneTestCase {

  private byte[] outBytes = new byte[128];
  private ByteArrayDataOutput out = new ByteArrayDataOutput(outBytes);

  public void testEncodeOne() throws Exception {
    long[] longs = {0};
    DenseUtil.encodeDeltas(longs, 1, out);
    assertEquals(2, out.getPosition());
    // length byte
    assertEquals((byte) (0x80 | 1), outBytes[0]);
    // first bit set
    assertEquals(1, outBytes[1]);
  }

  public void testEncodeMoreThanOneLong() throws Exception {
    // sum(longs) = 66
    long[] longs = {0, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1};
    // start sum at 1 since even delta=0 requires 1 bit
    long sum = 1;
    for (long l : longs) {
      sum += l;
    }
    assertEquals(67, sum);
    DenseUtil.encodeDeltas(longs, 67, out);
    assertEquals(10, out.getPosition());
    // length byte
    assertEquals((byte) (0x80 | 9), outBytes[0]);
    // 0 (1) + (11 - 4)
    assertEquals(1, outBytes[1]);
    // (11 - 7) + 2 + (3 - 1)
    assertEquals(0b00101000, outBytes[2]);
    // (3 - 2) + 4 + (5 - 2)
    assertEquals(0b00010001, outBytes[3]);
    // (5 - 3) + 6
    assertEquals((byte) 0b10000010, outBytes[4]);
    // (7) + (8 - 7)
    assertEquals(0b01000000, outBytes[5]);
    // (8 - 1) + (9 - 8)
    assertEquals(0b01000000, outBytes[6]);
    // (9 - 1)
    assertEquals((byte) 0b10000000, outBytes[7]);
    // (10 - 2)
    assertEquals(0, outBytes[8]);
    // (10 - 8) + 1
    assertEquals(0b00000110, outBytes[9]);
  }

  public void testDecodeRandom() throws Exception {
    // max id we can encode is 8 * 128 = 1024
    int numDocs = random().nextInt(64);
    long[] deltas = new long[numDocs];
    long[] docids = new long[numDocs];
    long sum = 1;
    deltas[0] = random().nextInt(16); // can be zero
    sum += deltas[0];
    docids[0] = deltas[0];
    long lastDocId = docids[0];
    for (int i = 1; i < numDocs; i++) {
      deltas[i] = random().nextInt(1, 16);
      sum += deltas[i];
      docids[i] = lastDocId + deltas[i];
      lastDocId = docids[i];
    }
    DenseUtil.encodeDeltas(deltas, (int) sum, out);
    ByteArrayDataInput in = new ByteArrayDataInput(outBytes);
    byte headerByte = in.readByte();
    PostingBits bits = new PostingBits(1);
    DenseUtil.readBlock(headerByte & 0x7f, in, bits);

    int i = 0, nextBit = -1;
    do {
      nextBit = bits.nextSetBit(nextBit + 1);
      assertEquals("wrong doc @" + i, docids[i++], nextBit);
    } while (i < numDocs);
  }
}
