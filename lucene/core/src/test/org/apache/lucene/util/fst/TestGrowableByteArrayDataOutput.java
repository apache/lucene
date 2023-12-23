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
package org.apache.lucene.util.fst;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;

public class TestGrowableByteArrayDataOutput extends LuceneTestCase {

  public void testRandom() throws Exception {

    final int iters = atLeast(10);
    final int maxBytes = TEST_NIGHTLY ? 200000 : 20000;
    for (int iter = 0; iter < iters; iter++) {
      final int numBytes = TestUtil.nextInt(random(), 1, maxBytes);
      final byte[] expected = new byte[numBytes];
      final GrowableByteArrayDataOutput bytes = new GrowableByteArrayDataOutput();
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter + " numBytes=" + numBytes);
      }

      int pos = 0;
      while (pos < numBytes) {
        int op = random().nextInt(2);
        if (VERBOSE) {
          System.out.println("  cycle pos=" + pos);
        }
        switch (op) {
          case 0:
            {
              // write random byte
              byte b = (byte) random().nextInt(256);
              if (VERBOSE) {
                System.out.println("    writeByte b=" + b);
              }

              expected[pos++] = b;
              bytes.writeByte(b);
            }
            break;

          case 1:
            {
              // write random byte[]
              int len = random().nextInt(Math.min(numBytes - pos, 100));
              byte[] temp = new byte[len];
              random().nextBytes(temp);
              if (VERBOSE) {
                System.out.println("    writeBytes len=" + len + " bytes=" + Arrays.toString(temp));
              }
              System.arraycopy(temp, 0, expected, pos, temp.length);
              bytes.writeBytes(temp, 0, temp.length);
              pos += len;
            }
            break;
        }

        assertEquals(pos, bytes.getPosition());

        if (pos > 0 && random().nextInt(50) == 17) {
          // truncate
          int len = TestUtil.nextInt(random(), 1, Math.min(pos, 100));
          bytes.setPosition(pos - len);
          pos -= len;
          Arrays.fill(expected, pos, pos + len, (byte) 0);
          if (VERBOSE) {
            System.out.println("    truncate len=" + len + " newPos=" + pos);
          }
        }

        if ((pos > 0 && random().nextInt(200) == 17)) {
          verify(bytes, expected, pos);
        }
      }

      GrowableByteArrayDataOutput bytesToVerify;

      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("TEST: save/load final bytes");
        }
        Directory dir = newDirectory();
        IndexOutput out = dir.createOutput("bytes", IOContext.DEFAULT);
        bytes.writeTo(out);
        out.close();
        IndexInput in = dir.openInput("bytes", IOContext.DEFAULT);
        bytesToVerify = new GrowableByteArrayDataOutput();
        bytesToVerify.copyBytes(in, numBytes);
        in.close();
        dir.close();
      } else {
        bytesToVerify = bytes;
      }

      verify(bytesToVerify, expected, numBytes);
    }
  }

  public void testCopyBytesOnByteStore() throws IOException {
    byte[] bytes = new byte[1024 * 8 + 10];
    byte[] bytesout = new byte[bytes.length];
    random().nextBytes(bytes);
    int offset = TestUtil.nextInt(random(), 0, 100);
    int len = bytes.length - offset;
    ByteArrayDataInput in = new ByteArrayDataInput(bytes, offset, len);
    final GrowableByteArrayDataOutput o = new GrowableByteArrayDataOutput();
    o.copyBytes(in, len);
    o.writeTo(0, bytesout, 0, len);
    assertArrayEquals(
        ArrayUtil.copyOfSubArray(bytesout, 0, len),
        ArrayUtil.copyOfSubArray(bytes, offset, offset + len));
  }

  private void verify(GrowableByteArrayDataOutput bytes, byte[] expected, int totalLength)
      throws Exception {
    assertEquals(totalLength, bytes.getPosition());
    if (totalLength == 0) {
      return;
    }
    if (VERBOSE) {
      System.out.println("  verify...");
    }

    // First verify whole thing in one blast:
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bytes.writeTo(new OutputStreamDataOutput(baos));
    byte[] actual = baos.toByteArray();

    assertEquals(totalLength, actual.length);

    for (int i = 0; i < totalLength; i++) {
      assertEquals("byte @ index=" + i, expected[i], actual[i]);
    }
  }
}
