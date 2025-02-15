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
package org.apache.lucene.sandbox.vectorsearch;

import static org.apache.lucene.util.ArrayUtil.copyOfSubArray;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexOutputOutputStream extends LuceneTestCase {

  public void testBasic() throws IOException {
    try (var dir = newDirectory()) {
      try (var indexOut = dir.createOutput("test", IOContext.DEFAULT)) {
        var out = new IndexOutputOutputStream(indexOut);
        out.write(0x56);
        out.write(new byte[] {0x10, 0x11, 0x12, 0x13, 0x14});
        out.close();
      }

      try (var indexIn = dir.openInput("test", IOContext.DEFAULT)) {
        var in = new IndexInputInputStream(indexIn);
        // assertEquals(0x56, in.read());
        byte[] ba = new byte[6];
        assertEquals(6, in.read(ba));
        assertArrayEquals(new byte[] {0x56, 0x10, 0x11, 0x12, 0x13, 0x14}, ba);
      }
    }
  }

  public void testGetFilePointer() throws IOException {
    try (var dir = newDirectory()) {
      try (var indexOut = dir.createOutput("test", IOContext.DEFAULT)) {
        var out = new IndexOutputOutputStream(indexOut);
        out.write(0x56);
        out.write(new byte[] {0x10, 0x11, 0x12});
        assertEquals(4, indexOut.getFilePointer());
        out.close();
      }
    }
  }

  public void testWithRandom() throws IOException {
    byte[] data = new byte[Math.min(atLeast(10_000), 20_000)];
    Random random = random();
    random.nextBytes(data);

    try (var dir = newDirectory()) {
      try (var indexOut = dir.createOutput("test", IOContext.DEFAULT)) {
        var out = new IndexOutputOutputStream(indexOut);
        int i = 0;
        while (i < data.length) {
          if (random.nextBoolean()) {
            out.write(data[i]);
            i++;
          } else {
            int numBytes = random.nextInt(Math.min(data.length - i, 100));
            out.write(data, i, numBytes);
            i += numBytes;
          }
        }
        out.close();
      }

      try (var indexIn = dir.openInput("test", IOContext.DEFAULT)) {
        var in = new IndexInputInputStream(indexIn);
        int i = 0;
        while (i < data.length) {
          if (random.nextBoolean()) {
            int b = in.read();
            assertEquals(data[i], b);
            i++;
          } else {
            int numBytes = random.nextInt(Math.min(data.length - i, 100));
            byte[] ba = new byte[numBytes];
            in.read(ba, 0, numBytes);
            assertArrayEquals(copyOfSubArray(data, i, i + numBytes), ba);
            i += numBytes;
          }
        }
        assertEquals(-1, in.read());
        assertEquals(-1, in.read(new byte[2]));
      }
    }
  }
}
