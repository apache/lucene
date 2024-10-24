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
package org.apache.lucene.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestRandomAccessInputDataInput extends LuceneTestCase {

  public void testBasic() throws Exception {
    RandomAccessInput accessInput = new BytesRef(new byte[] {1, 65}, 0, 2);
    RandomAccessInputDataInput in = new RandomAccessInputDataInput();
    in.reset(accessInput);
    assertEquals("A", in.readString());
    assertEquals(accessInput.length(), in.getPosition());
  }

  public void testDatatypes() throws Exception {
    // write some primitives using ByteArrayDataOutput:
    final byte[] bytes = new byte[32];
    final ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
    out.writeByte((byte) 43);
    out.writeShort((short) 12345);
    out.writeInt(1234567890);
    out.writeLong(1234567890123456789L);
    final int size = out.getPosition();
    assertEquals(15, size);

    // read the primitives using ByteBuffer to ensure encoding in byte array is LE:
    final ByteBuffer buf = ByteBuffer.wrap(bytes, 0, size).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(43, buf.get());
    assertEquals(12345, buf.getShort());
    assertEquals(1234567890, buf.getInt());
    assertEquals(1234567890123456789L, buf.getLong());
    assertEquals(0, buf.remaining());

    // read the primitives using ByteArrayDataInput:
    final RandomAccessInputDataInput in = new RandomAccessInputDataInput();
    in.reset(new BytesRef(bytes, 0, size));
    assertEquals(43, in.readByte());
    assertEquals(12345, in.readShort());
    assertEquals(1234567890, in.readInt());
    assertEquals(1234567890123456789L, in.readLong());
    assertEquals(size, (int) in.getPosition());

    // copy all
    int offset = random().nextInt(10);
    byte[] copy = new byte[offset + size];
    in.rewind();
    in.readBytes(copy, offset, size);
    assertArrayEquals(
        Arrays.copyOfRange(bytes, 0, size), Arrays.copyOfRange(copy, offset, offset + size));
  }
}
