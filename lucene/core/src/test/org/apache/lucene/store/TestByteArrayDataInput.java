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
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestByteArrayDataInput extends LuceneTestCase {

  public void testBasic() throws Exception {
    byte[] bytes = new byte[] {1, 65};
    ByteArrayDataInput in = new ByteArrayDataInput(bytes);
    assertEquals("A", in.readString());
    assertTrue(in.eof());

    bytes = new byte[] {1, 1, 65};
    in.reset(bytes, 1, 2);
    assertEquals("A", in.readString());
    assertTrue(in.eof());
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
    final ByteArrayDataInput in = new ByteArrayDataInput(bytes, 0, size);
    assertEquals(43, in.readByte());
    assertEquals(12345, in.readShort());
    assertEquals(1234567890, in.readInt());
    assertEquals(1234567890123456789L, in.readLong());
    assertTrue(in.eof());
  }
}
