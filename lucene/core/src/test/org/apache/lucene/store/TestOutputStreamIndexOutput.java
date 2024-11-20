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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestOutputStreamIndexOutput extends LuceneTestCase {

  public void testDataTypes() throws Exception {
    for (int i = 0; i < 12; i++) {
      doTestDataTypes(i);
    }
  }

  private void doTestDataTypes(int offset) throws Exception {
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final IndexOutput out = new OutputStreamIndexOutput("test" + offset, "test", bos, 12);
    for (int i = 0; i < offset; i++) {
      out.writeByte((byte) i);
    }
    out.writeShort((short) 12345);
    out.writeInt(1234567890);
    out.writeLong(1234567890123456789L);
    assertEquals(offset + 14, out.getFilePointer());
    out.close();

    // read the primitives using ByteBuffer to ensure encoding in byte array is LE:
    final ByteBuffer buf = ByteBuffer.wrap(bos.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < offset; i++) {
      assertEquals(i, buf.get());
    }
    assertEquals(12345, buf.getShort());
    assertEquals(1234567890, buf.getInt());
    assertEquals(1234567890123456789L, buf.getLong());
    assertEquals(0, buf.remaining());
  }
}
