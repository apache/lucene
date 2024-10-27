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
package org.apache.lucene.util;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayRandomAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestRandomAccesInputRef extends LuceneTestCase {

  public void testFromBytes() throws IOException {
    RandomAccessInput bytes =
        new ByteArrayRandomAccessInput(new byte[] {(byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd'});
    RandomAccessInputRef b = new RandomAccessInputRef(bytes, 0, 4);
    assertEquals(bytes, b.bytes);
    assertEquals(0, b.offset);
    assertEquals(4, b.length);

    RandomAccessInputRef b2 = new RandomAccessInputRef(bytes, 1, 3);
    assertEquals("bcd", b2.utf8ToString());
  }

  public void testToBytesRefCopy() {
    RandomAccessInput bytes = new ByteArrayRandomAccessInput(new byte[] {1, 2});
    RandomAccessInputRef from = new RandomAccessInputRef(bytes, 0, 2);
    from.offset += 1; // now invalid
    expectThrows(
        IndexOutOfBoundsException.class,
        () -> {
          RandomAccessInputRef.toBytesRef(from);
        });
  }
}
