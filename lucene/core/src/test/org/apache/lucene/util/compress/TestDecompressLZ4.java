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
package org.apache.lucene.util.compress;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDecompressLZ4 extends LuceneTestCase {
  public void testDecompressOffset0() {
    byte[] input =
        new byte[] {
          // token
          0xE,
          // offset 0 (invalid)
          0,
          0,
          // last literal
          // token
          7 << 4,
          // literal
          0,
          0,
          0,
          0,
          0,
          0,
          0
        };

    byte[] output = new byte[18];

    var e =
        assertThrows(
            IOException.class,
            () -> LZ4.decompress(new ByteArrayDataInput(input), output.length, output, 0));
    assertEquals("offset 0 is invalid", e.getMessage());
  }
}
