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
package org.apache.lucene.facet.taxonomy.writercache;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.facet.FacetTestCase;

public class TestCharBlockArray extends FacetTestCase {

  public void testArray() throws Exception {
    CharBlockArray array = new CharBlockArray();
    StringBuilder builder = new StringBuilder();

    final int n;
    if (TEST_NIGHTLY) {
      n = 100 * 1000;
    } else {
      n = 1000;
    }

    byte[] buffer = new byte[50];

    for (int i = 0; i < n; i++) {
      random().nextBytes(buffer);
      int size = 1 + random().nextInt(50);
      // This test is turning random bytes into a string,
      // this is asking for trouble.
      CharsetDecoder decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onUnmappableCharacter(CodingErrorAction.REPLACE)
              .onMalformedInput(CodingErrorAction.REPLACE);
      String s = decoder.decode(ByteBuffer.wrap(buffer, 0, size)).toString();
      array.append(s);
      builder.append(s);
    }

    for (int i = 0; i < n; i++) {
      random().nextBytes(buffer);
      int size = 1 + random().nextInt(50);
      // This test is turning random bytes into a string,
      // this is asking for trouble.
      CharsetDecoder decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onUnmappableCharacter(CodingErrorAction.REPLACE)
              .onMalformedInput(CodingErrorAction.REPLACE);
      String s = decoder.decode(ByteBuffer.wrap(buffer, 0, size)).toString();
      array.append((CharSequence) s);
      builder.append(s);
    }

    for (int i = 0; i < n; i++) {
      random().nextBytes(buffer);
      int size = 1 + random().nextInt(50);
      // This test is turning random bytes into a string,
      // this is asking for trouble.
      CharsetDecoder decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onUnmappableCharacter(CodingErrorAction.REPLACE)
              .onMalformedInput(CodingErrorAction.REPLACE);
      String s = decoder.decode(ByteBuffer.wrap(buffer, 0, size)).toString();
      for (int j = 0; j < s.length(); j++) {
        array.append(s.charAt(j));
      }
      builder.append(s);
    }

    assertEqualsInternal("GrowingCharArray<->StringBuilder mismatch.", builder, array);
  }

  private static void assertEqualsInternal(
      String msg, StringBuilder expected, CharBlockArray actual) {
    assertEquals(msg, expected.length(), actual.length());
    for (int i = 0; i < expected.length(); i++) {
      assertEquals(msg, expected.charAt(i), actual.charAt(i));
    }
  }
}
