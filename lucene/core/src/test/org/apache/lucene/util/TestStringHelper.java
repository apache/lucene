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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestStringHelper extends LuceneTestCase {

  public void testBytesDifference() {
    BytesRef left = newBytesRef("foobar");
    BytesRef right = newBytesRef("foozo");
    assertEquals(3, StringHelper.bytesDifference(left, right));
    assertEquals(2, StringHelper.bytesDifference(newBytesRef("foo"), newBytesRef("for")));
    assertEquals(2, StringHelper.bytesDifference(newBytesRef("foo1234"), newBytesRef("for1234")));
    assertEquals(1, StringHelper.bytesDifference(newBytesRef("foo"), newBytesRef("fz")));
    assertEquals(0, StringHelper.bytesDifference(newBytesRef("foo"), newBytesRef("g")));
    assertEquals(3, StringHelper.bytesDifference(newBytesRef("foo"), newBytesRef("food")));
    // we can detect terms are out of order if we see a duplicate
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          StringHelper.bytesDifference(newBytesRef("ab"), newBytesRef("ab"));
        });
  }

  public void testStartsWith() {
    BytesRef ref = newBytesRef("foobar");
    BytesRef slice = newBytesRef("foo");
    assertTrue(StringHelper.startsWith(ref, slice));
  }

  public void testEndsWith() {
    BytesRef ref = newBytesRef("foobar");
    BytesRef slice = newBytesRef("bar");
    assertTrue(StringHelper.endsWith(ref, slice));
  }

  public void testStartsWithWhole() {
    BytesRef ref = newBytesRef("foobar");
    BytesRef slice = newBytesRef("foobar");
    assertTrue(StringHelper.startsWith(ref, slice));
  }

  public void testEndsWithWhole() {
    BytesRef ref = newBytesRef("foobar");
    BytesRef slice = newBytesRef("foobar");
    assertTrue(StringHelper.endsWith(ref, slice));
  }

  public void testMurmurHash3() throws Exception {
    // Hashes computed using murmur3_32 from https://code.google.com/p/pyfasthash
    assertEquals(0xf6a5c420, StringHelper.murmurhash3_x86_32(newBytesRef("foo"), 0));
    assertEquals(0xcd018ef6, StringHelper.murmurhash3_x86_32(newBytesRef("foo"), 16));
    assertEquals(
        0x111e7435,
        StringHelper.murmurhash3_x86_32(
            newBytesRef(
                "You want weapons? We're in a library! Books! The best weapons in the world!"),
            0));
    assertEquals(
        0x2c628cd0,
        StringHelper.murmurhash3_x86_32(
            newBytesRef(
                "You want weapons? We're in a library! Books! The best weapons in the world!"),
            3476));
  }

  public void testSortKeyLength() throws Exception {
    assertEquals(3, StringHelper.sortKeyLength(newBytesRef("foo"), newBytesRef("for")));
    assertEquals(3, StringHelper.sortKeyLength(newBytesRef("foo1234"), newBytesRef("for1234")));
    assertEquals(2, StringHelper.sortKeyLength(newBytesRef("foo"), newBytesRef("fz")));
    assertEquals(1, StringHelper.sortKeyLength(newBytesRef("foo"), newBytesRef("g")));
    assertEquals(4, StringHelper.sortKeyLength(newBytesRef("foo"), newBytesRef("food")));
    // we can detect terms are out of order if we see a duplicate
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          StringHelper.sortKeyLength(newBytesRef("ab"), newBytesRef("ab"));
        });
  }
}
