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
package org.apache.lucene.index;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestTermsEnumIndex extends LuceneTestCase {

  public void testPrefix8ToComparableUnsignedLong() {
    byte[] b = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    assertEquals(0L, TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 1, 0)));
    assertEquals(4L << 56, TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 1)));
    assertEquals(
        (4L << 56) | (5L << 48),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 2)));
    assertEquals(
        (4L << 56) | (5L << 48) | (6L << 40),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 3)));
    assertEquals(
        (4L << 56) | (5L << 48) | (6L << 40) | (7L << 32),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 4)));
    assertEquals(
        (4L << 56) | (5L << 48) | (6L << 40) | (7L << 32) | (8L << 24),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 5)));
    assertEquals(
        (4L << 56) | (5L << 48) | (6L << 40) | (7L << 32) | (8L << 24) | (9L << 16),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 6)));
    assertEquals(
        (4L << 56) | (5L << 48) | (6L << 40) | (7L << 32) | (8L << 24) | (9L << 16) | (10L << 8),
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 7)));
    assertEquals(
        (4L << 56)
            | (5L << 48)
            | (6L << 40)
            | (7L << 32)
            | (8L << 24)
            | (9L << 16)
            | (10L << 8)
            | 11L,
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 8)));
    assertEquals(
        (4L << 56)
            | (5L << 48)
            | (6L << 40)
            | (7L << 32)
            | (8L << 24)
            | (9L << 16)
            | (10L << 8)
            | 11L,
        TermsEnumIndex.prefix8ToComparableUnsignedLong(new BytesRef(b, 3, 9)));
  }
}
