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

import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestFSTSuffixNodeCache extends LuceneTestCase {

  public void testCopyFallbackNodeBytes() {
    // we don't need the FSTCompiler in this test
    FSTSuffixNodeCache<Object> suffixCache = new FSTSuffixNodeCache<>(null, 1);

    FSTSuffixNodeCache<Object>.PagedGrowableHash primaryHashTable =
        suffixCache.new PagedGrowableHash();
    FSTSuffixNodeCache<Object>.PagedGrowableHash fallbackHashTable =
        suffixCache.new PagedGrowableHash();
    int nodeLength = atLeast(500);
    long fallbackHashSlot = 1;
    byte[] fallbackBytes = RandomBytes.randomBytesOfLength(random(), nodeLength);
    fallbackHashTable.copyNodeBytes(fallbackHashSlot, fallbackBytes, nodeLength);

    // check if the bytes we wrote are the same as the original bytes
    byte[] storedBytes = fallbackHashTable.getBytes(fallbackHashSlot, nodeLength);
    for (int i = 0; i < nodeLength; i++) {
      assertEquals("byte @ index=" + i, fallbackBytes[i], storedBytes[i]);
    }

    long primaryHashSlot = 2;
    primaryHashTable.copyFallbackNodeBytes(
        primaryHashSlot, fallbackHashTable, fallbackHashSlot, nodeLength);

    // check if the bytes we copied are the same as the original bytes
    byte[] copiedBytes = primaryHashTable.getBytes(primaryHashSlot, nodeLength);
    for (int i = 0; i < nodeLength; i++) {
      assertEquals("byte @ index=" + i, fallbackBytes[i], copiedBytes[i]);
    }
  }
}
