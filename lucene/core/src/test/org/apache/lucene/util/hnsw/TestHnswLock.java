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

package org.apache.lucene.util.hnsw;

import java.util.Objects;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestHnswLock extends LuceneTestCase {

  public void testHash() {
    assertEquals(Objects.hash(1, 2), HnswLock.hash(1, 2));
    assertEquals(Objects.hash(3, 4), HnswLock.hash(3, 4));
    assertEquals(Objects.hash(55, 66), HnswLock.hash(55, 66));
    assertEquals(Objects.hash(777, 888), HnswLock.hash(777, 888));
    assertEquals(Objects.hash(9, 9), HnswLock.hash(9, 9));

    for (int iter = 0; iter < atLeast(100); iter++) {
      int v1 = random().nextInt();
      int v2 = random().nextInt();
      assertEquals(Objects.hash(v1, v2), HnswLock.hash(v1, v2));
    }
  }
}
