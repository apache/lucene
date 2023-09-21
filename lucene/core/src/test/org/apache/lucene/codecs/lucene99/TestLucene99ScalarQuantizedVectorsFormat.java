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
package org.apache.lucene.codecs.lucene99;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestLucene99ScalarQuantizedVectorsFormat extends LuceneTestCase {

  public void testDefaultQuantile() {
    float defaultQuantile = Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(99);
    assertEquals(0.99f, defaultQuantile, 1e-5);
    defaultQuantile = Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(1);
    assertEquals(0.9f, defaultQuantile, 1e-5);
    defaultQuantile =
        Lucene99ScalarQuantizedVectorsFormat.calculateDefaultQuantile(Integer.MAX_VALUE - 2);
    assertEquals(1.0f, defaultQuantile, 1e-5);
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99ScalarQuantizedVectorsFormat(0.89f));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene99ScalarQuantizedVectorsFormat(1.1f));
  }
}
