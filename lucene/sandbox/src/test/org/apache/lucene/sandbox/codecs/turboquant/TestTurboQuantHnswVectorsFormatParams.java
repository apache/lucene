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
package org.apache.lucene.sandbox.codecs.turboquant;

import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for TurboQuantHnswVectorsFormat parameter validation and toString. */
public class TestTurboQuantHnswVectorsFormatParams extends LuceneTestCase {

  public void testIllegalMaxConn() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new TurboQuantHnswVectorsFormat(TurboQuantEncoding.BITS_4, 0, 100));
    expectThrows(
        IllegalArgumentException.class,
        () -> new TurboQuantHnswVectorsFormat(TurboQuantEncoding.BITS_4, -1, 100));
  }

  public void testIllegalBeamWidth() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new TurboQuantHnswVectorsFormat(TurboQuantEncoding.BITS_4, 16, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new TurboQuantHnswVectorsFormat(TurboQuantEncoding.BITS_4, 16, -1));
  }

  public void testToString() {
    TurboQuantHnswVectorsFormat format =
        new TurboQuantHnswVectorsFormat(TurboQuantEncoding.BITS_4, 16, 100);
    String s = format.toString();
    assertTrue(s.contains("TurboQuant"));
    assertTrue(s.contains("maxConn=16"));
    assertTrue(s.contains("beamWidth=100"));
    assertTrue(s.contains("BITS_4"));
  }

  public void testMaxDimensions() {
    TurboQuantHnswVectorsFormat format = new TurboQuantHnswVectorsFormat();
    assertEquals(16384, format.getMaxDimensions("any"));
  }

  public void testFlatFormatToString() {
    TurboQuantFlatVectorsFormat flat = new TurboQuantFlatVectorsFormat(TurboQuantEncoding.BITS_2);
    String s = flat.toString();
    assertTrue(s.contains("TurboQuant"));
    assertTrue(s.contains("BITS_2"));
  }

  public void testFlatFormatMaxDimensions() {
    TurboQuantFlatVectorsFormat flat = new TurboQuantFlatVectorsFormat();
    assertEquals(16384, flat.getMaxDimensions("any"));
  }
}
