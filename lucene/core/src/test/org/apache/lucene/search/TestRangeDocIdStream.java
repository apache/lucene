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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestRangeDocIdStream extends LuceneTestCase {

  public void testForEach() throws IOException {
    RangeDocIdStream stream = new RangeDocIdStream(42, 100);
    int[] expected = new int[] {42};
    stream.forEach(
        doc -> {
          assertEquals(expected[0]++, doc);
        });
    assertEquals(100, expected[0]);
  }

  public void testCount() throws IOException {
    RangeDocIdStream stream = new RangeDocIdStream(42, 100);
    assertEquals(100 - 42, stream.count());
  }

  public void testForEachUpTo() throws IOException {
    RangeDocIdStream stream = new RangeDocIdStream(42, 100);
    int[] expected = new int[] {42};

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(20, _ -> fail());

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        65,
        doc -> {
          assertEquals(expected[0]++, doc);
        });
    assertEquals(65, expected[0]);

    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        120,
        doc -> {
          assertEquals(expected[0]++, doc);
        });
    assertEquals(100, expected[0]);

    assertFalse(stream.mayHaveRemaining());
  }

  public void testCountUpTo() throws IOException {
    RangeDocIdStream stream = new RangeDocIdStream(42, 100);
    assertTrue(stream.mayHaveRemaining());
    assertEquals(0, stream.count(20));
    assertTrue(stream.mayHaveRemaining());
    assertEquals(65 - 42, stream.count(65));
    assertTrue(stream.mayHaveRemaining());
    assertEquals(100 - 65, stream.count(120));
    assertFalse(stream.mayHaveRemaining());
  }

  public void testMixForEachCountUpTo() throws IOException {
    RangeDocIdStream stream = new RangeDocIdStream(42, 100);
    int[] expected = new int[] {42};
    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        65,
        doc -> {
          assertEquals(expected[0]++, doc);
        });
    assertEquals(65, expected[0]);

    assertTrue(stream.mayHaveRemaining());
    assertEquals(80 - 65, stream.count(80));

    expected[0] = 80;
    assertTrue(stream.mayHaveRemaining());
    stream.forEach(
        90,
        doc -> {
          assertEquals(expected[0]++, doc);
        });
    assertEquals(90, expected[0]);

    assertTrue(stream.mayHaveRemaining());
    assertEquals(100 - 90, stream.count(120));

    assertFalse(stream.mayHaveRemaining());
  }
}
