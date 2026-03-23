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
package org.apache.lucene.codecs;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests for merged vector values to ensure lastOrd is properly incremented during iteration. */
public class TestMergedVectorValues extends LuceneTestCase {

  /**
   * Test that skipping vectors in MergedByteVectorValues via nextDoc() and then loading a
   * subsequent vector via vectorValue() works correctly.
   */
  public void testSkipsInMergedByteVectorValues() throws IOException {
    // Data
    List<byte[]> vectors = List.of(new byte[] {0}, new byte[] {1});

    // Setup
    KnnVectorsWriter.ByteVectorValuesSub sub =
        new KnnVectorsWriter.ByteVectorValuesSub(x -> x, ByteVectorValues.fromBytes(vectors, 1));
    MergeState state =
        new MergeState(
            null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, false);

    // Run the test
    ByteVectorValues values =
        new KnnVectorsWriter.MergedVectorValues.MergedByteVectorValues(List.of(sub), state);

    // Skip doc 0 and load doc 1
    values.iterator().nextDoc(); // doc 0
    values.iterator().nextDoc(); // doc 1

    // Read vector for doc 1
    assertArrayEquals(vectors.get(1), values.vectorValue(1));
  }

  /**
   * Test that skipping vectors in MergedFloat32VectorValues via nextDoc() and then loading a
   * subsequent vector via vectorValue() works correctly.
   */
  public void testSkipsInMergedFloat32VectorValues() throws IOException {
    // Data
    List<float[]> vectors = List.of(new float[] {0.0f}, new float[] {1.0f});

    // Setup
    KnnVectorsWriter.FloatVectorValuesSub sub =
        new KnnVectorsWriter.FloatVectorValuesSub(x -> x, FloatVectorValues.fromFloats(vectors, 1));
    MergeState state =
        new MergeState(
            null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, false);

    // Run the test
    FloatVectorValues values =
        new KnnVectorsWriter.MergedVectorValues.MergedFloat32VectorValues(List.of(sub), state);

    // Skip doc 0 and load doc 1
    values.iterator().nextDoc(); // doc 0
    values.iterator().nextDoc(); // doc 1

    // Read vector for doc 1
    assertArrayEquals(vectors.get(1), values.vectorValue(1), 0.0f);
  }
}
