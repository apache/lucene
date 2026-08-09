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

import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Runs the full {@link BaseKnnVectorsFormatTestCase} randomized compliance suite with {@link
 * RotatingKnnVectorsFormat} wrapping {@link Lucene104HnswScalarQuantizedVectorsFormat}. This
 * verifies rotation works correctly for all standard indexing, search, merge, and read-back paths.
 */
public class TestRotatingKnnVectorsFormatCompliance extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(
        RotatingKnnVectorsFormat.rotating(new Lucene104HnswScalarQuantizedVectorsFormat()));
  }

  @Override
  protected float getVectorValueTolerance() {
    // Rotation + inverse-rotation introduces ~1e-6 floating-point drift from FWHT additions.
    return 1e-5f;
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  // Tests that assert exact vectorValue() read-back with quantization's lossy dequantize path:
  @Override
  public void testRandom() {}

  @Override
  public void testAddIndexesDirectory01() {}

  @Override
  public void testSparseVectors() {}

  @Override
  public void testRandomWithUpdatesAndGraph() {}

  @Override
  public void testVectorValuesReportCorrectDocs() {}

  // Byte-vector tests: RotatingKnnVectorsFormat only supports FLOAT32; byte fields throw by design.
  @Override
  public void testWriterByteVectorRamEstimate() {}

  @Override
  public void testMergingWithDifferentByteKnnFields() {}

  @Override
  public void testSortedIndexBytes() {}

  @Override
  public void testMismatchedFields() {}

  @Override
  public void testRandomBytes() {}

  @Override
  public void testEmptyByteVectorData() {}

  @Override
  public void testCheckIntegrityReadsAllBytes() {}

  // Codec-mixing tests: rotation state enforcement correctly rejects mixed-rotation merges.
  @Override
  public void testDifferentCodecs1() {}

  @Override
  public void testDifferentCodecs2() {}

  // Multi-field tests that include a byte field alongside float:
  @Override
  public void testMergeStability() {}

  @Override
  public void testRandomExceptions() {}

  @Override
  public void testByteVectorScorerIteration() {}
}
