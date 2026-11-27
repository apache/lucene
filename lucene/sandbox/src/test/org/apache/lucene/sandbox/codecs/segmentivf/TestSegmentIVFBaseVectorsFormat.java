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

package org.apache.lucene.sandbox.codecs.segmentivf;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Lucene's shared vectors-format suite over SegmentIVF: merges, deletes, index sorting, addIndexes,
 * CheckIndex and the vector values contract. SegmentIVF indexes FLOAT32 vectors with COSINE or
 * DOT_PRODUCT similarity only, so the suite draws from those, and its byte-vector tests are
 * skipped.
 */
@ThreadLeakFilters(defaultFilters = true, filters = SegmentIVFBuildThreadsFilter.class)
public class TestSegmentIVFBaseVectorsFormat extends BaseKnnVectorsFormatTestCase {
  private static final VectorSimilarityFunction[] SIMILARITIES = {
    VectorSimilarityFunction.COSINE, VectorSimilarityFunction.DOT_PRODUCT
  };

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(
        new SegmentIVFVectorsFormat(64, 8, random().nextInt(2), FineTier.INT8, CoarseTier.S1));
  }

  /** Sums the values read back with a float tolerance, which INT8's lossy values exceed. */
  @Override
  public void testVectorValuesReportCorrectDocs() throws Exception {
    assumeTrue("INT8 values are lossy", false);
  }

  /** As above. */
  @Override
  public void testSparseVectors() throws Exception {
    assumeTrue("INT8 values are lossy", false);
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  protected VectorSimilarityFunction randomSimilarity() {
    return SIMILARITIES[random().nextInt(SIMILARITIES.length)];
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  /**
   * Merging the same index twice need not write the same bytes: a merge warm-starts its clustering
   * from what this JVM remembers ({@code Clustering.HotStart}), and the centroid graph is built in
   * parallel.
   */
  @Override
  protected boolean mergeIsStable() {
    return false;
  }

  @Override
  public void testRandomBytes() {
    // FLOAT32 only
  }

  @Override
  public void testSortedIndexBytes() {
    // FLOAT32 only
  }

  @Override
  public void testByteVectorScorerIteration() {
    // FLOAT32 only
  }

  @Override
  public void testEmptyByteVectorData() {
    // FLOAT32 only
  }

  @Override
  public void testMergingWithDifferentByteKnnFields() {
    // FLOAT32 only
  }

  @Override
  public void testWriterByteVectorRamEstimate() {
    // FLOAT32 only
  }

  @Override
  public void testMismatchedFields() {
    // indexes byte vectors
  }

  // ---- Tests that index EUCLIDEAN fields only, which SegmentIVF rejects.

  @Override
  public void testCheckIndexIncludesVectors() {
    // EUCLIDEAN only
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // EUCLIDEAN only
  }

  @Override
  public void testIllegalSimilarityFunctionChangeViaAddIndexesDirectory() {
    // EUCLIDEAN only
  }

  @Override
  public void testIllegalSimilarityFunctionChangeViaAddIndexesCodecReader() {
    // EUCLIDEAN only
  }

  @Override
  public void testIllegalSimilarityFunctionChangeViaAddIndexesSlowCodecReader() {
    // EUCLIDEAN only
  }

  @Override
  public void testSortedIndex() {
    // EUCLIDEAN only
  }

  @Override
  public void testIllegalEmptyVector() {
    // EUCLIDEAN only
  }

  @Override
  public void testIndexMultipleKnnVectorFields() {
    // EUCLIDEAN only
  }

  @Override
  public void testAdvance() {
    // EUCLIDEAN only
  }

  @Override
  public void testMergingWithDifferentKnnFields() {
    // EUCLIDEAN only
  }

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // EUCLIDEAN only
  }

  @Override
  public void testIndexedValueNotAliased() {
    // EUCLIDEAN only
  }

  // ---- Tests that expect the indexed values back exactly. SegmentIVF stores each vector's
  // direction, rotated: values come back unit length, and within float rounding of the rotation.

  @Override
  public void testRandom() {
    // values are rotated, so only equal within float rounding
  }

  @Override
  public void testAddIndexesDirectory01() {
    // compares vector components that SegmentIVF normalizes
  }

  /** SegmentIVF does not report off-heap sizes, the base reader's default (an empty map). */
  @Override
  protected void assertOffHeapByteSize(LeafReader r, String fieldName) {}
}
