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
package org.apache.lucene.sandbox.codecs.faiss;

import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * Tests for {@link FaissKnnVectorsFormat}. Will run only if required shared libraries (including
 * dependencies) are present at runtime, or the {@value #FAISS_RUN_TESTS} JVM arg is set to {@code
 * true}
 */
public class TestFaissKnnVectorsFormat extends BaseKnnVectorsFormatTestCase {
  private static final String FAISS_RUN_TESTS = "tests.faiss.run";

  private static final VectorEncoding[] SUPPORTED_ENCODINGS = {FLOAT32};
  private static final VectorSimilarityFunction[] SUPPORTED_FUNCTIONS = {DOT_PRODUCT, EUCLIDEAN};

  @BeforeClass
  public static void maybeSuppress() {
    // Explicitly run tests
    if (Boolean.getBoolean(FAISS_RUN_TESTS)) {
      return;
    }

    // Otherwise check if dependencies are present
    boolean dependenciesPresent;
    try {
      FaissLibrary _ = FaissLibrary.INSTANCE;
      dependenciesPresent = true;
    } catch (LinkageError _) {
      dependenciesPresent = false;
    }
    assumeTrue("Dependencies present", dependenciesPresent);
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return SUPPORTED_ENCODINGS[random().nextInt(SUPPORTED_ENCODINGS.length)];
  }

  @Override
  protected VectorSimilarityFunction randomSimilarity() {
    return SUPPORTED_FUNCTIONS[random().nextInt(SUPPORTED_FUNCTIONS.length)];
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new FaissKnnVectorsFormat());
  }

  @Override
  public void testRecall() throws IOException {
    // only supports some functions
    for (VectorSimilarityFunction similarity : SUPPORTED_FUNCTIONS) {
      assertRecall(similarity, 0.5, 1.0);
    }
  }

  @Override
  @Ignore // does not honour visitedLimit
  public void testSearchWithVisitedLimit() {}

  @Override
  @Ignore // does not support byte vectors
  public void testByteVectorScorerIteration() {}

  @Override
  @Ignore // does not support byte vectors
  public void testMismatchedFields() {}

  @Override
  @Ignore // does not support byte vectors
  public void testSortedIndexBytes() {}

  @Override
  @Ignore // does not support byte vectors
  public void testRandomBytes() {}

  @Override
  @Ignore // does not support byte vectors
  public void testEmptyByteVectorData() {}

  @Override
  @Ignore // does not support byte vectors
  public void testMergingWithDifferentByteKnnFields() {}

  @Monster("Uses large amount of heap and RAM")
  public void testLargeVectorData() throws IOException {
    KnnVectorsFormat format =
        new FaissKnnVectorsFormat(
            "IDMap,Flat", // no need for special indexing like HNSW
            "");
    IndexWriterConfig config =
        newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));

    float[] largeVector =
        new float[format.getMaxDimensions("vector")]; // largest vector accepted by the format
    int numDocs =
        Math.ceilDivExact(
            Integer.MAX_VALUE, Float.BYTES * largeVector.length); // find minimum number of docs

    // Check that we can index vectors larger than Integer.MAX_VALUE number of bytes
    try (Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, config)) {
      writer.addDocuments(
          Collections.nCopies(numDocs, List.of(new KnnFloatVectorField("vector", largeVector))));
    }
  }
}
