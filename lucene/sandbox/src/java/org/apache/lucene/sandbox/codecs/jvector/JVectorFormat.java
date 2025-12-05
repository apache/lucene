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

package org.apache.lucene.sandbox.codecs.jvector;

import java.io.IOException;
import java.util.List;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/// Implements K-NN search using JVector library for indexing
public class JVectorFormat extends KnnVectorsFormat {
  public static final String NAME = "JVectorFormat";
  public static final String META_CODEC_NAME = "JVectorVectorsFormatMeta";
  public static final String VECTOR_INDEX_CODEC_NAME = "JVectorVectorsFormatIndex";
  public static final String JVECTOR_FILES_SUFFIX = "jvector";
  public static final String META_EXTENSION = "meta-" + JVECTOR_FILES_SUFFIX;
  public static final String VECTOR_INDEX_EXTENSION = "data-" + JVECTOR_FILES_SUFFIX;

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;
  public static final int DEFAULT_MAX_CONN = 32;
  public static final int DEFAULT_BEAM_WIDTH = 100;
  public static final int DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION = 1024;
  public static final float DEFAULT_NEIGHBOR_OVERFLOW = 2f;
  public static final float DEFAULT_ALPHA = 2f;
  public static final boolean DEFAULT_HIERARCHY_ENABLED = true;

  private final List<Integer> maxDegrees;
  private final int beamWidth;
  // As a function of the original dimension
  private final IntUnaryOperator numberOfSubspacesPerVectorSupplier;
  private final int minBatchSizeForQuantization;
  private final float alpha;
  private final float neighborOverflow;
  private final boolean hierarchyEnabled;

  public JVectorFormat() {
    this(
        NAME,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NEIGHBOR_OVERFLOW,
        DEFAULT_ALPHA,
        JVectorFormat::getDefaultNumberOfSubspacesPerVector,
        DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION,
        DEFAULT_HIERARCHY_ENABLED);
  }

  public JVectorFormat(int minBatchSizeForQuantization) {
    this(
        NAME,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NEIGHBOR_OVERFLOW,
        DEFAULT_ALPHA,
        JVectorFormat::getDefaultNumberOfSubspacesPerVector,
        minBatchSizeForQuantization,
        DEFAULT_HIERARCHY_ENABLED);
  }

  public JVectorFormat(
      int maxConn,
      int beamWidth,
      float neighborOverflow,
      float alpha,
      IntUnaryOperator numberOfSubspacesPerVectorSupplier,
      int minBatchSizeForQuantization,
      boolean hierarchyEnabled) {
    this(
        NAME,
        maxConn,
        beamWidth,
        neighborOverflow,
        alpha,
        numberOfSubspacesPerVectorSupplier,
        minBatchSizeForQuantization,
        hierarchyEnabled);
  }

  public JVectorFormat(
      String name,
      int maxConn,
      int beamWidth,
      float neighborOverflow,
      float alpha,
      IntUnaryOperator numberOfSubspacesPerVectorSupplier,
      int minBatchSizeForQuantization,
      boolean hierarchyEnabled) {
    this(
        name,
        hierarchyEnabled ? List.of(maxConn * 2, maxConn) : List.of(maxConn),
        beamWidth,
        neighborOverflow,
        alpha,
        numberOfSubspacesPerVectorSupplier,
        minBatchSizeForQuantization,
        hierarchyEnabled);
  }

  public JVectorFormat(
      String name,
      List<Integer> maxDegrees,
      int beamWidth,
      float neighborOverflow,
      float alpha,
      IntUnaryOperator numberOfSubspacesPerVectorSupplier,
      int minBatchSizeForQuantization,
      boolean hierarchyEnabled) {
    super(name);
    this.maxDegrees = maxDegrees;
    this.beamWidth = beamWidth;
    this.numberOfSubspacesPerVectorSupplier = numberOfSubspacesPerVectorSupplier;
    this.minBatchSizeForQuantization = minBatchSizeForQuantization;
    this.alpha = alpha;
    this.neighborOverflow = neighborOverflow;
    this.hierarchyEnabled = hierarchyEnabled;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new JVectorWriter(
        state,
        maxDegrees,
        beamWidth,
        neighborOverflow,
        alpha,
        numberOfSubspacesPerVectorSupplier,
        minBatchSizeForQuantization,
        hierarchyEnabled);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new JVectorReader(state);
  }

  @Override
  public int getMaxDimensions(String s) {
    // Not a hard limit, but a reasonable default
    return 8192;
  }

  /**
   * This method returns the default number of subspaces per vector for a given original dimension.
   * Should be used as a default value for the number of subspaces per vector in case no value is
   * provided.
   *
   * @param originalDimension original vector dimension
   * @return default number of subspaces per vector
   */
  public static int getDefaultNumberOfSubspacesPerVector(int originalDimension) {
    // the idea here is that higher dimensions compress well, but not so well that we should use
    // fewer bits
    // than a lower-dimension vector, which is what you could get with cutoff points to switch
    // between (e.g.)
    // D*0.5 and D*0.25. Thus, the following ensures that bytes per vector is strictly increasing
    // with D.
    int compressedBytes;
    if (originalDimension <= 32) {
      // We are compressing from 4-byte floats to single-byte codebook indexes,
      // so this represents compression of 4x
      // * GloVe-25 needs 25 BPV to achieve good recall
      compressedBytes = originalDimension;
    } else if (originalDimension <= 64) {
      // * GloVe-50 performs fine at 25
      compressedBytes = 32;
    } else if (originalDimension <= 200) {
      // * GloVe-100 and -200 perform well at 50 and 100 BPV, respectively
      compressedBytes = (int) (originalDimension * 0.5);
    } else if (originalDimension <= 400) {
      // * NYTimes-256 actually performs fine at 64 BPV but we'll be conservative
      // since we don't want BPV to decrease
      compressedBytes = 100;
    } else if (originalDimension <= 768) {
      // allow BPV to increase linearly up to 192
      compressedBytes = (int) (originalDimension * 0.25);
    } else if (originalDimension <= 1536) {
      // * ada002 vectors have good recall even at 192 BPV = compression of 32x
      compressedBytes = 192;
    } else {
      // We have not tested recall with larger vectors than this, let's let it increase linearly
      compressedBytes = (int) (originalDimension * 0.125);
    }
    return compressedBytes;
  }

  static io.github.jbellis.jvector.vector.VectorSimilarityFunction toJVectorSimilarity(
      final org.apache.lucene.index.VectorSimilarityFunction luceneFunction) {
    return switch (luceneFunction) {
      case COSINE -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE;
      case DOT_PRODUCT -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.DOT_PRODUCT;
      case EUCLIDEAN -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN;
      case MAXIMUM_INNER_PRODUCT ->
          throw new UnsupportedOperationException("JVector does not support MAXIMUM_INNER_PRODUCT");
    };
  }

  static org.apache.lucene.index.VectorSimilarityFunction toLuceneSimilarity(
      final io.github.jbellis.jvector.vector.VectorSimilarityFunction jVectorFunction) {
    return switch (jVectorFunction) {
      case COSINE -> org.apache.lucene.index.VectorSimilarityFunction.COSINE;
      case DOT_PRODUCT -> org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
      case EUCLIDEAN -> org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
    };
  }
}
