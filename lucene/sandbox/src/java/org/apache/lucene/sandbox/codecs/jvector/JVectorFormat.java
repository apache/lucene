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
import java.util.function.Function;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * A Lucene {@link KnnVectorsFormat} implementation for the JVector indexing format. This format
 * defines how vectors are stored, searched, and laid out on disk for maximum performance and
 * flexibility.Add commentMore actions
 */
public class JVectorFormat extends KnnVectorsFormat {
  public static final String NAME = "JVectorFormat";
  public static final String META_CODEC_NAME = "JVectorVectorsFormatMeta";
  public static final String VECTOR_INDEX_CODEC_NAME = "JVectorVectorsFormatIndex";
  public static final String JVECTOR_FILES_SUFFIX = "jvector";
  public static final String META_EXTENSION = "meta-" + JVECTOR_FILES_SUFFIX;
  public static final String VECTOR_INDEX_EXTENSION = "data-" + JVECTOR_FILES_SUFFIX;
  public static final int DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION =
      1024; // The minimum number of vectors required to trigger
  // quantization
  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;
  private static final int DEFAULT_MAX_CONN = 32;
  private static final int DEFAULT_BEAM_WIDTH = 100;
  private static final float DEFAULT_ALPHA = 2f;
  static final boolean DEFAULT_MERGE_ON_DISK = true;
  private static final float DEFAULT_NEIGHBOR_OVERFLOW = 2f;

  private final int maxConn;
  private final int beamWidth;
  private final int minBatchSizeForQuantization;
  private final Function<Integer, Integer> numberOfSubspacesPerVectorSupplier;
  private final boolean mergeOnDisk;
  private final float alpha;
  private final float neighborOverflow;

  public JVectorFormat() {
    this(
        NAME,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NEIGHBOR_OVERFLOW,
        DEFAULT_ALPHA,
        JVectorFormat::getDefaultNumberOfSubspacesPerVector,
        DEFAULT_MINIMUM_BATCH_SIZE_FOR_QUANTIZATION,
        DEFAULT_MERGE_ON_DISK);
  }

  public JVectorFormat(int minBatchSizeForQuantization, boolean mergeOnDisk) {
    this(
        NAME,
        DEFAULT_MAX_CONN,
        DEFAULT_BEAM_WIDTH,
        DEFAULT_NEIGHBOR_OVERFLOW,
        DEFAULT_ALPHA,
        JVectorFormat::getDefaultNumberOfSubspacesPerVector,
        minBatchSizeForQuantization,
        mergeOnDisk);
  }

  public JVectorFormat(
      int maxConn,
      int beamWidth,
      float neighborOverflow,
      float alpha,
      int minBatchSizeForQuantization,
      boolean mergeOnDisk) {
    this(
        NAME,
        maxConn,
        beamWidth,
        neighborOverflow,
        alpha,
        JVectorFormat::getDefaultNumberOfSubspacesPerVector,
        minBatchSizeForQuantization,
        mergeOnDisk);
  }

  public JVectorFormat(
      String name,
      int maxConn,
      int beamWidth,
      float neighborOverflow,
      float alpha,
      Function<Integer, Integer> numberOfSubspacesPerVectorSupplier,
      int minBatchSizeForQuantization,
      boolean mergeOnDisk) {
    super(name);
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.numberOfSubspacesPerVectorSupplier = numberOfSubspacesPerVectorSupplier;
    this.minBatchSizeForQuantization = minBatchSizeForQuantization;
    this.mergeOnDisk = mergeOnDisk;
    this.alpha = alpha;
    this.neighborOverflow = neighborOverflow;
  }

  @Override
  public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new JVectorWriter(
        state,
        maxConn,
        beamWidth,
        neighborOverflow,
        alpha,
        numberOfSubspacesPerVectorSupplier,
        minBatchSizeForQuantization,
        mergeOnDisk);
  }

  @Override
  public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new JVectorReader(state, mergeOnDisk);
  }

  @Override
  public int getMaxDimensions(String dim) {
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
    int compressedBytes;
    if (originalDimension <= 32) {
      compressedBytes = originalDimension;
    } else if (originalDimension <= 64) {
      compressedBytes = 32;
    } else if (originalDimension <= 200) {
      compressedBytes = (int) (originalDimension * 0.5);
    } else if (originalDimension <= 400) {
      compressedBytes = 100;
    } else if (originalDimension <= 768) {
      compressedBytes =
          64; // used for benchmarks, cohere wikipedia-768 achieves high recall w/ greater indexing
      // throughput
    } else if (originalDimension <= 1536) {
      compressedBytes = 192;
    } else if (originalDimension <= 4096) {
      compressedBytes = (int) (originalDimension * 0.0625);
    } else {
      return (int) (originalDimension * 0.0625);
    }
    return compressedBytes;
  }
}
