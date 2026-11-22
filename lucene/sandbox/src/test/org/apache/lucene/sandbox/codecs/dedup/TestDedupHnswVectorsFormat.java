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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * Runs the standard KNN vectors format suite against the de-duplicating HNSW format. De-duplication
 * behavior itself is covered by {@link TestDedupFlatVectorsFormat}.
 */
public class TestDedupHnswVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format = new DedupHnswVectorsFormat();

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false; // stores raw vectors, no quantized fallback
  }

  @Override
  protected void assertOffHeapByteSize(LeafReader r, String fieldName) throws IOException {
    var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);

    if (r instanceof CodecReader codecReader) {
      KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
      knnVectorsReader = knnVectorsReader.unwrapReaderForField(fieldName);
      var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
      long totalByteSize = offHeap.values().stream().mapToLong(Long::longValue).sum();
      if (knnVectorsReader instanceof Lucene99HnswVectorsReader) {
        if (getNumVectors(knnVectorsReader, fieldInfo) == 0) {
          assertEquals(0L, totalByteSize);
        } else {
          assertTrue(totalByteSize > 0);
          assertTrue(offHeap.get("vdd") > 0L); // NOTE: different from vec

          if (hasHNSW(knnVectorsReader, fieldInfo)) {
            assertTrue(offHeap.get("vex") > 0L);
          } else {
            assertTrue(offHeap.get("vex") == null || offHeap.get("vex") == 0);
          }
        }
      }
    } else {
      throw new AssertionError("unexpected:" + r.getClass());
    }
  }

  /** Near copy of the original test, this one checks for size of <b>unique</b> vector count. */
  @Override
  @SuppressWarnings("unchecked")
  public void testWriterRamEstimate() throws IOException {
    final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
    final KnnVectorsFormat format = codec.knnVectorsFormat();
    try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
      final long ramBytesUsed = writer.ramBytesUsed();
      int dim = random().nextInt(64) + 1;
      if (dim % 2 == 1) {
        ++dim;
      }
      int numDocs = atLeast(100);
      Set<FloatVector> unique = new HashSet<>();
      KnnFieldVectorsWriter<float[]> fieldWriter =
          (KnnFieldVectorsWriter<float[]>)
              writer.addField(
                  new FieldInfo(
                      "fieldA",
                      0,
                      false,
                      false,
                      false,
                      IndexOptions.NONE,
                      DocValuesType.NONE,
                      DocValuesSkipIndexType.NONE,
                      -1,
                      Map.of(),
                      0,
                      0,
                      0,
                      dim,
                      VectorEncoding.FLOAT32,
                      VectorSimilarityFunction.DOT_PRODUCT,
                      false,
                      false));
      for (int i = 0; i < numDocs; i++) {
        float[] vector = randomVector(dim);
        unique.add(new FloatVector(vector));
        fieldWriter.addValue(i, vector);
      }
      final long ramBytesUsed2 = writer.ramBytesUsed();
      assertThat(ramBytesUsed2, greaterThan(ramBytesUsed));
      assertThat(ramBytesUsed2, greaterThan((long) dim * unique.size() * Float.BYTES));
    }
    dir.close();
  }

  private record FloatVector(float[] vector) {
    @Override
    public boolean equals(Object obj) {
      return obj instanceof FloatVector(float[] other) && Arrays.equals(vector, other);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }

  /** Near copy of the original test, this one checks for size of <b>unique</b> vector count. */
  @Override
  @SuppressWarnings("unchecked")
  public void testWriterByteVectorRamEstimate() throws IOException {
    final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
    final KnnVectorsFormat format = codec.knnVectorsFormat();
    try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
      final long ramBytesUsed = writer.ramBytesUsed();
      int dim = random().nextInt(64) + 1;
      if (dim % 2 == 1) {
        ++dim;
      }
      int numDocs = atLeast(100);
      Set<ByteVector> unique = new HashSet<>();
      KnnFieldVectorsWriter<byte[]> fieldWriter =
          (KnnFieldVectorsWriter<byte[]>)
              writer.addField(
                  new FieldInfo(
                      "fieldA",
                      0,
                      false,
                      false,
                      false,
                      IndexOptions.NONE,
                      DocValuesType.NONE,
                      DocValuesSkipIndexType.NONE,
                      -1,
                      Map.of(),
                      0,
                      0,
                      0,
                      dim,
                      VectorEncoding.BYTE,
                      VectorSimilarityFunction.DOT_PRODUCT,
                      false,
                      false));
      for (int i = 0; i < numDocs; i++) {
        byte[] vector = randomVector8(dim);
        unique.add(new ByteVector(vector));
        fieldWriter.addValue(i, vector);
      }
      final long ramBytesUsed2 = writer.ramBytesUsed();
      assertThat(ramBytesUsed2, greaterThan(ramBytesUsed));
      assertThat(ramBytesUsed2, greaterThan((long) dim * unique.size() * Byte.BYTES));
    }
    dir.close();
  }

  private record ByteVector(byte[] vector) {
    @Override
    public boolean equals(Object obj) {
      return obj instanceof ByteVector(byte[] other) && Arrays.equals(vector, other);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }
}
