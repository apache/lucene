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

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.junit.Before;

/**
 * Runs the standard KNN vectors format suite against the de-duplicating scalar quantized HNSW
 * format, with a randomized quantization encoding. Quantization-specific de-duplication behavior is
 * covered by {@link TestDedupScalarQuantizedVectorsFormat}.
 */
public class TestDedupHnswScalarQuantizedVectorsFormat extends TestDedupHnswVectorsFormat {

  private KnnVectorsFormat format;

  @Before
  @Override
  public void setUp() throws Exception {
    ScalarEncoding[] encodingValues = ScalarEncoding.values();
    ScalarEncoding encoding = encodingValues[random().nextInt(encodingValues.length)];
    format =
        new DedupHnswScalarQuantizedVectorsFormat(encoding, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new DedupHnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new DedupHnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new DedupHnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class, () -> new DedupHnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new DedupHnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new DedupHnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new DedupHnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 20, 100, 1, new SameThreadExecutorService()));
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
          if (fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
            assertTrue(offHeap.get("vdqd") > 0L); // NOTE: different from veq
          } else {
            assertNull(offHeap.get("vdqd")); // BYTE and FLOAT16 fields are stored raw only
          }

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
}
