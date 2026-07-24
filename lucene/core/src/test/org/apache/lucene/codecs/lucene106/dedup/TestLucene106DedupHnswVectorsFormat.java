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
package org.apache.lucene.codecs.lucene106.dedup;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.simpletext.SimpleTextKnnVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Ignore;

/**
 * Runs the standard KNN vectors format suite against the de-duplicating HNSW format. De-duplication
 * behavior itself is covered by {@link TestDedupFlatVectorsFormat}.
 */
public class TestLucene106DedupHnswVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format = new Lucene106DedupHnswVectorsFormat();

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
      if (knnVectorsReader instanceof SimpleTextKnnVectorsReader) {
        assertEquals(0L, offHeap.size()); // all vectors are in memory
        assertEquals(0L, totalByteSize);
      } else {
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

  /**
   * This test indexes random vectors of small dimensions with high duplicates, checking that RAM
   * usage is above a threshold. The RAM usage assumption breaks with the de-duplicating format.
   */
  @Override
  @Ignore
  public void testWriterRamEstimate() {}

  /** The de-duplicating vector format does not attribute vectors to per-field writers. */
  @Override
  @Ignore
  public void testWriterByteVectorRamEstimate() {}
}
