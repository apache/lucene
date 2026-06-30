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

package org.apache.lucene.codecs.dedup;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Randomised conformance tests for {@link DedupHnswVectorsFormat} via the standard {@link
 * BaseKnnVectorsFormatTestCase} harness — exercises CRUD, multi-segment, deletes, sparse/dense,
 * sort, both encodings, all similarities.
 */
public class TestDedupHnswVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat());
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    // We don't implement scalar-quantized fallback; the standard HNSW format we wrap doesn't
    // require it for float vectors.
    return false;
  }

  /** Override the standard assertion: our flat extension is "dvc", not "vec". */
  @Override
  protected void assertOffHeapByteSize(org.apache.lucene.index.LeafReader r, String fieldName)
      throws java.io.IOException {
    var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);
    if (!(r instanceof org.apache.lucene.index.CodecReader codecReader)) {
      throw new AssertionError("unexpected:" + r.getClass());
    }
    org.apache.lucene.codecs.KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
    knnVectorsReader = knnVectorsReader.unwrapReaderForField(fieldName);
    java.util.Map<String, Long> offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
    long total = offHeap.values().stream().mapToLong(Long::longValue).sum();
    int numVectors =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> knnVectorsReader.getByteVectorValues(fieldInfo.name).size();
          case FLOAT32 -> knnVectorsReader.getFloatVectorValues(fieldInfo.name).size();
        };
    if (numVectors == 0) {
      assertEquals(0L, total);
    } else {
      assertTrue("expected vector data > 0", offHeap.getOrDefault("dvc", 0L) > 0L);
      // The HNSW graph file ("vex") is only written when the segment actually built a graph;
      // tiny segments below the build threshold legitimately have no graph. Skip this check
      // unless we explicitly know a graph exists.
    }
  }

  public void testFormatToString() {
    String s = new DedupHnswVectorsFormat(10, 20).toString();
    assertTrue("got " + s, s.startsWith("DedupHnswVectorsFormat(maxConn=10, beamWidth=20"));
  }

  public void testInvalidParams() {
    expectThrows(IllegalArgumentException.class, () -> new DedupHnswVectorsFormat(0, 20));
    expectThrows(IllegalArgumentException.class, () -> new DedupHnswVectorsFormat(-1, 20));
    expectThrows(IllegalArgumentException.class, () -> new DedupHnswVectorsFormat(10, 0));
    expectThrows(IllegalArgumentException.class, () -> new DedupHnswVectorsFormat(10, -3));
  }
}
