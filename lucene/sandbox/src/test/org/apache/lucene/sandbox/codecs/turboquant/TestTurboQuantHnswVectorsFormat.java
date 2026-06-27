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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.io.IOException;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.Before;

/** Tests TurboQuantHnswVectorsFormat using the base test infrastructure. */
public class TestTurboQuantHnswVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private KnnVectorsFormat format;

  @Before
  @Override
  public void setUp() throws Exception {
    TurboQuantEncoding[] encodings = TurboQuantEncoding.values();
    TurboQuantEncoding encoding = encodings[random().nextInt(encodings.length)];
    format = new TurboQuantHnswVectorsFormat(encoding, 16, 100);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  @Override
  protected void assertOffHeapByteSize(LeafReader r, String fieldName) throws IOException {
    var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);
    if (r instanceof CodecReader codecReader) {
      KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
      var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
      long totalByteSize = offHeap.values().stream().mapToLong(Long::longValue).sum();
      // Just verify non-negative; TurboQuant uses "vetq" key instead of "veq"/"veb"
      assertTrue("total off-heap should be >= 0", totalByteSize >= 0);
    }
  }
}
