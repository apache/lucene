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
package org.apache.lucene.backward_codecs.lucene92;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;

public class TestLucene92HnswVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new Lucene92RWCodec();
  }

  @Override
  protected VectorEncoding randomVectorEncoding() {
    // Older formats only support float vectors
    return VectorEncoding.FLOAT32;
  }

  public void testToString() {
    Codec customCodec =
        new Lucene92RWCodec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene92RWHnswVectorsFormat(10, 20);
          }
        };
    String expectedString =
        "Lucene92RWHnswVectorsFormat(name = Lucene92RWHnswVectorsFormat, maxConn = 10, beamWidth=20)";
    assertEquals(
        expectedString,
        ((Lucene92Codec) customCodec).getKnnVectorsFormatForField("bogus_field").toString());
  }

  @Override
  public void testRandomBytes() throws Exception {
    // unimplemented
  }

  @Override
  public void testSortedIndexBytes() throws Exception {
    // unimplemented
  }

  @Override
  public void testByteVectorScorerIteration() {
    // unimplemented
  }

  @Override
  public void testEmptyByteVectorData() {
    // unimplemented
  }
}
