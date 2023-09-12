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
package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene98HnswQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new FilterCodec("testing", TestUtil.getDefaultCodec()) {
      @Override
      public KnnVectorsFormat knnVectorsFormat() {
        return new Lucene98HnswVectorsFormat(
            Lucene98HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene98HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            new Lucene98ScalarQuantizedVectorsFormat());
      }
    };
  }

  public void testToString() {
    Lucene98Codec customCodec =
        new Lucene98Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return new Lucene98HnswVectorsFormat(
                10, 20, new Lucene98ScalarQuantizedVectorsFormat(0.9f));
          }
        };
    String expectedString =
        "Lucene98HnswVectorsFormat(name=Lucene98HnswVectorsFormat, maxConn=10, beamWidth=20, quantizer=Lucene98ScalarQuantizedVectorsFormat(name=Lucene98ScalarQuantizedVectorsFormat, quantile=0.9))";
    assertEquals(expectedString, customCodec.getKnnVectorsFormatForField("bogus_field").toString());
  }
}
