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
package org.apache.lucene.codecs.lucene912;

import static java.lang.String.format;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.util.Locale;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;

public class TestLucene912BinaryQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene912Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene912BinaryQuantizedVectorsFormat();
      }
    };
  }

  public void testSearch() throws Exception {}

  public void testQuantizedVectorsWriteAndRead() throws Exception {}

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene912BinaryQuantizedVectorsFormat(90_000_000);
          }
        };
    String expectedPattern =
        "Lucene912BinaryQuantizedVectorsFormat(name=Lucene912BinaryQuantizedVectorsFormat, numVectorsPerCluster=90000000, flatVectorScorer=Lucene912BinaryFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer()), rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s()))";
    var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
    var memSegScorer =
        format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene912BinaryQuantizedVectorsFormat(12));
  }

  @Override
  public void testRandomWithUpdatesAndGraph() {
    // graph not supported
  }

  @Override
  public void testSearchWithVisitedLimit() {
    // search not supported
  }
}
