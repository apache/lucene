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
package org.apache.lucene.codecs.lucene94;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.index.BaseFieldInfoFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestLucene94FieldInfosFormat extends BaseFieldInfoFormatTestCase {
  @Override
  protected Codec getCodec() {
    return TestUtil.getDefaultCodec();
  }

  // Ensures that all expected vector similarity functions are translatable
  // in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();

    assertEquals(Lucene94FieldInfosFormat.SIMILARITY_FUNCTIONS, expectedValues);
  }

  public void testEqualAttributesAreSharedAcrossNonAdjacentFields() throws Exception {
    Map<String, String> first = Map.of("format", "x", "suffix", "0");
    Map<String, String> second = Map.of("format", "y", "suffix", "0");
    try (Directory dir = newDirectory()) {
      Codec codec = getCodec();
      SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
      FieldInfos written =
          new FieldInfos(
              new FieldInfo[] {
                fieldWithAttributes("a", 0, first),
                fieldWithAttributes("b", 1, second),
                fieldWithAttributes("c", 2, first),
                fieldWithAttributes("d", 3, Map.of())
              });
      codec.fieldInfosFormat().write(dir, segmentInfo, "", written, IOContext.DEFAULT);
      FieldInfos read = codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);

      FieldInfo a = read.fieldInfo("a");
      FieldInfo b = read.fieldInfo("b");
      FieldInfo c = read.fieldInfo("c");
      assertEquals(first, a.attributes());
      assertEquals(second, b.attributes());
      assertEquals(Map.of(), read.fieldInfo("d").attributes());
      assertSame(a.attributes(), c.attributes());
      assertNotSame(a.attributes(), b.attributes());

      a.putAttribute("extra", "1");
      assertEquals("1", a.getAttribute("extra"));
      assertNull(c.getAttribute("extra"));
      assertEquals(first, c.attributes());
    }
  }

  private static FieldInfo fieldWithAttributes(
      String name, int number, Map<String, String> attributes) {
    return new FieldInfo(
        name,
        number,
        false,
        false,
        false,
        IndexOptions.DOCS,
        DocValuesType.NONE,
        DocValuesSkipIndexType.NONE,
        -1,
        new HashMap<>(attributes),
        0,
        0,
        0,
        0,
        VectorEncoding.FLOAT32,
        VectorSimilarityFunction.EUCLIDEAN,
        false,
        false);
  }
}
