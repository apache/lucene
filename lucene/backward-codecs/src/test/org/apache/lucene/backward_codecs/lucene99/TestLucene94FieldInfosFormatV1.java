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
package org.apache.lucene.backward_codecs.lucene99;

import java.util.HashMap;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.BaseFieldInfoFormatTestCase;

public class TestLucene94FieldInfosFormatV1 extends BaseFieldInfoFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new Lucene99Codec();
  }

  @Override
  protected boolean supportDocValuesSkipIndex() {
    return false;
  }

  /**
   * Verify the writer pins the .fnm header to format version 1 (FORMAT_PARENT_FIELD). A Lucene 9.11
   * reader uses checkIndexHeader with maxVersion=1 and would throw IndexFormatTooNewException if
   * the writer accidentally emits version 2.
   */
  public void testHeaderVersionPinnedToV1() throws Exception {
    Directory dir = newDirectory();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_test");

    FieldInfo fi =
        new FieldInfo(
            "field",
            0,
            false,
            false,
            false,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            DocValuesType.NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfos infos = new FieldInfos(new FieldInfo[] {fi});

    Lucene94FieldInfosFormatV1 format = new Lucene94FieldInfosFormatV1();
    format.write(dir, segmentInfo, "", infos, IOContext.DEFAULT);

    String fileName = IndexFileNames.segmentFileName(segmentInfo.name, "", "fnm");
    try (IndexInput input = dir.openInput(fileName, IOContext.DEFAULT)) {
      int version =
          CodecUtil.checkIndexHeader(
              input,
              Lucene94FieldInfosFormatV1.CODEC_NAME,
              Lucene94FieldInfosFormatV1.FORMAT_START,
              Lucene94FieldInfosFormatV1.FORMAT_PARENT_FIELD,
              segmentInfo.getId(),
              "");
      assertEquals(
          "Writer must pin to FORMAT_PARENT_FIELD (v1)",
          Lucene94FieldInfosFormatV1.FORMAT_PARENT_FIELD,
          version);
    }
    dir.close();
  }

  /** Verify the writer rejects fields with DocValuesSkipIndexType != NONE. */
  public void testRejectsDocValuesSkipIndex() throws Exception {
    Directory dir = newDirectory();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_test");

    FieldInfo fi =
        new FieldInfo(
            "skipfield",
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NUMERIC,
            DocValuesSkipIndexType.RANGE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfos infos = new FieldInfos(new FieldInfo[] {fi});

    Lucene94FieldInfosFormatV1 format = new Lucene94FieldInfosFormatV1();
    expectThrows(
        IllegalStateException.class,
        () -> format.write(dir, segmentInfo, "", infos, IOContext.DEFAULT));
    dir.close();
  }

  /**
   * Write diverse field types (text, doc values, points, vectors) through the V1 format and verify
   * round-trip preserves all field properties.
   */
  public void testDiverseFieldTypesRoundTrip() throws Exception {
    Directory dir = newDirectory();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_test");

    FieldInfo textField =
        new FieldInfo(
            "title",
            0,
            true,
            false,
            false,
            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfo numericDvField =
        new FieldInfo(
            "score",
            1,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfo sortedSetDvField =
        new FieldInfo(
            "tags",
            2,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_SET,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfo pointField =
        new FieldInfo(
            "location",
            3,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            2,
            2,
            8,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false);

    FieldInfo vectorField =
        new FieldInfo(
            "embedding",
            4,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            128,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.COSINE,
            false,
            false);

    FieldInfos infos =
        new FieldInfos(
            new FieldInfo[] {textField, numericDvField, sortedSetDvField, pointField, vectorField});

    Lucene94FieldInfosFormatV1 format = new Lucene94FieldInfosFormatV1();
    format.write(dir, segmentInfo, "", infos, IOContext.DEFAULT);
    FieldInfos infos2 = format.read(dir, segmentInfo, "", IOContext.DEFAULT);

    assertEquals(5, infos2.size());

    FieldInfo f = infos2.fieldInfo("title");
    assertNotNull(f);
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, f.getIndexOptions());
    assertTrue(f.hasTermVectors());
    assertEquals(DocValuesType.NONE, f.getDocValuesType());
    assertEquals(DocValuesSkipIndexType.NONE, f.docValuesSkipIndexType());

    f = infos2.fieldInfo("score");
    assertNotNull(f);
    assertEquals(DocValuesType.NUMERIC, f.getDocValuesType());
    assertEquals(DocValuesSkipIndexType.NONE, f.docValuesSkipIndexType());

    f = infos2.fieldInfo("tags");
    assertNotNull(f);
    assertEquals(DocValuesType.SORTED_SET, f.getDocValuesType());
    assertEquals(DocValuesSkipIndexType.NONE, f.docValuesSkipIndexType());

    f = infos2.fieldInfo("location");
    assertNotNull(f);
    assertEquals(2, f.getPointDimensionCount());
    assertEquals(2, f.getPointIndexDimensionCount());
    assertEquals(8, f.getPointNumBytes());

    f = infos2.fieldInfo("embedding");
    assertNotNull(f);
    assertEquals(128, f.getVectorDimension());
    assertEquals(VectorEncoding.FLOAT32, f.getVectorEncoding());
    assertEquals(VectorSimilarityFunction.COSINE, f.getVectorSimilarityFunction());
    assertEquals(DocValuesSkipIndexType.NONE, f.docValuesSkipIndexType());

    dir.close();
  }
}
