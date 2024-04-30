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
package org.apache.lucene.tests.index;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.tests.IndexPackageAccess;
import org.apache.lucene.internal.tests.TestSecrets;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper.Failure;
import org.apache.lucene.tests.store.MockDirectoryWrapper.FakeIOException;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * Abstract class to do basic tests for fis format. NOTE: This test focuses on the fis impl, nothing
 * else. The [stretch] goal is for this test to be so thorough in testing a new fis format that if
 * this test passes, then all Lucene tests should also pass. Ie, if there is some bug in a given fis
 * Format that this test fails to catch then this test needs to be improved!
 */
public abstract class BaseFieldInfoFormatTestCase extends BaseIndexFileFormatTestCase {

  private static final IndexPackageAccess INDEX_PACKAGE_ACCESS =
      TestSecrets.getIndexPackageAccess();

  /** Test field infos read/write with a single field */
  public void testOneField() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);

    FieldInfos infos2 = codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);
    assertEquals(1, infos2.size());
    assertNotNull(infos2.fieldInfo("field"));
    assertTrue(infos2.fieldInfo("field").getIndexOptions() != IndexOptions.NONE);
    assertFalse(infos2.fieldInfo("field").getDocValuesType() != DocValuesType.NONE);
    assertFalse(infos2.fieldInfo("field").omitsNorms());
    assertFalse(infos2.fieldInfo("field").hasPayloads());
    assertFalse(infos2.fieldInfo("field").hasVectors());
    assertEquals(0, infos2.fieldInfo("field").getPointDimensionCount());
    assertEquals(0, infos2.fieldInfo("field").getVectorDimension());
    assertFalse(infos2.fieldInfo("field").isSoftDeletesField());
    dir.close();
  }

  /** Test field infos attributes coming back are not mutable */
  public void testImmutableAttributes() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);
    fi.putAttribute("foo", "bar");
    fi.putAttribute("bar", "baz");

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);

    FieldInfos infos2 = codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);
    assertEquals(1, infos2.size());
    assertNotNull(infos2.fieldInfo("field"));
    Map<String, String> attributes = infos2.fieldInfo("field").attributes();
    // shouldn't be able to modify attributes
    expectThrows(
        UnsupportedOperationException.class,
        () -> {
          attributes.put("bogus", "bogus");
        });

    dir.close();
  }

  /**
   * Test field infos write that hits exception immediately on open. make sure we get our exception
   * back, no file handle leaks, etc.
   */
  public void testExceptionOnCreateOutput() throws Exception {
    Failure fail =
        new Failure() {
          @Override
          public void eval(MockDirectoryWrapper dir) throws IOException {
            if (doFail && callStackContainsAnyOf("createOutput")) {
              throw new FakeIOException();
            }
          }
        };

    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    fail.setDoFail();
    expectThrows(
        FakeIOException.class,
        () -> {
          codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);
        });
    fail.clearDoFail();

    dir.close();
  }

  /**
   * Test field infos write that hits exception on close. make sure we get our exception back, no
   * file handle leaks, etc.
   */
  public void testExceptionOnCloseOutput() throws Exception {
    Failure fail =
        new Failure() {
          @Override
          public void eval(MockDirectoryWrapper dir) throws IOException {
            if (doFail && callStackContainsAnyOf("close")) {
              throw new FakeIOException();
            }
          }
        };

    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    fail.setDoFail();
    expectThrows(
        FakeIOException.class,
        () -> {
          codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);
        });
    fail.clearDoFail();

    dir.close();
  }

  /**
   * Test field infos read that hits exception immediately on open. make sure we get our exception
   * back, no file handle leaks, etc.
   */
  public void testExceptionOnOpenInput() throws Exception {
    Failure fail =
        new Failure() {
          @Override
          public void eval(MockDirectoryWrapper dir) throws IOException {
            if (doFail && callStackContainsAnyOf("openInput")) {
              throw new FakeIOException();
            }
          }
        };

    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);

    fail.setDoFail();
    expectThrows(
        FakeIOException.class,
        () -> {
          codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);
        });
    fail.clearDoFail();

    dir.close();
  }

  /**
   * Test field infos read that hits exception on close. make sure we get our exception back, no
   * file handle leaks, etc.
   */
  public void testExceptionOnCloseInput() throws Exception {
    Failure fail =
        new Failure() {
          @Override
          public void eval(MockDirectoryWrapper dir) throws IOException {
            if (doFail && callStackContainsAnyOf("close")) {
              throw new FakeIOException();
            }
          }
        };

    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(fail);
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");
    FieldInfo fi = createFieldInfo();
    addAttributes(fi);

    FieldInfos infos = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(null, null).add(fi).finish();

    codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);

    fail.setDoFail();
    expectThrows(
        FakeIOException.class,
        () -> {
          codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);
        });
    fail.clearDoFail();

    dir.close();
  }

  // TODO: more tests

  /** Test field infos read/write with random fields, with different values. */
  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    Codec codec = getCodec();
    SegmentInfo segmentInfo = newSegmentInfo(dir, "_123");

    // generate a bunch of fields
    int numFields = atLeast(2000);
    Set<String> fieldNames = new HashSet<>();
    for (int i = 0; i < numFields; i++) {
      fieldNames.add(TestUtil.randomUnicodeString(random()));
    }

    String softDeletesField =
        random().nextBoolean() ? TestUtil.randomUnicodeString(random()) : null;

    String parentField = random().nextBoolean() ? TestUtil.randomUnicodeString(random()) : null;

    if (softDeletesField != null && softDeletesField.equals(parentField)) {
      parentField = null;
    }
    var builder = INDEX_PACKAGE_ACCESS.newFieldInfosBuilder(softDeletesField, parentField);

    for (String field : fieldNames) {
      IndexableFieldType fieldType = randomFieldType(random(), field);
      boolean storeTermVectors = false;
      boolean storePayloads = false;
      boolean omitNorms = false;
      if (fieldType.indexOptions() != IndexOptions.NONE) {
        storeTermVectors = fieldType.storeTermVectors();
        omitNorms = fieldType.omitNorms();
        if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
          storePayloads = random().nextBoolean();
        }
      }
      FieldInfo fi =
          new FieldInfo(
              field,
              -1,
              storeTermVectors,
              omitNorms,
              storePayloads,
              fieldType.indexOptions(),
              fieldType.docValuesType(),
              -1,
              new HashMap<>(),
              fieldType.pointDimensionCount(),
              fieldType.pointIndexDimensionCount(),
              fieldType.pointNumBytes(),
              fieldType.vectorDimension(),
              fieldType.vectorEncoding(),
              fieldType.vectorSimilarityFunction(),
              field.equals(softDeletesField),
              field.equals(parentField));
      addAttributes(fi);
      builder.add(fi);
    }
    FieldInfos infos = builder.finish();
    codec.fieldInfosFormat().write(dir, segmentInfo, "", infos, IOContext.DEFAULT);
    FieldInfos infos2 = codec.fieldInfosFormat().read(dir, segmentInfo, "", IOContext.DEFAULT);
    assertEquals(infos, infos2);
    dir.close();
  }

  private int getVectorsMaxDimensions(String fieldName) {
    return Codec.getDefault().knnVectorsFormat().getMaxDimensions(fieldName);
  }

  private IndexableFieldType randomFieldType(Random r, String fieldName) {
    FieldType type = new FieldType();

    if (r.nextBoolean()) {
      IndexOptions[] values = IndexOptions.values();
      type.setIndexOptions(values[r.nextInt(values.length)]);
      type.setOmitNorms(r.nextBoolean());

      if (r.nextBoolean()) {
        type.setStoreTermVectors(true);
        if (type.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
          type.setStoreTermVectorPositions(r.nextBoolean());
          type.setStoreTermVectorOffsets(r.nextBoolean());
          if (type.storeTermVectorPositions()) {
            type.setStoreTermVectorPayloads(r.nextBoolean());
          }
        }
      }
    }

    if (r.nextBoolean()) {
      DocValuesType values[] = DocValuesType.values();
      type.setDocValuesType(values[r.nextInt(values.length)]);
    }

    if (r.nextBoolean()) {
      int dimension = 1 + r.nextInt(PointValues.MAX_DIMENSIONS);
      int indexDimension = 1 + r.nextInt(Math.min(dimension, PointValues.MAX_INDEX_DIMENSIONS));
      int dimensionNumBytes = 1 + r.nextInt(PointValues.MAX_NUM_BYTES);
      type.setDimensions(dimension, indexDimension, dimensionNumBytes);
    }

    if (r.nextBoolean()) {
      int dimension = 1 + r.nextInt(getVectorsMaxDimensions(fieldName));
      VectorSimilarityFunction similarityFunction =
          RandomPicks.randomFrom(r, VectorSimilarityFunction.values());
      VectorEncoding encoding = RandomPicks.randomFrom(r, VectorEncoding.values());
      type.setVectorAttributes(dimension, encoding, similarityFunction);
    }

    return type;
  }

  /** Hook to add any codec attributes to fieldinfo instances added in this test. */
  protected void addAttributes(FieldInfo fi) {}

  /** equality for entirety of fieldinfos */
  protected void assertEquals(FieldInfos expected, FieldInfos actual) {
    assertEquals(expected.size(), actual.size());
    for (FieldInfo expectedField : expected) {
      FieldInfo actualField = actual.fieldInfo(expectedField.number);
      assertNotNull(actualField);
      assertEquals(expectedField, actualField);
    }
  }

  /** equality for two individual fieldinfo objects */
  protected void assertEquals(FieldInfo expected, FieldInfo actual) {
    assertEquals(expected.number, actual.number);
    assertEquals(expected.name, actual.name);
    assertEquals(expected.getDocValuesType(), actual.getDocValuesType());
    assertEquals(expected.getIndexOptions(), actual.getIndexOptions());
    assertEquals(expected.hasNorms(), actual.hasNorms());
    assertEquals(expected.hasPayloads(), actual.hasPayloads());
    assertEquals(expected.hasVectors(), actual.hasVectors());
    assertEquals(expected.omitsNorms(), actual.omitsNorms());
    assertEquals(expected.getDocValuesGen(), actual.getDocValuesGen());
  }

  /** Returns a new fake segment */
  protected static SegmentInfo newSegmentInfo(Directory dir, String name) {
    Version minVersion = random().nextBoolean() ? null : Version.LATEST;
    return new SegmentInfo(
        dir,
        Version.LATEST,
        minVersion,
        name,
        10000,
        false,
        false,
        Codec.getDefault(),
        Collections.emptyMap(),
        StringHelper.randomId(),
        Collections.emptyMap(),
        null);
  }

  @Override
  protected void addRandomFields(Document doc) {
    doc.add(new StoredField("foobar", TestUtil.randomSimpleString(random())));
  }

  private FieldInfo createFieldInfo() {
    return new FieldInfo(
        "field",
        -1,
        false,
        false,
        false,
        TextField.TYPE_STORED.indexOptions(),
        DocValuesType.NONE,
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
  }
}
