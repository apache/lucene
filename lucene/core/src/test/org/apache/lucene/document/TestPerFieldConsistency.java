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

package org.apache.lucene.document;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLong;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.Random;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestPerFieldConsistency extends LuceneTestCase {

  private static Field randomIndexedField(Random random, String fieldName) {
    FieldType fieldType = new FieldType();
    IndexOptions indexOptions = RandomPicks.randomFrom(random, IndexOptions.values());
    while (indexOptions == IndexOptions.NONE) {
      indexOptions = RandomPicks.randomFrom(random, IndexOptions.values());
    }
    fieldType.setIndexOptions(indexOptions);
    fieldType.setStoreTermVectors(random.nextBoolean());
    if (fieldType.storeTermVectors()) {
      fieldType.setStoreTermVectorPositions(random.nextBoolean());
      if (fieldType.storeTermVectorPositions()) {
        fieldType.setStoreTermVectorPayloads(random.nextBoolean());
        fieldType.setStoreTermVectorOffsets(random.nextBoolean());
      }
    }
    fieldType.setOmitNorms(random.nextBoolean());
    fieldType.setStored(random.nextBoolean());
    fieldType.freeze();

    return new Field(fieldName, "randomValue", fieldType);
  }

  private static Field randomPointField(Random random, String fieldName) {
    switch (random.nextInt(4)) {
      case 0:
        return new LongPoint(fieldName, randomLong());
      case 1:
        return new IntPoint(fieldName, randomInt());
      case 2:
        return new DoublePoint(fieldName, randomDouble());
      default:
        return new FloatPoint(fieldName, randomFloat());
    }
  }

  private static Field randomDocValuesField(Random random, String fieldName) {
    switch (random.nextInt(4)) {
      case 0:
        return new BinaryDocValuesField(fieldName, new BytesRef("randomValue"));
      case 1:
        return new NumericDocValuesField(fieldName, randomLong());
      case 2:
        return new DoubleDocValuesField(fieldName, randomDouble());
      default:
        return new SortedSetDocValuesField(fieldName, new BytesRef("randomValue"));
    }
  }

  private static Field randomKnnVectorField(Random random, String fieldName) {
    VectorSimilarityFunction similarityFunction =
        RandomPicks.randomFrom(random, VectorSimilarityFunction.values());
    float[] values = new float[randomIntBetween(1, 10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = randomFloat();
    }
    return new KnnVectorField(fieldName, values, similarityFunction);
  }

  private static Field[] randomFieldsWithTheSameName(String fieldName) {
    final Field textField = randomIndexedField(random(), fieldName);
    final Field docValuesField = randomDocValuesField(random(), fieldName);
    final Field pointField = randomPointField(random(), fieldName);
    final Field vectorField = randomKnnVectorField(random(), fieldName);
    return new Field[] {textField, docValuesField, pointField, vectorField};
  }

  private static void doTestDocWithMissingSchemaOptionsThrowsError(
      Field[] fields, int missing, IndexWriter writer, String errorMsg) {
    final Document doc = new Document();
    for (int i = 0; i < fields.length; i++) {
      if (i != missing) {
        doc.add(fields[i]);
      }
    }
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
    assertTrue(
        "'" + errorMsg + "' not found in '" + exception.getMessage() + "'",
        exception.getMessage().contains(errorMsg));
  }

  private static void doTestDocWithExtraSchemaOptionsThrowsError(
      Field existing, Field extra, IndexWriter writer, String errorMsg) {
    Document doc = new Document();
    doc.add(existing);
    doc.add(extra);
    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
    assertTrue(
        "'" + errorMsg + "' not found in '" + exception.getMessage() + "'",
        exception.getMessage().contains(errorMsg));
  }

  public void testDocWithMissingSchemaOptionsThrowsError() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)); ) {
      final Field[] fields = randomFieldsWithTheSameName("myfield");
      final Document doc0 = new Document();
      for (Field field : fields) {
        doc0.add(field);
      }
      writer.addDocument(doc0);

      // the same segment: indexing a doc with a missing field throws error
      int numNotIndexedDocs = 0;
      for (int missingFieldIdx = 0; missingFieldIdx < fields.length; missingFieldIdx++) {
        numNotIndexedDocs++;
        doTestDocWithMissingSchemaOptionsThrowsError(
            fields,
            missingFieldIdx,
            writer,
            "Inconsistency of field data structures across documents for field [myfield] of doc ["
                + numNotIndexedDocs
                + "].");
      }
      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(1, reader.leaves().size());
        assertEquals(1, reader.leaves().get(0).reader().numDocs());
        assertEquals(numNotIndexedDocs, reader.leaves().get(0).reader().numDeletedDocs());
      }

      // diff segment, same index: indexing a doc with a missing field throws error
      numNotIndexedDocs = 0;
      for (int missingFieldIdx = 0; missingFieldIdx < fields.length; missingFieldIdx++) {
        numNotIndexedDocs++;
        doTestDocWithMissingSchemaOptionsThrowsError(
            fields, missingFieldIdx, writer, "cannot change field \"myfield\" from ");
      }
      writer.addDocument(doc0); // add document with correct data structures
      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(2, reader.leaves().size());
        assertEquals(1, reader.leaves().get(1).reader().numDocs());
        assertEquals(numNotIndexedDocs, reader.leaves().get(1).reader().numDeletedDocs());
      }
    }
  }

  public void testDocWithExtraSchemaOptionsThrowsError() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)); ) {
      final Field[] fields = randomFieldsWithTheSameName("myfield");
      final Document doc0 = new Document();
      int existingFieldIdx = randomIntBetween(0, fields.length - 1);
      doc0.add(fields[existingFieldIdx]);
      writer.addDocument(doc0);

      // the same segment: indexing a field with extra field indexing options returns error
      int numNotIndexedDocs = 0;
      for (int extraFieldIndex = 0; extraFieldIndex < fields.length; extraFieldIndex++) {
        if (extraFieldIndex == existingFieldIdx) continue;
        numNotIndexedDocs++;
        doTestDocWithExtraSchemaOptionsThrowsError(
            fields[existingFieldIdx],
            fields[extraFieldIndex],
            writer,
            "Inconsistency of field data structures across documents for field [myfield] of doc ["
                + numNotIndexedDocs
                + "].");
      }
      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(1, reader.leaves().size());
        assertEquals(1, reader.leaves().get(0).reader().numDocs());
        assertEquals(numNotIndexedDocs, reader.leaves().get(0).reader().numDeletedDocs());
      }

      // diff segment, same index: indexing a field with extra field indexing options returns error
      numNotIndexedDocs = 0;
      for (int extraFieldIndex = 0; extraFieldIndex < fields.length; extraFieldIndex++) {
        if (extraFieldIndex == existingFieldIdx) continue;
        numNotIndexedDocs++;
        doTestDocWithExtraSchemaOptionsThrowsError(
            fields[existingFieldIdx],
            fields[extraFieldIndex],
            writer,
            "cannot change field \"myfield\" from ");
      }
      writer.addDocument(doc0); // add document with correct data structures
      writer.flush();
      try (IndexReader reader = DirectoryReader.open(writer)) {
        assertEquals(2, reader.leaves().size());
        assertEquals(1, reader.leaves().get(1).reader().numDocs());
        assertEquals(numNotIndexedDocs, reader.leaves().get(1).reader().numDeletedDocs());
      }
    }
  }
}
