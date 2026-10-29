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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.VectorColumn;
import org.apache.lucene.document.column.VectorValuesCursor;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdMappedArrayList;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Tests that {@link DedupHnswVectorsFormat} stores each distinct vector of a document once.
 * De-duplication is observed through the group view size: the number of vectors physically stored,
 * regardless of how many fields of a document reference them.
 */
public class TestDedupFlatVectorsFormat extends LuceneTestCase {

  private static IndexWriterConfig config() {
    return newIndexWriterConfig()
        .setCodec(TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat()));
  }

  /** Repeated float vectors within a document are stored once but still read back per field. */
  public void testFloatDuplicatesWithinDocument() throws Exception {
    float[] a = {1, 2, 3, 4};
    float[] b = {5, 6, 7, 8};
    float[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloatVectorField("f", docVectors[ord], EUCLIDEAN));
        doc.add(new KnnFloatVectorField("g", docVectors[ord], EUCLIDEAN)); // same vector as f
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(
            docVectors.length, groupNumVectors(values)); // one per document, shared by f and g
        NumericDocValues docValues = leafReader.getNumericDocValues("id");
        Integer[] expectedOrds = new Integer[docVectors.length];
        Integer[] ordsSeen = new Integer[docVectors.length];
        for (int ord = 0; ord < docVectors.length; ord++) {
          int docId = values.ordToDoc(ord);
          assertTrue("id does not exist for docId=" + docId, docValues.advanceExact(docId));
          int originalOrd = (int) docValues.longValue();
          assertArrayEquals(docVectors[originalOrd], values.vectorValue(ord), 0f);
          expectedOrds[ord] = ord;
          ordsSeen[ord] = originalOrd;
        }
        assertThat("all vectors not seen", ordsSeen, arrayContainingInAnyOrder(expectedOrds));
      }
    }
  }

  /** Repeated float16 vectors within a document are stored once but still read back per field. */
  public void testFloat16DuplicatesWithinDocument() throws Exception {
    short[] a = {Float.floatToFloat16(1f), Float.floatToFloat16(2f), Float.floatToFloat16(3f)};
    short[] b = {Float.floatToFloat16(4f), Float.floatToFloat16(5f), Float.floatToFloat16(6f)};
    short[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloat16VectorField("f", docVectors[ord], EUCLIDEAN));
        doc.add(new KnnFloat16VectorField("g", docVectors[ord], EUCLIDEAN)); // same vector as f
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        Float16VectorValues values = leafReader.getFloat16VectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(
            docVectors.length, groupNumVectors(values)); // one per document, shared by f and g
        NumericDocValues docValues = leafReader.getNumericDocValues("id");
        Integer[] expectedOrds = new Integer[docVectors.length];
        Integer[] ordsSeen = new Integer[docVectors.length];
        for (int ord = 0; ord < docVectors.length; ord++) {
          int docId = values.ordToDoc(ord);
          assertTrue("id does not exist for docId=" + docId, docValues.advanceExact(docId));
          int originalOrd = (int) docValues.longValue();
          assertArrayEquals(docVectors[originalOrd], values.vectorValue(ord));
          expectedOrds[ord] = ord;
          ordsSeen[ord] = originalOrd;
        }
        assertThat("all vectors not seen", ordsSeen, arrayContainingInAnyOrder(expectedOrds));
      }
    }
  }

  /** Repeated byte vectors within a document are stored once but still read back per field. */
  public void testByteDuplicatesWithinDocument() throws Exception {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {5, 6, 7, 8};
    byte[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnByteVectorField("f", docVectors[ord], EUCLIDEAN));
        doc.add(new KnnByteVectorField("g", docVectors[ord], EUCLIDEAN)); // same vector as f
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        ByteVectorValues values = leafReader.getByteVectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(
            docVectors.length, groupNumVectors(values)); // one per document, shared by f and g
        NumericDocValues docValues = leafReader.getNumericDocValues("id");
        Integer[] expectedOrds = new Integer[docVectors.length];
        Integer[] ordsSeen = new Integer[docVectors.length];
        for (int ord = 0; ord < docVectors.length; ord++) {
          int docId = values.ordToDoc(ord);
          assertTrue("id does not exist for docId=" + docId, docValues.advanceExact(docId));
          int originalOrd = (int) docValues.longValue();
          assertArrayEquals(docVectors[originalOrd], values.vectorValue(ord));
          expectedOrds[ord] = ord;
          ordsSeen[ord] = originalOrd;
        }
        assertThat("all vectors not seen", ordsSeen, arrayContainingInAnyOrder(expectedOrds));
      }
    }
  }

  /** Distinct vectors are all kept, i.e. nothing is collapsed by mistake. */
  public void testDistinctVectorsAllStored() throws Exception {
    // Vectors that are close to each other in bit representations.
    float[][] distinctDocVectors = {
      {+0f}, {-0f}, {Math.nextUp(0f)}, {Math.nextDown(0f)}, {1f}, {Math.nextUp(1f)}
    };
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] vector : distinctDocVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("f");
        assertEquals(distinctDocVectors.length, values.size());
        assertEquals(distinctDocVectors.length, groupNumVectors(values));
      }
    }
  }

  /** Check off-heap size of de-duplicated vectors. */
  public void testOffHeapSize() throws Exception {
    float[] a = {1, 2, 3, 4};
    float[] b = {5, 6, 7, 8};
    float[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] vector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, EUCLIDEAN));
        doc.add(new KnnFloatVectorField("g", vector, EUCLIDEAN)); // same vector as f
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        DedupFlatVectorsReader dedupReader = getDedupReader(leafReader, "f");
        FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");

        // fieldOrdToGroupOrd is packed with the minimum bits required for the largest group
        // ordinal. The group stores one vector per document, shared by f and g, so each entry needs
        // bitsRequired(docVectors.length - 1) bits rather than a full 32-bit int.
        int bitsPerValue = DirectWriter.bitsRequired(docVectors.length - 1);
        long expectedOffHeapSize =
            DirectWriter.bytesRequired(
                    docVectors.length, bitsPerValue) // fieldOrdToGroupOrd mapping
                + (long) docVectors.length * a.length * Float.BYTES; // raw vector size

        assertEquals(
            expectedOffHeapSize,
            dedupReader
                .getOffHeapByteSize(fieldInfo)
                .get("vdd") // vector data extension
                .longValue());
      }
    }
  }

  /**
   * Off-heap size grows with the packed bit width, i.e. log2(maxGroupOrd); more stored vectors need
   * more bits per entry than the 6-document case in {@link #testOffHeapSize()}.
   */
  public void testOffHeapSizeScalesWithGroupOrdWidth() throws Exception {
    // 20 documents -> group ords 0..19 -> maxGroupOrd = 19 -> bitsRequired(19) = 8 bits (5 rounded
    // up to a supported width), which is strictly greater than bitsRequired(5) = 4 bits for the
    // 6-document case.
    float[][] distinct = {{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}, {0, 0, 0, 1}, {1, 1, 1, 1}};
    // Reference each distinct vector four times: identical vectors of different documents are not
    // de-duplicated, so the group stores one vector per document.
    float[][] docVectors = new float[distinct.length * 4][];
    for (int i = 0; i < docVectors.length; i++) {
      docVectors[i] = distinct[i % distinct.length];
    }

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] vector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(docVectors.length, groupNumVectors(values)); // one vector per document

        DedupFlatVectorsReader dedupReader = getDedupReader(leafReader, "f");
        FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");

        int maxGroupOrd = docVectors.length - 1; // group ords are 0..(N-1)
        int bitsPerValue = DirectWriter.bitsRequired(maxGroupOrd);

        // The width must have grown relative to the 6-document case, confirming the packed width
        // tracks log2(maxGroupOrd) rather than being fixed.
        assertTrue(
            "bit width should grow with more stored vectors",
            bitsPerValue > DirectWriter.bitsRequired(5));

        long rawVectorBytes = (long) docVectors.length * distinct[0].length * Float.BYTES;
        long expectedOffHeapSize =
            DirectWriter.bytesRequired(
                    docVectors.length, bitsPerValue) // fieldOrdToGroupOrd mapping
                + rawVectorBytes; // raw vector size

        assertEquals(
            expectedOffHeapSize, dedupReader.getOffHeapByteSize(fieldInfo).get("vdd").longValue());
      }
    }
  }

  /** Fields with the same dimension and encoding share one copy of an identical vector. */
  public void testDuplicatesAcrossFieldsShareGroup() throws Exception {
    float[] shared = {9, 8, 7, 6};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f1", shared, EUCLIDEAN));
      doc.add(new KnnFloatVectorField("f2", shared, DOT_PRODUCT)); // different function
      w.addDocument(doc);
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        DedupFlatVectorsReader dedupReader1 = getDedupReader(leaf, "f1");
        DedupFlatVectorsReader dedupReader2 = getDedupReader(leaf, "f2");
        assertEquals(dedupReader1, dedupReader2); // de-duplication happened correctly

        assertEquals( // both fields DO resolve to the same group
            dedupReader1.getEntry("f1", FLOAT32).groupInfo(),
            dedupReader2.getEntry("f2", FLOAT32).groupInfo());

        FloatVectorValues v1 = leaf.getFloatVectorValues("f1");
        assertEquals(1, groupNumVectors(v1)); // the group has one vector
        assertArrayEquals(shared, v1.vectorValue(0), 0f);

        FloatVectorValues v2 = leaf.getFloatVectorValues("f2");
        assertEquals(1, groupNumVectors(v2)); // the group has one vector
        assertArrayEquals(shared, v2.vectorValue(0), 0f);
      }
    }
  }

  /** Fields differing in dimension use separate groups, even for otherwise similar vectors. */
  public void testDifferentDimensionsUseSeparateGroups() throws Exception {
    float[] vector1 = {1, 1};
    float[] vector2 = {1, 1, 0};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f2d", vector1, EUCLIDEAN));
      doc.add(new KnnFloatVectorField("f3d", vector2, EUCLIDEAN));
      w.addDocument(doc);
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        DedupFlatVectorsReader dedupReader1 = getDedupReader(leaf, "f2d");
        DedupFlatVectorsReader dedupReader2 = getDedupReader(leaf, "f3d");
        assertEquals(dedupReader1, dedupReader2); // de-duplication happened correctly

        assertNotEquals( // both fields DO NOT resolve to the same group
            dedupReader1.getEntry("f2d", FLOAT32).groupInfo(),
            dedupReader2.getEntry("f3d", FLOAT32).groupInfo());

        FloatVectorValues v1 = leaf.getFloatVectorValues("f2d");
        assertEquals(1, groupNumVectors(v1)); // the group has one vector
        assertArrayEquals(vector1, v1.vectorValue(0), 0f);

        FloatVectorValues v2 = leaf.getFloatVectorValues("f3d");
        assertEquals(1, groupNumVectors(v2)); // the group has one vector
        assertArrayEquals(vector2, v2.vectorValue(0), 0f);
      }
    }
  }

  /** Merging de-duplicates each document's vectors, here from segments of another format. */
  public void testDuplicatesWithinDocumentDedupOnMerge() throws Exception {
    float[] a = {1, 1, 1, 1};
    float[] b = {2, 2, 2, 2};
    float[][] docVectors = {a, b, a}; // 3 docs across 3 segments
    IndexWriterConfig otherConfig =
        newIndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, otherConfig)) {
        for (int ord = 0; ord < docVectors.length; ord++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("id", ord));
          doc.add(new KnnFloatVectorField("f", docVectors[ord], EUCLIDEAN));
          doc.add(new KnnFloatVectorField("g", docVectors[ord], EUCLIDEAN)); // same vector as f
          w.addDocument(doc);
          w.commit(); // one segment per document
        }
      }
      try (IndexWriter w = new IndexWriter(dir, config())) {
        w.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          LeafReader leafReader = getOnlyLeafReader(reader);
          FloatVectorValues values = leafReader.getFloatVectorValues("f");
          assertEquals(docVectors.length, values.size());
          assertEquals(
              docVectors.length, groupNumVectors(values)); // one per doc, shared by f and g
          NumericDocValues docValues = leafReader.getNumericDocValues("id");
          Integer[] expectedOrds = new Integer[docVectors.length];
          Integer[] ordsSeen = new Integer[docVectors.length];
          for (int ord = 0; ord < docVectors.length; ord++) {
            int docId = values.ordToDoc(ord);
            assertTrue("id does not exist for docId=" + docId, docValues.advanceExact(docId));
            int originalOrd = (int) docValues.longValue();
            assertArrayEquals(docVectors[originalOrd], values.vectorValue(ord), 0f);
            expectedOrds[ord] = ord;
            ordsSeen[ord] = originalOrd;
          }
          assertThat("all vectors not seen", ordsSeen, arrayContainingInAnyOrder(expectedOrds));
        }
      }
    }
  }

  /** Test that vectors not referenced are deleted from the group. */
  public void testDeletes() throws Exception {
    float[] a = {1, 1, 1, 1};
    float[] b = {2, 2, 2, 2};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {

      LongArrayList docsWithArrayB = new LongArrayList();
      boolean aIndexed = false, bIndexed = false;
      int numDocs = 50;
      for (int i = 0; i < numDocs; i++) { // many documents
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", i));
        if (random().nextBoolean()) { // index either a or b
          doc.add(new KnnFloatVectorField("f", a));
          aIndexed = true;
        } else {
          doc.add(new KnnFloatVectorField("f", b));
          docsWithArrayB.add(i);
          bIndexed = true;
        }
        w.addDocument(doc);
      }

      assumeTrue("Both vectors a and b indexed", aIndexed && bIndexed);

      w.forceMerge(1); // one segment

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(numDocs, groupNumVectors(values)); // one vector per document
      }

      Query matchDocsWithArrayB =
          NumericDocValuesField.newSlowSetQuery("id", docsWithArrayB.toArray());
      w.deleteDocuments(matchDocsWithArrayB); // delete all docs with vector b

      w.forceMerge(1); // drop the deleted documents

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(numDocs - docsWithArrayB.size(), groupNumVectors(values)); // their vectors too
        for (int ord = 0; ord < values.size(); ord++) {
          assertArrayEquals(a, values.vectorValue(ord), 0f);
        }
      }
    }
  }

  /** Test many duplicates spread across fields, documents, segments. */
  public void testManyDuplicate() throws Exception {
    float[] shared = {1, 2, 3, 4};
    List<String> fields = new ArrayList<>(List.of("a", "b", "c", "d", "e"));
    int numDocsWithVector = 0;

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int i = 0; i < 50; i++) { // many documents
        Document doc = new Document();

        // randomly pick [0, N) fields to index the same vector
        int numFields = random().nextInt(fields.size());
        Collections.shuffle(fields, random());
        for (int j = 0; j < numFields; j++) {
          doc.add(new KnnFloatVectorField(fields.get(j), shared));
        }
        if (numFields > 0) {
          numDocsWithVector++;
        }

        w.addDocument(doc);

        if (random().nextFloat() < 0.2f) { // randomly create segments
          w.commit();
        }
      }

      w.forceMerge(1); // de-duplicate everything

      assumeTrue("At least one vector indexed", numDocsWithVector > 0);

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);

        DedupFlatVectorsReader dedupReader = getDedupReader(leaf, "a");
        for (String field : fields) { // all fields DO resolve to the same group
          DedupFlatVectorsReader other = getDedupReader(leaf, field);
          assertEquals(dedupReader, other); // de-duplication happened correctly
          assertEquals( // both fields DO resolve to the same group
              dedupReader.getEntry("a", FLOAT32).groupInfo(),
              other.getEntry(field, FLOAT32).groupInfo());
        }

        FloatVectorValues values = leaf.getFloatVectorValues("a");
        assertEquals(
            numDocsWithVector, groupNumVectors(values)); // one per doc, shared by its fields
        assertArrayEquals(shared, values.vectorValue(0), 0f);
      }
    }
  }

  /** With an index sort, a group's vectors follow the sorted doc order on flush and on merge. */
  public void testIndexSort() throws Exception {
    int numDocs = atLeast(20);
    float[][] fVectors = new float[numDocs][];
    float[][] gVectors = new float[numDocs][];
    int numVectors = 0;
    for (int id = 0; id < numDocs; id++) {
      fVectors[id] = randomVector(4);
      gVectors[id] = id % 3 == 0 ? randomVector(4) : fVectors[id].clone(); // mostly the same as f
      numVectors += id % 3 == 0 ? 2 : 1;
    }
    IndexWriterConfig config =
        config().setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG, true)));
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config)) {
      for (int id = 0; id < numDocs; id++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", id));
        doc.add(new NumericDocValuesField("sort", id));
        doc.add(new KnnFloatVectorField("f", fVectors[id], EUCLIDEAN));
        doc.add(new KnnFloatVectorField("g", gVectors[id], EUCLIDEAN));
        w.addDocument(doc);
        if (id == numDocs / 2) {
          w.commit();
        }
      }
      for (boolean merged : new boolean[] {false, true}) {
        if (merged) {
          w.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (LeafReaderContext context : reader.leaves()) {
            LeafReader leaf = context.reader();
            for (String field : new String[] {"f", "g"}) {
              NumericDocValues ids = leaf.getNumericDocValues("id");
              FloatVectorValues values = leaf.getFloatVectorValues(field);
              assertGroupInDocOrder(values);
              KnnVectorValues.DocIndexIterator it = values.iterator();
              for (int doc = it.nextDoc(); doc != NO_MORE_DOCS; doc = it.nextDoc()) {
                assertTrue(ids.advanceExact(doc));
                float[][] expected = field.equals("f") ? fVectors : gVectors;
                assertArrayEquals(
                    expected[(int) ids.longValue()], values.vectorValue(it.index()), 0f);
              }
            }
          }
          if (merged) {
            assertEquals(
                numVectors, groupNumVectors(getOnlyLeafReader(reader).getFloatVectorValues("f")));
          }
        }
      }
    }
  }

  /**
   * {@link IndexWriter#addBatch} adds a column at a time rather than a document at a time, so the
   * flush stores the fields' vectors separately, in doc order; a merge de-duplicates them.
   */
  public void testAddBatch() throws Exception {
    int numDocs = atLeast(10);
    float[][] fVectors = new float[numDocs][];
    float[][] gVectors = new float[numDocs][];
    for (int doc = 0; doc < numDocs; doc++) {
      fVectors[doc] = randomVector(4);
      gVectors[doc] =
          doc % 3 == 0 ? randomVector(4) : fVectors[doc].clone(); // mostly the same as f
      fVectors[doc][0] = gVectors[doc][0] = doc; // identifies the document, as merges may reorder
    }
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      w.addBatch(
          batch(
              numDocs,
              new DenseFloatVectorColumn("f", fVectors),
              new DenseFloatVectorColumn("g", gVectors)));
      w.commit();
      w.addBatch(
          batch(
              numDocs,
              new DenseFloatVectorColumn("f", fVectors),
              new DenseFloatVectorColumn("g", gVectors)));
      for (boolean merged : new boolean[] {false, true}) {
        if (merged) {
          w.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          for (LeafReaderContext context : reader.leaves()) {
            for (String field : new String[] {"f", "g"}) {
              FloatVectorValues values = context.reader().getFloatVectorValues(field);
              assertGroupInDocOrder(values);
              float[][] expected = field.equals("f") ? fVectors : gVectors;
              for (int ord = 0; ord < values.size(); ord++) {
                float[] vector = values.vectorValue(ord);
                assertArrayEquals(expected[(int) vector[0]], vector, 0f);
              }
            }
          }
          int numVectors = 0;
          for (LeafReaderContext context : reader.leaves()) {
            numVectors += groupNumVectors(context.reader().getFloatVectorValues("f"));
          }
          int numDistinct =
              numDocs + (numDocs + 2) / 3; // per batch: g differs from f in every 3rd doc
          assertEquals(merged ? 2 * numDistinct : 2 * 2 * numDocs, numVectors);
        }
      }
    }
  }

  /**
   * A merge reads the vector data, in doc order, through inputs opened again with sequential advice
   * when the reader's own inputs read at random, and does not open anything otherwise.
   */
  public void testMergeOpensTheVectorsAgain() throws Exception {
    for (KnnVectorsFormat format :
        List.of(new DedupHnswVectorsFormat(), new DedupHnswScalarQuantizedVectorsFormat())) {
      for (boolean advises : new boolean[] {true, false}) {
        List<Open> opens = new CopyOnWriteArrayList<>();
        List<Open> merges;
        // two segments merged into one, read through their own files rather than a compound file
        IndexWriterConfig config =
            new IndexWriterConfig()
                .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
                .setUseCompoundFile(false);
        try (Directory dir = new RecordingDirectory(newDirectory(), opens, advises);
            IndexWriter w = new IndexWriter(dir, config)) {
          for (int segment = 0; segment < 2; segment++) {
            for (int i = 0; i < 4; i++) {
              Document doc = new Document();
              doc.add(new KnnFloatVectorField("f", randomVector(4), DOT_PRODUCT));
              w.addDocument(doc);
            }
            w.commit();
          }
          // open the segments first, so the merge reuses readers that read at random
          try (DirectoryReader reader = DirectoryReader.open(w)) {
            assertEquals(2, reader.leaves().size());
            opens.clear();
            w.forceMerge(1);
            merges = opens.stream().filter(Open::isSequentialVectorData).toList();
            // the vectors of both segments, plus their quantized copies
            int expected = advises ? (format instanceof DedupHnswVectorsFormat ? 2 : 4) : 0;
            assertEquals(opens.toString(), expected, merges.size());
            for (Open open : merges) {
              assertEquals(open.toString(), IOContext.Context.MERGE, open.context.context());
              assertTrue(open.toString(), open.read);
            }
          }
        }
        for (Open open : merges) {
          assertTrue("closed with the reader: " + open, open.closed);
        }
      }
    }
  }

  /** Tests specific to on-heap versions of the field ord -> group ord mapping. */
  public void testOnHeapFieldOrdToGroupOrd() {
    FieldOrdToGroupOrd arrayBacked = new FieldOrdToGroupOrdArrayList(new IntArrayList());
    expectThrows(UnsupportedOperationException.class, arrayBacked::copy); // not meant for copying.

    FieldOrdToGroupOrd mappedArrayBacked =
        new FieldOrdToGroupOrdMappedArrayList(new int[0], new IntArrayList());
    expectThrows(
        UnsupportedOperationException.class, mappedArrayBacked::copy); // not meant for copying.
  }

  /** Number of distinct vectors physically stored for a field's group. */
  private static int groupNumVectors(KnnVectorValues values) {
    assertThat(values, instanceOf(DedupVectorValues.class));
    return ((DedupVectorValues) values).getGroupView().size();
  }

  /** The group's vectors are in doc order, so a field's group ordinals grow with its ordinals. */
  private static void assertGroupInDocOrder(KnnVectorValues values) {
    FieldOrdToGroupOrd fieldOrdToGroupOrd = ((DedupVectorValues) values).getFieldOrdToGroupOrd();
    for (int ord = 1; ord < values.size(); ord++) {
      assertTrue(fieldOrdToGroupOrd.get(ord - 1) < fieldOrdToGroupOrd.get(ord));
    }
  }

  private static float[] randomVector(int dimension) {
    float[] vector = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      vector[i] = random().nextFloat();
    }
    return vector;
  }

  private static ColumnBatch batch(int numDocs, Column... columns) {
    return new ColumnBatch() {
      @Override
      public int numDocs() {
        return numDocs;
      }

      @Override
      public Iterable<Column> columns() {
        return List.of(columns);
      }
    };
  }

  /** A dense column over the given vectors, one per document. */
  private static final class DenseFloatVectorColumn extends VectorColumn<float[]> {
    private final float[][] vectors;

    DenseFloatVectorColumn(String name, float[][] vectors) {
      super(
          name,
          KnnFloatVectorField.createFieldType(vectors[0].length, EUCLIDEAN),
          Column.Density.DENSE);
      this.vectors = vectors;
    }

    @Override
    public ObjectTupleCursor<float[]> tuples() {
      return new ObjectTupleCursor<>() {
        private int doc = -1;

        @Override
        public int nextDoc() {
          return ++doc < vectors.length ? doc : NO_MORE_DOCS;
        }

        @Override
        public float[] value() {
          return vectors[doc];
        }
      };
    }

    @Override
    public VectorValuesCursor<float[]> values() {
      return new VectorValuesCursor<>(vectors.length, vectors[0].length) {
        private int doc;

        @Override
        public float[] next() {
          return vectors[doc++];
        }
      };
    }
  }

  /** Get underlying dedup vector reader instance. */
  private static DedupFlatVectorsReader getDedupReader(LeafReader leafReader, String fieldName) {
    assertThat(leafReader, instanceOf(CodecReader.class));
    KnnVectorsReader knnVectorsReader = ((CodecReader) leafReader).getVectorReader();
    knnVectorsReader = knnVectorsReader.unwrapReaderForField(fieldName);

    assertThat(knnVectorsReader, instanceOf(Lucene99HnswVectorsReader.class));
    FlatVectorsReader flatReader =
        ((Lucene99HnswVectorsReader) knnVectorsReader).getFlatVectorsReader();

    assertThat(flatReader, instanceOf(DedupFlatVectorsReader.class));
    return (DedupFlatVectorsReader) flatReader;
  }

  /** One {@link Directory#openInput} call. */
  private static final class Open {
    final String name;
    final IOContext context;
    volatile boolean closed;
    volatile boolean read; // whether the input was sliced, which is how vectors are read

    Open(String name, IOContext context) {
      this.name = name;
      this.context = context;
    }

    /** Whether this opened vector data (raw or quantized) for sequential reads, not read once. */
    boolean isSequentialVectorData() {
      return (name.endsWith(".vdd") || name.endsWith(".vdqd"))
          && context.hints().contains(DataAccessHint.SEQUENTIAL)
          && context.hints().contains(ReadOnceHint.INSTANCE) == false;
    }

    @Override
    public String toString() {
      return name + " " + context.context() + context.hints();
    }
  }

  /**
   * Records opens. Its inputs are efficient for a context like memory-mapped inputs using {@link
   * MMapDirectory#ADVISE_BY_CONTEXT}, or for every context if {@code advises} is false.
   */
  private static final class RecordingDirectory extends FilterDirectory {
    private final List<Open> opens;
    private final boolean advises;

    RecordingDirectory(Directory in, List<Open> opens, boolean advises) {
      super(in);
      this.opens = opens;
      this.advises = advises;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = new Open(name, context);
      opens.add(open);
      return new RecordingIndexInput(super.openInput(name, context), open, advises);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final Open open;
    private final boolean advises;

    RecordingIndexInput(IndexInput in, Open open, boolean advises) {
      super("Recording(" + open.name + ")", in);
      this.open = open;
      this.advises = advises;
    }

    @Override
    public boolean isEfficientFor(IOContext other) {
      if (advises == false) {
        return true;
      }
      ReadAdvice advice = MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, open.context).get();
      ReadAdvice otherAdvice = MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, other).get();
      return advice == ReadAdvice.NORMAL
          || otherAdvice == ReadAdvice.NORMAL
          || advice == otherAdvice;
    }

    @Override
    public void close() throws IOException {
      open.closed = true;
      in.close();
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(in.clone(), open, advises);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      open.read = true;
      return new RecordingIndexInput(in.slice(sliceDescription, offset, length), open, advises);
    }
  }
}
