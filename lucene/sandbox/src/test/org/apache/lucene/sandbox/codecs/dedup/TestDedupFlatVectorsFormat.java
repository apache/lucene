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
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdMappedArrayList;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests that {@link DedupHnswVectorsFormat} stores each distinct vector once. De-duplication is
 * observed through the group view size: the number of distinct vectors physically stored,
 * regardless of how many documents reference them.
 */
public class TestDedupFlatVectorsFormat extends LuceneTestCase {

  private static IndexWriterConfig config() {
    return newIndexWriterConfig()
        .setCodec(TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat()));
  }

  /** Repeated float vectors within a field are stored once but still read back per document. */
  public void testFloatDuplicatesWithinField() throws Exception {
    float[] a = {1, 2, 3, 4};
    float[] b = {5, 6, 7, 8};
    float[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloatVectorField("f", docVectors[ord], EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(2, groupNumVectors(values)); // only two distinct vectors stored
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

  /** Repeated float16 vectors within a field are stored once but still read back per document. */
  public void testFloat16DuplicatesWithinField() throws Exception {
    short[] a = {Float.floatToFloat16(1f), Float.floatToFloat16(2f), Float.floatToFloat16(3f)};
    short[] b = {Float.floatToFloat16(4f), Float.floatToFloat16(5f), Float.floatToFloat16(6f)};
    short[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloat16VectorField("f", docVectors[ord], EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        Float16VectorValues values = leafReader.getFloat16VectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(2, groupNumVectors(values)); // only two distinct vectors stored
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

  /** Repeated byte vectors within a field are stored once but still read back per document. */
  public void testByteDuplicatesWithinField() throws Exception {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {5, 6, 7, 8};
    byte[][] docVectors = {a, b, a, b, a, b}; // 3 copies each of 2 vectors
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnByteVectorField("f", docVectors[ord], EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        ByteVectorValues values = leafReader.getByteVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(2, groupNumVectors(values));
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
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        DedupFlatVectorsReader dedupReader = getDedupReader(leafReader, "f");
        FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo("f");

        long expectedOffHeapSize =
            (docVectors.length * Integer.BYTES) // fieldOrdToGroupOrd mapping
                + (a.length + b.length) * Float.BYTES; // raw vector size

        assertEquals(
            expectedOffHeapSize,
            dedupReader
                .getOffHeapByteSize(fieldInfo)
                .get("vdd") // vector data extension
                .longValue());
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

  /** Duplicates spanning multiple segments collapse to a single copy when merged. */
  public void testDuplicatesAcrossSegmentsDedupOnMerge() throws Exception {
    float[] a = {1, 1, 1, 1};
    float[] b = {2, 2, 2, 2};
    float[][] docVectors = {a, b, a}; // 3 docs across 3 segments, 2 distinct
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int ord = 0; ord < docVectors.length; ord++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("id", ord));
        doc.add(new KnnFloatVectorField("f", docVectors[ord], EUCLIDEAN));
        w.addDocument(doc);
        w.commit(); // one segment per document
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(2, groupNumVectors(values)); // a's duplicate collapsed across segments
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

  /** Test that vectors not referenced are deleted from the group. */
  public void testDeletes() throws Exception {
    float[] a = {1, 1, 1, 1};
    float[] b = {2, 2, 2, 2};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {

      LongArrayList docsWithArrayB = new LongArrayList();
      boolean aIndexed = false, bIndexed = false;
      for (int i = 0; i < 50; i++) { // many documents
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

      w.forceMerge(1); // de-duplicate everything

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(2, groupNumVectors(values)); // the group has both vectors
      }

      Query matchDocsWithArrayB =
          NumericDocValuesField.newSlowSetQuery("id", docsWithArrayB.toArray());
      w.deleteDocuments(matchDocsWithArrayB); // delete all docs with vector b

      w.forceMerge(1); // de-duplicate everything

      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        assertEquals(1, groupNumVectors(values)); // the group now has one vector
      }
    }
  }

  /** Test many duplicates spread across fields, documents, segments. */
  public void testManyDuplicate() throws Exception {
    float[] shared = {1, 2, 3, 4};
    List<String> fields = new ArrayList<>(List.of("a", "b", "c", "d", "e"));
    boolean atLeastOne = false;

    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (int i = 0; i < 50; i++) { // many documents
        Document doc = new Document();

        // randomly pick [0, N) fields to index the same vector
        int numFields = random().nextInt(fields.size());
        Collections.shuffle(fields, random());
        for (int j = 0; j < numFields; j++) {
          doc.add(new KnnFloatVectorField(fields.get(j), shared));
          atLeastOne = true;
        }

        w.addDocument(doc);

        if (random().nextFloat() < 0.2f) { // randomly create segments
          w.commit();
        }
      }

      w.forceMerge(1); // de-duplicate everything

      assumeTrue("At least one vector indexed", atLeastOne);

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
        assertEquals(1, groupNumVectors(values)); // the group has one vector
        assertArrayEquals(shared, values.vectorValue(0), 0f);
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
}
