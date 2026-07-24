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
package org.apache.lucene.codecs.lucene106.dedup;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.DedupVectorValues;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests that {@link Lucene106DedupHnswVectorsFormat} stores each distinct vector once.
 * De-duplication is observed through the group view size: the number of distinct vectors physically
 * stored, regardless of how many documents reference them.
 */
public class TestDedupFlatVectorsFormat extends LuceneTestCase {

  private IndexWriterConfig config() {
    return newIndexWriterConfig()
        .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene106DedupHnswVectorsFormat()));
  }

  /** Repeated float vectors within a field are stored once but still read back per document. */
  public void testFloatDuplicatesWithinField() throws Exception {
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
        FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("f");
        assertEquals(docVectors.length, values.size()); // one entry per document
        assertEquals(2, groupSize(values)); // only two distinct vectors stored
        for (int ord = 0; ord < values.size(); ord++) {
          assertArrayEquals(docVectors[ord], values.vectorValue(ord), 0f);
        }
      }
    }
  }

  /** Repeated byte vectors within a field are stored once but still read back per document. */
  public void testByteDuplicatesWithinField() throws Exception {
    byte[] a = {1, 2, 3, 4};
    byte[] b = {5, 6, 7, 8};
    byte[][] docVectors = {a, a, b, a, b};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (byte[] vector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnByteVectorField("f", vector, EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        ByteVectorValues values = getOnlyLeafReader(reader).getByteVectorValues("f");
        assertEquals(docVectors.length, values.size());
        assertEquals(2, groupSize(values));
        for (int ord = 0; ord < values.size(); ord++) {
          assertArrayEquals(docVectors[ord], values.vectorValue(ord));
        }
      }
    }
  }

  /** Distinct vectors are all kept, i.e. nothing is collapsed by mistake. */
  public void testDistinctVectorsAllStored() throws Exception {
    float[][] docVectors = {{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}};
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      for (float[] vector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("f");
        assertEquals(3, values.size());
        assertEquals(3, groupSize(values));
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
      doc.add(new KnnFloatVectorField("f2", shared, EUCLIDEAN));
      w.addDocument(doc);
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        FloatVectorValues v1 = leaf.getFloatVectorValues("f1");
        FloatVectorValues v2 = leaf.getFloatVectorValues("f2");
        assertEquals(1, groupSize(v1)); // both fields resolve to the same one-vector group
        assertEquals(1, groupSize(v2));
        assertArrayEquals(shared, v1.vectorValue(0), 0f);
        assertArrayEquals(shared, v2.vectorValue(0), 0f);
      }
    }
  }

  /** Fields differing in dimension use separate groups, even for otherwise similar vectors. */
  public void testDifferentDimensionsUseSeparateGroups() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, config())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f2d", new float[] {1, 1}, EUCLIDEAN));
      doc.add(new KnnFloatVectorField("f3d", new float[] {1, 1, 1}, EUCLIDEAN));
      w.addDocument(doc);
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        assertEquals(1, groupSize(leaf.getFloatVectorValues("f2d")));
        assertEquals(1, groupSize(leaf.getFloatVectorValues("f3d")));
        assertArrayEquals(new float[] {1, 1}, leaf.getFloatVectorValues("f2d").vectorValue(0), 0f);
        assertArrayEquals(
            new float[] {1, 1, 1}, leaf.getFloatVectorValues("f3d").vectorValue(0), 0f);
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
      for (float[] vector : docVectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("f", vector, EUCLIDEAN));
        w.addDocument(doc);
        w.commit(); // one segment per document
      }
      w.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        FloatVectorValues values = getOnlyLeafReader(reader).getFloatVectorValues("f");
        assertEquals(3, values.size());
        assertEquals(2, groupSize(values)); // a's duplicate collapsed across segments
        for (int ord = 0; ord < values.size(); ord++) {
          assertArrayEquals(docVectors[ord], values.vectorValue(ord), 0f);
        }
      }
    }
  }

  /** Number of distinct vectors physically stored for a field's group. */
  private static int groupSize(KnnVectorValues values) {
    return ((DedupVectorValues) values).getGroupView().size();
  }
}
