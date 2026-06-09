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
package org.apache.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.hnsw.HnswGraph;

public class TestKnnVectorValuesUpdates extends LuceneTestCase {

  private static final String ID = "id";
  private static final String VEC = "vec";

  // In-place vector updates only support the unquantized Lucene99 HNSW format; pin it so the random
  // test codec doesn't pick a quantized format (which is correctly rejected, see the dedicated
  // test).
  private static Codec unquantizedCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat());
  }

  private static IndexWriterConfig noMergeConfig() {
    return new IndexWriterConfig()
        .setCodec(unquantizedCodec())
        .setMergePolicy(NoMergePolicy.INSTANCE);
  }

  private static IndexWriterConfig mergeConfig() {
    return new IndexWriterConfig().setCodec(unquantizedCodec());
  }

  private static IndexWriterConfig deferredNoMergeConfig() {
    return new IndexWriterConfig()
        .setCodec(unquantizedCodec())
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setDeferVectorGraphRebuild(true);
  }

  /** Returns the HNSW graph size for {@code VEC} in the (single) leaf containing the given doc. */
  private static int graphSizeForDoc(DirectoryReader reader, String idValue) throws Exception {
    for (LeafReaderContext ctx : reader.leaves()) {
      CodecReader cr = (CodecReader) ctx.reader();
      // only consider the leaf that actually holds this doc
      boolean hasDoc = false;
      var sf = cr.storedFields();
      for (int d = 0; d < cr.maxDoc(); d++) {
        if (idValue.equals(sf.document(d).get(ID))) {
          hasDoc = true;
          break;
        }
      }
      if (!hasDoc) {
        continue;
      }
      KnnVectorsReader vr = cr.getVectorReader().unwrapReaderForField(VEC);
      HnswGraph g = ((HnswGraphProvider) vr).getGraph(VEC);
      return g == null ? 0 : g.size();
    }
    return -1;
  }

  private static Document floatDoc(int id, float[] vector) {
    Document doc = new Document();
    doc.add(new StringField(ID, "doc-" + id, Store.YES));
    doc.add(new KnnFloatVectorField(VEC, vector, VectorSimilarityFunction.EUCLIDEAN));
    return doc;
  }

  private static float[] floatVec(float base, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = base + i;
    }
    return v;
  }

  /** Returns the float vector for the doc identified by id-term value, or null if not present. */
  private static float[] readFloatVector(DirectoryReader reader, String idValue, int dim)
      throws Exception {
    for (LeafReaderContext ctx : reader.leaves()) {
      LeafReader leaf = ctx.reader();
      FloatVectorValues values = leaf.getFloatVectorValues(VEC);
      if (values == null) {
        continue;
      }
      KnnVectorValues.DocIndexIterator it = values.iterator();
      for (int doc = it.nextDoc();
          doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
          doc = it.nextDoc()) {
        if (idValue.equals(leaf.storedFields().document(doc).get(ID))) {
          return values.vectorValue(it.index()).clone();
        }
      }
    }
    return null;
  }

  public void testSimpleFloatUpdate() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        for (int i = 0; i < 5; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        float[] newVec = floatVec(100, dim);
        w.updateFloatVectorValue(new Term(ID, "doc-2"), VEC, newVec);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertArrayEquals(floatVec(100, dim), readFloatVector(reader, "doc-2", dim), 0f);
        assertArrayEquals(floatVec(0, dim), readFloatVector(reader, "doc-0", dim), 0f);
        assertArrayEquals(floatVec(4, dim), readFloatVector(reader, "doc-4", dim), 0f);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testFloatUpdateNrt() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
      for (int i = 0; i < 5; i++) {
        w.addDocument(floatDoc(i, floatVec(i, dim)));
      }
      w.commit();
      w.updateFloatVectorValue(new Term(ID, "doc-1"), VEC, floatVec(50, dim));
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertArrayEquals(floatVec(50, dim), readFloatVector(reader, "doc-1", dim), 0f);
        assertArrayEquals(floatVec(3, dim), readFloatVector(reader, "doc-3", dim), 0f);
      }
    }
  }

  public void testMultipleUpdatesSameDocLastWins() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        for (int i = 0; i < 4; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, floatVec(10, dim));
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, floatVec(20, dim));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertArrayEquals(floatVec(20, dim), readFloatVector(reader, "doc-0", dim), 0f);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testKnnSearchReflectsUpdate() throws Exception {
    int dim = 4;
    int n = 30;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        for (int i = 0; i < n; i++) {
          w.addDocument(floatDoc(i, floatVec(i * 10, dim)));
        }
        w.commit();
        // move doc-7 to be exactly the query target
        w.updateFloatVectorValue(new Term(ID, "doc-7"), VEC, floatVec(1000, dim));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(new KnnFloatVectorQuery(VEC, floatVec(1000, dim), 1), 1);
        assertEquals(1, hits.scoreDocs.length);
        String id = searcher.storedFields().document(hits.scoreDocs[0].doc).get(ID);
        assertEquals("doc-7", id);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testByteUpdate() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          doc.add(new StringField(ID, "doc-" + i, Store.YES));
          byte[] v = new byte[dim];
          for (int j = 0; j < dim; j++) {
            v[j] = (byte) (i + j);
          }
          doc.add(new KnnByteVectorField(VEC, v, VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(doc);
        }
        w.commit();
        byte[] newVec = new byte[] {100, 101, 102, 103};
        w.updateByteVectorValue(new Term(ID, "doc-3"), VEC, newVec);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        byte[] got = null;
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leaf = ctx.reader();
          ByteVectorValues values = leaf.getByteVectorValues(VEC);
          if (values == null) {
            continue;
          }
          KnnVectorValues.DocIndexIterator it = values.iterator();
          for (int doc = it.nextDoc();
              doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
              doc = it.nextDoc()) {
            if ("doc-3".equals(leaf.storedFields().document(doc).get(ID))) {
              got = values.vectorValue(it.index()).clone();
            }
          }
        }
        assertArrayEquals(new byte[] {100, 101, 102, 103}, got);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testUpdateThenMerge() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        // build several segments
        for (int i = 0; i < 6; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
          w.commit(); // one segment per doc
        }
        w.updateFloatVectorValue(new Term(ID, "doc-2"), VEC, floatVec(200, dim));
        w.updateFloatVectorValue(new Term(ID, "doc-5"), VEC, floatVec(500, dim));
        w.commit();
      }
      // now force-merge into a single segment
      try (IndexWriter w = new IndexWriter(dir, mergeConfig())) {
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected a single merged segment", 1, reader.leaves().size());
        assertArrayEquals(floatVec(200, dim), readFloatVector(reader, "doc-2", dim), 0f);
        assertArrayEquals(floatVec(500, dim), readFloatVector(reader, "doc-5", dim), 0f);
        assertArrayEquals(floatVec(0, dim), readFloatVector(reader, "doc-0", dim), 0f);
        TestUtil.checkIndex(dir);
      }
      // the merged segment should carry no vector update generation
      SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, infos.size());
      assertEquals(-1L, infos.info(0).getVectorGen());
      assertTrue(infos.info(0).getVectorUpdatesFiles().isEmpty());
    }
  }

  public void testUpdateThenForceMergeSameWriter() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      // Use a merging config so forceMerge actually merges. Updates buffered before forceMerge are
      // written (and fold into the merge) via writeDocValuesUpdatesForMerge; this also exercises
      // the
      // vector update + merge interaction in a single writer.
      try (IndexWriter w = new IndexWriter(dir, mergeConfig())) {
        for (int i = 0; i < 6; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-1"), VEC, floatVec(111, dim));
        w.updateFloatVectorValue(new Term(ID, "doc-4"), VEC, floatVec(444, dim));
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertArrayEquals(floatVec(111, dim), readFloatVector(reader, "doc-1", dim), 0f);
        assertArrayEquals(floatVec(444, dim), readFloatVector(reader, "doc-4", dim), 0f);
        assertArrayEquals(floatVec(3, dim), readFloatVector(reader, "doc-3", dim), 0f);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testUpdateAcrossMultipleSegments() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        // two segments, each containing a doc that matches term "tag:x"
        Document d0 = floatDoc(0, floatVec(0, dim));
        d0.add(new StringField("tag", "x", Store.NO));
        w.addDocument(d0);
        w.commit();
        Document d1 = floatDoc(1, floatVec(1, dim));
        d1.add(new StringField("tag", "x", Store.NO));
        w.addDocument(d1);
        w.commit();
        // update both docs across both segments with one call
        w.updateFloatVectorValue(new Term("tag", "x"), VEC, floatVec(999, dim));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(2, reader.leaves().size());
        assertArrayEquals(floatVec(999, dim), readFloatVector(reader, "doc-0", dim), 0f);
        assertArrayEquals(floatVec(999, dim), readFloatVector(reader, "doc-1", dim), 0f);
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testUpdateNonExistentFieldThrows() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
      w.addDocument(floatDoc(0, floatVec(0, 4)));
      w.commit();
      expectThrows(
          IllegalArgumentException.class,
          () -> w.updateFloatVectorValue(new Term(ID, "doc-0"), "nope", floatVec(1, 4)));
    }
  }

  public void testUpdateWrongDimensionThrows() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
      w.addDocument(floatDoc(0, floatVec(0, 4)));
      w.commit();
      expectThrows(
          IllegalArgumentException.class,
          () -> w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, floatVec(1, 8)));
    }
  }

  /**
   * An update can only replace an existing vector, not add one. Targeting a doc that has no vector
   * for the field must fail (loudly) when the update is applied, rather than silently disappearing.
   */
  public void testUpdateDocWithoutVectorThrows() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      IndexWriter w = new IndexWriter(dir, noMergeConfig());
      try {
        // doc-0 has a vector; doc-1 has the field "vec" absent (only the id field).
        w.addDocument(floatDoc(0, floatVec(0, dim)));
        Document noVec = new Document();
        noVec.add(new StringField(ID, "doc-1", Store.YES));
        w.addDocument(noVec);
        w.commit();

        // Updating doc-1 (no existing vector) must fail when applied at commit time.
        w.updateFloatVectorValue(new Term(ID, "doc-1"), VEC, floatVec(9, dim));
        expectThrows(IllegalArgumentException.class, w::commit);
      } finally {
        // the pending (orphan) update would re-throw on close(); discard it
        w.rollback();
      }
    }
  }

  public void testUpdateQuantizedFieldThrows() throws Exception {
    int dim = 4;
    IndexWriterConfig conf =
        new IndexWriterConfig()
            .setCodec(
                TestUtil.alwaysKnnVectorsFormat(
                    new org.apache.lucene.codecs.lucene104
                        .Lucene104HnswScalarQuantizedVectorsFormat()))
            .setMergePolicy(NoMergePolicy.INSTANCE);
    try (Directory dir = newDirectory()) {
      IndexWriter w = new IndexWriter(dir, conf);
      try {
        for (int i = 0; i < 4; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        // The validation passes (it's a float vector field of the right dim), but writing the gen
        // files rejects the quantized format, so the failure surfaces at commit time.
        w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, floatVec(9, dim));
        expectThrows(UnsupportedOperationException.class, w::commit);
      } finally {
        // the pending update would re-throw on close(); discard it instead
        w.rollback();
      }
    }
  }

  public void testUpdateNonFiniteValueThrows() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
      w.addDocument(floatDoc(0, floatVec(0, dim)));
      w.commit();
      float[] withNaN = floatVec(1, dim);
      withNaN[2] = Float.NaN;
      expectThrows(
          IllegalArgumentException.class,
          () -> w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, withNaN));
      float[] withInf = floatVec(1, dim);
      withInf[0] = Float.POSITIVE_INFINITY;
      expectThrows(
          IllegalArgumentException.class,
          () -> w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, withInf));
    }
  }

  public void testUpdateCosineZeroVectorThrows() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
      Document doc = new Document();
      doc.add(new StringField(ID, "doc-0", Store.YES));
      doc.add(new KnnFloatVectorField(VEC, floatVec(1, dim), VectorSimilarityFunction.COSINE));
      w.addDocument(doc);
      w.commit();
      expectThrows(
          IllegalArgumentException.class,
          () -> w.updateFloatVectorValue(new Term(ID, "doc-0"), VEC, new float[dim]));
    }
  }

  /**
   * The update must snapshot the caller's array; mutating it afterwards must not change the index.
   */
  public void testUpdateValueIsDefensivelyCopied() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, noMergeConfig())) {
        for (int i = 0; i < 4; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        float[] newVec = floatVec(50, dim);
        w.updateFloatVectorValue(new Term(ID, "doc-1"), VEC, newVec);
        // Mutate the caller's array before the update is applied (flush happens on commit below).
        java.util.Arrays.fill(newVec, 999f);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // The indexed value must be the snapshot taken at call time, not the mutated array.
        assertArrayEquals(floatVec(50, dim), readFloatVector(reader, "doc-1", dim), 0f);
        TestUtil.checkIndex(dir);
      }
    }
  }

  // ---- deferred graph rebuild (setDeferVectorGraphRebuild(true)) ----

  public void testDeferredUpdateValuesAndSearchCorrect() throws Exception {
    int dim = 4;
    int n = 64;
    // Always-build-graph format so we can prove the base segment HAS a graph while the deferred
    // gen segment does NOT.
    Codec alwaysGraph = TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(16, 100, 0));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig().setCodec(alwaysGraph).setDeferVectorGraphRebuild(true))) {
        for (int i = 0; i < n; i++) {
          w.addDocument(floatDoc(i, floatVec(i * 10, dim)));
        }
        w.commit();
        // sanity: base segment was built eagerly with a graph
        try (DirectoryReader base = DirectoryReader.open(dir)) {
          assertTrue(graphSizeForDoc(base, "doc-7") > 0);
        }
        w.updateFloatVectorValue(new Term(ID, "doc-7"), VEC, floatVec(1000, dim));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // value is correct
        assertArrayEquals(floatVec(1000, dim), readFloatVector(reader, "doc-7", dim), 0f);
        assertArrayEquals(floatVec(0, dim), readFloatVector(reader, "doc-0", dim), 0f);
        // the gen segment carries NO graph (deferred) => exact-scan fallback
        assertEquals(0, graphSizeForDoc(reader, "doc-7"));
        // ...yet KNN search is still correct (exact scan)
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(new KnnFloatVectorQuery(VEC, floatVec(1000, dim), 1), 1);
        assertEquals(1, hits.scoreDocs.length);
        assertEquals("doc-7", searcher.storedFields().document(hits.scoreDocs[0].doc).get(ID));
        TestUtil.checkIndex(dir);
      }
    }
  }

  public void testDeferredThenMergeRebuildsGraph() throws Exception {
    int dim = 4;
    int n = 64;
    // Pin a format that ALWAYS builds the graph (tinySegmentsThreshold=0), so graph presence
    // reflects the deferred/merge behavior under test rather than the segment-size heuristic.
    Codec alwaysGraph = TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(16, 100, 0));
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig().setCodec(alwaysGraph).setDeferVectorGraphRebuild(true))) {
        for (int i = 0; i < n; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-5"), VEC, floatVec(9999, dim));
        w.commit();
      }
      // after the deferred update, the gen segment has no graph (even though base did)
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(0, graphSizeForDoc(reader, "doc-5"));
      }
      // add a second segment so forceMerge(1) actually merges (rather than being a no-op on a
      // single segment), then merge: the graph is rebuilt by the codec's normal format at merge.
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(alwaysGraph))) {
        for (int i = n; i < n + 16; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertArrayEquals(floatVec(9999, dim), readFloatVector(reader, "doc-5", dim), 0f);
        // merged segment is a fresh base segment: vectorGen == -1 and the graph is back
        assertTrue(
            "merged segment should have a rebuilt graph", graphSizeForDoc(reader, "doc-5") > 0);
        TestUtil.checkIndex(dir);
      }
      SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, infos.size());
      assertEquals(-1L, infos.info(0).getVectorGen());
    }
  }

  public void testDeferredMultipleUpdatesLastWins() throws Exception {
    int dim = 4;
    try (Directory dir = newDirectory()) {
      try (IndexWriter w = new IndexWriter(dir, deferredNoMergeConfig())) {
        for (int i = 0; i < 10; i++) {
          w.addDocument(floatDoc(i, floatVec(i, dim)));
        }
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-3"), VEC, floatVec(30, dim));
        w.commit();
        w.updateFloatVectorValue(new Term(ID, "doc-3"), VEC, floatVec(60, dim));
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertArrayEquals(floatVec(60, dim), readFloatVector(reader, "doc-3", dim), 0f);
        assertEquals(0, graphSizeForDoc(reader, "doc-3"));
        TestUtil.checkIndex(dir);
      }
    }
  }
}
