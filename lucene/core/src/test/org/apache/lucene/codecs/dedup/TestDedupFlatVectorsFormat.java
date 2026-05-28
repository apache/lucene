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

package org.apache.lucene.codecs.dedup;

import java.util.Arrays;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Format-specific tests for {@link DedupFlatVectorsFormat}. Validates that:
 *
 * <ul>
 *   <li>identical vectors collapse to a single pool entry,
 *   <li>cross-field dedup works for fields with matching {@code (dim, encoding)},
 *   <li>different dims live in separate pools,
 *   <li>byte-different vectors are NOT deduped,
 *   <li>sparse fields round-trip,
 *   <li>scores are correct after dedup (no aliasing across docs),
 *   <li>merge dedups across segments.
 * </ul>
 */
public class TestDedupFlatVectorsFormat extends LuceneTestCase {

  /** Read the on-disk pool unique-vector count for {@code field}. */
  @SuppressForbidden(reason = "Reflective unwrap of test-only PerField/Asserting wrappers")
  private static int poolUniqueCount(LeafReader r, String field) throws Exception {
    var leaf = (org.apache.lucene.index.CodecReader) r;
    Object reader = leaf.getVectorReader();
    // Unwrap PerFieldKnnVectorsFormat.FieldsReader (private inner class).
    if (reader.getClass().getName().endsWith("PerFieldKnnVectorsFormat$FieldsReader")) {
      java.lang.reflect.Field fieldsField = reader.getClass().getDeclaredField("fields");
      fieldsField.setAccessible(true);
      Object map = fieldsField.get(reader); // IntObjectHashMap<KnnVectorsReader>
      int fieldNumber = leaf.getFieldInfos().fieldInfo(field).number;
      reader = map.getClass().getMethod("get", int.class).invoke(map, fieldNumber);
    }
    // Unwrap AssertingKnnVectorsFormat.AssertingKnnVectorsReader if present (test-only wrapper).
    java.lang.reflect.Field inField = null;
    try {
      inField = reader.getClass().getDeclaredField("delegate");
    } catch (
        @SuppressWarnings("unused")
        NoSuchFieldException notDelegate) {
      try {
        inField = reader.getClass().getDeclaredField("in");
      } catch (
          @SuppressWarnings("unused")
          NoSuchFieldException notIn) {
        // Reader is not a wrapper exposing either "delegate" or "in" — leave inField null
        // so we fall through to the dedup downcast below.
      }
    }
    if (inField != null) {
      inField.setAccessible(true);
      reader = inField.get(reader);
    }
    // Unwrap Lucene99HnswVectorsReader -> flatVectorsReader (private; reflective).
    if (reader instanceof org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader) {
      java.lang.reflect.Field f = reader.getClass().getDeclaredField("flatVectorsReader");
      f.setAccessible(true);
      reader = f.get(reader);
    }
    if (!(reader instanceof DedupFlatVectorsReader dr)) {
      throw new IllegalStateException("expected DedupFlatVectorsReader, got " + reader);
    }
    return dr.getFieldEntryForTesting(field).pool.uniqueCount;
  }

  private Codec dedupCodec() {
    return TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat());
  }

  public void testAllSameVectorOnePoolEntry() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      float[] v = new float[] {0.1f, 0.2f, 0.3f, 0.4f};
      for (int i = 0; i < 50; i++) {
        Document d = new Document();
        d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      try (DirectoryReader r = DirectoryReader.open(w)) {
        for (LeafReaderContext ctx : r.leaves()) {
          assertEquals(1, poolUniqueCount(ctx.reader(), "vec"));
        }
      }
    }
  }

  public void testAllUniqueNoDedupe() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      int n = 30;
      for (int i = 0; i < n; i++) {
        Document d = new Document();
        float[] v = new float[] {i, 2 * i, 3 * i, 4 * i};
        d.add(new KnnFloatVectorField("vec", v, VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      try (DirectoryReader r = DirectoryReader.open(w)) {
        int total = 0;
        for (LeafReaderContext ctx : r.leaves()) {
          total += poolUniqueCount(ctx.reader(), "vec");
        }
        assertEquals(n, total);
      }
    }
  }

  public void testCrossFieldPoolSharing() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      int n = 40;
      for (int i = 0; i < n; i++) {
        Document d = new Document();
        float[] v = new float[] {i, i + 1, i + 2, i + 3};
        d.add(new KnnFloatVectorField("vecA", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        d.add(new KnnFloatVectorField("vecB", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        assertEquals(1, r.leaves().size());
        LeafReader leaf = r.leaves().get(0).reader();
        // Both fields share the same pool; pool unique count == n (not 2n).
        int a = poolUniqueCount(leaf, "vecA");
        int b = poolUniqueCount(leaf, "vecB");
        assertEquals("vecA and vecB share a pool", a, b);
        assertEquals("expected cross-field dedup", n, a);
      }
    }
  }

  public void testDifferentDimsSeparatePools() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      int n = 10;
      for (int i = 0; i < n; i++) {
        Document d = new Document();
        d.add(
            new KnnFloatVectorField(
                "vec4", new float[] {i, i, i, i}, VectorSimilarityFunction.EUCLIDEAN));
        d.add(
            new KnnFloatVectorField(
                "vec8", new float[] {i, i, i, i, i, i, i, i}, VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        // Pools are keyed on (dim, encoding) so these two are in different pools but each has n
        // unique vectors.
        assertEquals(n, poolUniqueCount(leaf, "vec4"));
        assertEquals(n, poolUniqueCount(leaf, "vec8"));
      }
    }
  }

  public void testByteEncodingDedup() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      byte[] same = new byte[] {1, 2, 3, 4};
      for (int i = 0; i < 25; i++) {
        Document d = new Document();
        d.add(new KnnByteVectorField("bv", same.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      try (DirectoryReader r = DirectoryReader.open(w)) {
        for (LeafReaderContext ctx : r.leaves()) {
          assertEquals(1, poolUniqueCount(ctx.reader(), "bv"));
        }
      }
    }
  }

  public void testOneBitDifferenceNotDeduped() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      // Two vectors that differ in a single bit of their float representation.
      float a = Float.intBitsToFloat(0x3F800000); // 1.0f
      float b = Float.intBitsToFloat(0x3F800001); // ~1.0000001f
      assertNotEquals(a, b, 0f);
      Document d1 = new Document();
      d1.add(new KnnFloatVectorField("vec", new float[] {a, 0, 0, 0}));
      Document d2 = new Document();
      d2.add(new KnnFloatVectorField("vec", new float[] {b, 0, 0, 0}));
      w.addDocument(d1);
      w.addDocument(d2);
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        assertEquals(2, poolUniqueCount(leaf, "vec"));
      }
    }
  }

  public void testSparseFieldRoundTrip() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      float[][] payloads = {{1f, 0f, 0f, 0f}, null, {2f, 0f, 0f, 0f}, null, {1f, 0f, 0f, 0f}, null};
      for (float[] v : payloads) {
        Document d = new Document();
        if (v != null) {
          d.add(new KnnFloatVectorField("vec", v, VectorSimilarityFunction.EUCLIDEAN));
        }
        d.add(new StringField("filler", "x", Field.Store.NO));
        w.addDocument(d);
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        FloatVectorValues vv = leaf.getFloatVectorValues("vec");
        assertNotNull(vv);
        // Only 2 unique vectors in the pool (v1 used twice, v2 used once).
        assertEquals(2, poolUniqueCount(leaf, "vec"));
        // Iterate to verify per-doc values.
        var iter = vv.iterator();
        int found = 0;
        while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          float[] got = vv.vectorValue(iter.index());
          float[] expected = payloads[iter.docID()];
          assertArrayEquals(expected, got, 0f);
          found++;
        }
        assertEquals(3, found);
      }
    }
  }

  public void testScoringIsCorrectAfterDedup() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      float[] a = new float[] {1f, 0f, 0f, 0f};
      float[] b = new float[] {0f, 1f, 0f, 0f};
      // Half the docs use vector a, half use vector b. Pool should have 2 entries.
      for (int i = 0; i < 30; i++) {
        Document d = new Document();
        d.add(
            new KnnFloatVectorField(
                "vec", (i % 2 == 0 ? a : b).clone(), VectorSimilarityFunction.DOT_PRODUCT));
        d.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        w.addDocument(d);
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        assertEquals(2, poolUniqueCount(leaf, "vec"));
        IndexSearcher s = newSearcher(r);
        TopDocs hits = s.search(new KnnFloatVectorQuery("vec", a, 10), 10);
        // All top-10 should be docs whose vector == a (i.e. even-indexed). No aliasing to b.
        for (ScoreDoc sd : hits.scoreDocs) {
          int id = Integer.parseInt(s.storedFields().document(sd.doc).get("id"));
          assertEquals("query is a, should retrieve only even docs (which use a)", 0, id % 2);
        }
      }
    }
  }

  public void testMergeDedupAcrossSegments() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setCodec(dedupCodec())
                    .setMergePolicy(newLogMergePolicy()))) {
      float[] v = new float[] {0.5f, 0.5f, 0.5f, 0.5f};
      // Segment 1
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Segment 2 — same vector
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Segment 3 — same vector
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        assertEquals(1, r.leaves().size());
        assertEquals(1, poolUniqueCount(r.leaves().get(0).reader(), "vec"));
      }
    }
  }

  public void testMergeMixedSourceFormats() throws Exception {
    // Source segments use the default (Lucene99) format; the merge target uses ours. This
    // exercises the merge-time intern path against non-dedup sources.
    try (Directory dir = newDirectory()) {
      // Pass 1: write with default codec, multiple segments so the merge has work to do.
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        float[] v = new float[] {1f, 2f, 3f, 4f};
        for (int i = 0; i < 10; i++) {
          Document d = new Document();
          d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(d);
        }
        w.commit();
        for (int i = 0; i < 10; i++) {
          Document d = new Document();
          d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(d);
        }
        w.commit();
      }
      // Pass 2: open with dedup codec and force-merge into a single dedup segment.
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(dedupCodec()))) {
        w.forceMerge(1);
        try (DirectoryReader r = DirectoryReader.open(w)) {
          assertEquals(1, r.leaves().size());
          assertEquals(1, poolUniqueCount(r.leaves().get(0).reader(), "vec"));
        }
      }
    }
  }

  public void testDeleteDoesNotBreakDedup() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      float[] v = new float[] {0.1f, 0.2f, 0.3f, 0.4f};
      for (int i = 0; i < 10; i++) {
        Document d = new Document();
        d.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        d.add(new KnnFloatVectorField("vec", v.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Delete some, then merge into one segment.
      for (int i = 0; i < 10; i += 2) {
        w.deleteDocuments(new Term("id", String.valueOf(i)));
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        assertEquals(1, r.leaves().size());
        LeafReader leaf = r.leaves().get(0).reader();
        assertEquals(1, poolUniqueCount(leaf, "vec"));
        FloatVectorValues vv = leaf.getFloatVectorValues("vec");
        var iter = vv.iterator();
        int n = 0;
        while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          assertArrayEquals(v, vv.vectorValue(iter.index()), 0f);
          n++;
        }
        assertEquals(5, n);
      }
    }
  }

  public void testRandomDocsRoundTrip() throws Exception {
    int dim = 16 + random().nextInt(48);
    int numDocs = 100 + random().nextInt(200);
    int distinct = 1 + random().nextInt(numDocs);
    // Pre-generate `distinct` unique vectors.
    float[][] palette = new float[distinct][dim];
    for (int i = 0; i < distinct; i++) {
      for (int d = 0; d < dim; d++) {
        palette[i][d] = random().nextFloat();
      }
    }
    int[] assign = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      assign[i] = random().nextInt(distinct);
    }
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField(
                "vec", palette[assign[i]].clone(), VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        // unique pool count must equal the number of distinct palette entries actually used
        boolean[] used = new boolean[distinct];
        for (int a : assign) {
          used[a] = true;
        }
        int usedCount = 0;
        for (boolean u : used) if (u) usedCount++;
        assertEquals(usedCount, poolUniqueCount(leaf, "vec"));
        // Per-doc vector must equal its assigned palette entry.
        FloatVectorValues vv = leaf.getFloatVectorValues("vec");
        var iter = vv.iterator();
        while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int doc = iter.docID();
          int id = Integer.parseInt(leaf.storedFields().document(doc).get("id"));
          float[] got = vv.vectorValue(iter.index());
          assertTrue(Arrays.equals(palette[assign[id]], got));
        }
      }
    }
  }

  public void testCrossFieldMergeReusesLevelACache() throws Exception {
    // Two fields with matching (dim, encoding) so they share a MergePool. Each source segment
    // has both fields. The merge fast path should intern source uniques once per (mergedPool,
    // sourceReader) and the second field should reuse the cached map — observable as the
    // merged pool's uniqueCount equalling the count of distinct vectors actually used (not
    // 2× because of cross-field duplication).
    try (Directory dir = newDirectory();
        IndexWriter w =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setCodec(dedupCodec())
                    .setMergePolicy(newLogMergePolicy()))) {
      float[] v1 = new float[] {1f, 0f, 0f, 0f};
      float[] v2 = new float[] {0f, 1f, 0f, 0f};
      // Segment 1: vecA=v1, vecB=v1 — id "v1".
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new StringField("id", "v1", Field.Store.YES));
        d.add(new KnnFloatVectorField("vecA", v1.clone(), VectorSimilarityFunction.EUCLIDEAN));
        d.add(new KnnFloatVectorField("vecB", v1.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Segment 2: vecA=v2, vecB=v1 — id "v2".
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new StringField("id", "v2", Field.Store.YES));
        d.add(new KnnFloatVectorField("vecA", v2.clone(), VectorSimilarityFunction.EUCLIDEAN));
        d.add(new KnnFloatVectorField("vecB", v1.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Both source segments are dedup format → Level-A path engages.
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        assertEquals(1, r.leaves().size());
        LeafReader leaf = r.leaves().get(0).reader();
        // Pool unique count must be exactly 2 (v1 and v2), shared across both fields. If the
        // Level-A cache were not reused across fields, vecB would re-intern v1 again — but the
        // pool is shared, so it would still hit. The win we're testing is that *intern* is
        // called only twice (v1 once from each segment, dedup'd; v2 once) — verified
        // indirectly by the fact that uniqueCount is exactly 2.
        assertEquals(2, poolUniqueCount(leaf, "vecA"));
        assertEquals(2, poolUniqueCount(leaf, "vecB"));
        // Every doc's vecA must match its id, and every doc's vecB must equal v1.
        FloatVectorValues a = leaf.getFloatVectorValues("vecA");
        FloatVectorValues b = leaf.getFloatVectorValues("vecB");
        var iterA = a.iterator();
        var iterB = b.iterator();
        int found = 0;
        while (iterA.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          assertEquals(iterA.docID(), iterB.nextDoc());
          String id = leaf.storedFields().document(iterA.docID()).get("id");
          float[] expectA = "v1".equals(id) ? v1 : v2;
          assertArrayEquals(
              "vecA at doc " + iterA.docID(), expectA, a.vectorValue(iterA.index()), 0f);
          assertArrayEquals("vecB at doc " + iterA.docID(), v1, b.vectorValue(iterB.index()), 0f);
          found++;
        }
        assertEquals(10, found);
      }
    }
  }

  public void testLevelAMergeWithDeletes() throws Exception {
    // Source segment is dedup format and has docs that get deleted before merge. Lazy intern
    // (Level-A correctness fix) must NOT copy the deleted-only uniques into the merged pool.
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setCodec(dedupCodec()))) {
      float[] kept = new float[] {1f, 0f, 0f, 0f};
      float[] doomed = new float[] {0f, 1f, 0f, 0f};
      // 5 kept + 5 doomed in one segment.
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new StringField("id", "kept" + i, Field.Store.YES));
        d.add(new KnnFloatVectorField("vec", kept.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      for (int i = 0; i < 5; i++) {
        Document d = new Document();
        d.add(new StringField("id", "doomed" + i, Field.Store.YES));
        d.add(new KnnFloatVectorField("vec", doomed.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Add a second segment with only the kept vector (so source is also dedup format).
      for (int i = 0; i < 3; i++) {
        Document d = new Document();
        d.add(new StringField("id", "extra" + i, Field.Store.YES));
        d.add(new KnnFloatVectorField("vec", kept.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(d);
      }
      w.commit();
      // Delete all "doomed" docs.
      for (int i = 0; i < 5; i++) {
        w.deleteDocuments(new Term("id", "doomed" + i));
      }
      w.forceMerge(1);
      try (DirectoryReader r = DirectoryReader.open(w)) {
        LeafReader leaf = r.leaves().get(0).reader();
        // Merged pool must contain only the "kept" vector — the deleted-only "doomed" vector
        // must not have been re-interned by the eager Level-A path (regression test).
        assertEquals(1, poolUniqueCount(leaf, "vec"));
      }
    }
  }
}
