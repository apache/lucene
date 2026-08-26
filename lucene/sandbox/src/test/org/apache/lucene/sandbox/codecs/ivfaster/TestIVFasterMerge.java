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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests the donor-seeded merge.
 *
 * <p>On merge, the largest incoming segment donates its centroids; its documents keep their cells
 * and their codes are copied byte-for-byte, while the smaller segments' documents are routed into
 * that clustering. Two properties need proving, and neither is visible to a plain recall test:
 *
 * <ol>
 *   <li><b>The copies really are byte-identical.</b> A document's code is a deterministic function
 *       of its rotated vector, so a merge that re-encoded it would produce almost the same bytes --
 *       close enough that recall would not move, which is exactly why this needs a direct
 *       assertion.
 *   <li><b>Carried documents still get corrected.</b> Donor documents skip routing, so if the
 *       Reaper did not pick them up they would be pinned to cells the refined centroids have moved
 *       away from. That degrades slowly with each merge rather than failing, so it is tested by
 *       merging repeatedly and requiring recall to hold.
 * </ol>
 */
public class TestIVFasterMerge extends LuceneTestCase {

  private static final String FIELD = "vector";

  private static Codec codec(int nlist, int nprobe) {
    return TestUtil.alwaysKnnVectorsFormat(new IVFasterVectorsFormat(nlist, nprobe));
  }

  /**
   * A document's reconstructed vector must be IDENTICAL before and after a merge.
   *
   * <p>Reconstruction is a pure function of the stored code, so bit-identical reconstruction is
   * equivalent to a bit-identical code, which is the verbatim-copy claim. Only the donor's
   * documents are asserted, since the others are legitimately re-encoded.
   */
  public void testDonorCodesAreCopiedVerbatim() throws Exception {
    final int dim = 32;
    final int donorCount = 400;
    final int smallCount = 40;
    final int nlist = 8;
    final float[][] vectors = clusteredCorpus(donorCount + smallCount, 6, dim);

    try (Directory dir = newDirectory()) {
      final IndexWriterConfig cfg =
          new IndexWriterConfig()
              .setCodec(codec(nlist, nlist))
              .setMaxBufferedDocs(Integer.MAX_VALUE)
              .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);

      // Two segments: a large one (the donor) and a small one.
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        for (int i = 0; i < donorCount; i++) {
          w.addDocument(doc(i, vectors[i], dim));
        }
        w.commit();
        for (int i = donorCount; i < vectors.length; i++) {
          w.addDocument(doc(i, vectors[i], dim));
        }
        w.commit();
      }

      // Snapshot the donor segment's reconstructions, by document id.
      final Map<Integer, float[]> before = new HashMap<>();
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected two segments before merging", 2, reader.leaves().size());
        // The donor is the larger leaf.
        var donorLeaf = reader.leaves().get(0);
        for (var leaf : reader.leaves()) {
          if (leaf.reader().numDocs() > donorLeaf.reader().numDocs()) {
            donorLeaf = leaf;
          }
        }
        final var values = donorLeaf.reader().getFloatVectorValues(FIELD);
        final var it = values.iterator();
        for (int d = it.nextDoc();
            d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
            d = it.nextDoc()) {
          final int id = Integer.parseInt(donorLeaf.reader().storedFields().document(d).get("id"));
          before.put(id, values.vectorValue(it.index()).clone());
        }
      }
      assertEquals("snapshot should cover the donor segment", donorCount, before.size());

      // Merge, then compare.
      try (IndexWriter w =
          new IndexWriter(dir, new IndexWriterConfig().setCodec(codec(nlist, nlist)))) {
        w.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        final var leaf = reader.leaves().get(0);
        final var values = leaf.reader().getFloatVectorValues(FIELD);
        final var it = values.iterator();
        int compared = 0;
        for (int d = it.nextDoc();
            d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
            d = it.nextDoc()) {
          final int id = Integer.parseInt(leaf.reader().storedFields().document(d).get("id"));
          final float[] was = before.get(id);
          if (was == null) {
            continue; // from the smaller segment; legitimately re-encoded
          }
          final float[] now = values.vectorValue(it.index());
          assertArrayEquals(
              "document "
                  + id
                  + " came from the donor, so its code must have been copied verbatim, not"
                  + " re-encoded",
              was,
              now,
              0f);
          compared++;
        }
        assertEquals("every donor document should have been compared", donorCount, compared);
      }
    }
  }

  /**
   * Recall must not decay across repeated merges.
   *
   * <p>This is what catches carried documents that the Reaper failed to correct: they stay in cells
   * the centroids have drifted away from, and each merge compounds it. A single merge would not
   * show it.
   */
  public void testRecallHoldsAcrossManyMerges() throws Exception {
    final int dim = 32;
    final int perBatch = 150;
    final int batches = 8;
    final int nlist = 10;
    final float[][] all = clusteredCorpus(perBatch * batches, 8, dim);

    try (Directory dir = newDirectory()) {
      for (int b = 0; b < batches; b++) {
        final IndexWriterConfig cfg =
            new IndexWriterConfig()
                .setCodec(codec(nlist, nlist))
                .setMaxBufferedDocs(Integer.MAX_VALUE);
        try (IndexWriter w = new IndexWriter(dir, cfg)) {
          for (int i = 0; i < perBatch; i++) {
            final int id = b * perBatch + i;
            w.addDocument(doc(id, all[id], dim));
          }
          w.commit();
          w.forceMerge(1);
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertEquals(perBatch * batches, reader.numDocs());
        final IndexSearcher searcher = new IndexSearcher(reader);
        int found = 0;
        final int trials = 60;
        for (int t = 0; t < trials; t++) {
          final int probe = random().nextInt(all.length);
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, all[probe], 10), 10);
          for (var sd : td.scoreDocs) {
            if (Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")) == probe) {
              found++;
              break;
            }
          }
        }
        assertTrue(
            "after "
                + batches
                + " merge rounds a document should still be found by its own"
                + " vector: "
                + found
                + "/"
                + trials,
            found >= (int) (0.8 * trials));
      }
    }
  }

  /**
   * MERGE MUST PLACE DOCUMENTS IN THE RIGHT CELLS, tested at a NARROW nprobe.
   *
   * <p>Every other test in this class builds with {@code nprobe == nlist}, which probes every cell
   * so a document can be in an arbitrarily wrong cell and still be found, and no assertion here
   * moves. That blind spot is not hypothetical: it let a deliberately broken merge (donor vectors
   * rotated twice, so clustered in a space nothing else lives in) pass the entire suite. A vector
   * rotated twice is still a perfectly valid vector, so nothing throws; it simply clusters wrongly.
   *
   * <p>At {@code nprobe = 2} of 24 cells, finding a document by its own vector requires its cell to
   * be among the two nearest to it, which is the property a placement bug destroys.
   */
  public void testMergedPlacementSurvivesNarrowProbe() throws Exception {
    final int dim = 64;
    final int nlist = 24;
    final int nprobe = 2;
    final int donorCount = 900;
    final int smallCount = 150;
    final float[][] all = clusteredCorpus(donorCount + smallCount, 12, dim);

    try (Directory dir = newDirectory()) {
      // Two segments, then one merge: the donor's documents take the copy path.
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(codec(nlist, nprobe))
                  .setMaxBufferedDocs(Integer.MAX_VALUE)
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < donorCount; i++) {
          w.addDocument(doc(i, all[i], dim));
        }
        w.commit();
        for (int i = donorCount; i < all.length; i++) {
          w.addDocument(doc(i, all[i], dim));
        }
        w.commit();
      }
      try (IndexWriter w =
          new IndexWriter(dir, new IndexWriterConfig().setCodec(codec(nlist, nprobe)))) {
        w.forceMerge(1);
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        final IndexSearcher searcher = new IndexSearcher(reader);
        // Probe DONOR documents specifically: they take the copy-and-carry path.
        int found = 0;
        final int trials = 120;
        for (int t = 0; t < trials; t++) {
          final int probe = random().nextInt(donorCount);
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, all[probe], 10), 10);
          for (var sd : td.scoreDocs) {
            if (Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")) == probe) {
              found++;
              break;
            }
          }
        }
        // A correct build finds nearly all of these, and a placement bug collapses the rate.
        assertTrue(
            "at nprobe="
                + nprobe
                + " of "
                + nlist
                + " cells, merged donor documents must still be"
                + " found by their own vectors: "
                + found
                + "/"
                + trials
                + "; a shortfall means merge placed them in the wrong cells",
            found >= (int) (0.80 * trials));
      }
    }
  }

  /** Deletions must be dropped on merge, and surviving documents must stay findable. */
  public void testDeletionsAreDropped() throws Exception {
    final int dim = 32;
    final int count = 300;
    final int nlist = 8;
    final float[][] vectors = clusteredCorpus(count, 6, dim);

    try (Directory dir = newDirectory()) {
      final IndexWriterConfig cfg =
          new IndexWriterConfig()
              .setCodec(codec(nlist, nlist))
              .setMaxBufferedDocs(Integer.MAX_VALUE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        for (int i = 0; i < count; i++) {
          final Document d = doc(i, vectors[i], dim);
          d.add(
              new org.apache.lucene.document.StringField(
                  "key", Integer.toString(i), org.apache.lucene.document.Field.Store.NO));
          w.addDocument(d);
        }
        w.commit();
        // Delete every third document, then merge.
        final Set<Integer> deleted = new HashSet<>();
        for (int i = 0; i < count; i += 3) {
          w.deleteDocuments(new Term("key", Integer.toString(i)));
          deleted.add(i);
        }
        w.commit();
        w.forceMerge(1);

        try (IndexReader reader = DirectoryReader.open(dir)) {
          assertEquals("deleted documents must be gone", count - deleted.size(), reader.numDocs());
          final IndexSearcher searcher = new IndexSearcher(reader);
          for (var sd :
              searcher.search(new KnnFloatVectorQuery(FIELD, vectors[1], 20), 20).scoreDocs) {
            final int id = Integer.parseInt(searcher.storedFields().document(sd.doc).get("id"));
            assertFalse("a deleted document was returned: " + id, deleted.contains(id));
          }
        }
      }
    }
  }

  /** Merging segments written with different nlist must still work. */
  public void testMergeWithMismatchedNlist() throws Exception {
    final int dim = 32;
    final float[][] vectors = clusteredCorpus(400, 6, dim);
    try (Directory dir = newDirectory()) {
      // Unequal nlist across segments, so the donor gate's accept and reject sides both run.
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(codec(16, 16))
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < 200; i++) {
          w.addDocument(doc(i, vectors[i], dim));
        }
        w.commit();
      }
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(codec(4, 4))
                  .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {
        for (int i = 200; i < 400; i++) {
          w.addDocument(doc(i, vectors[i], dim));
        }
        w.commit();
      }
      try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec(8, 8)))) {
        w.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertEquals(400, reader.numDocs());
        final IndexSearcher searcher = new IndexSearcher(reader);
        final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, vectors[0], 10), 10);
        assertTrue("search must work after a mismatched-nlist merge", td.scoreDocs.length > 0);
      }
    }
  }

  private Document doc(int id, float[] vector, int dim) {
    final Document d = new Document();
    d.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.EUCLIDEAN));
    d.add(new StoredField("id", Integer.toString(id)));
    return d;
  }

  private float[][] clusteredCorpus(int count, int clusters, int dim) {
    final float[][] centres = new float[clusters][];
    for (int c = 0; c < clusters; c++) {
      centres[c] = new float[dim];
      for (int d = 0; d < dim; d++) {
        centres[c][d] = (float) random().nextGaussian();
      }
    }
    final float[][] out = new float[count][];
    for (int i = 0; i < count; i++) {
      final float[] centre = centres[random().nextInt(clusters)];
      out[i] = new float[dim];
      for (int d = 0; d < dim; d++) {
        out[i][d] = (float) (centre[d] + random().nextGaussian() * 0.3);
      }
    }
    return out;
  }
}
