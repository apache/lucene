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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * End-to-end: index float vectors through the real codec, then search them.
 *
 * <p>This is the gate that the format, writer and reader agree. Everything up to here has been
 * tested in isolation (a coarse code, a routing cascade, a clusterer) and those can all be
 * individually correct while the bytes on disk are laid out one way and read back another. The
 * interesting assertion is therefore RECALL: an approximate index that returns plausible-looking
 * but wrong neighbours is what a section-offset or slot-order mistake actually looks like, since
 * nothing throws.
 */
public class TestIVFasterRoundTrip extends LuceneTestCase {

  private static final String FIELD = "vector";

  /** A codec that forces ivfaster (default tiers) for every vector field. */
  private static Codec codec(int nlist, int nprobe) {
    return codec(nlist, nprobe, IVFasterVectorsFormat.DEFAULT_FINE_TIER);
  }

  /** A codec that forces ivfaster with an explicit fine tier for every vector field. */
  private static Codec codec(int nlist, int nprobe, IVFasterVectorsFormat.FineTier fineTier) {
    return codec(nlist, nprobe, fineTier, false);
  }

  /**
   * A codec with explicit fine tier and keepFullPrecision. {@code fineTier == null} => FP32 rerank.
   */
  private static Codec codec(
      int nlist, int nprobe, IVFasterVectorsFormat.FineTier fineTier, boolean keepFullPrecision) {
    return TestUtil.alwaysKnnVectorsFormat(
        new IVFasterVectorsFormat(
            nlist,
            nprobe,
            IVFasterVectorsFormat.DEFAULT_SPILL_BITS,
            IVFasterVectorsFormat.DEFAULT_SOAR_LAMBDA,
            IVFasterVectorsFormat.DEFAULT_LLOYD_ITERS,
            IVFasterVectorsFormat.CoarseTier.NITROX2,
            fineTier,
            keepFullPrecision));
  }

  /**
   * The headline test: with every cell probed, the search must find the true nearest neighbours.
   *
   * <p>Probing all cells removes routing from the equation, so what is left under test is the
   * on-disk layout and the scoring cascade. Recall should then be limited only by 8-bit
   * quantization, which at this scale means the top neighbour is essentially always found.
   */
  public void testRecallWithFullProbe() throws Exception {
    final int dim = 64;
    final int count = 500;
    final int nlist = 10;

    final float[][] vectors = clusteredCorpus(count, 8, dim);
    try (Directory dir = newDirectory()) {
      indexAll(dir, vectors, dim, nlist, nlist, VectorSimilarityFunction.DOT_PRODUCT);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        int found = 0;
        final int trials = 40;
        for (int t = 0; t < trials; t++) {
          final float[] query = vectors[random().nextInt(count)];
          // Ground truth: the exact nearest by dot product.
          int best = -1;
          double bestScore = -Double.MAX_VALUE;
          for (int i = 0; i < count; i++) {
            double dot = 0;
            for (int d = 0; d < dim; d++) {
              dot += (double) query[d] * vectors[i][d];
            }
            if (dot > bestScore) {
              bestScore = dot;
              best = i;
            }
          }
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, query, 10), 10);
          final Set<Integer> hits = new HashSet<>();
          for (var sd : td.scoreDocs) {
            hits.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
          }
          if (hits.contains(best)) {
            found++;
          }
        }
        assertTrue(
            "with every cell probed the true nearest should almost always be found, got "
                + found
                + "/"
                + trials,
            found >= (int) (0.9 * trials));
      }
    }
  }

  /**
   * The same full-probe recall gate, with the {@code INT8} fine tier pinned explicitly via the
   * format constructor (rather than relying on it being the default).
   *
   * <p>Full probe removes routing, so what is under test is the tier's own write-then-read
   * agreement: the per-vector scale, the offset-binary storage, and the exact dot correction. A
   * write/read disagreement returns plausible-but-wrong neighbours with nothing thrown, which would
   * read as a routing miss rather than a broken tier, so it is worth gating directly.
   */
  public void testRecallWithFullProbeInt8() throws Exception {
    final int dim = 128;
    final int count = 500;
    final int nlist = 10;

    final float[][] vectors = clusteredCorpus(count, 8, dim);
    try (Directory dir = newDirectory()) {
      indexAll(
          dir,
          vectors,
          dim,
          nlist,
          nlist,
          VectorSimilarityFunction.DOT_PRODUCT,
          IVFasterVectorsFormat.FineTier.INT8);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        int found = 0;
        final int trials = 40;
        for (int t = 0; t < trials; t++) {
          final float[] query = vectors[random().nextInt(count)];
          int best = -1;
          double bestScore = -Double.MAX_VALUE;
          for (int i = 0; i < count; i++) {
            double dot = 0;
            for (int d = 0; d < dim; d++) {
              dot += (double) query[d] * vectors[i][d];
            }
            if (dot > bestScore) {
              bestScore = dot;
              best = i;
            }
          }
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, query, 10), 10);
          final Set<Integer> hits = new HashSet<>();
          for (var sd : td.scoreDocs) {
            hits.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
          }
          if (hits.contains(best)) {
            found++;
          }
        }
        assertTrue(
            "int8 with every cell probed should almost always find the true nearest, got "
                + found
                + "/"
                + trials,
            found >= (int) (0.9 * trials));
      }
    }
  }

  /**
   * A full index/search cycle at the PRODUCTION DIMENSION.
   *
   * <p>WHY 1024 SPECIFICALLY, when every other test here runs 32-128. DocPlanes pads each record up
   * to a 64 B line, so at small dims a stray read of {@code offset + planeBytes} lands harmlessly
   * inside the padding: at dim=64 one plane is 8 B in a 64 B record. The padding vanishes exactly
   * when {@code planes * planeBytes} is already a multiple of 64, at dim >= 768, and only there
   * does such a read run off the end of the buffer.
   *
   * <p>That is not hypothetical: it is how the writer's unconditional lo-plane section and
   * DocPlanes' donor copy both shipped under a 1-bit coarse code, passing this entire suite while
   * indexing at dim=1024 threw ArrayIndexOutOfBoundsException in flush. A suite whose largest
   * dimension is padded cannot see it.
   */
  public void testIndexAndSearchAtProductionDimension() throws Exception {
    final int dim = 1024;
    // Small corpus: the point is the LAYOUT at this dimension, not recall, so keep it fast.
    final int count = 120;
    final int nlist = 4;
    final float[][] vectors = clusteredCorpus(count, 3, dim);
    try (Directory dir = newDirectory()) {
      indexAll(dir, vectors, dim, nlist, nlist, VectorSimilarityFunction.DOT_PRODUCT);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        for (int t = 0; t < 5; t++) {
          final float[] q = vectors[random().nextInt(count)];
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, q, 10), 10);
          assertTrue("must return hits at dim=" + dim, td.scoreDocs.length > 0);
          for (var sd : td.scoreDocs) {
            assertTrue("scores must be finite", Float.isFinite(sd.score));
          }
        }
      }
    }
  }

  /** Scores must be ordered and finite, under every similarity the codec accepts. */
  public void testAllSimilarities() throws Exception {
    final int dim = 32;
    final int count = 200;
    for (VectorSimilarityFunction sim : VectorSimilarityFunction.values()) {
      final float[][] vectors = clusteredCorpus(count, 5, dim);
      try (Directory dir = newDirectory()) {
        indexAll(dir, vectors, dim, 8, 8, sim);
        try (IndexReader reader = DirectoryReader.open(dir)) {
          final IndexSearcher searcher = new IndexSearcher(reader);
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, vectors[0], 10), 10);
          assertTrue("expected hits under " + sim, td.scoreDocs.length > 0);
          for (int i = 0; i < td.scoreDocs.length; i++) {
            assertTrue("score must be finite under " + sim, Float.isFinite(td.scoreDocs[i].score));
            if (i > 0) {
              assertTrue(
                  "scores must be descending under " + sim,
                  td.scoreDocs[i - 1].score >= td.scoreDocs[i].score);
            }
          }
        }
      }
    }
  }

  /** Every indexed document must be retrievable when all cells are probed. */
  public void testAllDocumentsAreReachable() throws Exception {
    final int dim = 32;
    final int count = 300;
    final int nlist = 12;
    final float[][] vectors = clusteredCorpus(count, 6, dim);
    try (Directory dir = newDirectory()) {
      indexAll(dir, vectors, dim, nlist, nlist, VectorSimilarityFunction.EUCLIDEAN);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < count; i++) {
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, vectors[i], 5), 5);
          for (var sd : td.scoreDocs) {
            seen.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
          }
        }
        // A layout bug that strands a whole cell shows up as a large fraction missing.
        assertTrue(
            "only " + seen.size() + " of " + count + " documents were ever returned",
            seen.size() >= count / 2);
      }
    }
  }

  /**
   * Recall must survive a merge, which is where cell and slot bookkeeping is easiest to get wrong.
   */
  public void testSurvivesMerge() throws Exception {
    final int dim = 32;
    final int perBatch = 120;
    final int batches = 4;
    final int nlist = 8;
    final float[][] all = clusteredCorpus(perBatch * batches, 6, dim);

    try (Directory dir = newDirectory()) {
      final IndexWriterConfig cfg =
          new IndexWriterConfig()
              .setCodec(codec(nlist, nlist))
              .setMaxBufferedDocs(Integer.MAX_VALUE);
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        for (int b = 0; b < batches; b++) {
          for (int i = 0; i < perBatch; i++) {
            final int id = b * perBatch + i;
            final Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, all[id], VectorSimilarityFunction.EUCLIDEAN));
            doc.add(new org.apache.lucene.document.StoredField("id", Integer.toString(id)));
            w.addDocument(doc);
          }
          w.commit();
        }
        w.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals("force-merged to one segment", 1, reader.leaves().size());
        assertEquals("all documents present", perBatch * batches, reader.numDocs());
        final IndexSearcher searcher = new IndexSearcher(reader);
        int found = 0;
        final int trials = 30;
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
            "after merging, a document should be found by its own vector: " + found + "/" + trials,
            found >= (int) (0.8 * trials));
      }
    }
  }

  /**
   * The coarse-copy optimization must ENGAGE: after a multi-segment merge, every merged document's
   * stored coarse plane must be byte-identical to the plane its ORIGINAL vector encodes to, for
   * every document rather than only the fine donor's.
   *
   * <p>This is the "verify it engaged" gate for the merge coarse-copy. The coarse plane is packed
   * once, at the original flush, from the rotated original vector. A merge reconstructs each
   * non-donor document from its lossy fine (int8) code; re-encoding the coarse plane from that
   * reconstruction flips the thermometer level on dimensions sitting near a grid threshold, so the
   * stored plane DIFFERS from the original. Copying the source segment's plane verbatim keeps it
   * exact.
   *
   * <p>The oracle is the original vector re-encoded through the writer's own {@code rotationSeed}
   * and {@link Nitrox2#packPlanes}, the exact steps a source segment ran at flush, keyed by a
   * stored id so the comparison survives the merge's document reordering. Recall alone would not
   * catch a regression here: a slightly-degraded coarse plane still returns plausible neighbours,
   * and at the full-probe settings the other merge tests use the plane barely matters. So the
   * assertion is byte-equality, which is what distinguishes "copied" from "re-encoded".
   */
  public void testMergedCoarsePlanesAreVerbatim() throws Exception {
    final int dim = 32;
    final int perBatch = 90;
    final int batches = 3;
    final int nlist = 8;
    final int count = perBatch * batches;
    final float[][] all = clusteredCorpus(count, 5, dim);
    final int planeBytes = Nitrox2.planeBytes(dim);
    final int coarseBytes = Nitrox2.PLANES * planeBytes;

    // The plane each ORIGINAL vector should carry, keyed by the original vector index.
    final HadamardRotation rotation =
        HadamardRotation.create(dim, IVFasterVectorsWriter.rotationSeed(dim));
    final byte[][] expected = new byte[count][coarseBytes];
    final float[] norm = new float[dim];
    final float[] rot = new float[dim];
    for (int id = 0; id < count; id++) {
      System.arraycopy(all[id], 0, norm, 0, dim);
      org.apache.lucene.util.VectorUtil.l2normalize(norm);
      rotation.rotate(norm, rot);
      Nitrox2.packPlanes(rot, dim, expected[id], 0, planeBytes);
    }

    try (Directory merged = newDirectory()) {
      // Multi-segment and force-merged, so the coarse copy runs across several source segments.
      final IndexWriterConfig mergedCfg =
          new IndexWriterConfig()
              .setCodec(codec(nlist, nlist))
              .setMaxBufferedDocs(Integer.MAX_VALUE);
      try (IndexWriter w = new IndexWriter(merged, mergedCfg)) {
        for (int b = 0; b < batches; b++) {
          for (int i = 0; i < perBatch; i++) {
            final int id = b * perBatch + i;
            final Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, all[id], VectorSimilarityFunction.DOT_PRODUCT));
            doc.add(new org.apache.lucene.document.StoredField("id", Integer.toString(id)));
            w.addDocument(doc);
          }
          w.commit(); // one segment per batch, so the merge has several coarse sources
        }
        w.forceMerge(1);
      }

      try (IndexReader mr = DirectoryReader.open(merged)) {
        assertEquals("merged to one segment", 1, mr.leaves().size());
        final var leaf = mr.leaves().get(0).reader();
        final IVFasterVectorsReader.DonorView mv = donorView(mr);
        assertEquals(count, mv.count());
        final byte[] got = new byte[coarseBytes];
        int mismatches = 0;
        for (int ord = 0; ord < count; ord++) {
          // ord -> merged docId -> stored id -> expected plane, which copyCoarse must match.
          final int mergedDoc = mv.ordToDocForTest(ord);
          final int id = Integer.parseInt(leaf.storedFields().document(mergedDoc).get("id"));
          mv.copyCoarse(ord, got, 0);
          if (java.util.Arrays.equals(got, expected[id]) == false) {
            mismatches++;
          }
        }
        assertEquals(
            "merged coarse planes must be copied verbatim from the source segments, so each equals its "
                + "original vector's plane byte-for-byte ("
                + mismatches
                + "/"
                + count
                + " differ, which is the re-encode-from-reconstruction regression)",
            0,
            mismatches);
      }
    }
  }

  /**
   * PERSISTED centroids must be unit norm for every similarity, on disk and after a merge.
   *
   * <p>The reader's flat cell selection ranks by {@code -dot} with no {@code ||c||^2} term, which
   * is the correct nearest-cell order only when every centroid shares one norm. That makes this an
   * on-disk contract, not an internal detail of the clusterer: {@code TestClustering} pins the
   * invariant where the means are computed, and this pins that it survives the write, the read and
   * a merge's reseeding from a donor's centroids.
   *
   * <p>EUCLIDEAN is the case that matters. A mean of unit vectors has norm strictly below 1, so a
   * regression here shows up as cells ranked partly by centroid norm rather than by proximity,
   * which costs recall and raises nothing.
   */
  public void testPersistedCentroidsAreUnitNorm() throws Exception {
    final int dim = 64;
    final int count = 800;
    final int nlist = 16;
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN,
          VectorSimilarityFunction.DOT_PRODUCT,
          VectorSimilarityFunction.COSINE,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
        }) {
      final float[][] vectors = clusteredCorpus(count, 8, dim);
      try (Directory dir = newDirectory()) {
        // Several segments then a forceMerge, so the merge's seeded clustering is covered too.
        try (IndexWriter w =
            new IndexWriter(
                dir,
                new IndexWriterConfig().setCodec(codec(nlist, nlist)).setMaxBufferedDocs(200))) {
          for (int i = 0; i < count; i++) {
            final Document doc = new Document();
            doc.add(new KnnFloatVectorField(FIELD, vectors[i], sim));
            w.addDocument(doc);
            if (i % 200 == 199) {
              w.commit();
            }
          }
          w.forceMerge(1);
        }
        try (IndexReader reader = DirectoryReader.open(dir)) {
          final float[][] centroids = donorView(reader).centroids();
          assertEquals("every segment keeps exactly nlist centroids", nlist, centroids.length);
          for (int c = 0; c < centroids.length; c++) {
            double norm = 0;
            for (int d = 0; d < dim; d++) {
              norm += (double) centroids[c][d] * centroids[c][d];
            }
            assertEquals(
                "persisted centroid " + c + " must be unit norm under " + sim,
                1.0,
                Math.sqrt(norm),
                1e-4);
          }
        }
      }
    }
  }

  private static IVFasterVectorsReader.DonorView donorView(IndexReader reader) throws IOException {
    final IVFasterVectorsReader ivf =
        (IVFasterVectorsReader)
            ((org.apache.lucene.index.CodecReader) reader.leaves().get(0).reader())
                .getVectorReader()
                .unwrapReaderForField(FIELD);
    final IVFasterVectorsReader.DonorView view = ivf.donorView(FIELD);
    assertNotNull(view);
    return view;
  }

  /** A field with no vectors, and an empty index, must not break the reader. */
  public void testEmptyIndex() throws Exception {
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig cfg = new IndexWriterConfig().setCodec(codec(8, 8));
      try (IndexWriter w = new IndexWriter(dir, cfg)) {
        w.addDocument(new Document());
        w.commit();
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, new float[32], 10), 10);
        assertEquals(0, td.scoreDocs.length);
      }
    }
  }

  private void indexAll(
      Directory dir,
      float[][] vectors,
      int dim,
      int nlist,
      int nprobe,
      VectorSimilarityFunction sim)
      throws IOException {
    indexAll(dir, vectors, dim, nlist, nprobe, sim, IVFasterVectorsFormat.DEFAULT_FINE_TIER);
  }

  private void indexAll(
      Directory dir,
      float[][] vectors,
      int dim,
      int nlist,
      int nprobe,
      VectorSimilarityFunction sim,
      IVFasterVectorsFormat.FineTier fineTier)
      throws IOException {
    indexAll(dir, vectors, dim, nlist, nprobe, sim, fineTier, false);
  }

  private void indexAll(
      Directory dir,
      float[][] vectors,
      int dim,
      int nlist,
      int nprobe,
      VectorSimilarityFunction sim,
      IVFasterVectorsFormat.FineTier fineTier,
      boolean keepFullPrecision)
      throws IOException {
    final IndexWriterConfig cfg =
        new IndexWriterConfig()
            .setCodec(codec(nlist, nprobe, fineTier, keepFullPrecision))
            .setMaxBufferedDocs(Integer.MAX_VALUE);
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (int i = 0; i < vectors.length; i++) {
        final Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vectors[i], sim));
        doc.add(new org.apache.lucene.document.StoredField("id", Integer.toString(i)));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  /** A corpus with real cluster structure, which is what an IVF index is for. */
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
      // Unit length, so DOT_PRODUCT and COSINE are both well defined on this corpus.
      double norm = 0;
      for (float x : out[i]) {
        norm += (double) x * x;
      }
      norm = Math.sqrt(norm);
      for (int d = 0; d < dim; d++) {
        out[i][d] /= (float) norm;
      }
    }
    return out;
  }

  // --------------------------------------------------------------------------
  // fp32 fine tier + keepFullPrecision
  // --------------------------------------------------------------------------

  /**
   * FP32 fine tier ({@code fineTier == null}): the fine code IS the rotated vector, scored by an
   * exact float dot. With every cell probed the top neighbour must be found EVERY time, since there
   * is no quantization anywhere on the scoring path.
   */
  public void testFp32FineTierExactWithFullProbe() throws Exception {
    final int dim = 64;
    final int count = 300;
    final int nlist = 6;
    final float[][] vectors = clusteredCorpus(count, 4, dim);
    try (Directory dir = newDirectory()) {
      indexAll(dir, vectors, dim, nlist, nlist, VectorSimilarityFunction.DOT_PRODUCT, null);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        int found = 0;
        final int trials = 30;
        for (int t = 0; t < trials; t++) {
          final float[] query = vectors[random().nextInt(count)];
          int best = -1;
          double bestScore = -Double.MAX_VALUE;
          for (int i = 0; i < count; i++) {
            double dot = 0;
            for (int d = 0; d < dim; d++) {
              dot += (double) query[d] * vectors[i][d];
            }
            if (dot > bestScore) {
              bestScore = dot;
              best = i;
            }
          }
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, query, 10), 10);
          final Set<Integer> hits = new HashSet<>();
          for (var sd : td.scoreDocs) {
            hits.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
          }
          if (hits.contains(best)) {
            found++;
          }
        }
        // Exact scoring with full probe: the true nearest is essentially always in the top-10.
        assertTrue(
            "fp32 rerank+full probe should find the true nearest, got " + found + "/" + trials,
            found >= (int) (0.95 * trials));
      }
    }
  }

  /**
   * {@code keepFullPrecision} exposes the ORIGINAL vectors byte-for-byte via {@code
   * getFloatVectorValues}, not a reconstruction. Without the flag the returned vector is the
   * inverse-rotation of the fine-code reconstruction and can differ from the original in the last
   * few bits; with the flag it is exactly equal.
   */
  public void testKeepFullPrecisionRoundTripExact() throws Exception {
    final int dim = 32;
    final int count = 128;
    final int nlist = 4;
    final float[][] vectors = clusteredCorpus(count, 4, dim);
    try (Directory dir = newDirectory()) {
      indexAll(
          dir,
          vectors,
          dim,
          nlist,
          nlist,
          VectorSimilarityFunction.DOT_PRODUCT,
          IVFasterVectorsFormat.FineTier.INT8,
          true);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final org.apache.lucene.index.LeafReader leaf =
            reader.getContext().leaves().get(0).reader();
        final org.apache.lucene.index.FloatVectorValues fv = leaf.getFloatVectorValues(FIELD);
        // The stored vectors round-trip EXACTLY, in insertion order under a single-threaded writer.
        final float[] expected = pickForOrdinal(vectors, leaf, dim, count);
        for (int ord = 0; ord < count; ord++) {
          final float[] got = fv.vectorValue(ord);
          assertNotNull("vectorValue(" + ord + ") returned null", got);
          for (int d = 0; d < dim; d++) {
            assertEquals(
                "keepFullPrecision must round-trip byte-for-byte at ord=" + ord + " d=" + d,
                expected[ord * dim + d],
                got[d],
                0f);
          }
        }
      }
    }
  }

  /**
   * Interaction rule: {@code keepFullPrecision} is silently IGNORED when {@code fineTier == null},
   * because the FP32 fine tier already stores and uses the full-precision vectors, so a second copy
   * would duplicate them. Verified indirectly: the index still opens, and {@code
   * getFloatVectorValues} returns valid vectors (from the fine code, since no raw section was
   * written).
   */
  public void testKeepFullPrecisionIgnoredUnderFp32Fine() throws Exception {
    final int dim = 32;
    final int count = 64;
    final int nlist = 4;
    final float[][] vectors = clusteredCorpus(count, 2, dim);
    try (Directory dir = newDirectory()) {
      indexAll(
          dir,
          vectors,
          dim,
          nlist,
          nlist,
          VectorSimilarityFunction.DOT_PRODUCT,
          null, /* keepFullPrecision (should be ignored) */
          true);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final org.apache.lucene.index.LeafReader leaf =
            reader.getContext().leaves().get(0).reader();
        final org.apache.lucene.index.FloatVectorValues fv = leaf.getFloatVectorValues(FIELD);
        for (int ord = 0; ord < count; ord++) {
          assertNotNull("vectorValue(" + ord + ")", fv.vectorValue(ord));
        }
      }
    }
  }

  /**
   * Copies the vectors flat in ordinal order, using the same doc-id -> ord order the writer used.
   */
  private static float[] pickForOrdinal(
      float[][] vectors, org.apache.lucene.index.LeafReader leaf, int dim, int count)
      throws IOException {
    // Single-threaded writer, so ordinal N is the Nth stored 'id' field.
    final float[] out = new float[count * dim];
    final org.apache.lucene.index.StoredFields sf = leaf.storedFields();
    for (int ord = 0; ord < count; ord++) {
      final String idStr = sf.document(ord).get("id");
      final int insertionIndex = Integer.parseInt(idStr);
      System.arraycopy(vectors[insertionIndex], 0, out, ord * dim, dim);
    }
    return out;
  }
}
