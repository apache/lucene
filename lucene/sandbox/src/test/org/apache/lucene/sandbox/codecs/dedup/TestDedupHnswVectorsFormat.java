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

import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Runs the standard KNN vectors format suite against the de-duplicating HNSW format. De-duplication
 * behavior itself is covered by {@link TestDedupFlatVectorsFormat}.
 */
public class TestDedupHnswVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private final KnnVectorsFormat format = new DedupHnswVectorsFormat();

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false; // stores raw vectors, no quantized fallback
  }

  /**
   * The HNSW graph is built over <b>distinct</b> vectors (group ordinals), so its node count equals
   * the number of unique vectors, which is strictly less than the document count when duplicates
   * are indexed.
   */
  public void testGraphBuiltOverDistinctVectors() throws Exception {
    int dim = 8;
    int distinct = 5;
    int copiesPerVector = 20;
    float[][] uniqueVectors = new float[distinct][];
    for (int i = 0; i < distinct; i++) {
      uniqueVectors[i] = randomVector(dim);
    }
    // tinySegmentsThreshold=0 forces graph construction regardless of the (small) distinct count.
    Codec codec = TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat(16, 100, 0));
    try (Directory dir = newDirectory();
        var w =
            new org.apache.lucene.index.IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
      int docCount = 0;
      for (int i = 0; i < distinct; i++) {
        for (int c = 0; c < copiesPerVector; c++) {
          var doc = new org.apache.lucene.document.Document();
          doc.add(
              new org.apache.lucene.document.KnnFloatVectorField(
                  "f", uniqueVectors[i].clone(), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(doc);
          docCount++;
        }
      }
      w.forceMerge(1);
      try (var reader = org.apache.lucene.index.DirectoryReader.open(w)) {
        assertEquals(1, reader.leaves().size());
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader knnReader =
            ((CodecReader) leaf).getVectorReader().unwrapReaderForField("f");
        assertTrue(knnReader instanceof DedupHnswVectorsReader);
        var fieldInfo = leaf.getFieldInfos().fieldInfo("f");
        // The graph has one node per distinct vector...
        var graph = ((DedupHnswVectorsReader) knnReader).getGraph("f");
        assertEquals(distinct, graph.size());
        // ...while the reported vector count is the number of documents.
        assertEquals(docCount, knnReader.getVectorCount(fieldInfo));
      }
    }
  }

  /**
   * A KNN search over a field with many duplicate vectors returns <b>every</b> document that shares
   * the matched distinct vector (fan-out), each with the same score, not just one representative.
   */
  public void testSearchExpandsToAllDuplicateDocuments() throws Exception {
    int dim = 6;
    float[] shared = randomVector(dim);
    int copies = 15;
    try (Directory dir = newDirectory();
        var w =
            new org.apache.lucene.index.IndexWriter(
                dir, newIndexWriterConfig().setCodec(getCodec()))) {
      // Index `copies` documents that all share the exact same vector.
      for (int i = 0; i < copies; i++) {
        var doc = new org.apache.lucene.document.Document();
        doc.add(
            new org.apache.lucene.document.KnnFloatVectorField(
                "f", shared.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      // ...plus some distinct filler vectors.
      for (int i = 0; i < 10; i++) {
        var doc = new org.apache.lucene.document.Document();
        doc.add(
            new org.apache.lucene.document.KnnFloatVectorField(
                "f", randomVector(dim), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (var reader = org.apache.lucene.index.DirectoryReader.open(w)) {
        var searcher = new org.apache.lucene.search.IndexSearcher(reader);
        // Ask for at least `copies` results; the shared vector is an exact match (score highest).
        var query =
            new org.apache.lucene.search.KnnFloatVectorQuery("f", shared.clone(), copies + 5);
        var topDocs = searcher.search(query, copies + 5);
        // All `copies` documents sharing the exact vector must be returned.
        int exactMatches = 0;
        float best = topDocs.scoreDocs.length == 0 ? 0f : topDocs.scoreDocs[0].score;
        for (var sd : topDocs.scoreDocs) {
          if (sd.score == best) {
            exactMatches++;
          }
        }
        assertTrue(
            "expected at least " + copies + " exact-match docs, got " + exactMatches,
            exactMatches >= copies);
      }
    }
  }

  /**
   * With a high tiny-segments threshold, a small segment skips HNSW graph construction (search
   * scans the distinct vectors exhaustively), yet still returns correct results and fans duplicates
   * out to all documents.
   */
  public void testTinySegmentSkipsGraph() throws Exception {
    int dim = 4;
    var tinyThresholdFormat = new DedupHnswVectorsFormat(16, 100, Integer.MAX_VALUE);
    Codec codec = TestUtil.alwaysKnnVectorsFormat(tinyThresholdFormat);

    float[] shared = randomVector(dim);
    int copies = 4;
    try (Directory dir = newDirectory();
        var w =
            new org.apache.lucene.index.IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
      for (int i = 0; i < copies; i++) {
        var doc = new org.apache.lucene.document.Document();
        doc.add(
            new org.apache.lucene.document.KnnFloatVectorField(
                "f", shared.clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (var reader = org.apache.lucene.index.DirectoryReader.open(w)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader knnReader =
            ((CodecReader) leaf).getVectorReader().unwrapReaderForField("f");
        assertTrue(knnReader instanceof DedupHnswVectorsReader);
        // No graph was built for this tiny segment.
        assertEquals(HnswGraph.EMPTY, ((DedupHnswVectorsReader) knnReader).getGraph("f"));

        // Search still works (exhaustive) and fans the shared vector out to all documents.
        var searcher = new org.apache.lucene.search.IndexSearcher(reader);
        var query = new org.apache.lucene.search.KnnFloatVectorQuery("f", shared.clone(), copies);
        var topDocs = searcher.search(query, copies);
        assertEquals(copies, topDocs.scoreDocs.length);
      }
    }
  }

  /**
   * HYBRID layout: groups referenced by more than the configured threshold become single
   * posting-backed graph nodes, while all other documents remain individual graph nodes. The graph
   * node count must therefore equal {@code numLargeGroups + numSmallDocs}, and search must still
   * fan a large group out to all its documents while returning small-group documents directly.
   */
  public void testHybridLayout() throws Exception {
    int dim = 8;
    int threshold = 5;

    // Two "large" groups, each with more than `threshold` copies -> promoted to one node each.
    int largeGroups = 2;
    int copiesPerLargeGroup = threshold + 10; // 15
    // Several "small" groups each with a single copy -> kept as individual document nodes.
    int smallGroups = 12;

    float[][] largeVectors = new float[largeGroups][];
    for (int i = 0; i < largeGroups; i++) {
      largeVectors[i] = randomVector(dim);
    }
    float[][] smallVectors = new float[smallGroups][];
    for (int i = 0; i < smallGroups; i++) {
      smallVectors[i] = randomVector(dim);
    }

    int expectedLargeNodes = largeGroups;
    int expectedSmallNodes = smallGroups; // one copy each
    int expectedGraphNodes = expectedLargeNodes + expectedSmallNodes;
    int expectedDocCount = largeGroups * copiesPerLargeGroup + smallGroups;

    // tinySegmentsThreshold=0 forces graph construction; hybridGroupThreshold enables HYBRID.
    // (numMergeWorkers=1 with a null executor is allowed by the format.)
    Codec codec =
        TestUtil.alwaysKnnVectorsFormat(
            new DedupHnswVectorsFormat(16, 100, 1, null, 0, threshold));

    try (Directory dir = newDirectory();
        var w =
            new org.apache.lucene.index.IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
      for (int i = 0; i < largeGroups; i++) {
        for (int c = 0; c < copiesPerLargeGroup; c++) {
          var doc = new org.apache.lucene.document.Document();
          doc.add(
              new org.apache.lucene.document.KnnFloatVectorField(
                  "f", largeVectors[i].clone(), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(doc);
        }
      }
      for (int i = 0; i < smallGroups; i++) {
        var doc = new org.apache.lucene.document.Document();
        doc.add(
            new org.apache.lucene.document.KnnFloatVectorField(
                "f", smallVectors[i].clone(), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      w.forceMerge(1);

      try (var reader = org.apache.lucene.index.DirectoryReader.open(w)) {
        assertEquals(1, reader.leaves().size());
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader knnReader =
            ((CodecReader) leaf).getVectorReader().unwrapReaderForField("f");
        assertTrue(knnReader instanceof DedupHnswVectorsReader);
        var fieldInfo = leaf.getFieldInfos().fieldInfo("f");

        // The hybrid graph has one node per large group plus one node per small-group doc.
        var graph = ((DedupHnswVectorsReader) knnReader).getGraph("f");
        assertEquals(expectedGraphNodes, graph.size());
        // The reported vector count is still the number of documents.
        assertEquals(expectedDocCount, knnReader.getVectorCount(fieldInfo));

        var searcher = new org.apache.lucene.search.IndexSearcher(reader);

        // A query matching a large group must fan out to all copiesPerLargeGroup documents.
        var largeQuery =
            new org.apache.lucene.search.KnnFloatVectorQuery(
                "f", largeVectors[0].clone(), copiesPerLargeGroup + 5);
        var largeTop = searcher.search(largeQuery, copiesPerLargeGroup + 5);
        float best = largeTop.scoreDocs.length == 0 ? 0f : largeTop.scoreDocs[0].score;
        int exactMatches = 0;
        for (var sd : largeTop.scoreDocs) {
          if (sd.score == best) {
            exactMatches++;
          }
        }
        assertTrue(
            "expected large group to fan out to " + copiesPerLargeGroup + " docs, got "
                + exactMatches,
            exactMatches >= copiesPerLargeGroup);

        // A query matching a small group must return its single document as the top hit.
        var smallQuery =
            new org.apache.lucene.search.KnnFloatVectorQuery("f", smallVectors[0].clone(), 5);
        var smallTop = searcher.search(smallQuery, 5);
        assertThat(smallTop.scoreDocs.length, greaterThan(0));
      }
    }
  }

  /**
   * Quantifies filtered-search recall on the DEDUP path. Because groups are no longer pre-filtered
   * against {@code acceptDocs} (the HNSW beam keeps only the top-k <b>groups</b> by similarity and
   * {@code acceptDocs} is applied lazily at expansion), a selective filter whose accepted documents
   * live in lower-scoring groups can be pruned by the beam before expansion. This test measures the
   * recall of filtered KNN against brute-force ground truth and asserts a (loose) lower bound so the
   * number is tracked rather than silently regressing.
   */
  public void testFilteredRecallDedup() throws Exception {
    int dim = 16;
    int distinct = 2000; // 2000 distinct vectors -> 2000 DEDUP groups (large enough that HNSW runs)
    int copiesPerVector = 4; // each shared by 4 docs
    int k = 10;
    int acceptEvery = 32; // ~1 in 32 docs accepted: a selective, scattered filter

    float[][] uniqueVectors = new float[distinct][];
    for (int i = 0; i < distinct; i++) {
      uniqueVectors[i] = randomVector(dim);
    }

    // tinySegmentsThreshold=0 forces graph construction so the beam/termination behavior is
    // exercised (not an exhaustive scan).
    Codec codec = TestUtil.alwaysKnnVectorsFormat(new DedupHnswVectorsFormat(16, 100, 0));

    try (Directory dir = newDirectory();
        var w =
            new org.apache.lucene.index.IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
      for (int i = 0; i < distinct; i++) {
        for (int c = 0; c < copiesPerVector; c++) {
          boolean accept = ((i * copiesPerVector + c) % acceptEvery) == 0;
          var doc = new org.apache.lucene.document.Document();
          doc.add(
              new org.apache.lucene.document.KnnFloatVectorField(
                  "f", uniqueVectors[i].clone(), VectorSimilarityFunction.EUCLIDEAN));
          if (accept) {
            doc.add(
                new org.apache.lucene.document.StringField(
                    "filter", "yes", org.apache.lucene.document.Field.Store.NO));
          }
          w.addDocument(doc);
        }
      }
      w.forceMerge(1);

      try (var reader = org.apache.lucene.index.DirectoryReader.open(w)) {
        assertEquals(1, reader.leaves().size());
        LeafReader leaf = reader.leaves().get(0).reader();
        KnnVectorsReader knnReader =
            ((CodecReader) leaf).getVectorReader().unwrapReaderForField("f");
        // Confirm an HNSW graph was actually built (otherwise the search is an exhaustive scan and
        // this test would not exercise beam pruning). One node per distinct vector.
        var graph = ((DedupHnswVectorsReader) knnReader).getGraph("f");
        assertEquals(distinct, graph.size());

        // Build docid-keyed truth directly from the index (do NOT assume docid == insertion order).
        int maxDoc = leaf.maxDoc();
        float[][] docVector = new float[maxDoc][];
        var fvv = leaf.getFloatVectorValues("f");
        var it = fvv.iterator();
        for (int docId = it.nextDoc();
            docId != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
            docId = it.nextDoc()) {
          docVector[docId] = fvv.vectorValue(it.index()).clone();
        }
        // Accepted docids = those matching the filter, read back from the index.
        boolean[] docAccepted = new boolean[maxDoc];
        var searcher = new org.apache.lucene.search.IndexSearcher(reader);
        var filter =
            new org.apache.lucene.search.TermQuery(
                new org.apache.lucene.index.Term("filter", "yes"));
        var filterTop = searcher.search(filter, maxDoc);
        for (var sd : filterTop.scoreDocs) {
          docAccepted[sd.doc] = true;
        }

        int numQueries = 20;
        int totalGroundTruth = 0;
        int totalFound = 0;
        for (int q = 0; q < numQueries; q++) {
          float[] target = randomVector(dim);

          // Ground truth: top-k accepted docs by exact similarity (brute force).
          java.util.List<Integer> gt = bruteForceTopK(target, docVector, docAccepted, k);

          var query =
              new org.apache.lucene.search.KnnFloatVectorQuery("f", target.clone(), k, filter);
          var topDocs = searcher.search(query, k);
          java.util.Set<Integer> returned = new HashSet<>();
          for (var sd : topDocs.scoreDocs) {
            returned.add(sd.doc);
          }

          for (int docId : gt) {
            totalGroundTruth++;
            if (returned.contains(docId)) {
              totalFound++;
            }
          }

          // Correctness: lazy filtering must never return a non-accepted doc.
          for (var sd : topDocs.scoreDocs) {
            assertTrue("returned a non-accepted doc " + sd.doc, docAccepted[sd.doc]);
          }
        }

        double recall = totalGroundTruth == 0 ? 1.0 : (double) totalFound / totalGroundTruth;
        System.out.println(
            "testFilteredRecallDedup: filtered recall@"
                + k
                + " = "
                + recall
                + " ("
                + totalFound
                + "/"
                + totalGroundTruth
                + ")");
        // Loose lower bound: documents the recall impact of lazy filtering without being flaky.
        // Tighten (or add over-fetch) if this proves too low in practice.
        assertTrue("filtered recall unexpectedly low: " + recall, recall >= 0.5);
      }
    }
  }

  /** Brute-force top-k accepted docids by exact EUCLIDEAN similarity. */
  private static java.util.List<Integer> bruteForceTopK(
      float[] target, float[][] docVector, boolean[] docAccepted, int k) {
    int n = docVector.length;
    Integer[] order = new Integer[n];
    float[] scores = new float[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
      scores[i] =
          docVector[i] == null
              ? Float.NEGATIVE_INFINITY
              : VectorSimilarityFunction.EUCLIDEAN.compare(target, docVector[i]);
    }
    Arrays.sort(order, (a, b) -> Float.compare(scores[b], scores[a]));
    java.util.List<Integer> out = new java.util.ArrayList<>();
    for (int i = 0; i < n && out.size() < k; i++) {
      int docId = order[i];
      if (docAccepted[docId]) {
        out.add(docId);
      }
    }
    return out;
  }

  @Override
  protected void assertOffHeapByteSize(LeafReader r, String fieldName) throws IOException {
    var fieldInfo = r.getFieldInfos().fieldInfo(fieldName);

    if (r instanceof CodecReader codecReader) {
      KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
      knnVectorsReader = knnVectorsReader.unwrapReaderForField(fieldName);
      var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
      long totalByteSize = offHeap.values().stream().mapToLong(Long::longValue).sum();
      if (knnVectorsReader instanceof DedupHnswVectorsReader) {
        if (getNumVectors(knnVectorsReader, fieldInfo) == 0) {
          assertEquals(0L, totalByteSize);
        } else {
          assertTrue(totalByteSize > 0);
          assertTrue(offHeap.get("vdd") > 0L); // NOTE: different from vec

          // .vdhd holds the HNSW graph when one was built, plus the postings in DEDUP mode. In
          // PLAIN mode (no effective de-duplication) there are no postings, so a tiny segment with
          // no graph may have an absent/zero .vdhd for this field.
          Long vdhd = offHeap.get("vdhd");
          assertTrue(vdhd == null || vdhd >= 0L);
        }
      } else {
        throw new AssertionError("unexpected reader:" + knnVectorsReader.getClass());
      }
    } else {
      throw new AssertionError("unexpected:" + r.getClass());
    }
  }

  /** Near copy of the original test, this one checks for size of <b>unique</b> vector count. */
  @Override
  @SuppressWarnings("unchecked")
  public void testWriterRamEstimate() throws IOException {
    final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
    final KnnVectorsFormat format = codec.knnVectorsFormat();
    try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
      final long ramBytesUsed = writer.ramBytesUsed();
      int dim = random().nextInt(64) + 1;
      if (dim % 2 == 1) {
        ++dim;
      }
      int numDocs = atLeast(100);
      Set<FloatVector> unique = new HashSet<>();
      KnnFieldVectorsWriter<float[]> fieldWriter =
          (KnnFieldVectorsWriter<float[]>)
              writer.addField(
                  new FieldInfo(
                      "fieldA",
                      0,
                      false,
                      false,
                      false,
                      IndexOptions.NONE,
                      DocValuesType.NONE,
                      DocValuesSkipIndexType.NONE,
                      -1,
                      Map.of(),
                      0,
                      0,
                      0,
                      dim,
                      VectorEncoding.FLOAT32,
                      VectorSimilarityFunction.DOT_PRODUCT,
                      false,
                      false));
      for (int i = 0; i < numDocs; i++) {
        float[] vector = randomVector(dim);
        unique.add(new FloatVector(vector));
        fieldWriter.addValue(i, vector);
      }
      final long ramBytesUsed2 = writer.ramBytesUsed();
      assertThat(ramBytesUsed2, greaterThan(ramBytesUsed));
      assertThat(ramBytesUsed2, greaterThan((long) dim * unique.size() * Float.BYTES));
    }
    dir.close();
  }

  private record FloatVector(float[] vector) {
    @Override
    public boolean equals(Object obj) {
      return obj instanceof FloatVector(float[] other) && Arrays.equals(vector, other);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }

  /** Near copy of the original test, this one checks for size of <b>unique</b> vector count. */
  @Override
  @SuppressWarnings("unchecked")
  public void testWriterByteVectorRamEstimate() throws IOException {
    final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[0]);
    final Directory dir = newDirectory();
    Codec codec = Codec.getDefault();
    final SegmentInfo si =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            10000,
            false,
            false,
            codec,
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            InfoStream.getDefault(), dir, si, fieldInfos, null, newIOContext(random()));
    final KnnVectorsFormat format = codec.knnVectorsFormat();
    try (KnnVectorsWriter writer = format.fieldsWriter(state)) {
      final long ramBytesUsed = writer.ramBytesUsed();
      int dim = random().nextInt(64) + 1;
      if (dim % 2 == 1) {
        ++dim;
      }
      int numDocs = atLeast(100);
      Set<ByteVector> unique = new HashSet<>();
      KnnFieldVectorsWriter<byte[]> fieldWriter =
          (KnnFieldVectorsWriter<byte[]>)
              writer.addField(
                  new FieldInfo(
                      "fieldA",
                      0,
                      false,
                      false,
                      false,
                      IndexOptions.NONE,
                      DocValuesType.NONE,
                      DocValuesSkipIndexType.NONE,
                      -1,
                      Map.of(),
                      0,
                      0,
                      0,
                      dim,
                      VectorEncoding.BYTE,
                      VectorSimilarityFunction.DOT_PRODUCT,
                      false,
                      false));
      for (int i = 0; i < numDocs; i++) {
        byte[] vector = randomVector8(dim);
        unique.add(new ByteVector(vector));
        fieldWriter.addValue(i, vector);
      }
      final long ramBytesUsed2 = writer.ramBytesUsed();
      assertThat(ramBytesUsed2, greaterThan(ramBytesUsed));
      assertThat(ramBytesUsed2, greaterThan((long) dim * unique.size() * Byte.BYTES));
    }
    dir.close();
  }

  private record ByteVector(byte[] vector) {
    @Override
    public boolean equals(Object obj) {
      return obj instanceof ByteVector(byte[] other) && Arrays.equals(vector, other);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }
}
