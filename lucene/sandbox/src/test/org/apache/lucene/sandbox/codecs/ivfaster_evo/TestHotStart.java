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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;

public class TestHotStart extends LuceneTestCase {
  private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

  protected IVFasterEvoVectorsFormat format() {
    return new IVFasterEvoVectorsFormat(8, 8, 2, 1.4, IVFasterEvoVectorsFormat.Tier.FP32);
  }

  private IndexWriterConfig config(boolean sorted) {
    IndexWriterConfig config =
        newIndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(format()))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            // Segment shapes are asserted below, so only explicit commits may flush.
            .setRAMBufferSizeMB(64)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setInfoStream(
                new InfoStream() {
                  @Override
                  public void message(String component, String message) {
                    messages.add(message);
                  }

                  @Override
                  public boolean isEnabled(String component) {
                    return component.equals("IVFE");
                  }

                  @Override
                  public void close() {}
                });
    if (sorted) {
      config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    }
    return config;
  }

  private static void add(IndexWriter writer, String batch, int count, int offset, boolean sparse)
      throws Exception {
    for (int i = 0; i < count; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", batch + i, Field.Store.NO));
      doc.add(new StringField("batch", batch, Field.Store.NO));
      doc.add(new NumericDocValuesField("sort", -offset - i));
      if (sparse == false || i % 5 != 0) {
        doc.add(
            new KnnFloatVectorField(
                "v", new float[] {offset + i, 1, 0}, VectorSimilarityFunction.EUCLIDEAN));
      }
      writer.addDocument(doc);
    }
  }

  private void assertMessage(String... parts) {
    synchronized (messages) {
      assertTrue(
          messages.toString(),
          messages.stream()
              .anyMatch(
                  message -> {
                    for (String part : parts) {
                      if (message.contains(part) == false) {
                        return false;
                      }
                    }
                    return true;
                  }));
    }
  }

  public void testMergeCarriesLargestLiveDonorThroughDeletesAndSort() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(true))) {
      add(writer, "large", 40, 0, true);
      writer.flush();
      add(writer, "small", 20, 100, true);
      writer.flush();
      for (int i = 0; i < 30; i++) {
        writer.deleteDocuments(new Term("id", "large" + i));
      }
      writer.commit();
      messages.clear();
      writer.getConfig().setMergePolicy(newLogMergePolicy());
      writer.forceMerge(1);
      assertMessage("source=mergeInput=", "liveVectors=16", "carried=16", "initialRouted=8");
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertEquals(1, reader.leaves().size());
        TestIVFasterEvoVectorsFormat.assertExact(
            reader.leaves().get(0).reader(), "v", VectorSimilarityFunction.EUCLIDEAN);
      }
    }
  }

  public void testFlushUsesOnlyLatestCommitAndRefreshesAfterDeletion() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false))) {
      add(writer, "committed", 20, 0, false);
      writer.commit();
      // Keep an old reader open: physically retained files must not become stale seed donors.
      try (DirectoryReader old = DirectoryReader.open(dir)) {
        add(writer, "uncommitted", 40, 100, false);
        writer.flush();
        messages.clear();
        add(writer, "tiny", 1, 500, false);
        writer.flush();
        assertMessage("source=commit=", "liveVectors=20", "seedCells=8", "initialRouted=1");
        writer.commit();
        messages.clear();
        add(writer, "afterCommit", 1, 700, false);
        writer.flush();
        assertMessage("source=commit=", "liveVectors=40", "initialRouted=1");
        writer.deleteDocuments(new Term("batch", "uncommitted"));
        writer.commit();
        messages.clear();
        add(writer, "afterDelete", 1, 900, false);
        writer.flush();
        assertMessage("source=commit=", "liveVectors=20", "initialRouted=1");
        assertEquals(20, old.numDocs());
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          for (var leaf : reader.leaves()) {
            CodecReader codec = (CodecReader) leaf.reader();
            var evo = (IVFasterEvoVectorsReader) codec.getVectorReader().unwrapReaderForField("v");
            assertEquals(8, evo.centroids("v").length);
            TestIVFasterEvoVectorsFormat.assertExact(
                codec, "v", VectorSimilarityFunction.EUCLIDEAN);
          }
        }
      }
    }
  }

  public void testRestartAndRollback() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, config(false))) {
        add(writer, "committed", 20, 0, false);
        writer.commit();
        add(writer, "rolledBack", 60, 100, false);
        writer.flush();
        writer.rollback();
      }
      messages.clear();
      // A fresh format/cache must recover centroids from the persisted commit.
      try (IndexWriter writer = new IndexWriter(dir, config(false))) {
        add(writer, "restart", 1, 700, false);
        writer.flush();
        assertMessage("source=commit=", "liveVectors=20", "initialRouted=1");
      }
    }
  }

  public void testCommittedCacheIsolationAndSeedCopy() throws Exception {
    var cache = new IVFasterEvoVectorsFormat.CommittedCentroids();
    try (Directory first = newDirectory();
        Directory second = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(first, config(false))) {
        add(writer, "first", 20, 0, false);
      }
      try (IndexWriter writer = new IndexWriter(second, config(false))) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", new float[] {100, 200}));
        writer.addDocument(doc);
      }
      try (DirectoryReader a = DirectoryReader.open(first);
          DirectoryReader b = DirectoryReader.open(second)) {
        var snapshot = cache.snapshot(first);
        assertSame(snapshot, cache.snapshot(first));
        var info = a.leaves().get(0).reader().getFieldInfos().fieldInfo("v");
        var incompatible = b.leaves().get(0).reader().getFieldInfos().fieldInfo("v");
        var seed = snapshot.seed(info, 8);
        assertEquals(20, seed.liveVectors());
        assertNull(snapshot.seed(incompatible, 8));
        assertNull(snapshot.seed(info, 1));
        assertNull(cache.snapshot(second).seed(info, 8));
        assertEquals(1, cache.snapshot(second).seed(incompatible, 8).liveVectors());
        float[] original = seed.centroids()[0].clone();
        TestClustering.cluster(
            List.of(new float[] {1000, 1, 0}),
            8,
            VectorSimilarityFunction.EUCLIDEAN,
            seed.centroids(),
            null);
        assertArrayEquals(original, seed.centroids()[0], 0f);
      }
    }
  }

  public void testHotStartNeverCrossesFields() throws Exception {
    // Two fields that differ only by name, over disjoint regions. An inherited cell that ends up
    // empty keeps its inherited position, so a seed taken from the wrong field would leave
    // centroids stranded in the other field's region, after a flush and after a merge alike.
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, config(false))) {
        for (int batch = 0; batch < 3; batch++) {
          for (int i = 0; i < 40; i++) {
            Document doc = new Document();
            float jitter = random().nextFloat();
            doc.add(new KnnFloatVectorField("near", new float[] {i + jitter, 1, 0}));
            doc.add(new KnnFloatVectorField("far", new float[] {10_000 + i + jitter, 1, 0}));
            writer.addDocument(doc);
          }
          writer.commit(); // later batches hot-start from this commit
        }
        assertMessage("field=near", "source=commit=");
        assertMessage("field=far", "source=commit=");
        assertFieldsStayApart(dir, 3);
        writer.getConfig().setMergePolicy(new LogDocMergePolicy());
        writer.forceMerge(1);
        writer.commit();
        assertMessage("field=near", "source=mergeInput=");
        assertMessage("field=far", "source=mergeInput=");
      }
      assertFieldsStayApart(dir, 1);
    }
  }

  private static void assertFieldsStayApart(Directory dir, int segments) throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      assertEquals(segments, reader.leaves().size());
      for (var leaf : reader.leaves()) {
        var vectors = ((CodecReader) leaf.reader()).getVectorReader();
        var near = (IVFasterEvoVectorsReader) vectors.unwrapReaderForField("near");
        var far = (IVFasterEvoVectorsReader) vectors.unwrapReaderForField("far");
        for (float[] centroid : near.centroids("near")) assertTrue(centroid[0] < 100);
        for (float[] centroid : far.centroids("far")) assertTrue(centroid[0] > 9_000);
      }
    }
  }

  public void testCarriedAssignmentsMatchReroutingAllVectors() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      double carriedObjective = 0, reroutedObjective = 0;
      for (int run = 0; run < 8; run++) {
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 44; i++) {
          vectors.add(
              new float[] {
                random().nextFloat() * 20 - 10,
                random().nextFloat() * 20 - 10,
                random().nextFloat() * 20 - 10
              });
        }
        var donor = TestClustering.cluster(vectors.subList(0, 32), 4, similarity);
        int[] carried = new int[vectors.size()];
        Arrays.fill(carried, -1);
        System.arraycopy(donor.assignments(), 0, carried, 0, 32);
        var hot = TestClustering.cluster(vectors, 4, similarity, donor.centroids(), carried);
        var rerouted = TestClustering.cluster(vectors, 4, similarity, donor.centroids(), null);
        assertEquals(12, hot.initialRouted());
        // Carrying donor members skips their initial routing, and reaper skips are approximate,
        // so the two runs may settle in different, equally good local optima.
        carriedObjective += TestClustering.objective(vectors, similarity, hot);
        reroutedObjective += TestClustering.objective(vectors, similarity, rerouted);
      }
      assertTrue(
          carriedObjective + " vs " + reroutedObjective,
          carriedObjective <= TestClustering.OBJECTIVE_TOLERANCE * reroutedObjective);
    }
  }

  public void testMergeWithoutCompatibleDonorFallsBackToCold() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig()
                  .setCodec(
                      TestUtil.alwaysKnnVectorsFormat(
                          new org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat()))
                  .setMergePolicy(NoMergePolicy.INSTANCE))) {
        add(writer, "first", 20, 0, false);
        writer.flush();
        add(writer, "second", 10, 100, false);
      }
      messages.clear();
      try (IndexWriter writer = new IndexWriter(dir, config(false))) {
        writer.getConfig().setMergePolicy(newLogMergePolicy());
        writer.forceMerge(1);
        assertMessage("source=cold", "carried=0", "initialRouted=30");
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          TestIVFasterEvoVectorsFormat.assertExact(
              reader.leaves().get(0).reader(), "v", VectorSimilarityFunction.EUCLIDEAN);
        }
      }
    }
  }

  public void testHotStartRetainsEmptyCellsAndExpandsSmallSeeds() throws Exception {
    float[][] seed = {{0}, {10}, {20}, {30}};
    var tiny =
        TestClustering.cluster(
            List.of(new float[] {11}), 4, VectorSimilarityFunction.EUCLIDEAN, seed, null);
    assertEquals(4, tiny.centroids().length);
    assertArrayEquals(new int[] {1}, tiny.assignments());
    assertArrayEquals(new float[] {11}, tiny.centroids()[1], 0f);
    assertArrayEquals(new float[] {30}, tiny.centroids()[3], 0f);
    var carried =
        TestClustering.cluster(
            List.of(new float[] {11}),
            4,
            VectorSimilarityFunction.EUCLIDEAN,
            tiny.centroids(),
            tiny.assignments());
    assertEquals(0, carried.initialRouted());
    assertEquals(1, carried.iterations());
    var expanded =
        TestClustering.cluster(
            List.of(new float[] {1}, new float[] {10}, new float[] {20}),
            3,
            VectorSimilarityFunction.EUCLIDEAN,
            new float[][] {{1}},
            new int[] {0, -1, -1});
    assertEquals(3, expanded.centroids().length);
    assertEquals(2, expanded.initialRouted());
    for (int i = 0; i < 3; i++) {
      assertEquals(
          0d,
          Clustering.distance(
              new float[] {i == 0 ? 1 : i * 10}, expanded.centroids()[expanded.assignments()[i]]),
          0d);
    }
  }
}
