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
package org.apache.lucene.util.hnsw;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;

public class TestCompletedNeighborEps extends LuceneTestCase {

  public void testUnmappedOrdReturnsNull() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(new OnHeapHnswGraph(8, 20), new HnswLock());
    HnswGraph[] graphs = new HnswGraph[] {sourceGraph()};
    assertNull(helper.getEps(0, 2, graphs));
    assertNull(helper.getEps(0, 19, graphs));
    assertNull(helper.getEps(1, 0, graphs));
    assertEquals(3, helper.leftoverWorkCount());
  }

  public void testGetEpsUsesSourceOrdNotMergedOrd() throws IOException {
    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    output.addNode(0, 11);
    output.getNeighbors(0, 11).addInOrder(7, 1f);
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {10, 11, -1, 13}}, graph());
    helper.bind(output, new HnswLock());
    helper.markCompleted(11);
    IntHashSet eps = helper.getEps(0, 0, new HnswGraph[] {sourceGraph()});
    assertEquals(2, eps.size());
    assertTrue(eps.contains(11));
    assertTrue(eps.contains(7));
    assertFalse(eps.contains(1));
    assertNull(helper.getEps(0, 10, new HnswGraph[] {sourceGraph()}));
  }

  public void testDeletedNeighborSkippedIncompleteOmittedCompletedContributesHops()
      throws IOException {
    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    // completed neighbor 1 lives in the output graph; incomplete 3 does not — getNeighbors(3)
    // would NPE/assert if the completion bit were ignored
    output.addNode(0, 1);
    output.getNeighbors(0, 1).addInOrder(10, 1f);
    output.getNeighbors(0, 1).addInOrder(11, 0.9f);

    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(output, new HnswLock());
    helper.markCompleted(1);

    IntHashSet eps = helper.getEps(0, 0, new HnswGraph[] {sourceGraph()});
    assertEquals(3, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
    assertTrue(eps.contains(11));
    assertFalse(eps.contains(2));
    assertFalse(eps.contains(3));
    assertFalse(helper.isCompleted(3));
  }

  public void testIncompleteNeighborDoesNotContributeHops() throws IOException {
    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    output.addNode(0, 3);
    output.getNeighbors(0, 3).addInOrder(12, 1f);

    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(output, new HnswLock());
    helper.markCompleted(1);
    output.addNode(0, 1);
    output.getNeighbors(0, 1).addInOrder(10, 1f);

    IntHashSet eps = helper.getEps(0, 0, new HnswGraph[] {sourceGraph()});
    assertEquals(2, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
    assertFalse(eps.contains(3));
    assertFalse(eps.contains(12));
  }

  public void testCollectLeftoverNeighborsIncludesIncompleteSkipsUnmapped() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    HnswGraph[] graphs = new HnswGraph[] {sourceGraph()};
    IntHashSet leftover = helper.collectLeftoverNeighbors(0, 0, graphs);
    assertEquals(2, leftover.size());
    assertTrue(leftover.contains(1));
    assertTrue(leftover.contains(3));
    assertFalse(leftover.contains(2));
    assertNull(helper.collectLeftoverNeighbors(0, 2, graphs));
    assertNull(helper.collectLeftoverNeighbors(0, 19, graphs));
    assertNull(helper.collectLeftoverNeighbors(1, 0, graphs));

    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    output.addNode(0, 1);
    output.getNeighbors(0, 1).addInOrder(10, 1f);
    helper.bind(output, new HnswLock());
    helper.markCompleted(1);
    IntHashSet eps = helper.getEps(0, 0, graphs);
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
    assertFalse(eps.contains(3));
    leftover = helper.collectLeftoverNeighbors(0, 0, graphs);
    assertTrue(leftover.contains(3));
    assertFalse(helper.isCompleted(3));
  }

  public void testBindRequired() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    expectThrows(
        IllegalStateException.class, () -> helper.getEps(0, 0, new HnswGraph[] {sourceGraph()}));
  }

  public void testCheapWhenOneHopFullBeamPresent() throws IOException {
    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    output.addNode(0, 1);
    output.getNeighbors(0, 1).addInOrder(10, 1f);
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(output, new HnswLock());
    helper.markCompleted(1);
    helper.markFullBeam(1);
    HnswGraph[] graphs = new HnswGraph[] {sourceGraph()};
    assertEquals(1, helper.leftoverFullBeamOneHopCount(0, 0, graphs));
    assertTrue(helper.hasFullBeamAnchor(0, 0, graphs));
    IntHashSet eps = helper.getEps(0, 0, graphs);
    assertEquals(2, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
  }

  public void testFullBeamWhenOnlyTwoHopOrBase() throws IOException {
    OnHeapHnswGraph output = new OnHeapHnswGraph(8, 20);
    output.addNode(0, 1);
    output.getNeighbors(0, 1).addInOrder(10, 1f);
    output.addNode(0, 10);
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(output, new HnswLock());
    // node 1 is copied-base (completed, not F); node 10 is a 2-hop that took full beam
    helper.markCompleted(1);
    helper.markCompleted(10);
    helper.markFullBeam(10);
    HnswGraph[] graphs = new HnswGraph[] {sourceGraph()};
    assertEquals(0, helper.leftoverFullBeamOneHopCount(0, 0, graphs));
    assertFalse(helper.hasFullBeamAnchor(0, 0, graphs));
    IntHashSet eps = helper.getEps(0, 0, graphs);
    assertEquals(2, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
  }

  public void testFullBeamWhenEmptyOneHop() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(new OnHeapHnswGraph(8, 20), new HnswLock());
    HnswGraph[] graphs = new HnswGraph[] {sourceGraph()};
    assertEquals(0, helper.leftoverFullBeamOneHopCount(0, 1, graphs));
    assertFalse(helper.hasFullBeamAnchor(0, 1, graphs));
    IntHashSet eps = helper.getEps(0, 1, graphs);
    assertEquals(0, eps.size());
  }

  public void testNewSourceGraphsCallsGetGraphOncePerCall() throws IOException {
    HnswGraph source = sourceGraph();
    CountingGraphReader reader = new CountingGraphReader(source);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            20, new int[][] {new int[] {0, 1, -1, 3}}, new KnnVectorsReader[] {reader}, "v");
    int afterCtor = reader.graphCount.get();
    assertEquals(1, afterCtor);
    HnswGraph[] first = helper.newSourceGraphs();
    assertEquals(afterCtor + 1, reader.graphCount.get());
    assertEquals(1, first.length);
    assertSame(source, first[0]);
    HnswGraph[] second = helper.newSourceGraphs();
    assertEquals(afterCtor + 2, reader.graphCount.get());
    assertEquals(1, second.length);
    assertSame(source, second[0]);
  }

  public void testConcurrentBuilderCallsGetGraphPerWorker() throws Exception {
    int size = 128;
    int dim = 8;
    int workers = 4;
    MockVectorValues vectors =
        MockVectorValues.fromValues(
            HnswGraphTestCase.createRandomFloatVectors(size, dim, random()));
    RandomVectorScorerSupplier scorerSupplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.EUCLIDEAN, vectors);
    CountingGraphReader reader = new CountingGraphReader(sourceGraph());
    int[] ordMap = new int[size];
    Arrays.fill(ordMap, -1);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(size, new int[][] {ordMap}, new KnnVectorsReader[] {reader}, "v");
    assertEquals(0, helper.leftoverWorkCount());
    ExecutorService exec =
        Executors.newFixedThreadPool(workers, new NamedThreadFactory("hnsw-eps-builder"));
    try {
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec),
              workers,
              scorerSupplier,
              16,
              new OnHeapHnswGraph(8, size),
              null,
              helper);
      builder.setBatchSize(8);
      builder.build(size);
      assertEquals(workers + 1, reader.graphCount.get());
      OnHeapHnswGraph graph = builder.getCompletedGraph();
      assertEquals(size, graph.size());
      for (int n = 0; n < size; n++) {
        assertTrue(helper.isCompleted(n));
        assertFalse(helper.isFullBeam(n));
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  public void testConcurrentMergeKeepsAllOrdinals() throws Exception {
    int segments = 4;
    int docsPerSegment = 32;
    int dim = 8;
    int workers = 8;
    int totalDocs = segments * docsPerSegment;
    ExecutorService exec =
        Executors.newFixedThreadPool(workers, new NamedThreadFactory("hnsw-eps-merge"));
    try (Directory dir = newDirectory()) {
      Lucene99HnswVectorsFormat format = new Lucene99HnswVectorsFormat(8, 16, workers, exec, 0);
      IndexWriterConfig writeCfg = new IndexWriterConfig();
      writeCfg.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      writeCfg.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, writeCfg)) {
        for (int s = 0; s < segments; s++) {
          for (int i = 0; i < docsPerSegment; i++) {
            Document doc = new Document();
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
              v[d] = random().nextFloat();
            }
            doc.add(new KnnFloatVectorField("v", v));
            w.addDocument(doc);
          }
          w.flush();
        }
        w.commit();
      }
      List<String> hnswMessages = new ArrayList<>();
      InfoStream capturing =
          new InfoStream() {
            @Override
            public void message(String component, String message) {
              if ("HNSW".equals(component)) {
                synchronized (hnswMessages) {
                  hnswMessages.add(message);
                }
              }
            }

            @Override
            public boolean isEnabled(String component) {
              return "HNSW".equals(component);
            }

            @Override
            public void close() {}
          };
      IndexWriterConfig mergeCfg = new IndexWriterConfig();
      mergeCfg.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      mergeCfg.setInfoStream(capturing);
      try (IndexWriter w = new IndexWriter(dir, mergeCfg)) {
        w.forceMerge(1);
      }
      boolean sawWorkers = false;
      synchronized (hnswMessages) {
        for (String message : hnswMessages) {
          if (message.contains("with " + workers + " workers")) {
            sawWorkers = true;
          }
          assertFalse("campaign log: " + message, message.contains("readahead-j"));
          assertFalse("campaign log: " + message, message.contains("extraF"));
          assertFalse("campaign log: " + message, message.contains("k=48"));
        }
      }
      assertTrue("concurrent merge path not taken: " + hnswMessages, sawWorkers);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext ctx = reader.leaves().get(0);
        assertEquals(totalDocs, ctx.reader().maxDoc());
        HnswGraph graph =
            ((Lucene99HnswVectorsReader)
                    ((CodecReader) ctx.reader()).getVectorReader().unwrapReaderForField("v"))
                .getGraph("v");
        assertEquals(totalDocs, graph.size());
        assertEquals(totalDocs, ctx.reader().getFloatVectorValues("v").size());
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  public void testLeftoverWorkIsGraphThenSourceNotMergedOrder() throws IOException {
    HnswGraph g0 = ringGraph();
    HnswGraph g1 = ringGraph();
    int n = g0.size();
    int[] map0 = new int[n];
    int[] map1 = new int[n];
    for (int s = 0; s < n; s++) {
      map0[s] = 1000 + s;
      map1[s] = s;
    }
    int maxOrd = 1000 + n;
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            maxOrd,
            new int[][] {map0, map1},
            new KnnVectorsReader[] {new CountingGraphReader(g0), new CountingGraphReader(g1)},
            "v");
    int[] j0 = leftoverJoinSetSourceOrds(g0, map0);
    int[] j1 = leftoverJoinSetSourceOrds(g1, map1);
    assertEquals(j0.length + j1.length, helper.joinSetWorkCount());
    assertEquals(n * 2, helper.leftoverWorkCount());
    int w = 0;
    for (int sourceOrd : j0) {
      assertEquals(0, helper.leftoverGraphIdx(w));
      assertEquals(sourceOrd, helper.leftoverSourceOrd(w));
      w++;
    }
    for (int sourceOrd : j1) {
      assertEquals(1, helper.leftoverGraphIdx(w));
      assertEquals(sourceOrd, helper.leftoverSourceOrd(w));
      w++;
    }
    int[] jMerged = new int[j0.length + j1.length];
    for (int i = 0; i < jMerged.length; i++) {
      jMerged[i] = helper.mergedOrd(helper.leftoverGraphIdx(i), helper.leftoverSourceOrd(i));
    }
    boolean nonDecreasing = true;
    for (int i = 1; i < jMerged.length; i++) {
      if (jMerged[i] < jMerged[i - 1]) {
        nonDecreasing = false;
        break;
      }
    }
    assertFalse("join-set prefix must not be merged-ord order", nonDecreasing);
    assertJoinSetPrefixThenRest(helper);
  }

  public void testLeftoverWorkPutsJoinSetBeforeRest() throws IOException {
    HnswGraph source = ringGraph();
    int maxOrd = source.size();
    int[] ordMap = identityMap(maxOrd);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            maxOrd,
            new int[][] {ordMap},
            new KnnVectorsReader[] {new CountingGraphReader(source)},
            "v");
    int[] expectedJ = leftoverJoinSetSourceOrds(source, ordMap);
    assertEquals(expectedJ.length, helper.joinSetWorkCount());
    assertJoinSetPrefixThenRest(helper);
    assertEquals(maxOrd, helper.leftoverWorkCount());
    for (int i = 0; i < expectedJ.length; i++) {
      assertEquals(0, helper.leftoverGraphIdx(i));
      assertEquals(expectedJ[i], helper.leftoverSourceOrd(i));
    }
  }

  public void testLeftoverWorkOmitsCopiedBaseMaps() throws IOException {
    HnswGraph leftover = ringGraph();
    int leftoverSize = leftover.size();
    int baseSize = leftoverSize;
    int[] leftoverMap = new int[leftoverSize];
    for (int i = 0; i < leftoverSize; i++) {
      leftoverMap[i] = baseSize + i;
    }
    int maxOrd = baseSize + leftoverSize;
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            maxOrd,
            new int[][] {leftoverMap},
            new KnnVectorsReader[] {new CountingGraphReader(leftover)},
            "v");
    int[] expectedJ = leftoverJoinSetSourceOrds(leftover, leftoverMap);
    assertEquals(expectedJ.length, helper.joinSetWorkCount());
    assertEquals(leftoverSize, helper.leftoverWorkCount());
    for (int i = 0; i < helper.leftoverWorkCount(); i++) {
      int merged = helper.mergedOrd(helper.leftoverGraphIdx(i), helper.leftoverSourceOrd(i));
      assertTrue(merged >= baseSize);
    }
    assertJoinSetPrefixThenRest(helper);
  }

  public void testUnmappedLeftoverNotInWorkAppearsAsHole() throws Exception {
    HnswGraph source = ringGraph();
    int mapped = source.size();
    int maxOrd = mapped + 2;
    int[] ordMap = identityMap(mapped);
    CountingGraphReader reader = new CountingGraphReader(source);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            maxOrd, new int[][] {ordMap}, new KnnVectorsReader[] {reader}, "v");
    int[] expectedJ = leftoverJoinSetSourceOrds(source, ordMap);
    assertEquals(expectedJ.length, helper.joinSetWorkCount());
    assertEquals(mapped, helper.leftoverWorkCount());
    assertJoinSetPrefixThenRest(helper);
    for (int i = 0; i < helper.leftoverWorkCount(); i++) {
      int merged = helper.mergedOrd(helper.leftoverGraphIdx(i), helper.leftoverSourceOrd(i));
      assertTrue(merged < mapped);
    }
    MockVectorValues vectors =
        MockVectorValues.fromValues(
            HnswGraphTestCase.createRandomFloatVectors(maxOrd, 8, random()));
    RandomVectorScorerSupplier scorerSupplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.EUCLIDEAN, vectors);
    ExecutorService exec = Executors.newFixedThreadPool(1, new NamedThreadFactory("hnsw-hole"));
    try {
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec),
              1,
              scorerSupplier,
              16,
              new OnHeapHnswGraph(8, maxOrd),
              null,
              helper);
      builder.setBatchSize(2);
      builder.build(maxOrd);
      OnHeapHnswGraph graph = builder.getCompletedGraph();
      assertEquals(maxOrd, graph.size());
      assertTrue(helper.isCompleted(mapped));
      assertTrue(helper.isCompleted(mapped + 1));
      assertFalse(helper.isFullBeam(mapped));
      assertFalse(helper.isFullBeam(mapped + 1));
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  public void testIsJoinSetIsSourceOrdNotMergedOrd() throws IOException {
    HnswGraph source = ringGraph();
    int mapped = source.size();
    int[] ordMap = new int[mapped];
    for (int s = 0; s < mapped; s++) {
      ordMap[s] = 10 + s;
    }
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            10 + mapped,
            new int[][] {ordMap},
            new KnnVectorsReader[] {new CountingGraphReader(source)},
            "v");
    IntHashSet expected = UpdateGraphsUtils.computeJoinSet(source);
    for (int s = 0; s < mapped; s++) {
      assertEquals(expected.contains(s), helper.isJoinSet(0, s));
    }
    assertFalse(helper.isJoinSet(0, 10));
    assertFalse(helper.isJoinSet(1, 0));
  }

  public void testJoinSetNodeFullBeamsEvenWithFullBeamAnchor() throws Exception {
    HnswGraph source = ringGraph();
    int size = source.size();
    int[] ordMap = identityMap(size);
    int[] joinOrds = leftoverJoinSetSourceOrds(source, ordMap);
    assertTrue("join set empty", joinOrds.length > 0);
    int joinNode = -1;
    int anchor = -1;
    for (int candidate : joinOrds) {
      source.seek(0, candidate);
      int neighbor = source.nextNeighbor();
      if (neighbor != NO_MORE_DOCS) {
        joinNode = candidate;
        anchor = neighbor;
        break;
      }
    }
    assertTrue(joinNode >= 0);
    assertTrue(anchor >= 0);
    assertNotEquals(joinNode, anchor);

    FixedBitSet initialized = new FixedBitSet(size);
    initialized.set(anchor);
    CountingGraphReader reader = new CountingGraphReader(source);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(size, new int[][] {ordMap}, new KnnVectorsReader[] {reader}, "v");
    OnHeapHnswGraph hnsw = new OnHeapHnswGraph(8, size);
    hnsw.addNode(0, anchor);
    helper.markCompleted(anchor);
    helper.markFullBeam(anchor);

    MockVectorValues vectors =
        MockVectorValues.fromValues(HnswGraphTestCase.createRandomFloatVectors(size, 8, random()));
    RandomVectorScorerSupplier scorerSupplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.EUCLIDEAN, vectors);
    ExecutorService exec =
        Executors.newFixedThreadPool(1, new NamedThreadFactory("hnsw-j-alwaysf"));
    try {
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec), 1, scorerSupplier, 16, hnsw, initialized, helper);
      assertTrue(helper.isJoinSet(0, joinNode));
      assertTrue(helper.hasFullBeamAnchor(0, joinNode, helper.newSourceGraphs()));
      builder.setBatchSize(size);
      builder.build(size);
      assertTrue(helper.isFullBeam(joinNode));
      for (int i = 0; i < helper.joinSetWorkCount(); i++) {
        int sourceOrd = helper.leftoverSourceOrd(i);
        if (initialized.get(helper.mergedOrd(0, sourceOrd))) {
          continue;
        }
        assertTrue(helper.isFullBeam(helper.mergedOrd(0, sourceOrd)));
      }
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  public void testBuilderStealsJoinSetThenRest() throws Exception {
    HnswGraph source = ringGraph();
    int size = source.size();
    int[] ordMap = identityMap(size);
    CountingGraphReader reader = new CountingGraphReader(source);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(size, new int[][] {ordMap}, new KnnVectorsReader[] {reader}, "v");
    MockVectorValues vectors =
        MockVectorValues.fromValues(HnswGraphTestCase.createRandomFloatVectors(size, 8, random()));
    RandomVectorScorerSupplier scorerSupplier =
        DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
            VectorSimilarityFunction.EUCLIDEAN, vectors);
    ExecutorService exec = Executors.newFixedThreadPool(1, new NamedThreadFactory("hnsw-j-order"));
    try {
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec),
              1,
              scorerSupplier,
              16,
              new OnHeapHnswGraph(8, size),
              null,
              helper);
      int[] expectedJ = leftoverJoinSetSourceOrds(source, ordMap);
      assertEquals(expectedJ.length, helper.joinSetWorkCount());
      for (int i = 0; i < expectedJ.length; i++) {
        assertEquals(expectedJ[i], helper.leftoverSourceOrd(i));
      }
      assertJoinSetPrefixThenRest(helper);
      builder.setBatchSize(2);
      builder.build(size);
      assertEquals(size, builder.getCompletedGraph().size());
      for (int i = 0; i < helper.joinSetWorkCount(); i++) {
        assertTrue(helper.isFullBeam(helper.mergedOrd(0, helper.leftoverSourceOrd(i))));
      }
      boolean sawCheapRest = false;
      for (int i = helper.joinSetWorkCount(); i < helper.leftoverWorkCount(); i++) {
        int sourceOrd = helper.leftoverSourceOrd(i);
        int merged = helper.mergedOrd(0, sourceOrd);
        source.seek(0, sourceOrd);
        boolean joinNeighborInF = false;
        for (int v = source.nextNeighbor(); v != NO_MORE_DOCS; v = source.nextNeighbor()) {
          if (v >= 0
              && v < ordMap.length
              && helper.isJoinSet(0, v)
              && helper.isFullBeam(ordMap[v])) {
            joinNeighborInF = true;
            break;
          }
        }
        if (joinNeighborInF) {
          assertFalse("rest with F 1-hop must be cheap: " + sourceOrd, helper.isFullBeam(merged));
          sawCheapRest = true;
        }
      }
      assertTrue("ring leftover rest should have an F 1-hop", sawCheapRest);
    } finally {
      exec.shutdown();
      assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private static void assertJoinSetPrefixThenRest(CompletedNeighborEps helper) {
    int jSize = helper.joinSetWorkCount();
    for (int i = 0; i < jSize; i++) {
      assertTrue(
          "prefix must be join-set: " + helper.leftoverSourceOrd(i),
          helper.isJoinSet(helper.leftoverGraphIdx(i), helper.leftoverSourceOrd(i)));
    }
    for (int i = jSize; i < helper.leftoverWorkCount(); i++) {
      assertFalse(
          "rest must not be join-set: " + helper.leftoverSourceOrd(i),
          helper.isJoinSet(helper.leftoverGraphIdx(i), helper.leftoverSourceOrd(i)));
    }
  }

  private static int[] leftoverJoinSetSourceOrds(HnswGraph source, int[] ordMap)
      throws IOException {
    IntHashSet j = UpdateGraphsUtils.computeJoinSet(source);
    int[] nodes = j.toArray();
    Arrays.sort(nodes);
    int[] mapped = new int[nodes.length];
    int w = 0;
    for (int sourceOrd : nodes) {
      if (sourceOrd < 0 || sourceOrd >= ordMap.length) {
        continue;
      }
      if (ordMap[sourceOrd] == -1) {
        continue;
      }
      mapped[w++] = sourceOrd;
    }
    return Arrays.copyOf(mapped, w);
  }

  private static int[] identityMap(int size) {
    int[] ordMap = new int[size];
    for (int i = 0; i < size; i++) {
      ordMap[i] = i;
    }
    return ordMap;
  }

  private static CompletedNeighborEps newHelper(
      int maxOrd, int[][] ordMaps, KnnVectorsReader reader) throws IOException {
    return new CompletedNeighborEps(maxOrd, ordMaps, new KnnVectorsReader[] {reader}, "v");
  }

  private static CountingGraphReader graph() {
    return new CountingGraphReader(sourceGraph());
  }

  private static HnswGraph ringGraph() {
    int[][][] nodes = new int[1][6][];
    nodes[0][0] = new int[] {1, 5};
    nodes[0][1] = new int[] {0, 2};
    nodes[0][2] = new int[] {1, 3};
    nodes[0][3] = new int[] {2, 4};
    nodes[0][4] = new int[] {3, 5};
    nodes[0][5] = new int[] {4, 0};
    return new TestHnswUtil.MockGraph(nodes);
  }

  private static HnswGraph sourceGraph() {
    int[][][] nodes = new int[1][4][];
    nodes[0][0] = new int[] {1, 2, 3};
    nodes[0][1] = new int[] {};
    nodes[0][2] = new int[] {};
    nodes[0][3] = new int[] {};
    return new TestHnswUtil.MockGraph(nodes);
  }

  private static final class CountingGraphReader extends KnnVectorsReader
      implements HnswGraphProvider {
    private final HnswGraph graph;
    final AtomicInteger graphCount = new AtomicInteger();

    CountingGraphReader(HnswGraph graph) {
      this.graph = graph;
    }

    @Override
    public HnswGraph getGraph(String field) {
      graphCount.incrementAndGet();
      return graph;
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) {}

    @Override
    public FloatVectorValues getFloatVectorValues(String field) {
      return null;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
      return null;
    }

    @Override
    public Float16VectorValues getFloat16VectorValues(String field) {
      return null;
    }

    @Override
    public void search(
        String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

    @Override
    public void search(
        String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

    @Override
    public void search(
        String field, short[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) {}

    @Override
    public void close() {}
  }
}
