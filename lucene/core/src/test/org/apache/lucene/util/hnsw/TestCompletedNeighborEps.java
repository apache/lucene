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
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;

public class TestCompletedNeighborEps extends LuceneTestCase {

  public void testUnmappedOrdReturnsNull() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    helper.bind(new OnHeapHnswGraph(8, 20), new HnswLock());
    assertNull(helper.getEps(2, new HnswGraph[] {sourceGraph()}));
    assertNull(helper.getEps(19, new HnswGraph[] {sourceGraph()}));
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

    IntHashSet eps = helper.getEps(0, new HnswGraph[] {sourceGraph()});
    assertEquals(3, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
    assertTrue(eps.contains(11));
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

    IntHashSet eps = helper.getEps(0, new HnswGraph[] {sourceGraph()});
    assertEquals(2, eps.size());
    assertTrue(eps.contains(1));
    assertTrue(eps.contains(10));
    assertFalse(eps.contains(3));
    assertFalse(eps.contains(12));
  }

  public void testBindRequired() throws IOException {
    CompletedNeighborEps helper = newHelper(20, new int[][] {new int[] {0, 1, -1, 3}}, graph());
    expectThrows(
        IllegalStateException.class, () -> helper.getEps(0, new HnswGraph[] {sourceGraph()}));
  }

  public void testNewSourceGraphsCallsGetGraphOncePerCall() throws IOException {
    HnswGraph source = sourceGraph();
    CountingGraphReader reader = new CountingGraphReader(source);
    CompletedNeighborEps helper =
        new CompletedNeighborEps(
            20, new int[][] {new int[] {0, 1, -1, 3}}, new KnnVectorsReader[] {reader}, "v");
    HnswGraph[] first = helper.newSourceGraphs();
    assertEquals(1, reader.graphCount.get());
    assertEquals(1, first.length);
    assertSame(source, first[0]);
    HnswGraph[] second = helper.newSourceGraphs();
    assertEquals(2, reader.graphCount.get());
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
      assertEquals(workers, reader.graphCount.get());
      OnHeapHnswGraph graph = builder.getCompletedGraph();
      assertEquals(size, graph.size());
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

  private static CompletedNeighborEps newHelper(
      int maxOrd, int[][] ordMaps, KnnVectorsReader reader) {
    return new CompletedNeighborEps(maxOrd, ordMaps, new KnnVectorsReader[] {reader}, "v");
  }

  private static CountingGraphReader graph() {
    return new CountingGraphReader(sourceGraph());
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
