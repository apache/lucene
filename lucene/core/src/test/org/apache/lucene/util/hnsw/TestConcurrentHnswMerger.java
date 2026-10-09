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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Compares {@link ConcurrentHnswMerger} with the single-threaded {@link IncrementalHnswGraphMerger}
 * when merging several graphs.
 */
public class TestConcurrentHnswMerger extends LuceneTestCase {

  private static final int DIM = 16;
  private static final int M = 16;
  private static final int BEAM_WIDTH = 100;
  private static final String VECTOR_FIELD = "v";
  private static final String ID_FIELD = "id";

  /**
   * The concurrent merger must reuse the structure of every graph without deletions, as the serial
   * merger does, rather than only the structure of the largest one. Rebuilding the other graphs
   * from scratch shows up as many more vector comparisons than the serial merge.
   */
  public void testScoresAboutAsManyVectorsAsSerialMerge() throws IOException {
    int[] segmentSizes = {
      TestUtil.nextInt(random(), 1500, 2000),
      TestUtil.nextInt(random(), 1000, 1500),
      TestUtil.nextInt(random(), 500, 1000)
    };
    List<float[]> vectors = randomVectors(segmentSizes);
    try (Directory dir = newDirectory()) {
      buildIndex(dir, vectors, segmentSizes);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        MergeResult serial = merge(reader, vectors, 0);
        MergeResult concurrent = merge(reader, vectors, TestUtil.nextInt(random(), 1, 4));
        String message =
            String.format(
                Locale.ROOT,
                "serial merge scored %d vectors, concurrent merge scored %d",
                serial.scoreCount,
                concurrent.scoreCount);
        if (VERBOSE) {
          System.out.println(message);
        }
        assertTrue(message, concurrent.scoreCount < serial.scoreCount * 1.25);
      }
    }
  }

  /**
   * With one worker the concurrent join inserts the same nodes in the same order, from the same
   * entry points, as the serial merger, so it must build exactly the same graph.
   */
  public void testOneWorkerBuildsSameGraphAsSerialMerge() throws IOException {
    int[] segmentSizes = {
      TestUtil.nextInt(random(), 500, 1000),
      TestUtil.nextInt(random(), 200, 500),
      TestUtil.nextInt(random(), 50, 200)
    };
    List<float[]> vectors = randomVectors(segmentSizes);
    try (Directory dir = newDirectory()) {
      buildIndex(dir, vectors, segmentSizes);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        OnHeapHnswGraph serial = merge(reader, vectors, 0).graph;
        OnHeapHnswGraph concurrent = merge(reader, vectors, 1).graph;
        assertEquals(serial.numLevels(), concurrent.numLevels());
        assertEquals(serial.entryNode(), concurrent.entryNode());
        for (int level = 0; level < serial.numLevels(); level++) {
          assertEquals(neighborsByNode(serial, level), neighborsByNode(concurrent, level));
        }
      }
    }
  }

  /** The concurrent merge must not trade graph quality for its speed. */
  public void testRecallComparableToSerialMerge() throws IOException {
    int[] segmentSizes = {
      TestUtil.nextInt(random(), 1000, 1500),
      TestUtil.nextInt(random(), 500, 1000),
      TestUtil.nextInt(random(), 200, 500),
      TestUtil.nextInt(random(), 50, 200)
    };
    List<float[]> vectors = randomVectors(segmentSizes);
    try (Directory dir = newDirectory()) {
      buildIndex(dir, vectors, segmentSizes);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        MergeResult serial = merge(reader, vectors, 0);
        MergeResult concurrent = merge(reader, vectors, TestUtil.nextInt(random(), 2, 4));
        assertEquals(vectors.size(), concurrent.graph.size());
        assertNoIsolatedNodes(concurrent.graph);
        assertRecallComparable(serial, concurrent);
      }
    }
  }

  /**
   * When the base graph has deletions, the concurrent merger repairs it with all workers before it
   * joins the other graphs into it.
   */
  public void testRecallComparableToSerialMergeWithDeletesInBaseGraph() throws IOException {
    int[] segmentSizes = {
      TestUtil.nextInt(random(), 1500, 2000),
      TestUtil.nextInt(random(), 500, 800),
      TestUtil.nextInt(random(), 200, 400)
    };
    List<float[]> vectors = randomVectors(segmentSizes);
    // a fifth of the largest segment: few enough for it to stay the base graph
    Set<Integer> deleted = new HashSet<>();
    while (deleted.size() < segmentSizes[0] / 5) {
      deleted.add(random().nextInt(segmentSizes[0]));
    }
    try (Directory dir = newDirectory()) {
      buildIndex(dir, vectors, segmentSizes, deleted);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(deleted.size(), reader.leaves().get(0).reader().numDeletedDocs());
        MergeResult serial = merge(reader, vectors, 0);
        MergeResult concurrent = merge(reader, vectors, TestUtil.nextInt(random(), 2, 4));
        assertEquals(vectors.size() - deleted.size(), concurrent.graph.size());
        assertNoIsolatedNodes(concurrent.graph);
        assertRecallComparable(serial, concurrent);
      }
    }
  }

  private static void assertRecallComparable(MergeResult serial, MergeResult concurrent)
      throws IOException {
    List<float[]> queries = randomVectors(new int[] {100});
    double serialRecall = recall(serial.graph, serial.vectors, queries);
    double concurrentRecall = recall(concurrent.graph, concurrent.vectors, queries);
    String message =
        String.format(
            Locale.ROOT,
            "serial recall %.3f, concurrent recall %.3f",
            serialRecall,
            concurrentRecall);
    if (VERBOSE) {
      System.out.println(message);
    }
    assertTrue(message, concurrentRecall > 0.9);
    assertTrue(message, concurrentRecall > serialRecall - 0.05);
  }

  /** The merged graph, the vectors it was built from, and how many vectors the merge scored. */
  private record MergeResult(OnHeapHnswGraph graph, List<float[]> vectors, long scoreCount) {}

  /**
   * Merges every segment of the reader, dropping deleted documents; uses the serial merger when
   * {@code numWorkers} is 0. {@code vectors} holds the vector of every document, in doc ID order.
   */
  private static MergeResult merge(DirectoryReader reader, List<float[]> vectors, int numWorkers)
      throws IOException {
    List<float[]> liveVectors = new ArrayList<>();
    MergeState.DocMap[] docMaps = new MergeState.DocMap[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      Bits liveDocs = ctx.reader().getLiveDocs();
      int[] newDocIds = new int[ctx.reader().maxDoc()];
      for (int doc = 0; doc < newDocIds.length; doc++) {
        if (liveDocs == null || liveDocs.get(doc)) {
          newDocIds[doc] = liveVectors.size();
          liveVectors.add(vectors.get(ctx.docBase + doc));
        } else {
          newDocIds[doc] = -1;
        }
      }
      docMaps[ctx.ord] = doc -> newDocIds[doc];
    }
    FloatVectorValues mergedVectors = FloatVectorValues.fromFloats(liveVectors, DIM);
    LongAdder scoreCount = new LongAdder();
    RandomVectorScorerSupplier scorerSupplier =
        new CountingScorerSupplier(
            DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(
                VectorSimilarityFunction.EUCLIDEAN, mergedVectors),
            scoreCount);
    FieldInfo fieldInfo = reader.leaves().get(0).reader().getFieldInfos().fieldInfo(VECTOR_FIELD);
    ExecutorService exec = null;
    try {
      IncrementalHnswGraphMerger merger;
      if (numWorkers == 0) {
        merger = new IncrementalHnswGraphMerger(fieldInfo, scorerSupplier, M, BEAM_WIDTH);
      } else {
        exec = Executors.newFixedThreadPool(numWorkers, new NamedThreadFactory("hnswMerge"));
        merger =
            new ConcurrentHnswMerger(
                fieldInfo, scorerSupplier, M, BEAM_WIDTH, new TaskExecutor(exec), numWorkers);
      }
      for (LeafReaderContext ctx : reader.leaves()) {
        CodecReader segment = (CodecReader) ctx.reader();
        merger.addReader(segment.getVectorReader(), docMaps[ctx.ord], segment.getLiveDocs());
      }
      OnHeapHnswGraph graph =
          merger.merge(mergedVectors, InfoStream.NO_OUTPUT, mergedVectors.size());
      return new MergeResult(graph, liveVectors, scoreCount.sum());
    } finally {
      if (exec != null) {
        TestUtil.shutdownExecutorService(exec);
      }
    }
  }

  /** The sorted neighbors of every node on a level, keyed by node. */
  private static Map<Integer, List<Integer>> neighborsByNode(OnHeapHnswGraph graph, int level)
      throws IOException {
    Map<Integer, List<Integer>> neighborsByNode = new HashMap<>();
    HnswGraph.NodesIterator nodes = graph.getNodesOnLevel(level);
    while (nodes.hasNext()) {
      int node = nodes.nextInt();
      List<Integer> neighbors = new ArrayList<>();
      graph.seek(level, node);
      for (int n = graph.nextNeighbor(); n != NO_MORE_DOCS; n = graph.nextNeighbor()) {
        neighbors.add(n);
      }
      Collections.sort(neighbors);
      neighborsByNode.put(node, neighbors);
    }
    return neighborsByNode;
  }

  private static void assertNoIsolatedNodes(OnHeapHnswGraph graph) throws IOException {
    for (int node = 0; node < graph.size(); node++) {
      graph.seek(0, node);
      assertNotEquals("node " + node + " has no neighbors", NO_MORE_DOCS, graph.nextNeighbor());
    }
  }

  /** Recall of the top 10, searching with a beam of 50. */
  private static double recall(OnHeapHnswGraph graph, List<float[]> vectors, List<float[]> queries)
      throws IOException {
    int topK = 10;
    int beamWidth = 50;
    FloatVectorValues values = FloatVectorValues.fromFloats(vectors, DIM);
    int matches = 0;
    for (float[] query : queries) {
      RandomVectorScorer scorer =
          DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(
              VectorSimilarityFunction.EUCLIDEAN, values.copy(), query);
      ScoreDoc[] actual =
          HnswGraphSearcher.search(scorer, beamWidth, graph, null, Integer.MAX_VALUE)
              .topDocs()
              .scoreDocs;
      NeighborQueue expected = new NeighborQueue(topK, false);
      for (int ord = 0; ord < vectors.size(); ord++) {
        expected.add(ord, VectorSimilarityFunction.EUCLIDEAN.compare(query, vectors.get(ord)));
        if (expected.size() > topK) {
          expected.pop();
        }
      }
      for (int ord : expected.nodes()) {
        for (int i = 0; i < Math.min(topK, actual.length); i++) {
          if (actual[i].doc == ord) {
            matches++;
            break;
          }
        }
      }
    }
    return matches / (double) (topK * queries.size());
  }

  private static List<float[]> randomVectors(int[] segmentSizes) {
    List<float[]> vectors = new ArrayList<>();
    for (int size : segmentSizes) {
      for (int i = 0; i < size; i++) {
        float[] v = new float[DIM];
        for (int j = 0; j < DIM; j++) {
          v[j] = random().nextFloat();
        }
        vectors.add(v);
      }
    }
    return vectors;
  }

  /** Flushes one segment per entry of {@code segmentSizes}, in order. */
  private static void buildIndex(Directory dir, List<float[]> vectors, int[] segmentSizes)
      throws IOException {
    buildIndex(dir, vectors, segmentSizes, Set.of());
  }

  /**
   * Flushes one segment per entry of {@code segmentSizes}, in order, then deletes the documents
   * whose position in {@code vectors} is in {@code deleted}.
   */
  private static void buildIndex(
      Directory dir, List<float[]> vectors, int[] segmentSizes, Set<Integer> deleted)
      throws IOException {
    IndexWriterConfig cfg = new IndexWriterConfig();
    cfg.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, BEAM_WIDTH, 0)));
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      int ord = 0;
      for (int size : segmentSizes) {
        for (int i = 0; i < size; i++) {
          Document doc = new Document();
          doc.add(new StringField(ID_FIELD, Integer.toString(ord), Field.Store.NO));
          doc.add(
              new KnnFloatVectorField(
                  VECTOR_FIELD, vectors.get(ord++), VectorSimilarityFunction.EUCLIDEAN));
          w.addDocument(doc);
        }
        w.flush();
      }
      for (int deletedOrd : deleted) {
        w.deleteDocuments(new Term(ID_FIELD, Integer.toString(deletedOrd)));
      }
      w.commit();
    }
  }

  /** Counts how many vectors the scorers it creates have scored, across all copies. */
  private record CountingScorerSupplier(RandomVectorScorerSupplier in, LongAdder count)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      UpdateableRandomVectorScorer scorer = in.scorer();
      return new UpdateableRandomVectorScorer() {
        @Override
        public void setScoringOrdinal(int node) throws IOException {
          scorer.setScoringOrdinal(node);
        }

        @Override
        public float score(int node) throws IOException {
          count.increment();
          return scorer.score(node);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
          count.add(numNodes);
          return scorer.bulkScore(nodes, scores, numNodes);
        }

        @Override
        public int maxOrd() {
          return scorer.maxOrd();
        }

        @Override
        public int ordToDoc(int ord) {
          return scorer.ordToDoc(ord);
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
          return scorer.getAcceptOrds(acceptDocs);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new CountingScorerSupplier(in.copy(), count);
    }
  }
}
