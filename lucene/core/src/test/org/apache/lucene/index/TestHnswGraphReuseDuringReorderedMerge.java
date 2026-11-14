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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.hnsw.HnswGraph;

/**
 * Tests that merging through a {@link SortingCodecReader} still reuses the HNSW graphs of the
 * source segments. When a merge policy reorders a merge, the codec sees a single {@link
 * SortingCodecReader} over a {@link SlowCompositeCodecReaderWrapper}, so the source graphs are only
 * reachable through those wrappers.
 *
 * <p>The assertions key on the InfoStream message of {@code MergingHnswGraphBuilder}, used by the
 * single-threaded merger that the default format selects.
 */
public class TestHnswGraphReuseDuringReorderedMerge extends LuceneTestCase {

  private static final int DIM = 8;

  /**
   * Above the tiny-segment cutoff of {@code Lucene99HnswVectorsWriter#shouldCreateGraph}, so graphs
   * are built at both flush and merge time.
   */
  private static final int DOCS_PER_SEGMENT = 1500;

  public void testReorderedMergeReusesGraphs() throws Exception {
    int numSegments = 2;
    try (Directory dir = newDirectory()) {
      writeSegments(dir, numSegments, false);
      List<String> hnswMessages = forceMergeReordered(dir, null);
      assertMergedFromGraphs(hnswMessages, numSegments);
      assertMergedIndex(dir, numSegments * DOCS_PER_SEGMENT);
    }
  }

  public void testReorderedSingletonMergeReusesGraph() throws Exception {
    try (Directory dir = newDirectory()) {
      writeSegments(dir, 1, true);
      // a single segment is only merged when it has deletions
      List<String> hnswMessages = forceMergeReordered(dir, new Term("id", "0"));
      assertMergedFromGraphs(hnswMessages, 1);
      assertMergedIndex(dir, DOCS_PER_SEGMENT - 1);
    }
  }

  /**
   * Same as {@link #testReorderedSingletonMergeReusesGraph} with the concurrent merger, which only
   * logs graph reuse when it repairs a reused graph that has deletions.
   */
  public void testReorderedSingletonConcurrentMergeReusesGraph() throws Exception {
    ExecutorService mergeExec =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("hnsw-merge-worker"));
    try (Directory dir = newDirectory()) {
      Codec codec =
          TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(16, 100, 2, mergeExec));
      writeSegments(dir, codec, 1, true);
      List<String> hnswMessages = forceMergeReordered(dir, codec, new Term("id", "0"));
      synchronized (hnswMessages) {
        assertTrue(
            "expected the reordered merge to reuse the source graph, but HNSW messages were: "
                + hnswMessages,
            hnswMessages.stream().anyMatch(m -> m.startsWith("repaired reused graph")));
      }
      assertMergedIndex(dir, DOCS_PER_SEGMENT - 1);
    } finally {
      mergeExec.shutdown();
      assertTrue(mergeExec.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /**
   * Adding a sorted view of a deletion-free segment, as {@code BpVectorReorderer} does to reorder
   * an index, copies the source graph with renumbered nodes instead of rebuilding it.
   */
  public void testAddIndexesOfSortedReaderCopiesGraph() throws Exception {
    try (Directory source = newDirectory();
        Directory target = newDirectory()) {
      writeSegments(source, 1, false);
      List<String> hnswMessages = new ArrayList<>();
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.getDefaultCodec());
      cfg.setInfoStream(hnswInfoStream(hnswMessages));
      try (DirectoryReader reader = DirectoryReader.open(source);
          IndexWriter w = new IndexWriter(target, cfg)) {
        CodecReader leaf = (CodecReader) reader.leaves().getFirst().reader();
        w.addIndexes(SortingCodecReader.wrap(leaf, reverse(leaf.maxDoc()), null));
      }
      assertMergedFromGraphs(hnswMessages, 1);

      try (DirectoryReader sourceReader = DirectoryReader.open(source);
          DirectoryReader targetReader = DirectoryReader.open(target)) {
        HnswGraph sourceGraph = getGraph(sourceReader);
        HnswGraph targetGraph = getGraph(targetReader);
        // every doc has a vector, so ordinals equal doc IDs and reversing docs reverses ordinals
        int size = sourceGraph.size();
        assertEquals(size, targetGraph.size());
        assertEquals(sourceGraph.numLevels(), targetGraph.numLevels());
        // the entry node is not stored but read as the smallest node on the top level, so it is
        // checked by comparing the nodes on that level
        for (int level = 0; level < sourceGraph.numLevels(); level++) {
          Set<Integer> reversedSourceNodes = new HashSet<>();
          for (int node : nodesOnLevel(sourceGraph, level)) {
            reversedSourceNodes.add(size - 1 - node);
          }
          assertEquals(reversedSourceNodes, nodesOnLevel(targetGraph, level));
          for (int node : nodesOnLevel(sourceGraph, level)) {
            Set<Integer> reversedNeighbors = new HashSet<>();
            for (int neighbor : neighbors(sourceGraph, level, node)) {
              reversedNeighbors.add(size - 1 - neighbor);
            }
            assertEquals(
                "neighbors of node " + node + " on level " + level,
                reversedNeighbors,
                neighbors(targetGraph, level, size - 1 - node));
          }
        }
      }
    }
  }

  /**
   * The live docs that the merge sees for a view can differ from those of the readers it wraps,
   * here because a filter hides documents of a sorted view over two segments. The merger must judge
   * the wrapped readers by the documents that the view hides, or it treats their graphs as
   * deletion-free while their hidden vectors have no ordinal in the merged segment.
   */
  public void testAddIndexesOfFilteredSortedView() throws Exception {
    try (Directory source = newDirectory();
        Directory target = newDirectory()) {
      writeSegments(source, TestUtil.getDefaultCodec(), false, true, 2000, 1500);
      List<String> hnswMessages = new ArrayList<>();
      IndexWriterConfig cfg = new IndexWriterConfig();
      cfg.setCodec(TestUtil.getDefaultCodec());
      cfg.setInfoStream(hnswInfoStream(hnswMessages));
      int expectedNumDocs;
      try (DirectoryReader reader = DirectoryReader.open(source);
          IndexWriter w = new IndexWriter(target, cfg)) {
        List<CodecReader> leaves = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
          leaves.add((CodecReader) ctx.reader());
        }
        CodecReader composite = SlowCompositeCodecReaderWrapper.wrap(leaves);
        CodecReader sorted = SortingCodecReader.wrap(composite, reverse(composite.maxDoc()), null);
        FixedBitSet liveDocs = new FixedBitSet(sorted.maxDoc());
        liveDocs.set(0, sorted.maxDoc());
        for (int doc = 0; doc < sorted.maxDoc(); doc += 10) {
          liveDocs.clear(doc);
        }
        expectedNumDocs = liveDocs.cardinality();
        w.addIndexes(
            new FilterCodecReader(sorted) {
              @Override
              public Bits getLiveDocs() {
                return liveDocs;
              }

              @Override
              public int numDocs() {
                return expectedNumDocs;
              }

              @Override
              public CacheHelper getCoreCacheHelper() {
                return null;
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }
            });
      }
      // both graphs lost vectors, so only the larger one is reused
      assertMergedFromGraphs(hnswMessages, 1);
      try (DirectoryReader reader = DirectoryReader.open(target)) {
        assertEquals(expectedNumDocs, reader.numDocs());
      }
      assertTrue(TestUtil.checkIndex(target).clean);
      assertRecall(target);
    }
  }

  private static void writeSegments(Directory dir, int numSegments, boolean withIds)
      throws IOException {
    writeSegments(dir, TestUtil.getDefaultCodec(), numSegments, withIds);
  }

  private static void writeSegments(Directory dir, Codec codec, int numSegments, boolean withIds)
      throws IOException {
    int[] segmentSizes = new int[numSegments];
    Arrays.fill(segmentSizes, DOCS_PER_SEGMENT);
    writeSegments(dir, codec, withIds, false, segmentSizes);
  }

  /**
   * @param sparse whether every fourth document has no vector, so that vector ordinals differ from
   *     doc IDs
   */
  private static void writeSegments(
      Directory dir, Codec codec, boolean withIds, boolean sparse, int... segmentSizes)
      throws IOException {
    IndexWriterConfig cfg = new IndexWriterConfig();
    cfg.setCodec(codec);
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (int segmentSize : segmentSizes) {
        for (int i = 0; i < segmentSize; i++) {
          Document doc = new Document();
          if (sparse == false || i % 4 != 0) {
            doc.add(new KnnFloatVectorField("v", randomVector()));
          }
          if (withIds) {
            doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          }
          w.addDocument(doc);
        }
        w.flush();
      }
    }
  }

  private static List<String> forceMergeReordered(Directory dir, Term toDelete) throws IOException {
    return forceMergeReordered(dir, TestUtil.getDefaultCodec(), toDelete);
  }

  private static List<String> forceMergeReordered(Directory dir, Codec codec, Term toDelete)
      throws IOException {
    List<String> hnswMessages = new ArrayList<>();
    IndexWriterConfig cfg = new IndexWriterConfig();
    cfg.setCodec(codec);
    cfg.setMergePolicy(new ReversingMergePolicy(newLogMergePolicy()));
    cfg.setInfoStream(hnswInfoStream(hnswMessages));
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      if (toDelete != null) {
        w.deleteDocuments(toDelete);
      }
      w.forceMerge(1);
    }
    return hnswMessages;
  }

  private static void assertMergedFromGraphs(List<String> hnswMessages, int numGraphs) {
    synchronized (hnswMessages) {
      assertTrue(
          "expected the merge to reuse "
              + numGraphs
              + " source graphs, but HNSW messages were: "
              + hnswMessages,
          hnswMessages.stream()
              .anyMatch(m -> m.startsWith("build graph from merging " + numGraphs + " graphs")));
    }
  }

  private static void assertMergedIndex(Directory dir, int expectedNumDocs) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      assertEquals(1, reader.leaves().size());
      assertEquals(expectedNumDocs, reader.numDocs());
      SegmentReader leaf = (SegmentReader) reader.leaves().getFirst().reader();
      assertEquals(
          "true", leaf.getSegmentInfo().info.getDiagnostics().get(ReversingMergePolicy.REVERSED));
    }
    CheckIndex.Status status = TestUtil.checkIndex(dir);
    assertTrue(status.clean);
    assertRecall(dir);
  }

  /**
   * A merged graph built from wrongly mapped source graphs is still a valid graph, but its edges
   * connect unrelated vectors, so check that searching it finds the true nearest neighbors.
   */
  private static void assertRecall(Directory dir) throws IOException {
    int k = 10;
    int numQueries = 20;
    int found = 0;
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      LeafReader leaf = reader.leaves().getFirst().reader();
      for (int q = 0; q < numQueries; q++) {
        float[] query = randomVector();
        Set<Integer> expected = exactNearestDocs(leaf, query, k);
        for (ScoreDoc hit : searcher.search(new KnnFloatVectorQuery("v", query, k), k).scoreDocs) {
          if (expected.contains(hit.doc)) {
            found++;
          }
        }
      }
    }
    double recall = (double) found / (k * numQueries);
    assertTrue("recall of the merged graph is too low: " + recall, recall >= 0.5);
  }

  private static Set<Integer> exactNearestDocs(LeafReader leaf, float[] query, int k)
      throws IOException {
    FloatVectorValues values = leaf.getFloatVectorValues("v");
    Bits liveDocs = leaf.getLiveDocs();
    PriorityQueue<ScoreDoc> topK =
        new PriorityQueue<>(k + 1, (a, b) -> Float.compare(a.score, b.score));
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int doc = it.nextDoc(); doc != NO_MORE_DOCS; doc = it.nextDoc()) {
      if (liveDocs != null && liveDocs.get(doc) == false) {
        continue;
      }
      float score =
          VectorSimilarityFunction.EUCLIDEAN.compare(query, values.vectorValue(it.index()));
      topK.add(new ScoreDoc(doc, score));
      if (topK.size() > k) {
        topK.poll();
      }
    }
    Set<Integer> docs = new HashSet<>();
    for (ScoreDoc scoreDoc : topK) {
      docs.add(scoreDoc.doc);
    }
    return docs;
  }

  private static HnswGraph getGraph(DirectoryReader reader) throws IOException {
    assertEquals(1, reader.leaves().size());
    CodecReader leaf = (CodecReader) reader.leaves().getFirst().reader();
    return ((HnswGraphProvider) leaf.getVectorReader()).getGraph("v");
  }

  private static Set<Integer> nodesOnLevel(HnswGraph graph, int level) throws IOException {
    Set<Integer> nodes = new HashSet<>();
    HnswGraph.NodesIterator it = graph.getNodesOnLevel(level);
    while (it.hasNext()) {
      nodes.add(it.nextInt());
    }
    return nodes;
  }

  private static Set<Integer> neighbors(HnswGraph graph, int level, int node) throws IOException {
    Set<Integer> neighbors = new HashSet<>();
    graph.seek(level, node);
    for (int n = graph.nextNeighbor(); n != NO_MORE_DOCS; n = graph.nextNeighbor()) {
      neighbors.add(n);
    }
    return neighbors;
  }

  private static float[] randomVector() {
    float[] v = new float[DIM];
    for (int j = 0; j < DIM; j++) {
      v[j] = random().nextFloat();
    }
    return v;
  }

  private static Sorter.DocMap reverse(int maxDoc) {
    return new Sorter.DocMap() {
      @Override
      public int size() {
        return maxDoc;
      }

      @Override
      public int oldToNew(int docID) {
        return maxDoc - 1 - docID;
      }

      @Override
      public int newToOld(int docID) {
        return maxDoc - 1 - docID;
      }
    };
  }

  private static InfoStream hnswInfoStream(List<String> sink) {
    return new InfoStream() {
      @Override
      public void message(String component, String message) {
        if ("HNSW".equals(component)) {
          synchronized (sink) {
            sink.add(message);
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
  }

  /** Reverses the doc order of every forced merge, like a reordering merge policy would. */
  private static class ReversingMergePolicy extends FilterMergePolicy {

    static final String REVERSED = "test.reversed";

    ReversingMergePolicy(MergePolicy in) {
      super(in);
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos segmentInfos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext)
        throws IOException {
      MergeSpecification spec =
          super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
      if (spec == null) {
        return null;
      }
      MergeSpecification reversed = new MergeSpecification();
      for (OneMerge merge : spec.merges) {
        reversed.add(
            new OneMerge(merge) {
              @Override
              public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) {
                return reverse(reader.maxDoc());
              }

              @Override
              public void setMergeInfo(SegmentCommitInfo info) {
                info.info.addDiagnostics(Map.of(REVERSED, "true"));
                super.setMergeInfo(info);
              }
            });
      }
      return reversed;
    }
  }
}
