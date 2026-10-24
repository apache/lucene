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
package org.apache.lucene.codecs.lucene99;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.HNSW_GRAPH_THRESHOLD;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Runs the format tests with merged graphs built off-heap. */
public class TestLucene99HnswVectorsFormatOffHeapGraph extends BaseKnnVectorsFormatTestCase {

  private static ExecutorService mergeExec;
  private static Codec codec;

  @BeforeClass
  public static void beforeClass() {
    int numMergeWorkers = random().nextBoolean() ? 1 : random().nextInt(2, 5);
    if (numMergeWorkers > 1) {
      mergeExec =
          Executors.newFixedThreadPool(numMergeWorkers, new NamedThreadFactory("hnswMerge"));
    }
    codec =
        TestUtil.alwaysKnnVectorsFormat(
            new Lucene99HnswVectorsFormat(
                random().nextInt(4, 32),
                random().nextInt(20, 120),
                numMergeWorkers,
                mergeExec,
                // build graphs for small segments too, so that merges exercise the off-heap graph
                random().nextBoolean() ? 0 : HNSW_GRAPH_THRESHOLD,
                true));
  }

  @AfterClass
  public static void afterClass() {
    if (mergeExec != null) {
      mergeExec.shutdownNow();
      mergeExec = null;
    }
    codec = null;
  }

  @Override
  protected Codec getCodec() {
    return codec;
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }

  /* with a single merge worker, merging on and off heap writes the same graph */
  public void testMergedGraphMatchesOnHeap() throws IOException {
    int numDocs = atLeast(1000);
    int dim = random().nextInt(4, 64);
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    boolean withDeletes = random().nextBoolean();
    try (Directory onHeapDir = newDirectory();
        Directory offHeapDir = newDirectory()) {
      indexAndMerge(onHeapDir, vectors, withDeletes, false);
      indexAndMerge(offHeapDir, vectors, withDeletes, true);
      try (DirectoryReader onHeapReader = DirectoryReader.open(onHeapDir);
          DirectoryReader offHeapReader = DirectoryReader.open(offHeapDir)) {
        assertGraphEquals(graph(onHeapReader), graph(offHeapReader));
      }
    }
  }

  /* the native memory reserved by the merged graph is reported through the info stream */
  public void testInfoStreamReportsOffHeapBytes() throws IOException {
    int numDocs = atLeast(200);
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(8);
    }
    List<String> messages = new CopyOnWriteArrayList<>();
    InfoStream infoStream =
        new InfoStream() {
          @Override
          public void message(String component, String message) {
            if (HnswGraphBuilder.HNSW_COMPONENT.equals(component)) {
              messages.add(message);
            }
          }

          @Override
          public boolean isEnabled(String component) {
            return HnswGraphBuilder.HNSW_COMPONENT.equals(component);
          }

          @Override
          public void close() {}
        };
    try (Directory dir = newDirectory()) {
      indexAndMerge(dir, vectors, false, true, infoStream);
    }
    // level 0 alone reserves a (2M+1)-neighbor slot for every node
    long minNativeBytes = (long) numDocs * (8 + 8 * (2 * DEFAULT_MAX_CONN + 1));
    String expectedPrefix = "off-heap graph for field f: " + numDocs + " nodes, ";
    String message =
        messages.stream()
            .filter(m -> m.startsWith(expectedPrefix))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no off-heap graph message in " + messages));
    long nativeBytes =
        Long.parseLong(message.replaceAll(".* levels, (\\d+) bytes native reserved.*", "$1"));
    assertTrue(message, nativeBytes >= minNativeBytes);
  }

  private static void indexAndMerge(
      Directory dir, float[][] vectors, boolean withDeletes, boolean offHeapGraphBuild)
      throws IOException {
    indexAndMerge(dir, vectors, withDeletes, offHeapGraphBuild, InfoStream.NO_OUTPUT);
  }

  private static void indexAndMerge(
      Directory dir,
      float[][] vectors,
      boolean withDeletes,
      boolean offHeapGraphBuild,
      InfoStream infoStream)
      throws IOException {
    Codec codec =
        TestUtil.alwaysKnnVectorsFormat(
            new Lucene99HnswVectorsFormat(
                DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, 1, null, 0, offHeapGraphBuild));
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(codec)
            .setMaxBufferedDocs(vectors.length / 4 + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergeScheduler(new SerialMergeScheduler())
            .setMergePolicy(new LogDocMergePolicy())
            .setInfoStream(infoStream);
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < vectors.length; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        doc.add(new KnnFloatVectorField("f", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
      }
      if (withDeletes) {
        for (int i = 0; i < vectors.length; i += 3) {
          w.deleteDocuments(new Term("id", Integer.toString(i)));
        }
      }
      w.forceMerge(1);
    }
  }

  private static HnswGraph graph(DirectoryReader reader) throws IOException {
    KnnVectorsReader vectorsReader =
        ((CodecReader) getOnlyLeafReader(reader)).getVectorReader().unwrapReaderForField("f");
    HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph("f");
    assertNotNull(graph);
    return graph;
  }

  private static void assertGraphEquals(HnswGraph expected, HnswGraph actual) throws IOException {
    assertEquals(expected.size(), actual.size());
    assertEquals(expected.numLevels(), actual.numLevels());
    assertEquals(expected.entryNode(), actual.entryNode());
    for (int level = 0; level < expected.numLevels(); level++) {
      HnswGraph.NodesIterator expectedNodes = expected.getNodesOnLevel(level);
      HnswGraph.NodesIterator actualNodes = actual.getNodesOnLevel(level);
      assertEquals(expectedNodes.size(), actualNodes.size());
      while (expectedNodes.hasNext()) {
        int node = expectedNodes.nextInt();
        assertEquals(node, actualNodes.nextInt());
        expected.seek(level, node);
        actual.seek(level, node);
        int neighbor;
        while ((neighbor = expected.nextNeighbor()) != NO_MORE_DOCS) {
          assertEquals("level=" + level + " node=" + node, neighbor, actual.nextNeighbor());
        }
        assertEquals(NO_MORE_DOCS, actual.nextNeighbor());
      }
    }
  }
}
