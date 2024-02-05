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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.hnsw.HnswGraphBuilder.randSeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.junit.After;
import org.junit.Before;

/** Tests indexing of a knn-graph */
abstract class KnnGraphTestCase<Vector> extends LuceneTestCase {

  protected abstract VectorEncoding getVectorEncoding();

  protected abstract Vector[] initEmptyArray(int size);

  protected abstract Vector[][] init2dArray(int size);

  protected abstract Vector[] randomVectors(int size, int dimension);

  protected abstract Vector randomVector(int dimension);

  protected abstract Vector vector(float[] values);

  protected abstract FieldType vectorFieldType(
      int dims, VectorSimilarityFunction similarityFunction);

  protected abstract Field vectorField(String name, Vector vector, FieldType fieldType);

  protected abstract void addKnnFieldToDoc(
      Document doc, Vector vector, VectorSimilarityFunction similarityFunction);

  protected abstract void validateArray(Vector expected, Vector actual, int doc, int id);

  protected abstract DocIdSetIterator vectorValues(LeafReader reader, String vectorField)
      throws IOException;

  protected abstract Vector vectorValue(DocIdSetIterator vectorValues) throws IOException;

  protected abstract TopDocs search(LeafReaderContext ctx, Vector vector, int k, Bits liveDocs)
      throws IOException;

  protected abstract Query generateQuery(String field, Vector queryVector, int k);

  protected abstract Map<Vector, int[]> getGridQueries();

  protected static final String KNN_GRAPH_FIELD = "vector";

  protected static int M = HnswGraphBuilder.DEFAULT_MAX_CONN;

  protected VectorSimilarityFunction similarityFunction;

  private Codec codec;
  private Codec float32Codec;

  @Before
  void setup() {
    randSeed = random().nextLong();
    if (random().nextBoolean()) {
      M = random().nextInt(256) + 3;
    }

    similarityFunction = randomSimilarityForEncoding(getVectorEncoding());
    boolean quantized = randomBoolean();
    codec =
        new FilterCodec(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return quantized
                    ? new Lucene99HnswScalarQuantizedVectorsFormat(
                        M, HnswGraphBuilder.DEFAULT_BEAM_WIDTH)
                    : new Lucene99HnswVectorsFormat(M, HnswGraphBuilder.DEFAULT_BEAM_WIDTH);
              }
            };
          }
        };

    float32Codec =
        new FilterCodec(TestUtil.getDefaultCodec().getName(), TestUtil.getDefaultCodec()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new Lucene99HnswVectorsFormat(M, HnswGraphBuilder.DEFAULT_BEAM_WIDTH);
              }
            };
          }
        };
  }

  private VectorSimilarityFunction randomSimilarityForEncoding(VectorEncoding encoding) {
    List<VectorSimilarityFunction> supportedSimilarities =
        Arrays.stream(VectorSimilarityFunction.values())
            .filter(x -> x.supportedVectorEncodings().contains(encoding))
            .toList();
    return supportedSimilarities.get(random().nextInt(supportedSimilarities.size()));
  }

  @After
  void cleanup() {
    M = HnswGraphBuilder.DEFAULT_MAX_CONN;
  }

  /** Basic test of creating documents in a graph */
  public void testBasic() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(codec))) {
      int numDoc = atLeast(10);
      int dimension = atLeast(3);
      Vector[] values = initEmptyArray(numDoc);
      for (int i = 0; i < numDoc; i++) {
        if (random().nextBoolean()) {
          values[i] = randomVector(dimension);
        }
        add(iw, i, values[i], similarityFunction);
      }
      assertConsistentGraph(iw, values, KNN_GRAPH_FIELD);
    }
  }

  public void testSingleDocument() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(codec))) {
      Vector[] values = initEmptyArray(1);
      values[0] = vector(new float[] {0, 1, 2});
      add(iw, 0, values[0], similarityFunction);
      assertConsistentGraph(iw, values, KNN_GRAPH_FIELD);
      iw.commit();
      assertConsistentGraph(iw, values, KNN_GRAPH_FIELD);
    }
  }

  /** Verify that the graph properties are preserved when merging */
  public void testMerge() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(codec))) {
      int numDoc = atLeast(100);
      int dimension = atLeast(10);
      Vector[] vectors = randomVectors(numDoc, dimension);
      for (int i = 0; i < numDoc; i++) {
        add(iw, i, vectors[i], similarityFunction);
        if (random().nextInt(10) == 3) {
          iw.commit();
        }
      }
      if (random().nextBoolean()) {
        iw.forceMerge(1);
      }
      assertConsistentGraph(iw, vectors, KNN_GRAPH_FIELD);
    }
  }

  /** Test writing and reading of multiple vector fields * */
  public void testMultipleVectorFields() throws Exception {
    int numVectorFields = randomIntBetween(2, 5);
    int numDoc = atLeast(100);
    int[] dims = new int[numVectorFields];
    Vector[][] values = init2dArray(numVectorFields);
    FieldType[] fieldTypes = new FieldType[numVectorFields];
    for (int field = 0; field < numVectorFields; field++) {
      dims[field] = atLeast(3);
      values[field] = randomVectors(numDoc, dims[field]);
      fieldTypes[field] = vectorFieldType(dims[field], similarityFunction);
    }

    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null).setCodec(codec))) {
      for (int docID = 0; docID < numDoc; docID++) {
        Document doc = new Document();
        for (int field = 0; field < numVectorFields; field++) {
          Vector vector = values[field][docID];
          if (vector != null) {
            doc.add(vectorField(KNN_GRAPH_FIELD + field, vector, fieldTypes[field]));
          }
        }
        String idString = Integer.toString(docID);
        doc.add(new StringField("id", idString, Field.Store.YES));
        iw.addDocument(doc);
      }
      for (int field = 0; field < numVectorFields; field++) {
        assertConsistentGraph(iw, values[field], KNN_GRAPH_FIELD + field);
      }
    }
  }

  /** Verify that searching does something reasonable */
  public void testSearch() throws Exception {
    // We can't use dot product here since the vectors are laid out on a grid, not a sphere.
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    IndexWriterConfig config = newIndexWriterConfig();
    config.setCodec(float32Codec);
    try (Directory dir = newDirectory();
        IndexWriter iw = new IndexWriter(dir, config)) {
      indexData(iw);
      try (DirectoryReader dr = DirectoryReader.open(iw)) {
        // results are ordered by score (descending) and docid (ascending);
        // This is the insertion order:
        // column major, origin at upper left
        //  0 15  5 20 10
        //  3 18  8 23 13
        //  6 21 11  1 16
        //  9 24 14  4 19
        // 12  2 17  7 22

        /* For this small graph the "search" is exhaustive, so this mostly tests the APIs, the
         * orientation of the various priority queues, the scoring function, but not so much the
         * approximate KNN search algorithm
         */

        // Tiebreaking by docid must be done after search.
        // assertGraphSearch(new int[]{11, 1, 8, 14, 21}, new float[]{2, 2}, dr);
        for (Map.Entry<Vector, int[]> gridQueryAndResponse : getGridQueries().entrySet()) {
          assertGraphSearch(gridQueryAndResponse.getValue(), gridQueryAndResponse.getKey(), dr);
        }
      }
    }
  }

  private void indexData(IndexWriter iw) throws IOException {
    // Add a document for every cartesian point in an NxN square so we can
    // easily know which are the nearest neighbors to every point. Insert by iterating
    // using a prime number that is not a divisor of N*N so that we will hit each point once,
    // and chosen so that points will be inserted in a deterministic
    // but somewhat distributed pattern
    int n = 5, stepSize = 17;
    Vector[] values = initEmptyArray(n * n);
    int index = 0;
    for (int i = 0; i < values.length; i++) {
      // System.out.printf("%d: (%d, %d)\n", i, index % n, index / n);
      int x = index % n, y = index / n;
      values[i] = vector(new float[] {x, y});
      index = (index + stepSize) % (n * n);
      add(iw, i, values[i], similarityFunction);
      if (i == 13) {
        // create 2 segments
        iw.commit();
      }
    }
    boolean forceMerge = random().nextBoolean();
    if (forceMerge) {
      iw.forceMerge(1);
    }
    assertConsistentGraph(iw, values, KNN_GRAPH_FIELD);
  }

  public void testMultiThreadedSearch() throws Exception {
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    IndexWriterConfig config = newIndexWriterConfig();
    config.setCodec(float32Codec);
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, config);
    indexData(iw);

    final SearcherManager manager = new SearcherManager(iw, new SearcherFactory());
    Thread[] threads = new Thread[randomIntBetween(2, 5)];
    final CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < threads.length; i++) {
      threads[i] =
          new Thread(
              () -> {
                try {
                  latch.await();
                  IndexSearcher searcher = manager.acquire();
                  Map.Entry<Vector, int[]> gridQueryAndResponse =
                      getGridQueries().entrySet().stream().findFirst().get();
                  try {
                    Query query = generateQuery("vector", gridQueryAndResponse.getKey(), 5);
                    TopDocs results = searcher.search(query, 5);
                    StoredFields storedFields = searcher.storedFields();
                    for (ScoreDoc doc : results.scoreDocs) {
                      // map docId to insertion id
                      doc.doc = Integer.parseInt(storedFields.document(doc.doc).get("id"));
                    }

                    assertResults(gridQueryAndResponse.getValue(), results);
                  } finally {
                    manager.release(searcher);
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      threads[i].start();
    }

    latch.countDown();
    for (Thread t : threads) {
      t.join();
    }
    IOUtils.close(manager, iw, dir);
  }

  private void assertGraphSearch(int[] expected, Vector vector, IndexReader reader)
      throws IOException {
    TopDocs results = doKnnSearch(reader, vector, 5);
    StoredFields storedFields = reader.storedFields();
    for (ScoreDoc doc : results.scoreDocs) {
      // map docId to insertion id
      doc.doc = Integer.parseInt(storedFields.document(doc.doc).get("id"));
    }
    assertResults(expected, results);
  }

  TopDocs doKnnSearch(IndexReader reader, Vector vector, int k) throws IOException {
    TopDocs[] results = new TopDocs[reader.leaves().size()];
    for (LeafReaderContext ctx : reader.leaves()) {
      Bits liveDocs = ctx.reader().getLiveDocs();
      results[ctx.ord] = search(ctx, vector, k, liveDocs);
      if (ctx.docBase > 0) {
        for (ScoreDoc doc : results[ctx.ord].scoreDocs) {
          doc.doc += ctx.docBase;
        }
      }
    }
    return TopDocs.merge(k, results);
  }

  private void assertResults(int[] expected, TopDocs results) {
    assertEquals(results.toString(), expected.length, results.scoreDocs.length);
    for (int i = expected.length - 1; i >= 0; i--) {
      assertEquals(Arrays.toString(results.scoreDocs), expected[i], results.scoreDocs[i].doc);
    }
  }

  private void add(
      IndexWriter iw, int id, Vector vector, VectorSimilarityFunction similarityFunction)
      throws IOException {
    Document doc = new Document();
    if (vector != null) {
      addKnnFieldToDoc(doc, vector, similarityFunction);
    }
    String idString = Integer.toString(id);
    doc.add(new StringField("id", idString, Field.Store.YES));
    doc.add(new SortedDocValuesField("id", new BytesRef(idString)));
    iw.updateDocument(new Term("id", idString), doc);
  }

  // For each leaf, verify that its graph nodes are 1-1 with vectors, that the vectors are the
  // expected values, and that the graph is fully connected and symmetric.
  // NOTE: when we impose max-fanout on the graph it wil no longer be symmetric, but should still
  // be fully connected. Is there any other invariant we can test? Well, we can check that max
  // fanout is respected. We can test *desirable* properties of the graph like small-world
  // (the graph diameter should be tightly bounded).
  private void assertConsistentGraph(IndexWriter iw, Vector[] values, String vectorField)
      throws IOException {
    int numDocsWithVectors = 0;
    try (DirectoryReader dr = DirectoryReader.open(iw)) {
      for (LeafReaderContext ctx : dr.leaves()) {
        LeafReader reader = ctx.reader();
        PerFieldKnnVectorsFormat.FieldsReader perFieldReader =
            (PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) reader).getVectorReader();
        if (perFieldReader == null) {
          continue;
        }
        Lucene99HnswVectorsReader vectorReader =
            (Lucene99HnswVectorsReader) perFieldReader.getFieldReader(vectorField);
        if (vectorReader == null) {
          continue;
        }
        HnswGraph graphValues = vectorReader.getGraph(vectorField);
        DocIdSetIterator vectorValues = vectorValues(reader, vectorField);
        if (vectorValues == null) {
          assert graphValues == null;
          continue;
        }

        // assert vector values:
        // stored vector values are the same as original
        int nextDocWithVectors = 0;
        StoredFields storedFields = reader.storedFields();
        for (int i = 0; i < reader.maxDoc(); i++) {
          nextDocWithVectors = vectorValues.advance(i);
          while (i < nextDocWithVectors && i < reader.maxDoc()) {
            int id = Integer.parseInt(storedFields.document(i).get("id"));
            assertNull("document " + id + " has no vector, but was expected to", values[id]);
            ++i;
          }
          if (nextDocWithVectors == NO_MORE_DOCS) {
            break;
          }
          int id = Integer.parseInt(storedFields.document(i).get("id"));
          // documents with KnnGraphValues have the expected vectors
          Vector scratch = vectorValue(vectorValues);
          validateArray(values[id], scratch, i, id);
          numDocsWithVectors++;
        }
        // if IndexDisi.doc == NO_MORE_DOCS, we should not call IndexDisi.nextDoc()
        if (nextDocWithVectors != NO_MORE_DOCS) {
          assertEquals(NO_MORE_DOCS, vectorValues.nextDoc());
        } else {
          assertEquals(NO_MORE_DOCS, vectorValues.docID());
        }

        // assert graph values:
        // For each level of the graph assert that:
        // 1. There are no orphan nodes without any friends
        // 2. If orphans are found, than the level must contain only 0 or a single node
        // 3. If the number of nodes on the level doesn't exceed maxConnOnLevel, assert that the
        // graph is
        //   fully connected, i.e. any node is reachable from any other node.
        // 4. If the number of nodes on the level exceeds maxConnOnLevel, assert that maxConnOnLevel
        // is respected.
        for (int level = 0; level < graphValues.numLevels(); level++) {
          int maxConnOnLevel = level == 0 ? M * 2 : M;
          int[][] graphOnLevel = new int[graphValues.size()][];
          int countOnLevel = 0;
          boolean foundOrphan = false;
          HnswGraph.NodesIterator nodesItr = graphValues.getNodesOnLevel(level);
          while (nodesItr.hasNext()) {
            int node = nodesItr.nextInt();
            graphValues.seek(level, node);
            int arc;
            List<Integer> friends = new ArrayList<>();
            while ((arc = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
              friends.add(arc);
            }
            if (friends.size() == 0) {
              foundOrphan = true;
            } else {
              int[] friendsCopy = new int[friends.size()];
              Arrays.setAll(friendsCopy, friends::get);
              graphOnLevel[node] = friendsCopy;
            }
            countOnLevel++;
          }
          assertEquals(nodesItr.size(), countOnLevel);
          assertFalse("No nodes on level [" + level + "]", countOnLevel == 0);
          if (countOnLevel == 1) {
            assertTrue(
                "Graph with 1 node has unexpected neighbors on level [" + level + "]", foundOrphan);
          } else {
            assertFalse(
                "Graph has orphan nodes with no friends on level [" + level + "]", foundOrphan);
            if (maxConnOnLevel > countOnLevel) {
              // assert that the graph is fully connected,
              // i.e. any node can be reached from any other node
              assertConnected(graphOnLevel);
            } else {
              // assert that max-connections was respected
              assertMaxConn(graphOnLevel, maxConnOnLevel);
            }
          }
        }
      }
    }

    int expectedNumDocsWithVectors = 0;
    for (Vector vector : values) {
      if (vector != null) {
        ++expectedNumDocsWithVectors;
      }
    }
    assertEquals(expectedNumDocsWithVectors, numDocsWithVectors);
  }

  public static void assertMaxConn(int[][] graph, int maxConn) {
    for (int[] ints : graph) {
      if (ints != null) {
        assert (ints.length <= maxConn);
        for (int k : ints) {
          assertNotNull(graph[k]);
        }
      }
    }
  }

  /** Assert that every node is reachable from some other node */
  private static void assertConnected(int[][] graph) {
    List<Integer> nodes = new ArrayList<>();
    Set<Integer> visited = new HashSet<>();
    List<Integer> queue = new LinkedList<>();
    for (int i = 0; i < graph.length; i++) {
      if (graph[i] != null) {
        nodes.add(i);
      }
    }

    // start from any node
    int startIdx = random().nextInt(nodes.size());
    queue.add(nodes.get(startIdx));
    while (queue.isEmpty() == false) {
      int i = queue.remove(0);
      assertNotNull("expected neighbors of " + i, graph[i]);
      visited.add(i);
      for (int j : graph[i]) {
        if (visited.contains(j) == false) {
          queue.add(j);
        }
      }
    }
    // assert that every node is reachable from some other node as it was visited
    for (int node : nodes) {
      assertTrue(
          "Attempted to walk entire graph but never visited node [" + node + "]",
          visited.contains(node));
    }
  }
}
