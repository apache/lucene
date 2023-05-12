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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.tests.util.RamUsageTester.ramUsed;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;

/** Tests HNSW KNN graphs */
abstract class ConcurrentHnswGraphTestCase<T> extends LuceneTestCase {

  VectorSimilarityFunction similarityFunction;

  abstract VectorEncoding getVectorEncoding();

  abstract Query knnQuery(String field, T vector, int k);

  abstract T randomVector(int dim);

  abstract AbstractMockVectorValues<T> vectorValues(int size, int dimension);

  abstract AbstractMockVectorValues<T> vectorValues(float[][] values);

  abstract AbstractMockVectorValues<T> vectorValues(LeafReader reader, String fieldName)
      throws IOException;

  abstract AbstractMockVectorValues<T> vectorValues(
      int size,
      int dimension,
      AbstractMockVectorValues<T> pregeneratedVectorValues,
      int pregeneratedOffset);

  abstract Field knnVectorField(String name, T vector, VectorSimilarityFunction similarityFunction);

  abstract RandomAccessVectorValues<T> circularVectorValues(int nDoc);

  abstract T getTargetVector();

  // test writing out and reading in a graph gives the expected graph
  public void testReadWrite() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    int M = random().nextInt(4) + 2;
    int beamWidth = random().nextInt(10) + 5;
    long seed = random().nextLong();
    AbstractMockVectorValues<T> vectors = vectorValues(nDoc, dim);
    AbstractMockVectorValues<T> v2 = vectors.copy(), v3 = vectors.copy();
    HnswGraphBuilder<T> builder =
        HnswGraphBuilder.create(
            vectors, getVectorEncoding(), similarityFunction, M, beamWidth, seed);
    HnswGraph hnsw = builder.build(vectors.copy());

    // Recreate the graph while indexing with the same random seed and write it out
    HnswGraphBuilder.randSeed = seed;
    try (Directory dir = newDirectory()) {
      int nVec = 0, indexedDoc = 0;
      // Don't merge randomly, create a single segment because we rely on the docid ordering for
      // this test
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(
                  new Lucene95Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene95HnswVectorsFormat(M, beamWidth);
                    }
                  });
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        while (v2.nextDoc() != NO_MORE_DOCS) {
          while (indexedDoc < v2.docID()) {
            // increment docId in the index by adding empty documents
            iw.addDocument(new Document());
            indexedDoc++;
          }
          Document doc = new Document();
          doc.add(knnVectorField("field", v2.vectorValue(), similarityFunction));
          doc.add(new StoredField("id", v2.docID()));
          iw.addDocument(doc);
          nVec++;
          indexedDoc++;
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          AbstractMockVectorValues<T> values = vectorValues(ctx.reader(), "field");
          assertEquals(dim, values.dimension());
          assertEquals(nVec, values.size());
          assertEquals(indexedDoc, ctx.reader().maxDoc());
          assertEquals(indexedDoc, ctx.reader().numDocs());
          assertVectorsEqual(v3, values);
          HnswGraph graphValues =
              ((Lucene95HnswVectorsReader)
                      ((PerFieldKnnVectorsFormat.FieldsReader)
                              ((CodecReader) ctx.reader()).getVectorReader())
                          .getFieldReader("field"))
                  .getGraph("field");
          assertGraphEqual(hnsw, graphValues);
        }
      }
    }
  }

  // test that sorted index returns the same search results are unsorted
  public void testSortedAndUnsortedIndicesReturnSameResults() throws IOException {
    int dim = random().nextInt(10) + 3;
    int nDoc = random().nextInt(200) + 100;
    AbstractMockVectorValues<T> vectors = vectorValues(nDoc, dim);

    int M = random().nextInt(10) + 5;
    int beamWidth = random().nextInt(10) + 5;
    VectorSimilarityFunction similarityFunction =
        RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(
                new Lucene95Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new Lucene95HnswVectorsFormat(M, beamWidth);
                  }
                });
    IndexWriterConfig iwc2 =
        new IndexWriterConfig()
            .setCodec(
                new Lucene95Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new Lucene95HnswVectorsFormat(M, beamWidth);
                  }
                })
            .setIndexSort(new Sort(new SortField("sortkey", SortField.Type.LONG)));

    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      int indexedDoc = 0;
      try (IndexWriter iw = new IndexWriter(dir, iwc);
          IndexWriter iw2 = new IndexWriter(dir2, iwc2)) {
        while (vectors.nextDoc() != NO_MORE_DOCS) {
          while (indexedDoc < vectors.docID()) {
            // increment docId in the index by adding empty documents
            iw.addDocument(new Document());
            indexedDoc++;
          }
          Document doc = new Document();
          doc.add(knnVectorField("vector", vectors.vectorValue(), similarityFunction));
          doc.add(new StoredField("id", vectors.docID()));
          doc.add(new NumericDocValuesField("sortkey", random().nextLong()));
          iw.addDocument(doc);
          iw2.addDocument(doc);
          indexedDoc++;
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir);
          IndexReader reader2 = DirectoryReader.open(dir2)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        IndexSearcher searcher2 = new IndexSearcher(reader2);
        OUTER:
        for (int i = 0; i < 10; i++) {
          // ask to explore a lot of candidates to ensure the same returned hits,
          // as graphs of 2 indices are organized differently
          Query query = knnQuery("vector", randomVector(dim), 50);
          int searchSize = 5;
          List<String> ids1 = new ArrayList<>(searchSize);
          List<Integer> docs1 = new ArrayList<>(searchSize);
          List<String> ids2 = new ArrayList<>(searchSize);
          List<Integer> docs2 = new ArrayList<>(searchSize);

          // Check if a duplicate score exists in n+1, if so, this test is invalid
          // Else, continue to fail on ID equality as this test failed
          TopDocs topDocs = searcher.search(query, searchSize + 1);
          float lastScore = -1;
          StoredFields storedFields = reader.storedFields();
          for (int j = 0; j < searchSize + 1; j++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[j];
            if (scoreDoc.score == lastScore) {
              // if we have repeated score this test is invalid
              continue OUTER;
            } else {
              lastScore = scoreDoc.score;
            }
            if (j < searchSize) {
              Document doc = storedFields.document(scoreDoc.doc, Set.of("id"));
              ids1.add(doc.get("id"));
              docs1.add(scoreDoc.doc);
            }
          }
          TopDocs topDocs2 = searcher2.search(query, searchSize);
          StoredFields storedFields2 = reader2.storedFields();
          for (ScoreDoc scoreDoc : topDocs2.scoreDocs) {
            Document doc = storedFields2.document(scoreDoc.doc, Set.of("id"));
            ids2.add(doc.get("id"));
            docs2.add(scoreDoc.doc);
          }
          assertEquals(ids1, ids2);
          // doc IDs are not equal, as in the second sorted index docs are organized differently
          assertNotEquals(docs1, docs2);
        }
      }
    }
  }

  List<Integer> sortedNodesOnLevel(HnswGraph h, int level) throws IOException {
    NodesIterator nodesOnLevel = h.getNodesOnLevel(level);
    List<Integer> nodes = new ArrayList<>();
    while (nodesOnLevel.hasNext()) {
      nodes.add(nodesOnLevel.next());
    }
    Collections.sort(nodes);
    return nodes;
  }

  void assertGraphEqual(HnswGraph g, HnswGraph h) throws IOException {
    // construct these up front since they call seek which will mess up our test loop
    String prettyG = prettyPrint(g);
    String prettyH = prettyPrint(h);
    assertEquals(
        String.format(
            Locale.ROOT,
            "the number of levels in the graphs are different:%n%s%n%s",
            prettyG,
            prettyH),
        g.numLevels(),
        h.numLevels());
    assertEquals(
        String.format(
            Locale.ROOT,
            "the number of nodes in the graphs are different:%n%s%n%s",
            prettyG,
            prettyH),
        g.size(),
        h.size());

    // assert equal nodes on each level
    for (int level = 0; level < g.numLevels(); level++) {
      List<Integer> hNodes = sortedNodesOnLevel(h, level);
      List<Integer> gNodes = sortedNodesOnLevel(g, level);
      assertEquals(
          String.format(
              Locale.ROOT,
              "nodes in the graphs are different on level %d:%n%s%n%s",
              level,
              prettyG,
              prettyH),
          gNodes,
          hNodes);
    }

    // assert equal nodes' neighbours on each level
    for (int level = 0; level < g.numLevels(); level++) {
      NodesIterator nodesOnLevel = g.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        g.seek(level, node);
        h.seek(level, node);
        assertEquals(
            String.format(
                Locale.ROOT,
                "arcs differ for node %d on level %d:%n%s%n%s",
                node,
                level,
                prettyG,
                prettyH),
            getNeighborNodes(g),
            getNeighborNodes(h));
      }
    }
  }

  // Make sure we actually approximately find the closest k elements. Mostly this is about
  // ensuring that we have all the distance functions, comparators, priority queues and so on
  // oriented in the right directions
  @SuppressWarnings("unchecked")
  public void testAknnDiverse() throws IOException {
    int nDoc = 100;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 10, 100);
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    // run some searches
    NeighborQueue nn =
        switch (getVectorEncoding()) {
          case BYTE -> HnswGraphSearcher.search(
              (byte[]) getTargetVector(),
              10,
              (RandomAccessVectorValues<byte[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              null,
              Integer.MAX_VALUE);
          case FLOAT32 -> HnswGraphSearcher.search(
              (float[]) getTargetVector(),
              10,
              (RandomAccessVectorValues<float[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              null,
              Integer.MAX_VALUE);
        };

    int[] nodes = nn.nodes();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.length);
    int sum = 0;
    for (int node : nodes) {
      sum += node;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);

    for (int i = 0; i < nDoc; i++) {
      ConcurrentNeighborSet neighbors = hnsw.getNeighbors(0, i);
      Iterator<Integer> it = neighbors.nodeIterator();
      while (it.hasNext()) {
        // all neighbors should be valid node ids.
        assertTrue(it.next() < nDoc);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void testSearchWithAcceptOrds() throws IOException {
    int nDoc = 100;
    RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 16, 100);
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    // the first 10 docs must not be deleted to ensure the expected recall
    Bits acceptOrds = createRandomAcceptOrds(10, nDoc);
    NeighborQueue nn =
        switch (getVectorEncoding()) {
          case BYTE -> HnswGraphSearcher.search(
              (byte[]) getTargetVector(),
              10,
              (RandomAccessVectorValues<byte[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              acceptOrds,
              Integer.MAX_VALUE);
          case FLOAT32 -> HnswGraphSearcher.search(
              (float[]) getTargetVector(),
              10,
              (RandomAccessVectorValues<float[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              acceptOrds,
              Integer.MAX_VALUE);
        };
    int[] nodes = nn.nodes();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.length);
    int sum = 0;
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
      sum += node;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);
  }

  @SuppressWarnings("unchecked")
  public void testConnections() throws IOException {
    for (int i = 0; i < 100; i++) {
      for (int j = 1; j < 1000; j *= 10) {
        int nDoc = atLeast(5 * j);
        RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
        similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
        VectorEncoding vectorEncoding = getVectorEncoding();
        ConcurrentHnswGraphBuilder<T> builder =
            new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 16, 100) {
              @Override
              int getRandomGraphLevel(double ml) {
                int level = 0;
                while (ThreadLocalRandom.current().nextDouble() < 0.5) {
                  level++;
                }
                return level;
              }
            };
        ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
        new HnswGraphValidator(hnsw).validateReachability();
      }
    }
  }

  public void testSearchWithSelectiveAcceptOrds() throws IOException {
    // searchWithSelectiveAcceptOrds seems particularly good at exposing problems with the graph
    for (int i = 0; i < 10; i++) {
      searchWithSelectiveAcceptOrds(50, 5);
      searchWithSelectiveAcceptOrds(100, 5);
      searchWithSelectiveAcceptOrds(100, 15);
      searchWithSelectiveAcceptOrds(500, 10);
      searchWithSelectiveAcceptOrds(500, 50);
      searchWithSelectiveAcceptOrds(1000, 10);
      searchWithSelectiveAcceptOrds(1000, 50);
      searchWithSelectiveAcceptOrds(1000, 100);
    }
  }

  @SuppressWarnings("unchecked")
  public void searchWithSelectiveAcceptOrds(int nDoc, int toAccept) throws IOException {
    nDoc = atLeast(nDoc);
    RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    VectorEncoding vectorEncoding = getVectorEncoding();
    // builder that more aggressively creates new levels, whicn increases the chances of
    // exposing a concurrency bug
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 16, 100) {
          @Override
          int getRandomGraphLevel(double ml) {
            int level = 0;
            while (ThreadLocalRandom.current().nextDouble() < 0.5) {
              level++;
            }
            return level;
          }
        };
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    // Only mark a few vectors as accepted
    BitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 0; i < atLeast(toAccept); i++) {
      acceptOrds.set(random().nextInt(nDoc));
    }

    // Check the search finds all accepted vectors
    int numAccepted = acceptOrds.cardinality();
    NeighborQueue nn =
        switch (getVectorEncoding()) {
          case FLOAT32 -> HnswGraphSearcher.search(
              (float[]) getTargetVector(),
              numAccepted,
              (RandomAccessVectorValues<float[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              acceptOrds,
              Integer.MAX_VALUE);
          case BYTE -> HnswGraphSearcher.search(
              (byte[]) getTargetVector(),
              numAccepted,
              (RandomAccessVectorValues<byte[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              acceptOrds,
              Integer.MAX_VALUE);
        };

    int[] nodes = nn.nodes();
    assertEquals(numAccepted, nodes.length);
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
    }
  }

  public void testBuildOnHeapHnswGraphOutOfOrder() throws IOException {
    int maxNumLevels = randomIntBetween(2, 10);
    int nodeCount = randomIntBetween(1, 100);

    List<List<Integer>> nodesPerLevel = new ArrayList<>();
    for (int i = 0; i < maxNumLevels; i++) {
      nodesPerLevel.add(new ArrayList<>());
    }

    int numLevels = 0;
    for (int currNode = 0; currNode < nodeCount; currNode++) {
      int nodeMaxLevel = random().nextInt(1, maxNumLevels + 1);
      numLevels = Math.max(numLevels, nodeMaxLevel);
      for (int currLevel = 0; currLevel < nodeMaxLevel; currLevel++) {
        nodesPerLevel.get(currLevel).add(currNode);
      }
    }

    ConcurrentOnHeapHnswGraph topDownOrderReversedHnsw = new ConcurrentOnHeapHnswGraph(10);
    for (int currLevel = numLevels - 1; currLevel >= 0; currLevel--) {
      List<Integer> currLevelNodes = nodesPerLevel.get(currLevel);
      int currLevelNodesSize = currLevelNodes.size();
      for (int currNodeInd = currLevelNodesSize - 1; currNodeInd >= 0; currNodeInd--) {
        topDownOrderReversedHnsw.addNode(currLevel, currLevelNodes.get(currNodeInd));
      }
    }

    ConcurrentOnHeapHnswGraph bottomUpOrderReversedHnsw = new ConcurrentOnHeapHnswGraph(10);
    for (int currLevel = 0; currLevel < numLevels; currLevel++) {
      List<Integer> currLevelNodes = nodesPerLevel.get(currLevel);
      int currLevelNodesSize = currLevelNodes.size();
      for (int currNodeInd = currLevelNodesSize - 1; currNodeInd >= 0; currNodeInd--) {
        bottomUpOrderReversedHnsw.addNode(currLevel, currLevelNodes.get(currNodeInd));
      }
    }

    ConcurrentOnHeapHnswGraph topDownOrderRandomHnsw = new ConcurrentOnHeapHnswGraph(10);
    for (int currLevel = numLevels - 1; currLevel >= 0; currLevel--) {
      List<Integer> currLevelNodes = new ArrayList<>(nodesPerLevel.get(currLevel));
      Collections.shuffle(currLevelNodes, random());
      for (Integer currNode : currLevelNodes) {
        topDownOrderRandomHnsw.addNode(currLevel, currNode);
      }
    }

    ConcurrentOnHeapHnswGraph bottomUpExpectedHnsw = new ConcurrentOnHeapHnswGraph(10);
    for (int currLevel = 0; currLevel < numLevels; currLevel++) {
      for (Integer currNode : nodesPerLevel.get(currLevel)) {
        bottomUpExpectedHnsw.addNode(currLevel, currNode);
      }
    }

    assertEquals(nodeCount, bottomUpExpectedHnsw.getNodesOnLevel(0).size());
    for (Integer node : nodesPerLevel.get(0)) {
      assertEquals(0, bottomUpExpectedHnsw.getNeighbors(0, node).size());
    }

    for (int currLevel = 1; currLevel < numLevels; currLevel++) {
      List<Integer> expectedNodesOnLevel = nodesPerLevel.get(currLevel);
      List<Integer> sortedNodes = sortedNodesOnLevel(bottomUpExpectedHnsw, currLevel);
      assertEquals(
          String.format(Locale.ROOT, "Nodes on level %d do not match", currLevel),
          expectedNodesOnLevel,
          sortedNodes);
    }

    assertGraphEqual(bottomUpExpectedHnsw.getView(), topDownOrderReversedHnsw.getView());
    assertGraphEqual(bottomUpExpectedHnsw.getView(), bottomUpOrderReversedHnsw.getView());
    assertGraphEqual(bottomUpExpectedHnsw.getView(), topDownOrderRandomHnsw.getView());
  }

  public void testHnswGraphBuilderInitializationFromGraph_withOffsetZero() throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = 0;
    int dim = atLeast(10);
    long seed = random().nextLong();

    AbstractMockVectorValues<T> initializerVectors = vectorValues(initializerSize, dim);
    HnswGraphBuilder<T> initializerBuilder =
        HnswGraphBuilder.create(
            initializerVectors, getVectorEncoding(), similarityFunction, 10, 30, seed);

    OnHeapHnswGraph initializerGraph = initializerBuilder.build(initializerVectors.copy());
    AbstractMockVectorValues<T> finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors, docIdOffset);

    Map<Integer, Integer> initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues, docIdOffset);

    HnswGraphBuilder<T> finalBuilder =
        HnswGraphBuilder.create(
            finalVectorValues,
            getVectorEncoding(),
            similarityFunction,
            10,
            30,
            seed,
            initializerGraph,
            initializerOrdMap);

    // When offset is 0, the graphs should be identical before vectors are added
    assertGraphEqual(initializerGraph, finalBuilder.getGraph());

    OnHeapHnswGraph finalGraph = finalBuilder.build(finalVectorValues.copy());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
  }

  public void testHnswGraphBuilderInitializationFromGraph_withNonZeroOffset() throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = random().nextInt(1, totalSize - initializerSize + 1);
    int dim = atLeast(10);
    long seed = random().nextLong();

    AbstractMockVectorValues<T> initializerVectors = vectorValues(initializerSize, dim);
    HnswGraphBuilder<T> initializerBuilder =
        HnswGraphBuilder.create(
            initializerVectors.copy(), getVectorEncoding(), similarityFunction, 10, 30, seed);
    OnHeapHnswGraph initializerGraph = initializerBuilder.build(initializerVectors.copy());
    AbstractMockVectorValues<T> finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors.copy(), docIdOffset);
    Map<Integer, Integer> initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues.copy(), docIdOffset);

    HnswGraphBuilder<T> finalBuilder =
        HnswGraphBuilder.create(
            finalVectorValues,
            getVectorEncoding(),
            similarityFunction,
            10,
            30,
            seed,
            initializerGraph,
            initializerOrdMap);

    assertGraphInitializedFromGraph(finalBuilder.getGraph(), initializerGraph, initializerOrdMap);

    // Confirm that the graph is appropriately constructed by checking that the nodes in the old
    // graph are present in the levels of the new graph
    OnHeapHnswGraph finalGraph = finalBuilder.build(finalVectorValues.copy());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
  }

  private void assertGraphContainsGraph(
      HnswGraph g, HnswGraph h, Map<Integer, Integer> oldToNewOrdMap) throws IOException {
    for (int i = 0; i < h.numLevels(); i++) {
      int[] finalGraphNodesOnLevel = nodesIteratorToArray(g.getNodesOnLevel(i));
      int[] initializerGraphNodesOnLevel =
          mapArrayAndSort(nodesIteratorToArray(h.getNodesOnLevel(i)), oldToNewOrdMap);
      int overlap = computeOverlap(finalGraphNodesOnLevel, initializerGraphNodesOnLevel);
      assertEquals(initializerGraphNodesOnLevel.length, overlap);
    }
  }

  private void assertGraphInitializedFromGraph(
      HnswGraph g, HnswGraph h, Map<Integer, Integer> oldToNewOrdMap) throws IOException {
    assertEquals("the number of levels in the graphs are different!", g.numLevels(), h.numLevels());
    // Confirm that the size of the new graph includes all nodes up to an including the max new
    // ordinal in the old to
    // new ordinal mapping
    assertEquals(
        "the number of nodes in the graphs are different!",
        g.size(),
        Collections.max(oldToNewOrdMap.values()) + 1);

    // assert the nodes from the previous graph are successfully to levels > 0 in the new graph
    for (int level = 1; level < g.numLevels(); level++) {
      List<Integer> nodesOnLevel = sortedNodesOnLevel(g, level);
      List<Integer> nodesOnLevel2 =
          sortedNodesOnLevel(h, level).stream().map(oldToNewOrdMap::get).toList();
      assertEquals(nodesOnLevel, nodesOnLevel2);
    }

    // assert that the neighbors from the old graph are successfully transferred to the new graph
    for (int level = 0; level < g.numLevels(); level++) {
      NodesIterator nodesOnLevel = h.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        g.seek(level, oldToNewOrdMap.get(node));
        h.seek(level, node);
        assertEquals(
            "arcs differ for node " + node,
            getNeighborNodes(g),
            getNeighborNodes(h).stream().map(oldToNewOrdMap::get).collect(Collectors.toSet()));
      }
    }
  }

  private Map<Integer, Integer> createOffsetOrdinalMap(
      int docIdSize, AbstractMockVectorValues<T> totalVectorValues, int docIdOffset) {
    // Compute the offset for the ordinal map to be the number of non-null vectors in the total
    // vector values
    // before the docIdOffset
    int ordinalOffset = 0;
    while (totalVectorValues.nextDoc() < docIdOffset) {
      ordinalOffset++;
    }

    Map<Integer, Integer> offsetOrdinalMap = new HashMap<>();
    for (int curr = 0;
        totalVectorValues.docID() < docIdOffset + docIdSize;
        totalVectorValues.nextDoc()) {
      offsetOrdinalMap.put(curr, ordinalOffset + curr++);
    }

    return offsetOrdinalMap;
  }

  private int[] nodesIteratorToArray(NodesIterator nodesIterator) {
    int[] arr = new int[nodesIterator.size()];
    int i = 0;
    while (nodesIterator.hasNext()) {
      arr[i++] = nodesIterator.nextInt();
    }
    return arr;
  }

  private int[] mapArrayAndSort(int[] arr, Map<Integer, Integer> map) {
    int[] mappedA = new int[arr.length];
    for (int i = 0; i < arr.length; i++) {
      mappedA[i] = map.get(arr[i]);
    }
    Arrays.sort(mappedA);
    return mappedA;
  }

  @SuppressWarnings("unchecked")
  public void testVisitedLimit() throws IOException {
    int nDoc = 500;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    RandomAccessVectorValues<T> vectors = circularVectorValues(nDoc);
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 16, 100);
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());

    int topK = 50;
    int visitedLimit = topK + random().nextInt(5);
    NeighborQueue nn =
        switch (getVectorEncoding()) {
          case FLOAT32 -> HnswGraphSearcher.search(
              (float[]) getTargetVector(),
              topK,
              (RandomAccessVectorValues<float[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              createRandomAcceptOrds(0, nDoc),
              visitedLimit);
          case BYTE -> HnswGraphSearcher.search(
              (byte[]) getTargetVector(),
              topK,
              (RandomAccessVectorValues<byte[]>) vectors.copy(),
              getVectorEncoding(),
              similarityFunction,
              hnsw.getView(),
              createRandomAcceptOrds(0, nDoc),
              visitedLimit);
        };

    assertTrue(nn.incomplete());
    // The visited count shouldn't exceed the limit
    assertTrue(nn.visitedCount() <= visitedLimit);
  }

  public void testHnswGraphBuilderInvalid() {
    expectThrows(
        NullPointerException.class, () -> new ConcurrentHnswGraphBuilder<>(null, null, null, 0, 0));
    // M must be > 0
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          RandomAccessVectorValues<T> vectors = vectorValues(1, 1);
          VectorEncoding vectorEncoding = getVectorEncoding();
          new ConcurrentHnswGraphBuilder<>(
              vectors, vectorEncoding, VectorSimilarityFunction.EUCLIDEAN, 0, 10);
        });
    // beamWidth must be > 0
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          RandomAccessVectorValues<T> vectors = vectorValues(1, 1);
          VectorEncoding vectorEncoding = getVectorEncoding();
          new ConcurrentHnswGraphBuilder<>(
              vectors, vectorEncoding, VectorSimilarityFunction.EUCLIDEAN, 10, 0);
        });
  }

  public void testRamUsageEstimate() throws IOException {
    int size = atLeast(2000);
    int dim = randomIntBetween(100, 1024);
    int M = randomIntBetween(4, 96);

    VectorSimilarityFunction similarityFunction =
        RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    RandomAccessVectorValues<T> vectors = vectorValues(size, dim);

    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextLong();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, M, M * 2);
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    long estimated = RamUsageEstimator.sizeOfObject(hnsw);
    long actual = ramUsed(hnsw);

    assertEquals((double) actual, (double) estimated, (double) actual * 0.3);
  }

  @SuppressWarnings("unchecked")
  public void testDiversity() throws IOException {
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    // Some carefully checked test cases with simple 2d vectors on the unit circle:
    float[][] values = {
      unitVector2d(0.5),
      unitVector2d(0.75),
      unitVector2d(0.2),
      unitVector2d(0.9),
      unitVector2d(0.8),
      unitVector2d(0.77),
      unitVector2d(0.6)
    };
    AbstractMockVectorValues<T> vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 2, 10);
    // node 0 is added by the builder constructor
    RandomAccessVectorValues<T> vectorsCopy = vectors.copy();
    builder.addGraphNode(0, vectorsCopy);
    builder.addGraphNode(1, vectorsCopy);
    builder.addGraphNode(2, vectorsCopy);
    // now every node has tried to attach every other node as a neighbor, but
    // some were excluded based on diversity check.
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3, vectorsCopy);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // we added 3 here
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 1);

    // supplant an existing neighbor
    builder.addGraphNode(4, vectorsCopy);
    // 4 is the same distance from 0 that 2 is; we leave the existing node in place
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3, 4);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    // 1 survives the diversity check
    assertLevel0Neighbors(builder.hnsw, 3, 1, 4);
    assertLevel0Neighbors(builder.hnsw, 4, 1, 3);

    builder.addGraphNode(5, vectorsCopy);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3, 4, 5);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    // even though 5 is closer, 3 is not a neighbor of 5, so no update to *its* neighbors occurs
    assertLevel0Neighbors(builder.hnsw, 3, 1, 4);
    assertLevel0Neighbors(builder.hnsw, 4, 1, 3, 5);
    assertLevel0Neighbors(builder.hnsw, 5, 1, 4);
  }

  public void testDiversityFallback() throws IOException {
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    // Some test cases can't be exercised in two dimensions;
    // in particular if a new neighbor displaces an existing neighbor
    // by being closer to the target, yet none of the existing neighbors is closer to the new vector
    // than to the target -- ie they all remain diverse, so we simply drop the farthest one.
    float[][] values = {
      {0, 0, 0},
      {0, 10, 0},
      {0, 0, 20},
      {10, 0, 0},
      {0, 4, 0}
    };
    AbstractMockVectorValues<T> vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 1, 10);
    RandomAccessVectorValues<T> vectorsCopy = vectors.copy();
    builder.addGraphNode(0, vectorsCopy);
    builder.addGraphNode(1, vectorsCopy);
    builder.addGraphNode(2, vectorsCopy);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // 2 is closer to 0 than 1, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    // 1 is closer to 0 than 2, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3, vectorsCopy);
    // this is one case we are testing; 2 has been displaced by 3
    assertLevel0Neighbors(builder.hnsw, 0, 1, 3);
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 0);
  }

  public void testDiversity3d() throws IOException {
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    // test the case when a neighbor *becomes* non-diverse when a newer better neighbor arrives
    float[][] values = {
      {0, 0, 0},
      {0, 10, 0},
      {0, 0, 20},
      {0, 9, 0}
    };
    AbstractMockVectorValues<T> vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    VectorEncoding vectorEncoding = getVectorEncoding();
    random().nextInt();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 1, 10);
    RandomAccessVectorValues<T> vectorsCopy = vectors.copy();
    builder.addGraphNode(0, vectorsCopy);
    builder.addGraphNode(1, vectorsCopy);
    builder.addGraphNode(2, vectorsCopy);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // 2 is closer to 0 than 1, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    // 1 is closer to 0 than 2, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3, vectorsCopy);
    // this is one case we are testing; 1 has been displaced by 3
    assertLevel0Neighbors(builder.hnsw, 0, 2, 3);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 0, 1);
  }

  private void assertLevel0Neighbors(ConcurrentOnHeapHnswGraph graph, int node, int... expected) {
    Arrays.sort(expected);
    ConcurrentNeighborSet nn = graph.getNeighbors(0, node);
    Iterator<Integer> it = nn.nodeIterator();
    int[] actual = new int[nn.size()];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = it.next();
    }
    Arrays.sort(actual);
    assertArrayEquals(
        "expected: " + Arrays.toString(expected) + " actual: " + Arrays.toString(actual),
        expected,
        actual);
  }

  @SuppressWarnings("unchecked")
  public void testRandom() throws IOException {
    int size = atLeast(100);
    int dim = atLeast(10);
    AbstractMockVectorValues<T> vectors = vectorValues(size, dim);
    int topK = 5;
    VectorEncoding vectorEncoding = getVectorEncoding();
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, 10, 30);
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    Bits acceptOrds = random().nextBoolean() ? null : createRandomAcceptOrds(0, size);

    int totalMatches = 0;
    for (int i = 0; i < 100; i++) {
      NeighborQueue actual;
      T query = randomVector(dim);
      actual =
          switch (getVectorEncoding()) {
            case BYTE -> HnswGraphSearcher.search(
                (byte[]) query,
                100,
                (RandomAccessVectorValues<byte[]>) vectors,
                getVectorEncoding(),
                similarityFunction,
                hnsw.getView(),
                acceptOrds,
                Integer.MAX_VALUE);
            case FLOAT32 -> HnswGraphSearcher.search(
                (float[]) query,
                100,
                (RandomAccessVectorValues<float[]>) vectors,
                getVectorEncoding(),
                similarityFunction,
                hnsw.getView(),
                acceptOrds,
                Integer.MAX_VALUE);
          };

      while (actual.size() > topK) {
        actual.pop();
      }
      NeighborQueue expected = new NeighborQueue(topK, false);
      for (int j = 0; j < size; j++) {
        if (vectors.vectorValue(j) != null && (acceptOrds == null || acceptOrds.get(j))) {
          if (getVectorEncoding() == VectorEncoding.BYTE) {
            assert query instanceof byte[];
            expected.add(
                j, similarityFunction.compare((byte[]) query, (byte[]) vectors.vectorValue(j)));
          } else {
            assert query instanceof float[];
            expected.add(
                j, similarityFunction.compare((float[]) query, (float[]) vectors.vectorValue(j)));
          }
          if (expected.size() > topK) {
            expected.pop();
          }
        }
      }
      assertEquals(topK, actual.size());
      totalMatches += computeOverlap(actual.nodes(), expected.nodes());
    }
    double overlap = totalMatches / (double) (100 * topK);
    assertTrue("overlap=" + overlap, overlap > 0.9);
  }

  private int computeOverlap(int[] a, int[] b) {
    Arrays.sort(a);
    Arrays.sort(b);
    int overlap = 0;
    for (int i = 0, j = 0; i < a.length && j < b.length; ) {
      if (a[i] == b[j]) {
        ++overlap;
        ++i;
        ++j;
      } else if (a[i] > b[j]) {
        ++j;
      } else {
        ++i;
      }
    }
    return overlap;
  }

  public void testConcurrentNeighbors() throws IOException {
    RandomAccessVectorValues<T> vectors = circularVectorValues(3);
    ConcurrentHnswGraphBuilder<T> builder =
        new ConcurrentHnswGraphBuilder<>(vectors, getVectorEncoding(), similarityFunction, 1, 30) {
          @Override
          protected float scoreBetween(T v1, T v2) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              throw new ThreadInterruptedException(e);
            }
            return super.scoreBetween(v1, v2);
          }
        };
    ConcurrentOnHeapHnswGraph hnsw = builder.build(vectors.copy());
    for (int i = 0; i < vectors.size(); i++) {
      assertTrue(hnsw.getNeighbors(0, i).size() <= 2); // Level 0 gets 2x neighbors
      assertEquals(hnsw.getNeighbors(0, i).rawSize(), hnsw.getNeighbors(0, i).size());
    }
  }

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularFloatVectorValues extends FloatVectorValues
      implements RandomAccessVectorValues<float[]> {
    private final int size;

    int doc = -1;

    CircularFloatVectorValues(int size) {
      this.size = size;
    }

    @Override
    public CircularFloatVectorValues copy() {
      return new CircularFloatVectorValues(size);
    }

    @Override
    public int dimension() {
      return 2;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public float[] vectorValue() {
      return vectorValue(doc);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      if (target >= 0 && target < size) {
        doc = target;
      } else {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public float[] vectorValue(int ord) {
      return unitVector2d(ord / (double) size);
    }
  }

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularByteVectorValues extends ByteVectorValues
      implements RandomAccessVectorValues<byte[]> {
    private final int size;

    int doc = -1;

    CircularByteVectorValues(int size) {
      this.size = size;
    }

    @Override
    public CircularByteVectorValues copy() {
      return new CircularByteVectorValues(size);
    }

    @Override
    public int dimension() {
      return 2;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public byte[] vectorValue() {
      return vectorValue(doc);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      if (target >= 0 && target < size) {
        doc = target;
      } else {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public byte[] vectorValue(int ord) {
      float[] value = unitVector2d(ord / (double) size);
      byte[] bValue = new byte[value.length];
      for (int i = 0; i < value.length; i++) {
        bValue[i] = (byte) (value[i] * 127);
      }
      return bValue;
    }
  }

  private static float[] unitVector2d(double piRadians) {
    return unitVector2d(piRadians, new float[2]);
  }

  private static float[] unitVector2d(double piRadians, float[] value) {
    return new float[] {
      (float) Math.cos(Math.PI * piRadians), (float) Math.sin(Math.PI * piRadians)
    };
  }

  private Set<Integer> getNeighborNodes(HnswGraph g) throws IOException {
    Set<Integer> neighbors = new HashSet<>();
    for (int n = g.nextNeighbor(); n != NO_MORE_DOCS; n = g.nextNeighbor()) {
      neighbors.add(n);
    }
    return neighbors;
  }

  void assertVectorsEqual(AbstractMockVectorValues<T> u, AbstractMockVectorValues<T> v)
      throws IOException {
    int uDoc, vDoc;
    while (true) {
      uDoc = u.nextDoc();
      vDoc = v.nextDoc();
      assertEquals(uDoc, vDoc);
      if (uDoc == NO_MORE_DOCS) {
        break;
      }
      switch (getVectorEncoding()) {
        case BYTE -> assertArrayEquals(
            "vectors do not match for doc=" + uDoc,
            (byte[]) u.vectorValue(),
            (byte[]) v.vectorValue());
        case FLOAT32 -> assertArrayEquals(
            "vectors do not match for doc=" + uDoc,
            (float[]) u.vectorValue(),
            (float[]) v.vectorValue(),
            1e-4f);
        default -> throw new IllegalArgumentException(
            "unknown vector encoding: " + getVectorEncoding());
      }
    }
  }

  static float[][] createRandomFloatVectors(int size, int dimension, Random random) {
    float[][] vectors = new float[size][];
    for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
      vectors[offset] = randomVector(random, dimension);
    }
    return vectors;
  }

  static byte[][] createRandomByteVectors(int size, int dimension, Random random) {
    byte[][] vectors = new byte[size][];
    for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
      vectors[offset] = randomVector8(random, dimension);
    }
    return vectors;
  }

  /**
   * Generate a random bitset where before startIndex all bits are set, and after startIndex each
   * entry has a 2/3 probability of being set.
   */
  private static Bits createRandomAcceptOrds(int startIndex, int length) {
    FixedBitSet bits = new FixedBitSet(length);
    // all bits are set before startIndex
    for (int i = 0; i < startIndex; i++) {
      bits.set(i);
    }
    // after startIndex, bits are set with 2/3 probability
    for (int i = startIndex; i < bits.length(); i++) {
      if (random().nextFloat() < 0.667f) {
        bits.set(i);
      }
    }
    return bits;
  }

  static float[] randomVector(Random random, int dim) {
    float[] vec = new float[dim];
    for (int i = 0; i < dim; i++) {
      vec[i] = random.nextFloat();
      if (random.nextBoolean()) {
        vec[i] = -vec[i];
      }
    }
    VectorUtil.l2normalize(vec);
    return vec;
  }

  static byte[] randomVector8(Random random, int dim) {
    float[] fvec = randomVector(random, dim);
    byte[] bvec = new byte[dim];
    for (int i = 0; i < dim; i++) {
      bvec[i] = (byte) (fvec[i] * 127);
    }
    return bvec;
  }

  static String prettyPrint(HnswGraph hnsw) {
    if (hnsw instanceof ConcurrentOnHeapHnswGraph) {
      hnsw = ((ConcurrentOnHeapHnswGraph) hnsw).getView();
    }
    StringBuilder sb = new StringBuilder();
    sb.append(hnsw);
    sb.append("\n");

    try {
      for (int level = 0; level < hnsw.numLevels(); level++) {
        sb.append("# Level ").append(level).append("\n");
        NodesIterator it = hnsw.getNodesOnLevel(level);
        while (it.hasNext()) {
          int node = it.nextInt();
          sb.append("  ").append(node).append(" -> ");
          hnsw.seek(level, node);
          while (true) {
            int neighbor = hnsw.nextNeighbor();
            if (neighbor == NO_MORE_DOCS) {
              break;
            }
            sb.append(" ").append(neighbor);
          }
          sb.append("\n");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return sb.toString();
  }
}
