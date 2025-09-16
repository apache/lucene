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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntHashSet;
import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;

/** Tests HNSW KNN graphs */
abstract class HnswGraphTestCase<T> extends LuceneTestCase {

  VectorSimilarityFunction similarityFunction;
  DefaultFlatVectorScorer flatVectorScorer = DefaultFlatVectorScorer.INSTANCE;

  abstract VectorEncoding getVectorEncoding();

  abstract Query knnQuery(String field, T vector, int k);

  abstract T randomVector(int dim);

  abstract KnnVectorValues vectorValues(int size, int dimension);

  abstract KnnVectorValues vectorValues(float[][] values);

  abstract KnnVectorValues vectorValues(LeafReader reader, String fieldName) throws IOException;

  abstract KnnVectorValues vectorValues(
      int size, int dimension, KnnVectorValues pregeneratedVectorValues, int pregeneratedOffset);

  abstract Field knnVectorField(String name, T vector, VectorSimilarityFunction similarityFunction);

  abstract KnnVectorValues circularVectorValues(int nDoc);

  abstract T getTargetVector();

  protected RandomVectorScorerSupplier buildScorerSupplier(KnnVectorValues vectors)
      throws IOException {
    return flatVectorScorer.getRandomVectorScorerSupplier(similarityFunction, vectors);
  }

  protected RandomVectorScorer buildScorer(KnnVectorValues vectors, T query) throws IOException {
    KnnVectorValues vectorsCopy = vectors.copy();
    return switch (getVectorEncoding()) {
      case BYTE ->
          flatVectorScorer.getRandomVectorScorer(similarityFunction, vectorsCopy, (byte[]) query);
      case FLOAT32 ->
          flatVectorScorer.getRandomVectorScorer(similarityFunction, vectorsCopy, (float[]) query);
    };
  }

  // Tests writing segments of various sizes and merging to ensure there are no errors
  // in the HNSW graph merging logic.
  @SuppressWarnings("unchecked")
  public void testRandomReadWriteAndMerge() throws IOException {
    int dim = random().nextInt(100) + 1;
    int[] segmentSizes =
        new int[] {random().nextInt(20) + 1, random().nextInt(10) + 30, random().nextInt(10) + 20};
    // Randomly delete vector documents
    boolean[] addDeletes =
        new boolean[] {random().nextBoolean(), random().nextBoolean(), random().nextBoolean()};
    // randomly index other documents besides vector docs
    boolean[] isSparse =
        new boolean[] {random().nextBoolean(), random().nextBoolean(), random().nextBoolean()};
    int numVectors = segmentSizes[0] + segmentSizes[1] + segmentSizes[2];
    int M = random().nextInt(4) + 2;
    int beamWidth = random().nextInt(10) + 5;
    long seed = random().nextLong();
    KnnVectorValues vectors = vectorValues(numVectors, dim);
    HnswGraphBuilder.randSeed = seed;

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(
                  TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, beamWidth)))
              // set a random merge policy
              .setMergePolicy(newMergePolicy(random()));
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < segmentSizes.length; i++) {
          int size = segmentSizes[i];
          for (int ord = 0; ord < size; ord++) {
            if (isSparse[i] && random().nextBoolean()) {
              int d = random().nextInt(10) + 1;
              for (int j = 0; j < d; j++) {
                Document doc = new Document();
                iw.addDocument(doc);
              }
            }
            Document doc = new Document();
            switch (vectors.getEncoding()) {
              case BYTE -> {
                doc.add(
                    knnVectorField(
                        "field",
                        (T) ((ByteVectorValues) vectors).vectorValue(ord),
                        similarityFunction));
              }
              case FLOAT32 -> {
                doc.add(
                    knnVectorField(
                        "field",
                        (T) ((FloatVectorValues) vectors).vectorValue(ord),
                        similarityFunction));
              }
            }
            doc.add(new StringField("id", Integer.toString(vectors.ordToDoc(ord)), Field.Store.NO));
            iw.addDocument(doc);
          }
          iw.commit();
          if (addDeletes[i] && size > 1) {
            for (int d = 0; d < size; d += random().nextInt(5) + 1) {
              iw.deleteDocuments(new Term("id", Integer.toString(d)));
            }
            iw.commit();
          }
        }
        iw.commit();
        iw.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          KnnVectorValues values = vectorValues(ctx.reader(), "field");
          assertEquals(dim, values.dimension());
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private T vectorValue(KnnVectorValues vectors, int ord) throws IOException {
    switch (vectors.getEncoding()) {
      case BYTE -> {
        return (T) ((ByteVectorValues) vectors).vectorValue(ord);
      }
      case FLOAT32 -> {
        return (T) ((FloatVectorValues) vectors).vectorValue(ord);
      }
    }
    throw new AssertionError("unknown encoding " + vectors.getEncoding());
  }

  // test writing out and reading in a graph gives the expected graph
  public void testReadWrite() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    int M = random().nextInt(4) + 2;
    int beamWidth = random().nextInt(10) + 5;
    long seed = random().nextLong();
    KnnVectorValues vectors = vectorValues(nDoc, dim);
    KnnVectorValues v2 = vectors.copy(), v3 = vectors.copy();
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, M, beamWidth, seed);
    HnswGraph hnsw = builder.build(vectors.size());
    expectThrows(IllegalStateException.class, () -> builder.addGraphNode(0));

    // Recreate the graph while indexing with the same random seed and write it out
    HnswGraphBuilder.randSeed = seed;
    try (Directory dir = newDirectory()) {
      int nVec = 0, indexedDoc = 0;
      // Don't merge randomly, create a single segment because we rely on the docid ordering for
      // this test
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(
                  TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, beamWidth)));
      try (IndexWriter iw = new IndexWriter(dir, iwc)) {
        KnnVectorValues.DocIndexIterator it2 = v2.iterator();
        while (it2.nextDoc() != NO_MORE_DOCS) {
          while (indexedDoc < it2.docID()) {
            // increment docId in the index by adding empty documents
            iw.addDocument(new Document());
            indexedDoc++;
          }
          Document doc = new Document();
          doc.add(knnVectorField("field", vectorValue(v2, it2.index()), similarityFunction));
          doc.add(new StoredField("id", it2.docID()));
          iw.addDocument(doc);
          nVec++;
          indexedDoc++;
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          KnnVectorValues values = vectorValues(ctx.reader(), "field");
          assertEquals(dim, values.dimension());
          assertEquals(nVec, values.size());
          assertEquals(indexedDoc, ctx.reader().maxDoc());
          assertEquals(indexedDoc, ctx.reader().numDocs());
          assertVectorsEqual(v3, values);
          HnswGraph graphValues =
              ((Lucene99HnswVectorsReader)
                      ((PerFieldKnnVectorsFormat.FieldsReader)
                              ((CodecReader) ctx.reader()).getVectorReader())
                          .getFieldReader("field"))
                  .getGraph("field");
          assertGraphEqual(hnsw, graphValues);
        }
      }
    }
  }

  // test that sorted index returns the same search results as unsorted
  public void testSortedAndUnsortedIndicesReturnSameResults() throws IOException {
    int dim = random().nextInt(10) + 3;
    int nDoc = random().nextInt(200) + 100;
    KnnVectorValues vectors = vectorValues(nDoc, dim);

    int M = random().nextInt(10) + 5;
    int beamWidth = random().nextInt(10) + 10;
    VectorSimilarityFunction similarityFunction =
        RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    long seed = random().nextLong();
    HnswGraphBuilder.randSeed = seed;
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, beamWidth)));
    IndexWriterConfig iwc2 =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, beamWidth)))
            .setIndexSort(new Sort(new SortField("sortkey", SortField.Type.LONG)));

    try (Directory dir = newDirectory();
        Directory dir2 = newDirectory()) {
      int indexedDoc = 0;
      try (IndexWriter iw = new IndexWriter(dir, iwc);
          IndexWriter iw2 = new IndexWriter(dir2, iwc2)) {
        for (int ord = 0; ord < vectors.size(); ord++) {
          while (indexedDoc < vectors.ordToDoc(ord)) {
            // increment docId in the index by adding empty documents
            iw.addDocument(new Document());
            indexedDoc++;
          }
          Document doc = new Document();
          doc.add(knnVectorField("vector", vectorValue(vectors, ord), similarityFunction));
          doc.add(new StoredField("id", vectors.ordToDoc(ord)));
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
          Query query = knnQuery("vector", randomVector(dim), 60);
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
    KnnVectorValues vectors = circularVectorValues(nDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 10, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    // run some searches
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()), 10, hnsw, null, Integer.MAX_VALUE);
    TopDocs topDocs = nn.topDocs();
    assertEquals("Number of found results is not equal to [10].", 10, topDocs.scoreDocs.length);
    int sum = 0;
    for (ScoreDoc node : topDocs.scoreDocs) {
      sum += node.doc;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);

    for (int i = 0; i < nDoc; i++) {
      NeighborArray neighbors = hnsw.getNeighbors(0, i);
      int[] nnodes = neighbors.nodes();
      for (int j = 0; j < neighbors.size(); j++) {
        // all neighbors should be valid node ids.
        assertTrue(nnodes[j] < nDoc);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void testSearchWithAcceptOrds() throws IOException {
    int nDoc = 100;
    KnnVectorValues vectors = circularVectorValues(nDoc);
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    // the first 10 docs must not be deleted to ensure the expected recall
    Bits acceptOrds = createRandomAcceptOrds(10, nDoc);
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()), 10, hnsw, acceptOrds, Integer.MAX_VALUE);
    TopDocs nodes = nn.topDocs();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.scoreDocs.length);
    int sum = 0;
    for (ScoreDoc node : nodes.scoreDocs) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node.doc));
      sum += node.doc;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);
  }

  @SuppressWarnings("unchecked")
  public void testSearchWithSelectiveAcceptOrds() throws IOException {
    int nDoc = 100;
    KnnVectorValues vectors = circularVectorValues(nDoc);
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    // Only mark a few vectors as accepted
    BitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 0; i < nDoc; i += random().nextInt(15, 20)) {
      acceptOrds.set(i);
    }

    // Check the search finds all accepted vectors
    int numAccepted = acceptOrds.cardinality();
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()),
            numAccepted,
            hnsw,
            acceptOrds,
            Integer.MAX_VALUE);
    TopDocs nodes = nn.topDocs();
    assertEquals(numAccepted, nodes.scoreDocs.length);
    for (ScoreDoc node : nodes.scoreDocs) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node.doc));
    }
  }

  public void testBuildingJoinSet() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(500) + 100;
    int M = random().nextInt(16) + 16;
    int beamWidth = random().nextInt(100) + 16;
    long seed = random().nextLong();
    KnnVectorValues vectors = vectorValues(nDoc, dim);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, M, beamWidth, seed);
    HnswGraph graph = builder.build(vectors.size());

    IntHashSet j = UpdateGraphsUtils.computeJoinSet(graph);
    assertTrue(
        "Join set size [" + j.size() + "] is not less than graph size [" + graph.size() + "]",
        j.size() < graph.size());
  }

  public void testHnswGraphBuilderInitializationFromGraph_withOffsetZero() throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = 0;
    int dim = atLeast(10);
    long seed = random().nextLong();

    KnnVectorValues initializerVectors = vectorValues(initializerSize, dim);
    RandomVectorScorerSupplier initialscorerSupplier = buildScorerSupplier(initializerVectors);
    HnswGraphBuilder initializerBuilder =
        HnswGraphBuilder.create(initialscorerSupplier, 10, 30, seed);

    OnHeapHnswGraph initializerGraph = initializerBuilder.build(initializerVectors.size());
    KnnVectorValues finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors, docIdOffset);
    int[] initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues, docIdOffset);

    RandomVectorScorerSupplier finalscorerSupplier = buildScorerSupplier(finalVectorValues);

    // we cannot call getNodesOnLevel before the graph reaches the size it claimed, so here we
    // create
    // another graph to do the assertion
    OnHeapHnswGraph graphAfterInit =
        InitializedHnswGraphBuilder.initGraph(
            initializerGraph, initializerOrdMap, initializerGraph.size());

    HnswGraphBuilder finalBuilder =
        InitializedHnswGraphBuilder.fromGraph(
            finalscorerSupplier,
            30,
            seed,
            initializerGraph,
            initializerOrdMap,
            BitSet.of(
                DocIdSetIterator.range(docIdOffset, initializerSize + docIdOffset), totalSize + 1),
            totalSize);

    // When offset is 0, the graphs should be identical before vectors are added
    assertGraphEqual(initializerGraph, graphAfterInit);

    OnHeapHnswGraph finalGraph = finalBuilder.build(finalVectorValues.size());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
  }

  public void testHnswGraphBuilderInitializationFromGraph_withNonZeroOffset() throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = random().nextInt(1, totalSize - initializerSize + 1);
    int dim = atLeast(10);
    long seed = random().nextLong();

    KnnVectorValues initializerVectors = vectorValues(initializerSize, dim);
    RandomVectorScorerSupplier initialscorerSupplier = buildScorerSupplier(initializerVectors);
    HnswGraphBuilder initializerBuilder =
        HnswGraphBuilder.create(initialscorerSupplier, 10, 30, seed);

    OnHeapHnswGraph initializerGraph = initializerBuilder.build(initializerVectors.size());
    KnnVectorValues finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors.copy(), docIdOffset);
    int[] initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues, docIdOffset);

    RandomVectorScorerSupplier finalscorerSupplier = buildScorerSupplier(finalVectorValues);
    HnswGraphBuilder finalBuilder =
        InitializedHnswGraphBuilder.fromGraph(
            finalscorerSupplier,
            30,
            seed,
            initializerGraph,
            initializerOrdMap,
            BitSet.of(
                DocIdSetIterator.range(docIdOffset, initializerSize + docIdOffset), totalSize + 1),
            totalSize);

    assertGraphInitializedFromGraph(finalBuilder.getGraph(), initializerGraph, initializerOrdMap);

    // Confirm that the graph is appropriately constructed by checking that the nodes in the old
    // graph are present in the levels of the new graph
    OnHeapHnswGraph finalGraph = finalBuilder.build(finalVectorValues.size());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
  }

  private void assertGraphContainsGraph(HnswGraph g, HnswGraph initializer, int[] newOrdinals)
      throws IOException {
    for (int i = 0; i < initializer.numLevels(); i++) {
      int[] finalGraphNodesOnLevel = nodesIteratorToArray(g.getNodesOnLevel(i));
      int[] initializerGraphNodesOnLevel =
          mapArrayAndSort(nodesIteratorToArray(initializer.getNodesOnLevel(i)), newOrdinals);
      int overlap = computeOverlap(finalGraphNodesOnLevel, initializerGraphNodesOnLevel);
      assertEquals(initializerGraphNodesOnLevel.length, overlap);
    }
  }

  private void assertGraphInitializedFromGraph(
      HnswGraph g, HnswGraph initializer, int[] newOrdinals) throws IOException {
    assertEquals(
        "the number of levels in the graphs are different!",
        initializer.numLevels(),
        g.numLevels());
    // Confirm that the size of the new graph includes all nodes up to an including the max new
    // ordinal in the old to
    // new ordinal mapping
    assertEquals("the number of nodes in the graphs are different!", initializer.size(), g.size());

    // assert that the neighbors from the old graph are successfully transferred to the new graph
    for (int level = 0; level < g.numLevels(); level++) {
      NodesIterator nodesOnLevel = initializer.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        g.seek(level, newOrdinals[node]);
        initializer.seek(level, node);
        assertEquals(
            "arcs differ for node " + node,
            getNeighborNodes(g),
            getNeighborNodes(initializer).stream()
                .map(n -> newOrdinals[n])
                .collect(Collectors.toSet()));
      }
    }
  }

  private int[] nodesIteratorToArray(NodesIterator nodesIterator) {
    int[] arr = new int[nodesIterator.size()];
    int i = 0;
    while (nodesIterator.hasNext()) {
      arr[i++] = nodesIterator.nextInt();
    }
    return arr;
  }

  private int[] mapArrayAndSort(int[] arr, int[] offset) {
    int[] mappedA = new int[arr.length];
    for (int i = 0; i < arr.length; i++) {
      mappedA[i] = offset[arr[i]];
    }
    Arrays.sort(mappedA);
    return mappedA;
  }

  private int[] createOffsetOrdinalMap(
      int docIdSize, KnnVectorValues totalVectorValues, int docIdOffset) throws IOException {
    // Compute the offset for the ordinal map to be the number of non-null vectors in the total
    // vector values before the docIdOffset
    int ordinalOffset = 0;
    KnnVectorValues.DocIndexIterator it = totalVectorValues.iterator();
    while (it.nextDoc() < docIdOffset) {
      ordinalOffset++;
    }
    int[] offsetOrdinalMap = new int[docIdSize];

    for (int curr = 0; it.docID() < docIdOffset + docIdSize; it.nextDoc()) {
      offsetOrdinalMap[curr] = ordinalOffset + curr++;
    }

    return offsetOrdinalMap;
  }

  @SuppressWarnings("unchecked")
  public void testVisitedLimit() throws IOException {
    int nDoc = 500;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    KnnVectorValues vectors = circularVectorValues(nDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    int topK = 50;
    int visitedLimit = topK + random().nextInt(5);
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()),
            topK,
            hnsw,
            createRandomAcceptOrds(0, nDoc),
            visitedLimit);
    assertTrue(nn.earlyTerminated());
    // The visited count shouldn't exceed the limit
    assertTrue(nn.visitedCount() <= visitedLimit);
  }

  public void testFindAll() throws IOException {
    int numVectors = 10;
    KnnVectorValues vectorValues = circularVectorValues(numVectors);
    T target = getTargetVector();
    float minScore = Float.POSITIVE_INFINITY;
    for (int i = 0; i < numVectors; i++) {
      float score =
          switch (getVectorEncoding()) {
            case BYTE ->
                similarityFunction.compare(
                    ((ByteVectorValues) vectorValues).vectorValue(i), (byte[]) target);
            case FLOAT32 ->
                similarityFunction.compare(
                    ((FloatVectorValues) vectorValues).vectorValue(i), (float[]) target);
          };
      minScore = Math.min(minScore, score);
    }
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectorValues);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(numVectors);
    float finalMinScore = Math.nextDown(minScore);

    AbstractKnnCollector collector =
        new AbstractKnnCollector(numVectors, Long.MAX_VALUE, KnnSearchStrategy.Hnsw.DEFAULT) {
          int collected;

          @Override
          public boolean collect(int docId, float similarity) {
            collected++;
            return true;
          }

          @Override
          public int numCollected() {
            return collected;
          }

          @Override
          public float minCompetitiveSimilarity() {
            return finalMinScore;
          }

          @Override
          public TopDocs topDocs() {
            return null;
          }
        };
    HnswGraphSearcher.search(
        buildScorer(vectorValues, target), collector, hnsw, new BitSet.MatchAllBits(numVectors));
    assertEquals(numVectors, collector.numCollected());
  }

  public void testHnswGraphBuilderInvalid() throws IOException {
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectorValues(1, 1));
    // M must be > 0
    expectThrows(
        IllegalArgumentException.class, () -> HnswGraphBuilder.create(scorerSupplier, 0, 10, 0));
    // beamWidth must be > 0
    expectThrows(
        IllegalArgumentException.class, () -> HnswGraphBuilder.create(scorerSupplier, 10, 0, 0));
  }

  public void testRamUsageEstimate() throws IOException {
    int size = atLeast(2000);
    int dim = randomIntBetween(100, 1024);
    int M = randomIntBetween(4, 96);

    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    KnnVectorValues vectors = vectorValues(size, dim);

    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder =
        HnswGraphBuilder.create(scorerSupplier, M, M * 2, random().nextLong());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
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
    KnnVectorValues vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 2, 10, random().nextInt());
    // node 0 is added by the builder constructor
    builder.addGraphNode(0);
    builder.addGraphNode(1);
    builder.addGraphNode(2);
    // now every node has tried to attach every other node as a neighbor, but
    // some were excluded based on diversity check.
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // we added 3 here
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 1);

    // supplant an existing neighbor
    builder.addGraphNode(4);
    // 4 is the same distance from 0 that 2 is; we leave the existing node in place
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3, 4);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    // 1 survives the diversity check
    assertLevel0Neighbors(builder.hnsw, 3, 1, 4);
    assertLevel0Neighbors(builder.hnsw, 4, 1, 3);

    builder.addGraphNode(5);
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
    KnnVectorValues vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 1, 10, random().nextInt());
    builder.addGraphNode(0);
    builder.addGraphNode(1);
    builder.addGraphNode(2);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // 2 is closer to 0 than 1, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    // 1 is closer to 0 than 2, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3);
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
    KnnVectorValues vectors = vectorValues(values);
    // First add nodes until everybody gets a full neighbor list
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 1, 10, random().nextInt());
    builder.addGraphNode(0);
    builder.addGraphNode(1);
    builder.addGraphNode(2);
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // 2 is closer to 0 than 1, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    // 1 is closer to 0 than 2, so it is excluded as non-diverse
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3);
    // this is one case we are testing; 1 has been displaced by 3
    assertLevel0Neighbors(builder.hnsw, 0, 2, 3);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 0, 1);
  }

  private void assertLevel0Neighbors(OnHeapHnswGraph graph, int node, int... expected) {
    Arrays.sort(expected);
    NeighborArray nn = graph.getNeighbors(0, node);
    int[] actual = ArrayUtil.copyOfSubArray(nn.nodes(), 0, nn.size());
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
    KnnVectorValues vectors = vectorValues(size, dim);
    int topK = 5;
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 10, 30, random().nextLong());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    Bits acceptOrds = random().nextBoolean() ? null : createRandomAcceptOrds(0, size);

    int totalMatches = 0;
    for (int i = 0; i < 100; i++) {
      KnnCollector actual;
      T query = randomVector(dim);
      actual =
          HnswGraphSearcher.search(
              buildScorer(vectors, query), 100, hnsw, acceptOrds, Integer.MAX_VALUE);
      TopDocs topDocs = actual.topDocs();
      NeighborQueue expected = new NeighborQueue(topK, false);
      for (int j = 0; j < size; j++) {
        if (vectorValue(vectors, j) != null && (acceptOrds == null || acceptOrds.get(j))) {
          if (getVectorEncoding() == VectorEncoding.BYTE) {
            expected.add(
                j, similarityFunction.compare((byte[]) query, (byte[]) vectorValue(vectors, j)));
          } else {
            expected.add(
                j, similarityFunction.compare((float[]) query, (float[]) vectorValue(vectors, j)));
          }
          if (expected.size() > topK) {
            expected.pop();
          }
        }
      }
      int[] actualTopKDocs = new int[topK];
      for (int j = 0; j < topK; j++) {
        actualTopKDocs[j] = topDocs.scoreDocs[j].doc;
      }
      totalMatches += computeOverlap(actualTopKDocs, expected.nodes());
    }
    double overlap = totalMatches / (double) (100 * topK);
    System.out.println("overlap=" + overlap + " totalMatches=" + totalMatches);
    assertTrue("overlap=" + overlap, overlap > 0.9);
  }

  /* test thread-safety of searching OnHeapHnswGraph */
  @SuppressWarnings("unchecked")
  public void testOnHeapHnswGraphSearch()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    int size = atLeast(100);
    int dim = atLeast(10);
    KnnVectorValues vectors = vectorValues(size, dim);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 10, 30, random().nextLong());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    Bits acceptOrds = random().nextBoolean() ? null : createRandomAcceptOrds(0, size);

    List<T> queries = new ArrayList<>();
    List<KnnCollector> expects = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      KnnCollector expect;
      T query = randomVector(dim);
      queries.add(query);
      expect =
          HnswGraphSearcher.search(
              buildScorer(vectors, query), 100, hnsw, acceptOrds, Integer.MAX_VALUE);
      expects.add(expect);
    }

    ExecutorService exec =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("onHeapHnswSearch"));
    List<Future<KnnCollector>> futures = new ArrayList<>();
    for (T query : queries) {
      futures.add(
          exec.submit(
              () -> {
                KnnCollector actual;
                try {
                  actual =
                      HnswGraphSearcher.search(
                          buildScorer(vectors, query), 100, hnsw, acceptOrds, Integer.MAX_VALUE);
                } catch (IOException ioe) {
                  throw new RuntimeException(ioe);
                }
                return actual;
              }));
    }
    List<KnnCollector> actuals = new ArrayList<>();
    for (Future<KnnCollector> future : futures) {
      actuals.add(future.get(10, TimeUnit.SECONDS));
    }
    exec.shutdownNow();
    for (int i = 0; i < expects.size(); i++) {
      TopDocs expect = expects.get(i).topDocs();
      TopDocs actual = actuals.get(i).topDocs();
      int[] expectedDocs = new int[expect.scoreDocs.length];
      for (int j = 0; j < expect.scoreDocs.length; j++) {
        expectedDocs[j] = expect.scoreDocs[j].doc;
      }
      int[] actualDocs = new int[actual.scoreDocs.length];
      for (int j = 0; j < actual.scoreDocs.length; j++) {
        actualDocs[j] = actual.scoreDocs[j].doc;
      }
      assertArrayEquals(expectedDocs, actualDocs);
    }
  }

  /*
   * A very basic test ensure the concurrent merge does not throw exceptions, it by no means guarantees the
   * true correctness of the concurrent merge and that must be checked manually by running a KNN benchmark
   * and comparing the recall
   */
  public void testConcurrentMergeBuilder() throws IOException {
    int size = atLeast(1000);
    int dim = atLeast(10);
    KnnVectorValues vectors = vectorValues(size, dim);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    ExecutorService exec = Executors.newFixedThreadPool(4, new NamedThreadFactory("hnswMerge"));
    TaskExecutor taskExecutor = new TaskExecutor(exec);
    HnswGraphBuilder.randSeed = random().nextLong();
    HnswConcurrentMergeBuilder builder =
        new HnswConcurrentMergeBuilder(
            taskExecutor, 4, scorerSupplier, 30, new OnHeapHnswGraph(10, size), null);
    builder.setBatchSize(100);
    builder.build(size);
    exec.shutdownNow();
    OnHeapHnswGraph graph = builder.getCompletedGraph();
    assertTrue(graph.entryNode() != -1);
    assertEquals(size, graph.size());
    assertEquals(size - 1, graph.maxNodeId());
    for (int l = 0; l < graph.numLevels(); l++) {
      assertNotNull(graph.getNodesOnLevel(l));
    }
    // cannot build twice
    expectThrows(IllegalStateException.class, () -> builder.build(size));
  }

  public void testAllNodesVisitedInSingleLevel() throws IOException {
    int size = atLeast(100);
    int dim = atLeast(50);

    // Search for a large number of results
    int topK = size - 1;

    KnnVectorValues docVectors = vectorValues(size, dim);
    HnswGraph graph =
        HnswGraphBuilder.create(buildScorerSupplier(docVectors), 10, 30, random().nextLong())
            .build(size);

    HnswGraph singleLevelGraph =
        new DelegateHnswGraph(graph) {
          @Override
          public int numLevels() {
            // Only retain the last level
            return 1;
          }
        };

    KnnVectorValues queryVectors = vectorValues(1, dim);
    RandomVectorScorer queryScorer = buildScorer(docVectors, vectorValue(queryVectors, 0));

    KnnCollector collector = new TopKnnCollector(topK, Integer.MAX_VALUE);
    HnswGraphSearcher.search(queryScorer, collector, singleLevelGraph, null);

    // Check that we visit all nodes
    assertEquals(graph.size(), collector.visitedCount());
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

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularFloatVectorValues extends FloatVectorValues {
    private final int size;
    private final float[] value;

    int doc = -1;

    CircularFloatVectorValues(int size) {
      this.size = size;
      value = new float[2];
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

    public float[] vectorValue() {
      return vectorValue(doc);
    }

    public int docID() {
      return doc;
    }

    public int nextDoc() {
      return advance(doc + 1);
    }

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
      return unitVector2d(ord / (double) size, value);
    }

    @Override
    public VectorScorer scorer(float[] target) {
      throw new UnsupportedOperationException();
    }
  }

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularByteVectorValues extends ByteVectorValues {
    private final int size;
    private final float[] value;
    private final byte[] bValue;

    int doc = -1;

    CircularByteVectorValues(int size) {
      this.size = size;
      value = new float[2];
      bValue = new byte[2];
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

    public byte[] vectorValue() {
      return vectorValue(doc);
    }

    public int docID() {
      return doc;
    }

    public int nextDoc() {
      return advance(doc + 1);
    }

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
      unitVector2d(ord / (double) size, value);
      for (int i = 0; i < value.length; i++) {
        bValue[i] = (byte) (value[i] * 127);
      }
      return bValue;
    }

    @Override
    public VectorScorer scorer(byte[] target) {
      throw new UnsupportedOperationException();
    }
  }

  private static float[] unitVector2d(double piRadians) {
    return unitVector2d(piRadians, new float[2]);
  }

  private static float[] unitVector2d(double piRadians, float[] value) {
    value[0] = (float) Math.cos(Math.PI * piRadians);
    value[1] = (float) Math.sin(Math.PI * piRadians);
    return value;
  }

  private Set<Integer> getNeighborNodes(HnswGraph g) throws IOException {
    Set<Integer> neighbors = new HashSet<>();
    for (int n = g.nextNeighbor(); n != NO_MORE_DOCS; n = g.nextNeighbor()) {
      neighbors.add(n);
    }
    return neighbors;
  }

  void assertVectorsEqual(KnnVectorValues u, KnnVectorValues v) throws IOException {
    int uDoc, vDoc;
    assertEquals(u.size(), v.size());
    for (int ord = 0; ord < u.size(); ord++) {
      uDoc = u.ordToDoc(ord);
      vDoc = v.ordToDoc(ord);
      assertEquals(uDoc, vDoc);
      assertNotEquals(NO_MORE_DOCS, uDoc);
      switch (getVectorEncoding()) {
        case BYTE ->
            assertArrayEquals(
                "vectors do not match for doc=" + uDoc,
                (byte[]) vectorValue(u, ord),
                (byte[]) vectorValue(v, ord));
        case FLOAT32 ->
            assertArrayEquals(
                "vectors do not match for doc=" + uDoc,
                (float[]) vectorValue(u, ord),
                (float[]) vectorValue(v, ord),
                1e-4f);
        default ->
            throw new IllegalArgumentException("unknown vector encoding: " + getVectorEncoding());
      }
    }
  }

  static float[][] createRandomFloatVectors(int size, int dimension, Random random) {
    float[][] vectors = new float[size][];
    for (int offset = 0; offset < size; offset++) {
      vectors[offset] = randomVector(random, dimension);
    }
    return vectors;
  }

  static byte[][] createRandomByteVectors(int size, int dimension, Random random) {
    byte[][] vectors = new byte[size][];
    for (int offset = 0; offset < size; offset++) {
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

  private static class DelegateHnswGraph extends HnswGraph {
    final HnswGraph delegate;

    DelegateHnswGraph(HnswGraph delegate) {
      this.delegate = delegate;
    }

    @Override
    public void seek(int level, int target) throws IOException {
      delegate.seek(level, target);
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public int nextNeighbor() throws IOException {
      return delegate.nextNeighbor();
    }

    @Override
    public int numLevels() throws IOException {
      return delegate.numLevels();
    }

    @Override
    public int entryNode() throws IOException {
      return delegate.entryNode();
    }

    @Override
    public int neighborCount() {
      return delegate.neighborCount();
    }

    @Override
    public int maxConn() {
      return delegate.maxConn();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) throws IOException {
      return delegate.getNodesOnLevel(level);
    }
  }
}
