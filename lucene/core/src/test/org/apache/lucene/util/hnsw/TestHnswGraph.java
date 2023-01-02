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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;
import org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;

/** Tests HNSW KNN graphs */
public class TestHnswGraph extends LuceneTestCase {

  // test writing out and reading in a graph gives the expected graph
  public void testReadWrite() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    RandomVectorValues vectors = new RandomVectorValues(nDoc, dim, random());
    RandomVectorValues v2 = vectors.copy(), v3 = vectors.copy();

    int M = random().nextInt(10) + 5;
    int beamWidth = random().nextInt(10) + 5;
    long seed = random().nextLong();
    VectorSimilarityFunction similarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length - 1) + 1];
    HnswGraphBuilder builder =
        new HnswGraphBuilder(vectors, similarityFunction, M, beamWidth, seed);
    HnswGraph hnsw = builder.build(vectors);

    // Recreate the graph while indexing with the same random seed and write it out
    HnswGraphBuilder.randSeed = seed;
    try (Directory dir = newDirectory()) {
      int nVec = 0, indexedDoc = 0;
      // Don't merge randomly, create a single segment because we rely on the docid ordering for
      // this test
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(
                  new Lucene92Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                      return new Lucene92HnswVectorsFormat(M, beamWidth);
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
          doc.add(new KnnVectorField("field", v2.vectorValue(), similarityFunction));
          doc.add(new StoredField("id", v2.docID()));
          iw.addDocument(doc);
          nVec++;
          indexedDoc++;
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          VectorValues values = ctx.reader().getVectorValues("field");
          assertEquals(dim, values.dimension());
          assertEquals(nVec, values.size());
          assertEquals(indexedDoc, ctx.reader().maxDoc());
          assertEquals(indexedDoc, ctx.reader().numDocs());
          assertVectorsEqual(v3, values);
          HnswGraph graphValues =
              ((Lucene92HnswVectorsReader)
                      ((PerFieldKnnVectorsFormat.FieldsReader)
                              ((CodecReader) ctx.reader()).getVectorReader())
                          .getFieldReader("field"))
                  .getGraph("field");
          assertGraphEqual(hnsw, graphValues);
        }
      }
    }
  }

  private void assertGraphEqual(HnswGraph g, HnswGraph h) throws IOException {
    assertEquals("the number of levels in the graphs are different!", g.numLevels(), h.numLevels());
    assertEquals("the number of nodes in the graphs are different!", g.size(), h.size());

    // assert equal nodes on each level
    for (int level = 0; level < g.numLevels(); level++) {
      NodesIterator nodesOnLevel = g.getNodesOnLevel(level);
      NodesIterator nodesOnLevel2 = h.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext() && nodesOnLevel2.hasNext()) {
        int node = nodesOnLevel.nextInt();
        int node2 = nodesOnLevel2.nextInt();
        assertEquals("nodes in the graphs are different", node, node2);
      }
    }

    // assert equal nodes' neighbours on each level
    for (int level = 0; level < g.numLevels(); level++) {
      NodesIterator nodesOnLevel = g.getNodesOnLevel(level);
      while (nodesOnLevel.hasNext()) {
        int node = nodesOnLevel.nextInt();
        g.seek(level, node);
        h.seek(level, node);
        assertEquals("arcs differ for node " + node, getNeighborNodes(g), getNeighborNodes(h));
      }
    }
  }

  // Make sure we actually approximately find the closest k elements. Mostly this is about
  // ensuring that we have all the distance functions, comparators, priority queues and so on
  // oriented in the right directions
  public void testAknnDiverse() throws IOException {
    int nDoc = 100;
    CircularVectorValues vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.DOT_PRODUCT, 10, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors);
    // run some searches
    NeighborQueue nn =
        HnswGraphSearcher.search(
            new float[] {1, 0},
            10,
            vectors.randomAccess(),
            VectorSimilarityFunction.DOT_PRODUCT,
            hnsw,
            null,
            Integer.MAX_VALUE);

    int[] nodes = nn.nodes();
    assertTrue("Number of found results is not equal to [10].", nodes.length == 10);
    int sum = 0;
    for (int node : nodes) {
      sum += node;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);

    for (int i = 0; i < nDoc; i++) {
      NeighborArray neighbors = hnsw.getNeighbors(0, i);
      int[] nnodes = neighbors.node;
      for (int j = 0; j < neighbors.size(); j++) {
        // all neighbors should be valid node ids.
        assertTrue(nnodes[j] < nDoc);
      }
    }
  }

  public void testSearchWithAcceptOrds() throws IOException {
    int nDoc = 100;
    CircularVectorValues vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.DOT_PRODUCT, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors);
    // the first 10 docs must not be deleted to ensure the expected recall
    Bits acceptOrds = createRandomAcceptOrds(10, vectors.size);
    NeighborQueue nn =
        HnswGraphSearcher.search(
            new float[] {1, 0},
            10,
            vectors.randomAccess(),
            VectorSimilarityFunction.DOT_PRODUCT,
            hnsw,
            acceptOrds,
            Integer.MAX_VALUE);
    int[] nodes = nn.nodes();
    assertTrue("Number of found results is not equal to [10].", nodes.length == 10);
    int sum = 0;
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
      sum += node;
    }
    // We expect to get approximately 100% recall;
    // the lowest docIds are closest to zero; sum(0,9) = 45
    assertTrue("sum(result docs)=" + sum, sum < 75);
  }

  public void testSearchWithSelectiveAcceptOrds() throws IOException {
    int nDoc = 100;
    CircularVectorValues vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.DOT_PRODUCT, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors);
    // Only mark a few vectors as accepted
    BitSet acceptOrds = new FixedBitSet(vectors.size);
    for (int i = 0; i < vectors.size; i += 15 + random().nextInt(5)) {
      acceptOrds.set(i);
    }

    // Check the search finds all accepted vectors
    int numAccepted = acceptOrds.cardinality();
    NeighborQueue nn =
        HnswGraphSearcher.search(
            new float[] {1, 0},
            numAccepted,
            vectors.randomAccess(),
            VectorSimilarityFunction.DOT_PRODUCT,
            hnsw,
            acceptOrds,
            Integer.MAX_VALUE);
    int[] nodes = nn.nodes();
    assertEquals(numAccepted, nodes.length);
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
    }
  }

  public void testSearchWithSkewedAcceptOrds() throws IOException {
    int nDoc = 1000;
    CircularVectorValues vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.EUCLIDEAN, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors);

    // Skip over half of the documents that are closest to the query vector
    FixedBitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 500; i < nDoc; i++) {
      acceptOrds.set(i);
    }
    NeighborQueue nn =
        HnswGraphSearcher.search(
            new float[] {1, 0},
            10,
            vectors.randomAccess(),
            VectorSimilarityFunction.EUCLIDEAN,
            hnsw,
            acceptOrds,
            Integer.MAX_VALUE);
    int[] nodes = nn.nodes();
    assertTrue("Number of found results is not equal to [10].", nodes.length == 10);
    int sum = 0;
    for (int node : nodes) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node));
      sum += node;
    }
    // We still expect to get reasonable recall. The lowest non-skipped docIds
    // are closest to the query vector: sum(500,509) = 5045
    assertTrue("sum(result docs)=" + sum, sum < 5100);
  }

  public void testVisitedLimit() throws IOException {
    int nDoc = 500;
    CircularVectorValues vectors = new CircularVectorValues(nDoc);
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.DOT_PRODUCT, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors);

    int topK = 50;
    int visitedLimit = topK + random().nextInt(5);
    NeighborQueue nn =
        HnswGraphSearcher.search(
            new float[] {1, 0},
            topK,
            vectors.randomAccess(),
            VectorSimilarityFunction.DOT_PRODUCT,
            hnsw,
            createRandomAcceptOrds(0, vectors.size),
            visitedLimit);
    assertTrue(nn.incomplete());
    // The visited count shouldn't exceed the limit
    assertTrue(nn.visitedCount() <= visitedLimit);
  }

  public void testHnswGraphBuilderInvalid() {
    expectThrows(NullPointerException.class, () -> new HnswGraphBuilder(null, null, 0, 0, 0));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new HnswGraphBuilder(
                new RandomVectorValues(1, 1, random()),
                VectorSimilarityFunction.EUCLIDEAN,
                0,
                10,
                0));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new HnswGraphBuilder(
                new RandomVectorValues(1, 1, random()),
                VectorSimilarityFunction.EUCLIDEAN,
                10,
                0,
                0));
  }

  public void testDiversity() throws IOException {
    // Some carefully checked test cases with simple 2d vectors on the unit circle:
    MockVectorValues vectors =
        new MockVectorValues(
            new float[][] {
              unitVector2d(0.5),
              unitVector2d(0.75),
              unitVector2d(0.2),
              unitVector2d(0.9),
              unitVector2d(0.8),
              unitVector2d(0.77),
            });
    // First add nodes until everybody gets a full neighbor list
    HnswGraphBuilder builder =
        new HnswGraphBuilder(
            vectors, VectorSimilarityFunction.DOT_PRODUCT, 2, 10, random().nextInt());
    // node 0 is added by the builder constructor
    // builder.addGraphNode(vectors.vectorValue(0));
    builder.addGraphNode(1, vectors.vectorValue(1));
    builder.addGraphNode(2, vectors.vectorValue(2));
    // now every node has tried to attach every other node as a neighbor, but
    // some were excluded based on diversity check.
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0);
    assertLevel0Neighbors(builder.hnsw, 2, 0);

    builder.addGraphNode(3, vectors.vectorValue(3));
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    // we added 3 here
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    assertLevel0Neighbors(builder.hnsw, 3, 1);

    // supplant an existing neighbor
    builder.addGraphNode(4, vectors.vectorValue(4));
    // 4 is the same distance from 0 that 2 is; we leave the existing node in place
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3, 4);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    // 1 survives the diversity check
    assertLevel0Neighbors(builder.hnsw, 3, 1, 4);
    assertLevel0Neighbors(builder.hnsw, 4, 1, 3);

    builder.addGraphNode(5, vectors.vectorValue(5));
    assertLevel0Neighbors(builder.hnsw, 0, 1, 2);
    assertLevel0Neighbors(builder.hnsw, 1, 0, 3, 4, 5);
    assertLevel0Neighbors(builder.hnsw, 2, 0);
    // even though 5 is closer, 3 is not a neighbor of 5, so no update to *its* neighbors occurs
    assertLevel0Neighbors(builder.hnsw, 3, 1, 4);
    assertLevel0Neighbors(builder.hnsw, 4, 1, 3, 5);
    assertLevel0Neighbors(builder.hnsw, 5, 1, 4);
  }

  private void assertLevel0Neighbors(OnHeapHnswGraph graph, int node, int... expected) {
    Arrays.sort(expected);
    NeighborArray nn = graph.getNeighbors(0, node);
    int[] actual = ArrayUtil.copyOfSubArray(nn.node, 0, nn.size());
    Arrays.sort(actual);
    assertArrayEquals(
        "expected: " + Arrays.toString(expected) + " actual: " + Arrays.toString(actual),
        expected,
        actual);
  }

  public void testRandom() throws IOException {
    int size = atLeast(100);
    int dim = atLeast(10);
    RandomVectorValues vectors = new RandomVectorValues(size, dim, random());
    VectorSimilarityFunction similarityFunction =
        VectorSimilarityFunction.values()[
            random().nextInt(VectorSimilarityFunction.values().length - 1) + 1];
    int topK = 5;
    HnswGraphBuilder builder =
        new HnswGraphBuilder(vectors, similarityFunction, 10, 30, random().nextLong());
    OnHeapHnswGraph hnsw = builder.build(vectors);
    Bits acceptOrds = random().nextBoolean() ? null : createRandomAcceptOrds(0, size);

    int totalMatches = 0;
    for (int i = 0; i < 100; i++) {
      float[] query = randomVector(random(), dim);
      NeighborQueue actual =
          HnswGraphSearcher.search(
              query, 100, vectors, similarityFunction, hnsw, acceptOrds, Integer.MAX_VALUE);
      while (actual.size() > topK) {
        actual.pop();
      }
      NeighborQueue expected = new NeighborQueue(topK, false);
      for (int j = 0; j < size; j++) {
        if (vectors.vectorValue(j) != null && (acceptOrds == null || acceptOrds.get(j))) {
          expected.add(j, similarityFunction.compare(query, vectors.vectorValue(j)));
          if (expected.size() > topK) {
            expected.pop();
          }
        }
      }
      assertEquals(topK, actual.size());
      totalMatches += computeOverlap(actual.nodes(), expected.nodes());
    }
    double overlap = totalMatches / (double) (100 * topK);
    System.out.println("overlap=" + overlap + " totalMatches=" + totalMatches);
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

  /** Returns vectors evenly distributed around the upper unit semicircle. */
  static class CircularVectorValues extends VectorValues
      implements RandomAccessVectorValues, RandomAccessVectorValuesProducer {
    private final int size;
    private final float[] value;

    int doc = -1;

    CircularVectorValues(int size) {
      this.size = size;
      value = new float[2];
    }

    public CircularVectorValues copy() {
      return new CircularVectorValues(size);
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
    public RandomAccessVectorValues randomAccess() {
      return new CircularVectorValues(size);
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
    public long cost() {
      return size;
    }

    @Override
    public float[] vectorValue(int ord) {
      return unitVector2d(ord / (double) size, value);
    }

    @Override
    public BytesRef binaryValue(int ord) {
      return null;
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

  private void assertVectorsEqual(VectorValues u, VectorValues v) throws IOException {
    int uDoc, vDoc;
    while (true) {
      uDoc = u.nextDoc();
      vDoc = v.nextDoc();
      assertEquals(uDoc, vDoc);
      if (uDoc == NO_MORE_DOCS) {
        break;
      }
      assertArrayEquals(
          "vectors do not match for doc=" + uDoc, u.vectorValue(), v.vectorValue(), 1e-4f);
    }
  }

  /** Produces random vectors and caches them for random-access. */
  static class RandomVectorValues extends MockVectorValues {

    RandomVectorValues(int size, int dimension, Random random) {
      super(createRandomVectors(size, dimension, random));
    }

    RandomVectorValues(RandomVectorValues other) {
      super(other.values);
    }

    @Override
    public RandomVectorValues copy() {
      return new RandomVectorValues(this);
    }

    private static float[][] createRandomVectors(int size, int dimension, Random random) {
      float[][] vectors = new float[size][];
      for (int offset = 0; offset < size; offset += random.nextInt(3) + 1) {
        vectors[offset] = randomVector(random, dimension);
      }
      return vectors;
    }
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

  private static float[] randomVector(Random random, int dim) {
    float[] vec = new float[dim];
    for (int i = 0; i < dim; i++) {
      vec[i] = random.nextFloat();
    }
    VectorUtil.l2normalize(vec);
    return vec;
  }
}
