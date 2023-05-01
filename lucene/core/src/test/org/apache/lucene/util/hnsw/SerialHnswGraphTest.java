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
import static org.apache.lucene.tests.util.LuceneTestCase.atLeast;
import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.apache.lucene.util.hnsw.HnswGraphTestCase.assertGraphContainsGraph;
import static org.apache.lucene.util.hnsw.HnswGraphTestCase.assertGraphEqual;
import static org.apache.lucene.util.hnsw.HnswGraphTestCase.assertVectorsEqual;
import static org.apache.lucene.util.hnsw.HnswGraphTestCase.getNeighborNodes;
import static org.apache.lucene.util.hnsw.HnswGraphTestCase.sortedNodesOnLevel;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;

public interface SerialHnswGraphTest<T> {
  // test writing out and reading in a graph gives the expected graph
  public default void testReadWrite() throws IOException {
    int dim = random().nextInt(100) + 1;
    int nDoc = random().nextInt(100) + 1;
    int M = random().nextInt(4) + 2;
    int beamWidth = random().nextInt(10) + 5;
    long seed = random().nextLong();
    AbstractMockVectorValues<T> vectors = vectorValues(nDoc, dim);
    AbstractMockVectorValues<T> v2 = vectors.copy(), v3 = vectors.copy();
    OnHeapHnswGraphBuilder<T> builder =
        OnHeapHnswGraphFactory.instance.createBuilder(
            vectors, getVectorEncoding(), getSimilarityFunction(), M, beamWidth, seed);
    HnswGraph hnsw = builder.build(vectors.copy());

    // Recreate the graph while indexing with the same random seed and write it out
    OnHeapHnswGraphBuilder.randSeed = seed;
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
          doc.add(knnVectorField("field", v2.vectorValue(), getSimilarityFunction()));
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
          assertVectorsEqual(getVectorEncoding(), v3, values);
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

  public default void testHnswGraphBuilderInitializationFromGraph_withOffsetZero()
      throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = 0;
    int dim = atLeast(10);
    long seed = random().nextLong();

    AbstractMockVectorValues<T> initializerVectors = vectorValues(initializerSize, dim);
    OnHeapHnswGraphBuilder<T> initializerBuilder =
        OnHeapHnswGraphFactory.instance.createBuilder(
            initializerVectors, getVectorEncoding(), getSimilarityFunction(), 10, 30, seed);

    HnswGraph initializerGraph = initializerBuilder.build(initializerVectors.copy());
    AbstractMockVectorValues<T> finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors, docIdOffset);

    Map<Integer, Integer> initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues, docIdOffset);

    OnHeapHnswGraphBuilder<T> finalBuilder =
        OnHeapHnswGraphFactory.instance.createBuilder(
            finalVectorValues,
            getVectorEncoding(),
            getSimilarityFunction(),
            10,
            30,
            seed,
            initializerGraph,
            initializerOrdMap);

    // When offset is 0, the graphs should be identical before vectors are added
    assertGraphEqual(initializerGraph, finalBuilder.getGraph());

    HnswGraph finalGraph = finalBuilder.build(finalVectorValues.copy());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
  }

  public default void testHnswGraphBuilderInitializationFromGraph_withNonZeroOffset()
      throws IOException {
    int totalSize = atLeast(100);
    int initializerSize = random().nextInt(5, totalSize);
    int docIdOffset = random().nextInt(1, totalSize - initializerSize + 1);
    int dim = atLeast(10);
    long seed = random().nextLong();

    AbstractMockVectorValues<T> initializerVectors = vectorValues(initializerSize, dim);
    OnHeapHnswGraphBuilder<T> initializerBuilder =
        OnHeapHnswGraphFactory.instance.createBuilder(
            initializerVectors.copy(), getVectorEncoding(), getSimilarityFunction(), 10, 30, seed);
    HnswGraph initializerGraph = initializerBuilder.build(initializerVectors.copy());
    AbstractMockVectorValues<T> finalVectorValues =
        vectorValues(totalSize, dim, initializerVectors.copy(), docIdOffset);
    Map<Integer, Integer> initializerOrdMap =
        createOffsetOrdinalMap(initializerSize, finalVectorValues.copy(), docIdOffset);

    OnHeapHnswGraphBuilder<T> finalBuilder =
        OnHeapHnswGraphFactory.instance.createBuilder(
            finalVectorValues,
            getVectorEncoding(),
            getSimilarityFunction(),
            10,
            30,
            seed,
            initializerGraph,
            initializerOrdMap);

    assertGraphInitializedFromGraph(finalBuilder.getGraph(), initializerGraph, initializerOrdMap);

    // Confirm that the graph is appropriately constructed by checking that the nodes in the old
    // graph are present in the levels of the new graph
    HnswGraph finalGraph = finalBuilder.build(finalVectorValues.copy());
    assertGraphContainsGraph(finalGraph, initializerGraph, initializerOrdMap);
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
      HnswGraph.NodesIterator nodesOnLevel = h.getNodesOnLevel(level);
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

  HnswGraphFactory getFactory();

  Field knnVectorField(String name, T vector, VectorSimilarityFunction similarityFunction);

  VectorSimilarityFunction getSimilarityFunction();

  VectorEncoding getVectorEncoding();

  AbstractMockVectorValues<T> vectorValues(int nDoc, int dim);

  AbstractMockVectorValues<T> vectorValues(LeafReader reader, String fieldName) throws IOException;

  AbstractMockVectorValues<T> vectorValues(
      int size,
      int dimension,
      AbstractMockVectorValues<T> pregeneratedVectorValues,
      int pregeneratedOffset);
}
