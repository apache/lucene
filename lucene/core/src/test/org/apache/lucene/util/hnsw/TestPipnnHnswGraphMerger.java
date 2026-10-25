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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.VectorUtil;

/** Exercises {@link PipnnHnswGraphMerger} through {@link Lucene99HnswVectorsFormat}. */
public class TestPipnnHnswGraphMerger extends LuceneTestCase {

  private static final String VECTOR_FIELD = "v";
  private static final String ID_FIELD = "id";
  private static final int M = 16;
  private static final int BEAM_WIDTH = 100;
  private static final PipnnParams PARAMS =
      new PipnnParams(64, new int[] {4, 2}, 40, 0.02, 2, 0, 12, 1.0f);

  public void testForceMergeBuildsSearchableGraph() throws IOException {
    runMergeTest(1, null);
  }

  public void testForceMergeWithMergeExecutor() throws IOException {
    ExecutorService exec = Executors.newFixedThreadPool(3, new NamedThreadFactory("pipnn-merge"));
    try {
      runMergeTest(3, exec);
    } finally {
      exec.shutdownNow();
    }
  }

  public void testFormatToStringMentionsPipnn() {
    Lucene99HnswVectorsFormat format =
        new Lucene99HnswVectorsFormat(M, BEAM_WIDTH, 1, null, PARAMS);
    assertTrue(format.toString().contains("pipnnParams=" + PARAMS));
    assertFalse(new Lucene99HnswVectorsFormat(M, BEAM_WIDTH).toString().contains("pipnnParams"));
  }

  private void runMergeTest(int numMergeWorkers, ExecutorService mergeExec) throws IOException {
    // enough live vectors that the merged segment is above the tiny segment threshold and gets
    // a graph at all, see Lucene99HnswVectorsFormat#HNSW_GRAPH_THRESHOLD
    int numDocs = TestUtil.nextInt(random(), 1000, 2500);
    int dim = TestUtil.nextInt(random(), 4, 12);
    int numSegments = TestUtil.nextInt(random(), 2, 5);
    VectorSimilarityFunction similarity =
        RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    float[][] vectors = HnswGraphTestCase.createRandomFloatVectors(numDocs, dim, random());
    for (float[] vector : vectors) {
      VectorUtil.l2normalize(vector);
    }

    KnnVectorsFormat format =
        new Lucene99HnswVectorsFormat(M, BEAM_WIDTH, numMergeWorkers, mergeExec, PARAMS);
    Codec codec = TestUtil.alwaysKnnVectorsFormat(format);
    AtomicInteger pipnnBuilds = new AtomicInteger();
    InfoStream infoStream =
        new InfoStream() {
          @Override
          public void message(String component, String message) {
            if (message.startsWith("PiPNN build completed")) {
              pipnnBuilds.incrementAndGet();
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
      IndexWriterConfig config = newIndexWriterConfig().setCodec(codec).setInfoStream(infoStream);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        int perSegment = numDocs / numSegments + 1;
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField(ID_FIELD, Integer.toString(i), Field.Store.YES));
          doc.add(new KnnFloatVectorField(VECTOR_FIELD, vectors[i], similarity));
          writer.addDocument(doc);
          if (i % perSegment == perSegment - 1) {
            writer.flush();
          }
        }
        // some deletes, so that the incremental merger would also have to rebuild
        for (int i = 0; i < numDocs; i += 50) {
          writer.deleteDocuments(new Term(ID_FIELD, Integer.toString(i)));
        }
        writer.commit();
        writer.forceMerge(1);
      }
      assertTrue("merging must have run a PiPNN build", pipnnBuilds.get() >= 1);

      try (IndexReader reader = DirectoryReader.open(dir)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(1, leaves.size());
        assertTrue(HnswUtil.graphIsRooted(reader, VECTOR_FIELD));
        IndexSearcher searcher = newSearcher(reader);
        int k = 10;
        int numQueries = 30;
        int hits = 0;
        for (int q = 0; q < numQueries; q++) {
          float[] query = VectorUtil.l2normalize(HnswGraphTestCase.randomVector(random(), dim));
          Set<String> expected = exactNeighbors(vectors, query, similarity, k, numDocs);
          // collect more candidates than k, as a real query would, and keep the k best
          TopDocs topDocs = searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, query, 5 * k), k);
          assertEquals(k, topDocs.scoreDocs.length);
          for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            String id = searcher.storedFields().document(scoreDoc.doc).get(ID_FIELD);
            if (expected.contains(id)) {
              hits++;
            }
          }
        }
        float recall = hits / (float) (k * numQueries);
        assertTrue("recall@" + k + " too low: " + recall, recall >= 0.9f);
      }
    }
  }

  private static Set<String> exactNeighbors(
      float[][] vectors, float[] query, VectorSimilarityFunction similarity, int k, int numDocs) {
    NeighborQueue queue = new NeighborQueue(k, false);
    for (int i = 0; i < numDocs; i++) {
      if (i % 50 == 0) {
        continue; // deleted
      }
      queue.insertWithOverflow(i, similarity.compare(query, vectors[i]));
    }
    Set<String> ids = new HashSet<>();
    while (queue.size() > 0) {
      ids.add(Integer.toString(queue.pop()));
    }
    return ids;
  }
}
