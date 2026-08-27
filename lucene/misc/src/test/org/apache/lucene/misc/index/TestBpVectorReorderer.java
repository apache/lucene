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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;

/** Tests reordering vector values using Binary Partitioning */
public class TestBpVectorReorderer extends LuceneTestCase {

  public static final String FIELD_NAME = "knn";
  BpVectorReorderer reorderer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    reorderer = new BpVectorReorderer(FIELD_NAME);
    reorderer.setMinPartitionSize(1);
    reorderer.setMaxIters(10);
  }

  private void createQuantizedIndex(Directory dir, List<float[]> vectors) throws IOException {
    IndexWriterConfig cfg = new IndexWriterConfig();
    cfg.setCodec(
        TestUtil.alwaysKnnVectorsFormat(new Lucene104HnswScalarQuantizedVectorsFormat(8, 32)));
    try (IndexWriter writer = new IndexWriter(dir, cfg)) {
      int i = 0;
      for (float[] vector : vectors) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD_NAME, vector));
        doc.add(new StoredField("id", i++));
        writer.addDocument(doc);
      }
    }
  }

  public void testRandom() {
    List<float[]> points = new ArrayList<>();
    // This test may fail for small N; 100 seems big enough for the law of large numbers to make it
    // work w/very high probability
    for (int i = 0; i < 100; i++) {
      points.add(new float[] {random().nextFloat(), random().nextFloat(), random().nextFloat()});
    }
    double closestDistanceSum = sumClosestDistances(points);
    // run one iter so we can see what it did
    reorderer.setMaxIters(1);
    Sorter.DocMap map =
        reorderer.computeValueMap(
            FloatVectorValues.fromFloats(points, 3), VectorSimilarityFunction.EUCLIDEAN, null);
    List<float[]> reordered = new ArrayList<>();
    for (int i = 0; i < points.size(); i++) {
      reordered.add(points.get(map.newToOld(i)));
    }
    double reorderedClosestDistanceSum = sumClosestDistances(reordered);
    assertTrue(
        reorderedClosestDistanceSum + ">" + closestDistanceSum,
        reorderedClosestDistanceSum <= closestDistanceSum);
  }

  // Compute the sum of (for each point, the absolute difference between its ordinal and the ordinal
  // of its closest neighbor in Euclidean space) as a measure of whether the reordering successfully
  // brought vector-space neighbors closer together in ordinal space.
  private static double sumClosestDistances(List<float[]> points) {
    int sum = 0;
    for (int i = 0; i < points.size(); i++) {
      int closest = -1;
      double closeness = Double.MAX_VALUE;
      for (int j = 0; j < points.size(); j++) {
        if (j == i) {
          continue;
        }
        double distance = VectorUtil.squareDistance(points.get(i), points.get(j));
        if (distance < closeness) {
          closest = j;
          closeness = distance;
        }
      }
      sum += Math.abs(closest - i);
    }
    return sum;
  }

  public void testEuclideanLinear() {
    doTestEuclideanLinear(null);
  }

  public void testQuantizedIndex() throws Exception {
    doTestQuantizedIndex(null);
  }

  public void testEuclideanLinearConcurrent() {
    int concurrency = random().nextInt(7) + 1;
    // The default ForkJoinPool implementation uses a thread factory that removes all permissions on
    // threads, so we need to create our own to avoid tests failing with FS-based directories.
    ForkJoinPool pool =
        new ForkJoinPool(
            concurrency, p -> new ForkJoinWorkerThread(p) {}, null, random().nextBoolean());
    try {
      doTestEuclideanLinear(pool);
    } finally {
      pool.shutdown();
    }
  }

  private void doTestEuclideanLinear(Executor executor) {
    // a set of 2d points on a line
    List<float[]> vectors = randomLinearVectors();
    List<float[]> shuffled = shuffleVectors(vectors);
    TaskExecutor taskExecutor = getTaskExecutor(executor);
    Sorter.DocMap map =
        reorderer.computeValueMap(
            FloatVectorValues.fromFloats(shuffled, 2),
            VectorSimilarityFunction.EUCLIDEAN,
            taskExecutor);
    verifyEuclideanLinear(map, vectors, shuffled);
  }

  private static TaskExecutor getTaskExecutor(Executor executor) {
    TaskExecutor taskExecutor;
    if (executor != null) {
      taskExecutor = new TaskExecutor(executor);
    } else {
      taskExecutor = null;
    }
    return taskExecutor;
  }

  private void doTestQuantizedIndex(Executor executor) throws IOException {
    // a set of 2d points on a line
    List<float[]> vectors = randomLinearVectors();
    List<float[]> shuffled = shuffleVectors(vectors);
    try (Directory dir = newDirectory()) {
      createQuantizedIndex(dir, shuffled);
      reorderer.reorderIndexDirectory(dir, executor);
      int[] newToOld = new int[vectors.size()];
      int[] oldToNew = new int[vectors.size()];
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        for (int docid = 0; docid < reader.maxDoc(); docid++) {
          if (leafReader.getLiveDocs() == null || leafReader.getLiveDocs().get(docid)) {
            int oldid = Integer.parseInt(leafReader.storedFields().document(docid).get("id"));
            newToOld[docid] = oldid;
            oldToNew[oldid] = docid;
          } else {
            newToOld[docid] = -1;
          }
        }
      }
      verifyEuclideanLinear(
          new Sorter.DocMap() {
            @Override
            public int oldToNew(int docID) {
              return oldToNew[docID];
            }

            @Override
            public int newToOld(int docID) {
              return newToOld[docID];
            }

            @Override
            public int size() {
              return newToOld.length;
            }
          },
          vectors,
          shuffled);
    }
  }

  private static List<float[]> shuffleVectors(List<float[]> vectors) {
    List<float[]> shuffled = new ArrayList<>(vectors);
    Collections.shuffle(shuffled, random());
    return shuffled;
  }

  private static List<float[]> randomLinearVectors() {
    int n = random().nextInt(100) + 10;
    List<float[]> vectors = new ArrayList<>();
    float b = random().nextFloat();
    float m = random().nextFloat();
    float x = random().nextFloat();
    for (int i = 0; i < n; i++) {
      vectors.add(new float[] {x, m * x + b});
      x += random().nextFloat();
    }
    return vectors;
  }

  private static void verifyEuclideanLinear(
      Sorter.DocMap map, List<float[]> vectors, List<float[]> shuffled) {
    int count = shuffled.size();
    assertEquals(count, map.size());
    float[] midPoint = vectors.get(count / 2);
    float[] first = shuffled.get(map.newToOld(0));
    boolean lowFirst = first[0] < midPoint[0];
    for (int i = 0; i < count; i++) {
      int oldIndex = map.newToOld(i);
      assertEquals(i, map.oldToNew(oldIndex));
      // check the "new" order
      float[] v = shuffled.get(oldIndex);
      // first the low vectors, then the high ones, or the other way. Within any given block the
      // partitioning is kind of arbitrary -
      // we don't get a global ordering
      if (i < count / 2 == lowFirst) {
        assertTrue("out of order at " + i, v[0] <= midPoint[0] && v[1] <= midPoint[1]);
      } else {
        assertTrue("out of order at " + i, v[0] >= midPoint[0] && v[1] >= midPoint[1]);
      }
    }
  }

  public void testDotProductCircular() {
    doTestDotProductCircular(null);
  }

  public void testDotProductConcurrent() {
    int concurrency = random().nextInt(7) + 1;
    // The default ForkJoinPool implementation uses a thread factory that removes all permissions on
    // threads, so we need to create our own to avoid tests failing with FS-based directories.
    ForkJoinPool pool =
        new ForkJoinPool(
            concurrency, p -> new ForkJoinWorkerThread(p) {}, null, random().nextBoolean());
    try {
      doTestDotProductCircular(new TaskExecutor(pool));
    } finally {
      pool.shutdown();
    }
  }

  public void doTestDotProductCircular(TaskExecutor executor) {
    // a set of 2d points on a line
    int n = random().nextInt(100) + 10;
    List<float[]> vectors = new ArrayList<>();
    double t = random().nextDouble();
    for (int i = 0; i < n; i++) {
      vectors.add(new float[] {(float) Math.cos(t), (float) Math.sin(t)});
      t += random().nextDouble();
    }
    Sorter.DocMap map =
        reorderer.computeValueMap(
            FloatVectorValues.fromFloats(vectors, 2),
            VectorSimilarityFunction.DOT_PRODUCT,
            executor);
    assertEquals(n, map.size());
    double t0min = 2 * Math.PI, t0max = 0;
    double t1min = 2 * Math.PI, t1max = 0;
    // find the range of the lower half and the range of the upper half
    // they should be non-overlapping
    for (int i = 0; i < n; i++) {
      int oldIndex = map.newToOld(i);
      assertEquals(i, map.oldToNew(oldIndex));
      // check the "new" order
      float[] v = vectors.get(oldIndex);
      t = angle2pi(Math.atan2(v[1], v[0]));
      if (i < n / 2) {
        t0min = Math.min(t0min, t);
        t0max = Math.max(t0max, t);
      } else {
        t1min = Math.min(t1min, t);
        t1max = Math.max(t1max, t);
      }
    }
    assertTrue(
        "ranges overlap",
        (angularDifference(t0min, t0max) < angularDifference(t0min, t1min)
                && angularDifference(t0min, t0max) < angularDifference(t0min, t1max))
            || (angularDifference(t1min, t1max) < angularDifference(t1min, t0min)
                && angularDifference(t1min, t1max) < angularDifference(t1min, t0max)));
  }

  public void testIndexReorderDense() throws Exception {
    List<float[]> vectors = shuffleVectors(randomLinearVectors());

    Path tmpdir = createTempDir();
    try (Directory dir = newFSDirectory(tmpdir)) {
      // create an index with a single leaf
      try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
        int id = 0;
        for (float[] vector : vectors) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
          doc.add(new StoredField("id", id++));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
      }

      // The docId of the documents might have changed due to merging. Compute a mapping from
      // the stored id to the current docId and repopulate the vector list.
      int[] storedIdToDocId = new int[vectors.size()];
      vectors.clear();
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        StoredFields storedFields = reader.storedFields();
        KnnVectorValues.DocIndexIterator it = values.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int storedId = Integer.parseInt(storedFields.document(it.docID()).get("id"));
          vectors.add(values.vectorValue(it.index()).clone());
          storedIdToDocId[storedId] = it.docID();
        }
      }

      // compute the expected ordering
      Sorter.DocMap expected =
          reorderer.computeValueMap(
              FloatVectorValues.fromFloats(vectors, 2), VectorSimilarityFunction.EUCLIDEAN, null);

      int threadCount = random().nextInt(4) + 1;
      threadCount = 1;
      // reorder using the index reordering tool
      BpVectorReorderer.main(
          tmpdir.toString(),
          "f",
          "--min-partition-size",
          "1",
          "--max-iters",
          "10",
          "--thread-count",
          Integer.toString(threadCount));
      // verify the ordering is the same
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        int newId = 0;
        StoredFields storedFields = reader.storedFields();
        KnnVectorValues.DocIndexIterator it = values.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int oldDocId =
              storedIdToDocId[Integer.parseInt(storedFields.document(it.docID()).get("id"))];
          assertEquals(expected.oldToNew(oldDocId), newId);
          float[] expectedVector = vectors.get(expected.newToOld(it.docID()));
          float[] actualVector = values.vectorValue(it.index());
          assertArrayEquals(
              "values differ at index " + oldDocId + "->" + newId + " docid=" + it.docID(),
              expectedVector,
              actualVector,
              0);
          newId++;
        }
      }
    }
  }

  public void testIndexReorderSparse() throws Exception {
    List<float[]> vectors = shuffleVectors(randomLinearVectors());
    // compute the expected ordering
    Sorter.DocMap expected =
        reorderer.computeValueMap(
            FloatVectorValues.fromFloats(vectors, 2), VectorSimilarityFunction.EUCLIDEAN, null);
    Path tmpdir = createTempDir();
    int maxDoc = 0;
    try (Directory dir = newFSDirectory(tmpdir)) {
      // create an index with a single leaf
      try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
        for (float[] vector : vectors) {
          Document doc = new Document();
          if (random().nextBoolean()) {
            for (int i = 0; i < random().nextInt(3); i++) {
              // insert some gaps -- docs with no vectors
              writer.addDocument(doc);
              maxDoc++;
            }
          }
          doc.add(new KnnFloatVectorField("f", vector, VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
          maxDoc++;
        }
      }
      // reorder using the index reordering tool
      BpVectorReorderer.main(
          tmpdir.toString(), "f", "--min-partition-size", "1", "--max-iters", "10");
      // verify the ordering is the same
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReader leafReader = getOnlyLeafReader(reader);
        assertEquals(maxDoc, leafReader.maxDoc());
        FloatVectorValues values = leafReader.getFloatVectorValues("f");
        int lastDocID = 0;
        KnnVectorValues.DocIndexIterator it = values.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          lastDocID = it.docID();
          float[] expectedVector = vectors.get(expected.newToOld(lastDocID));
          float[] actualVector = values.vectorValue(it.index());
          assertArrayEquals(expectedVector, actualVector, 0);
        }
        // docs with no vectors sort at the end
        assertEquals(vectors.size() - 1, lastDocID);
      }
    }
  }

  static double angularDifference(double a, double b) {
    return angle2pi(b - a);
  }

  static double angle2pi(double a) {
    while (a > 2 * Math.PI) {
      a -= 2 * Math.PI;
    }
    while (a < 0) {
      a += 2 * Math.PI;
    }
    return a;
  }
}
