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
package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.English;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSysoutChecks(bugUrl = "prints info from within cuvs")
public class TestCuVS extends LuceneTestCase {

  protected static Logger log = Logger.getLogger(TestCuVS.class.getName());

  static final Codec codec = TestUtil.alwaysKnnVectorsFormat(new CuVSVectorsFormat());
  static IndexSearcher searcher;
  static IndexReader reader;
  static Directory directory;

  static int DATASET_SIZE_LIMIT = 1000;
  static int DIMENSIONS_LIMIT = 2048;
  static int NUM_QUERIES_LIMIT = 10;
  static int TOP_K_LIMIT = 64; // TODO This fails beyond 64

  public static float[][] dataset;

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeTrue("cuvs not supported", CuVSVectorsFormat.supported());
    directory = newDirectory();

    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
                .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                .setCodec(codec)
                .setMergePolicy(newTieredMergePolicy()));

    log.info("Merge Policy: " + writer.w.getConfig().getMergePolicy());

    Random random = random();
    int datasetSize = random.nextInt(DATASET_SIZE_LIMIT) + 1;
    int dimensions = random.nextInt(DIMENSIONS_LIMIT) + 1;
    dataset = generateDataset(random, datasetSize, dimensions);
    for (int i = 0; i < datasetSize; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
      doc.add(newTextField("field", English.intToEnglish(i), Field.Store.YES));
      boolean skipVector =
          random.nextInt(10) < 0; // disable testing with holes for now, there's some bug.
      if (!skipVector
          || datasetSize < 100) { // about 10th of the documents shouldn't have a single vector
        doc.add(new KnnFloatVectorField("vector", dataset[i], VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new KnnFloatVectorField("vector2", dataset[i], VectorSimilarityFunction.EUCLIDEAN));
      }

      writer.addDocument(doc);
    }

    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (reader != null) reader.close();
    if (directory != null) directory.close();
    searcher = null;
    reader = null;
    directory = null;
    log.info("Test finished");
  }

  @Test
  public void testVectorSearch() throws IOException {
    Random random = random();
    int numQueries = random.nextInt(NUM_QUERIES_LIMIT) + 1;
    int topK = Math.min(random.nextInt(TOP_K_LIMIT) + 1, dataset.length);

    if (dataset.length < topK) topK = dataset.length;

    float[][] queries = generateQueries(random, dataset[0].length, numQueries);
    List<List<Integer>> expected = generateExpectedResults(topK, dataset, queries);

    log.info("Dataset size: " + dataset.length + "x" + dataset[0].length);
    log.info("Query size: " + numQueries + "x" + queries[0].length);
    log.info("TopK: " + topK);

    // Query query = new CuVSKnnFloatVectorQuery("vector", queries[0], topK, topK, 1);
    Query query = new KnnFloatVectorQuery("vector", queries[0], topK);
    int correct[] = new int[topK];
    for (int i = 0; i < topK; i++) correct[i] = expected.get(0).get(i);

    ScoreDoc[] hits = searcher.search(query, topK).scoreDocs;
    log.info("RESULTS: " + Arrays.toString(hits));
    log.info("EXPECTD: " + expected.get(0));

    for (ScoreDoc hit : hits) {
      log.info("\t" + reader.storedFields().document(hit.doc).get("id") + ": " + hit.score);
    }

    for (ScoreDoc hit : hits) {
      int doc = Integer.parseInt(reader.storedFields().document(hit.doc).get("id"));
      assertTrue("Result returned was not in topk*2: " + doc, expected.get(0).contains(doc));
    }
  }

  private static float[][] generateQueries(Random random, int dimensions, int numQueries) {
    // Generate random query vectors
    float[][] queries = new float[numQueries][dimensions];
    for (int i = 0; i < numQueries; i++) {
      for (int j = 0; j < dimensions; j++) {
        queries[i][j] = random.nextFloat() * 100;
      }
    }
    return queries;
  }

  private static float[][] generateDataset(Random random, int datasetSize, int dimensions) {
    // Generate a random dataset
    float[][] dataset = new float[datasetSize][dimensions];
    for (int i = 0; i < datasetSize; i++) {
      for (int j = 0; j < dimensions; j++) {
        dataset[i][j] = random.nextFloat() * 100;
      }
    }
    return dataset;
  }

  private static List<List<Integer>> generateExpectedResults(
      int topK, float[][] dataset, float[][] queries) {
    List<List<Integer>> neighborsResult = new ArrayList<>();
    int dimensions = dataset[0].length;

    for (float[] query : queries) {
      Map<Integer, Double> distances = new TreeMap<>();
      for (int j = 0; j < dataset.length; j++) {
        double distance = 0;
        for (int k = 0; k < dimensions; k++) {
          distance += (query[k] - dataset[j][k]) * (query[k] - dataset[j][k]);
        }
        distances.put(j, (distance));
      }

      Map<Integer, Double> sorted = new TreeMap<Integer, Double>(distances);
      log.info("EXPECTED: " + sorted);

      // Sort by distance and select the topK nearest neighbors
      List<Integer> neighbors =
          distances.entrySet().stream()
              .sorted(Map.Entry.comparingByValue())
              .map(Map.Entry::getKey)
              .toList();
      neighborsResult.add(
          neighbors.subList(
              0,
              Math.min(
                  topK * 3,
                  dataset.length))); // generate double the topK results in the expected array
    }

    log.info("Expected results generated successfully.");
    return neighborsResult;
  }
}
