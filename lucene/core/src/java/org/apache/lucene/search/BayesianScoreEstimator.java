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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ArrayUtil;

/**
 * Estimates {@link BayesianScoreQuery} parameters (alpha, beta, base rate) from corpus statistics
 * via pseudo-query sampling.
 *
 * <p>The estimation algorithm:
 *
 * <ol>
 *   <li>Sample N documents randomly from the index
 *   <li>For each document, create a pseudo-query from its first few tokens in the target field
 *   <li>Run each pseudo-query via BM25 and collect the score distribution
 *   <li>Estimate: beta = median(scores), alpha = 1 / std(scores)
 *   <li>Estimate base rate: mean fraction of documents scoring above the 95th percentile
 * </ol>
 *
 * @lucene.experimental
 */
public class BayesianScoreEstimator {

  /** Estimated parameters for {@link BayesianScoreQuery}. */
  public record Parameters(float alpha, float beta, float baseRate) {}

  private static final int DEFAULT_N_SAMPLES = 50;
  private static final int DEFAULT_TOKENS_PER_QUERY = 5;
  private static final double PERCENTILE_THRESHOLD = 0.95;
  private static final float BASE_RATE_MIN = 1e-6f;
  private static final float BASE_RATE_MAX = 0.5f;

  private BayesianScoreEstimator() {}

  /**
   * Estimates BayesianScoreQuery parameters from the given index.
   *
   * @param searcher the index searcher to sample from
   * @param field the text field to create pseudo-queries for
   * @param nSamples number of documents to sample (default 50)
   * @param tokensPerQuery number of tokens per pseudo-query (default 5)
   * @param seed random seed for reproducible sampling
   * @return estimated alpha, beta, and base rate
   * @throws IOException if an I/O error occurs reading the index
   */
  public static Parameters estimate(
      IndexSearcher searcher, String field, int nSamples, int tokensPerQuery, long seed)
      throws IOException {
    IndexReader reader = searcher.getIndexReader();
    int maxDoc = reader.maxDoc();
    if (maxDoc == 0) {
      return new Parameters(1.0f, 0.0f, 0.01f);
    }

    nSamples = Math.min(nSamples, maxDoc);
    Random rng = new Random(seed);

    // Sample document IDs
    int[] sampledDocs = sampleDocIds(maxDoc, nSamples, rng);

    // Create pseudo-queries and collect scores
    List<float[]> allScoreArrays = new ArrayList<>();
    List<Float> baseRateFractions = new ArrayList<>();
    StoredFields storedFields = reader.storedFields();

    for (int docId : sampledDocs) {
      String fieldValue = storedFields.document(docId).get(field);
      if (fieldValue == null || fieldValue.isEmpty()) {
        continue;
      }

      // Extract first N tokens as pseudo-query terms
      String[] tokens = tokenize(fieldValue, tokensPerQuery);
      if (tokens.length == 0) {
        continue;
      }

      // Build a BooleanQuery from the tokens
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (String token : tokens) {
        builder.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD);
      }
      Query pseudoQuery = builder.build();

      // Collect all scores
      float[] scores = collectScores(searcher, pseudoQuery, maxDoc);
      if (scores.length == 0) {
        continue;
      }
      allScoreArrays.add(scores);

      // Base rate: fraction of docs above 95th percentile
      float[] sorted = scores.clone();
      Arrays.sort(sorted);
      int pIdx = (int) (sorted.length * PERCENTILE_THRESHOLD);
      pIdx = Math.min(pIdx, sorted.length - 1);
      float threshold = sorted[pIdx];
      int highCount = 0;
      for (float s : scores) {
        if (s >= threshold) {
          highCount++;
        }
      }
      baseRateFractions.add((float) highCount / maxDoc);
    }

    if (allScoreArrays.isEmpty()) {
      return new Parameters(1.0f, 0.0f, 0.01f);
    }

    // Flatten all scores for global statistics
    int totalScores = 0;
    for (float[] arr : allScoreArrays) {
      totalScores += arr.length;
    }
    float[] allScores = new float[totalScores];
    int offset = 0;
    for (float[] arr : allScoreArrays) {
      System.arraycopy(arr, 0, allScores, offset, arr.length);
      offset += arr.length;
    }

    // beta = median
    Arrays.sort(allScores);
    float beta = allScores[allScores.length / 2];

    // alpha = 1 / std
    double mean = 0;
    for (float s : allScores) {
      mean += s;
    }
    mean /= allScores.length;
    double variance = 0;
    for (float s : allScores) {
      double diff = s - mean;
      variance += diff * diff;
    }
    variance /= allScores.length;
    double std = Math.sqrt(variance);
    float alpha = std > 0 ? (float) (1.0 / std) : 1.0f;

    // base rate = mean of per-query fractions, clamped
    float baseRate = 0;
    for (float f : baseRateFractions) {
      baseRate += f;
    }
    baseRate /= baseRateFractions.size();
    baseRate = Math.clamp(baseRate, BASE_RATE_MIN, BASE_RATE_MAX);

    return new Parameters(alpha, beta, baseRate);
  }

  /**
   * Estimates parameters with default settings (50 samples, 5 tokens per query, seed 42).
   *
   * @param searcher the index searcher
   * @param field the text field
   * @return estimated parameters
   * @throws IOException if an I/O error occurs
   */
  public static Parameters estimate(IndexSearcher searcher, String field) throws IOException {
    return estimate(searcher, field, DEFAULT_N_SAMPLES, DEFAULT_TOKENS_PER_QUERY, 42);
  }

  private static int[] sampleDocIds(int maxDoc, int nSamples, Random rng) {
    // Fisher-Yates partial shuffle for sampling without replacement
    int[] all = new int[maxDoc];
    for (int i = 0; i < maxDoc; i++) {
      all[i] = i;
    }
    int n = Math.min(nSamples, maxDoc);
    for (int i = 0; i < n; i++) {
      int j = i + rng.nextInt(maxDoc - i);
      int tmp = all[i];
      all[i] = all[j];
      all[j] = tmp;
    }
    return ArrayUtil.copyOfSubArray(all, 0, n);
  }

  private static String[] tokenize(String text, int maxTokens) {
    // Simple whitespace tokenization with lowercasing
    String[] parts = text.toLowerCase(java.util.Locale.ROOT).split("\\s+");
    int n = Math.min(parts.length, maxTokens);
    List<String> tokens = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      String token = parts[i].replaceAll("[^a-z0-9]", "");
      if (token.isEmpty() == false) {
        tokens.add(token);
      }
    }
    return tokens.toArray(new String[0]);
  }

  private static float[] collectScores(IndexSearcher searcher, Query query, int maxDoc)
      throws IOException {
    int topN = Math.min(maxDoc, 10000);
    TopDocs topDocs = searcher.search(query, topN);
    float[] scores = new float[topDocs.scoreDocs.length];
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      scores[i] = topDocs.scoreDocs[i].score;
    }
    return scores;
  }
}
