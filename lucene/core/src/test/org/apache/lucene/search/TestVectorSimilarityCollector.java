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

import static org.apache.lucene.search.AbstractVectorSimilarityQuery.DECAY_MAX_APPROXIMATION;
import static org.apache.lucene.search.AbstractVectorSimilarityQuery.DECAY_MAX_QUALITY;
import static org.apache.lucene.search.AbstractVectorSimilarityQuery.DEFAULT_DECAY;

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestVectorSimilarityCollector extends LuceneTestCase {
  public void testIllegalParams() {
    expectThrows(
        AssertionError.class,
        () ->
            new VectorSimilarityCollector(
                Float.NaN, // illegal resultSimilarity
                DEFAULT_DECAY,
                Integer.MAX_VALUE));

    expectThrows(
        AssertionError.class,
        () ->
            new VectorSimilarityCollector(
                0f,
                Float.NaN, // illegal decay
                Integer.MAX_VALUE));

    expectThrows(
        AssertionError.class,
        () ->
            new VectorSimilarityCollector(
                0f,
                Math.nextDown(DECAY_MAX_APPROXIMATION), // illegal decay
                Integer.MAX_VALUE));

    expectThrows(
        AssertionError.class,
        () ->
            new VectorSimilarityCollector(
                0f,
                Math.nextUp(DECAY_MAX_QUALITY), // illegal decay
                Integer.MAX_VALUE));
  }

  public void testResultCollection() {
    float resultSimilarity = 0.5f;

    VectorSimilarityCollector collector =
        new VectorSimilarityCollector(resultSimilarity, DEFAULT_DECAY, Integer.MAX_VALUE);

    int[] nodes = {1, 5, 10, 4, 8, 3, 2, 6, 7, 9};
    float[] scores = {0.1f, 0.2f, 0.3f, 0.5f, 0.2f, 0.6f, 0.9f, 0.3f, 0.7f, 0.8f};

    for (int i = 0; i < nodes.length; i++) {
      collector.collect(nodes[i], scores[i]);
    }

    ScoreDoc[] scoreDocs = collector.topDocs().scoreDocs;
    int[] resultNodes = new int[scoreDocs.length];
    float[] resultScores = new float[scoreDocs.length];
    for (int i = 0; i < scoreDocs.length; i++) {
      resultNodes[i] = scoreDocs[i].doc;
      resultScores[i] = scoreDocs[i].score;
    }

    // All nodes above resultSimilarity appear in order of collection
    assertArrayEquals(new int[] {4, 3, 2, 7, 9}, resultNodes);
    assertArrayEquals(new float[] {0.5f, 0.6f, 0.9f, 0.7f, 0.8f}, resultScores, 1e-3f);
  }
}
