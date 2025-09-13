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

package org.apache.lucene.codecs.bitvectors;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/** A bit vector scorer for scoring byte vectors. */
public class FlatBitVectorsScorer implements FlatVectorsScorer {
  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof ByteVectorValues byteVectorValues) {
      return new BitRandomVectorScorerSupplier(byteVectorValues);
    }
    throw new IllegalArgumentException("vectorValues must be an instance of ByteVectorValues");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    throw new IllegalArgumentException("bit vectors do not support float[] targets");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    if (vectorValues instanceof ByteVectorValues byteVectorValues) {
      return new BitRandomVectorScorer(byteVectorValues, target);
    }
    throw new IllegalArgumentException("vectorValues must be an instance of ByteVectorValues");
  }

  static class BitRandomVectorScorer implements UpdateableRandomVectorScorer {
    private final ByteVectorValues vectorValues;
    private final int bitDimensions;
    private final byte[] query;

    BitRandomVectorScorer(ByteVectorValues vectorValues, byte[] query) {
      this.query = query;
      this.bitDimensions = vectorValues.dimension() * Byte.SIZE;
      this.vectorValues = vectorValues;
    }

    @Override
    public float score(int node) throws IOException {
      return (bitDimensions - VectorUtil.xorBitCount(query, vectorValues.vectorValue(node)))
          / (float) bitDimensions;
    }

    @Override
    public int maxOrd() {
      return vectorValues.size();
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      System.arraycopy(vectorValues.vectorValue(node), 0, query, 0, query.length);
    }

    @Override
    public int ordToDoc(int ord) {
      return vectorValues.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return vectorValues.getAcceptOrds(acceptDocs);
    }
  }

  static class BitRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    protected final ByteVectorValues vectorValues;
    protected final ByteVectorValues targetVectors;

    public BitRandomVectorScorerSupplier(ByteVectorValues vectorValues) throws IOException {
      this.vectorValues = vectorValues;
      this.targetVectors = vectorValues.copy();
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      byte[] query = new byte[vectorValues.dimension()];
      return new BitRandomVectorScorer(vectorValues, query);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BitRandomVectorScorerSupplier(vectorValues);
    }
  }

  @Override
  public String toString() {
    return "FlatBitVectorsScorer()";
  }
}
