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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/** A bit vector scorer for scoring byte vectors. */
public class FlatBitVectorsScorer implements FlatVectorsScorer {
  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (vectorValues instanceof RandomAccessVectorValues.Bytes) {
      return new BitRandomVectorScorerSupplier((RandomAccessVectorValues.Bytes) vectorValues);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Bytes");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    throw new IllegalArgumentException("bit vectors do not support float[] targets");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessVectorValues.Bytes;
    if (vectorValues instanceof RandomAccessVectorValues.Bytes) {
      return new BitRandomVectorScorer((RandomAccessVectorValues.Bytes) vectorValues, target);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessVectorValues.Bytes");
  }

  static class BitRandomVectorScorer implements RandomVectorScorer {
    private final RandomAccessVectorValues.Bytes vectorValues;
    private final int bitDimensions;
    private final byte[] query;

    BitRandomVectorScorer(RandomAccessVectorValues.Bytes vectorValues, byte[] query) {
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
    public int ordToDoc(int ord) {
      return vectorValues.ordToDoc(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return vectorValues.getAcceptOrds(acceptDocs);
    }
  }

  static class BitRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    protected final RandomAccessVectorValues.Bytes vectorValues;
    protected final RandomAccessVectorValues.Bytes vectorValues1;
    protected final RandomAccessVectorValues.Bytes vectorValues2;

    public BitRandomVectorScorerSupplier(RandomAccessVectorValues.Bytes vectorValues)
        throws IOException {
      this.vectorValues = vectorValues;
      this.vectorValues1 = vectorValues.copy();
      this.vectorValues2 = vectorValues.copy();
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      byte[] query = vectorValues1.vectorValue(ord);
      return new BitRandomVectorScorer(vectorValues2, query);
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BitRandomVectorScorerSupplier(vectorValues.copy());
    }
  }

  @Override
  public String toString() {
    return "FlatBitVectorsScorer()";
  }
}
