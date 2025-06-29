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

package org.apache.lucene.internal.vectorization;

import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.factory;
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.getSegment;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.FloatToFloatFunction;
import org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.MemorySegmentScorer;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

class Lucene99MemorySegmentScalarQuantizedScorerSupplier implements RandomVectorScorerSupplier {

  private final VectorSimilarityFunction function;
  private final QuantizedByteVectorValues values;
  private final MemorySegmentAccessInput input;
  private final MemorySegmentScorer scorer;
  private final FloatToFloatFunction scaler;
  private final float constMultiplier;
  private final int vectorByteSize;
  private final int entrySize;

  public Lucene99MemorySegmentScalarQuantizedScorerSupplier(
      VectorSimilarityFunction function,
      QuantizedByteVectorValues values,
      MemorySegmentAccessInput input) {

    this.function = function;
    this.values = values;
    this.input = input;
    this.scorer = factory(function, values, true);
    this.scaler = factory(function);
    this.constMultiplier = values.getScalarQuantizer().getConstantMultiplier();
    this.vectorByteSize = values.getVectorByteLength();
    this.entrySize = vectorByteSize + Float.BYTES;
  }

  @Override
  public UpdateableRandomVectorScorer scorer() {
    return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {

      private final MemorySegment[] doc = new MemorySegment[1];
      private final float[] docOffset = new float[1];
      private final byte[][] docScratch = new byte[1][];
      private final byte[][] queryScratch = new byte[1][];

      @Override
      public void setScoringOrdinal(int node) throws IOException {
        MemorySegment segment = getSegment(input, entrySize, node, docScratch);
        doc[0] = segment.reinterpret(vectorByteSize);
        docOffset[0] = segment.get(JAVA_FLOAT, vectorByteSize);
      }

      @Override
      public float score(int node) throws IOException {
        MemorySegment segment = getSegment(input, entrySize, node, queryScratch);
        MemorySegment query = segment.reinterpret(vectorByteSize);
        float queryOffset = segment.get(JAVA_FLOAT, vectorByteSize);
        return scaler.scale(
            scorer.score(query, doc[0]) * constMultiplier + queryOffset + docOffset[0]);
      }
    };
  }

  @Override
  public RandomVectorScorerSupplier copy() throws IOException {
    return new Lucene99MemorySegmentScalarQuantizedScorerSupplier(function, values, input);
  }
}
