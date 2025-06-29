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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer.quantizeQuery;
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.factory;
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.getSegment;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.FloatToFloatFunction;
import org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.MemorySegmentScorer;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

class Lucene99MemorySegmentScalarQuantizedScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer {

  private final VectorSimilarityFunction function;
  private final QuantizedByteVectorValues values;
  private final MemorySegmentAccessInput input;
  private final MemorySegmentScorer scorer;
  private final FloatToFloatFunction scaler;
  private final float constMultiplier;
  private final int vectorByteSize;
  private final int entrySize;
  private final MemorySegment query;
  private final float queryOffset;
  private final byte[][] docScratch;

  public Lucene99MemorySegmentScalarQuantizedScorer(
      VectorSimilarityFunction function,
      QuantizedByteVectorValues values,
      MemorySegmentAccessInput input,
      float[] target) {

    super(values);
    this.function = function;
    this.values = values;
    this.input = input;
    this.scorer = factory(function, values, false);
    this.scaler = factory(function);

    ScalarQuantizer quantizer = values.getScalarQuantizer();
    this.constMultiplier = quantizer.getConstantMultiplier();
    this.vectorByteSize = values.getVectorByteLength();
    this.entrySize = vectorByteSize + Float.BYTES;

    byte[] targetBytes = new byte[target.length];
    this.queryOffset = quantizeQuery(target, targetBytes, function, quantizer);
    this.query = Arena.ofAuto().allocateFrom(JAVA_BYTE, targetBytes);

    this.docScratch = new byte[1][];
  }

  @Override
  public float score(int node) throws IOException {
    MemorySegment segment = getSegment(input, entrySize, node, docScratch);
    MemorySegment doc = segment.reinterpret(vectorByteSize);
    float docOffset = segment.get(JAVA_FLOAT, vectorByteSize);
    return scaler.scale(scorer.score(query, doc) * constMultiplier + queryOffset + docOffset);
  }
}
