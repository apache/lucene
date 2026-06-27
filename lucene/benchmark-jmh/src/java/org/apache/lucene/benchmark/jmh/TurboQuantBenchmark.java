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
package org.apache.lucene.benchmark.jmh;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.sandbox.codecs.turboquant.BetaCodebook;
import org.apache.lucene.sandbox.codecs.turboquant.HadamardRotation;
import org.apache.lucene.sandbox.codecs.turboquant.TurboQuantBitPacker;
import org.apache.lucene.sandbox.codecs.turboquant.TurboQuantEncoding;
import org.apache.lucene.sandbox.codecs.turboquant.TurboQuantScoringUtil;
import org.openjdk.jmh.annotations.*;

/** JMH benchmarks for TurboQuant core operations. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class TurboQuantBenchmark {

  @Param({"4096"})
  int dim;

  @Param({"4"})
  int bits;

  private float[] vector;
  private float[] rotated;
  private float[] query;
  private byte[] indices;
  private byte[] packed;
  private float[] centroids;
  private HadamardRotation rotation;

  @Setup
  public void setup() {
    Random rng = new Random(42);
    TurboQuantEncoding enc =
        TurboQuantEncoding.fromWireNumber(
                switch (bits) {
                  case 2 -> 0;
                  case 3 -> 1;
                  case 4 -> 2;
                  case 8 -> 3;
                  default -> throw new IllegalArgumentException();
                })
            .orElseThrow();

    vector = new float[dim];
    float norm = 0;
    for (int i = 0; i < dim; i++) {
      vector[i] = (float) rng.nextGaussian();
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    for (int i = 0; i < dim; i++) vector[i] /= norm;

    rotation = HadamardRotation.create(dim, 12345L);
    rotated = new float[dim];
    rotation.rotate(vector, rotated);

    centroids = BetaCodebook.centroids(dim, bits);
    float[] boundaries = BetaCodebook.boundaries(dim, bits);

    indices = new byte[dim];
    for (int i = 0; i < dim; i++) {
      indices[i] = (byte) BetaCodebook.quantize(rotated[i], boundaries);
    }

    packed = new byte[enc.getPackedByteLength(dim)];
    TurboQuantBitPacker.pack(indices, dim, bits, packed);

    query = new float[dim];
    for (int i = 0; i < dim; i++) query[i] = (float) rng.nextGaussian() / (float) Math.sqrt(dim);
  }

  @Benchmark
  public void hadamardRotation() {
    rotation.rotate(vector, rotated);
  }

  @Benchmark
  public float dotProductScoring() {
    return TurboQuantScoringUtil.dotProduct(query, packed, centroids, bits, dim);
  }

  @Benchmark
  public void quantize() {
    float[] boundaries = BetaCodebook.boundaries(dim, bits);
    for (int i = 0; i < dim; i++) {
      indices[i] = (byte) BetaCodebook.quantize(rotated[i], boundaries);
    }
    TurboQuantBitPacker.pack(indices, dim, bits, packed);
  }
}
