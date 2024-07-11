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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.VectorUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class HammingDistanceBenchmark {
  @Param({"1000000"})
  int nb = 1_000_000;

  @Param({"1024"})
  int dims = 1024;

  byte[][] xb;
  byte[] xq;

  @Setup
  public void setup() throws IOException {
    Random rand = new Random();
    this.xb = new byte[nb][dims / 8];
    for (int i = 0; i < nb; i++) {
      for (int j = 0; j < dims / 8; j++) {
        xb[i][j] = (byte) rand.nextInt(0, 255);
      }
    }
    this.xq = new byte[dims / 8];
    for (int i = 0; i < xq.length; i++) {
      xq[i] = (byte) rand.nextInt(0, 255);
    }
  }

  @Benchmark
  public int xorBitCount() {
    int tot = 0;
    for (int i = 0; i < nb; i++) {
      tot += VectorUtil.xorBitCount(xb[i], xq);
    }
    return tot;
  }
}
