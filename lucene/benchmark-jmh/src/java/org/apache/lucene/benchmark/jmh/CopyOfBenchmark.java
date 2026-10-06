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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DenseLiveDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.SparseLiveDocs;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks for {@link FixedBitSet#copyOf(Bits)} with {@link DenseLiveDocs}, {@link
 * SparseLiveDocs}, and a generic {@link Bits} implementation.
 *
 * <p>The generic fallback method ({@code copyOfGenericBits}) is critical: it proves that the
 * per-bit loop does not regress when LiveDocs types exit early via a fast path.
 *
 * @see FixedBitSet#copyOf(Bits)
 * @see DenseLiveDocs
 * @see SparseLiveDocs
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class CopyOfBenchmark {

  @Param({"100000", "1000000"})
  int size;

  @Param({"0.001", "0.01", "0.10"})
  double deletionRate;

  private DenseLiveDocs denseLiveDocs;
  private SparseLiveDocs sparseLiveDocs;
  private Bits genericBits;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    int numDeleted = (int) (size * deletionRate);

    // Build a FixedBitSet for dense (set bits = live docs)
    FixedBitSet fixedSet = new FixedBitSet(size);
    fixedSet.set(0, size);

    // Build a SparseFixedBitSet for sparse (set bits = deleted docs)
    SparseFixedBitSet sparseSet = new SparseFixedBitSet(size);

    for (int i = 0; i < numDeleted; i++) {
      int docId = random.nextInt(size);
      fixedSet.clear(docId);
      sparseSet.set(docId);
    }

    denseLiveDocs = DenseLiveDocs.builder(fixedSet, size).build();
    sparseLiveDocs = SparseLiveDocs.builder(sparseSet, size).build();

    // Generic Bits that is NOT a LiveDocs — exercises the fallback loop
    FixedBitSet referenceBits = fixedSet.clone();
    genericBits =
        new Bits() {
          @Override
          public boolean get(int index) {
            return referenceBits.get(index);
          }

          @Override
          public int length() {
            return referenceBits.length();
          }
        };
  }

  @Benchmark
  public FixedBitSet copyOfDenseLiveDocs() {
    return FixedBitSet.copyOf(denseLiveDocs);
  }

  @Benchmark
  public FixedBitSet copyOfSparseLiveDocs() {
    return FixedBitSet.copyOf(sparseLiveDocs);
  }

  @Benchmark
  public FixedBitSet copyOfGenericBits() {
    return FixedBitSet.copyOf(genericBits);
  }
}
