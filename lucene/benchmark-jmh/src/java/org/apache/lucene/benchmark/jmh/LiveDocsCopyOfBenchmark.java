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
 * Benchmarks {@link FixedBitSet#copyOf(org.apache.lucene.util.Bits)} for {@link SparseLiveDocs} and
 * {@link DenseLiveDocs} inputs.
 *
 * <p>This benchmark measures the speedup from the fast paths added to {@code copyOf()} for the
 * {@link SparseLiveDocs} and {@link DenseLiveDocs} types introduced by GITHUB#15413. Without these
 * fast paths, both types fall through to the generic O(maxDoc) loop. With them:
 *
 * <ul>
 *   <li>{@link SparseLiveDocs}: O(deletedDocs) by iterating only the set bits of the deleted-docs
 *       bitset, then clearing those positions in the result
 *   <li>{@link DenseLiveDocs}: O(maxDoc/64) by cloning the backing {@link FixedBitSet} directly
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>Run all benchmarks:
 *
 * <pre>
 * java -jar lucene-benchmark-jmh.jar "LiveDocsCopyOfBenchmark"
 * </pre>
 *
 * @see SparseLiveDocs
 * @see DenseLiveDocs
 * @see LiveDocsBenchmark
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx2g", "-Xms2g"})
public class LiveDocsCopyOfBenchmark {

  /** Number of documents in the segment. */
  @Param({"1000000", "10000000", "100000000"})
  int maxDoc;

  /**
   * Percentage of documents to delete.
   *
   * <p>Kept low to stay in the SparseLiveDocs regime ({@literal <=}1%). At these rates the
   * O(deletedDocs) vs O(maxDoc) difference is most pronounced.
   */
  @Param({"0.001", "0.01"})
  double deletionRate;

  private SparseLiveDocs sparseLiveDocs;
  private DenseLiveDocs denseLiveDocs;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);
    int numDeleted = Math.max(1, (int) (maxDoc * deletionRate));

    SparseFixedBitSet sparseSet = new SparseFixedBitSet(maxDoc);
    FixedBitSet fixedSet = new FixedBitSet(maxDoc);
    fixedSet.set(0, maxDoc);

    for (int i = 0; i < numDeleted; i++) {
      int doc = random.nextInt(maxDoc);
      sparseSet.set(doc);
      fixedSet.clear(doc);
    }

    sparseLiveDocs = SparseLiveDocs.builder(sparseSet, maxDoc).build();
    denseLiveDocs = DenseLiveDocs.builder(fixedSet, maxDoc).build();
  }

  @Benchmark
  public FixedBitSet copyOfSparseLiveDocs() {
    return FixedBitSet.copyOf(sparseLiveDocs);
  }

  @Benchmark
  public FixedBitSet copyOfDenseLiveDocs() {
    return FixedBitSet.copyOf(denseLiveDocs);
  }
}
