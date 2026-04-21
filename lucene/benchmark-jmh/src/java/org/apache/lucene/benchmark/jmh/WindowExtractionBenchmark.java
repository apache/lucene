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
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOIntConsumer;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark comparing bitset extraction strategies used in
 * MaxScoreBulkScorer.scoreInnerWindowMultipleEssentialClauses().
 *
 * <p>Three strategies are compared:
 *
 * <ol>
 *   <li><b>oldCardinalityForEach</b>: cardinality() + forEach(lambda) + clear() — 3 passes
 *   <li><b>newForEachNoCardinality</b>: forEach(lambda) + clear() with pre-allocated buffer — 2
 *       passes (eliminates cardinality)
 *   <li><b>newIntoArray</b>: intoArray() + score gather loop + clear() — single extraction pass
 * </ol>
 *
 * <p>Both benchmarks include the populate step (setting bits + scores) to simulate the full
 * inner-window lifecycle.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class WindowExtractionBenchmark {

  static final int INNER_WINDOW_SIZE = 1 << 12; // 4096, same as MaxScoreBulkScorer

  /**
   * Number of matching documents in the window. Realistic values range from very sparse (10) to
   * moderately dense (2000). Multi-term boolean queries typically match 50-500 docs per window.
   */
  @Param({"10", "50", "128", "500", "1000", "2000"})
  int matchCount;

  private final SplittableRandom random = new SplittableRandom(42);

  // Simulates MaxScoreBulkScorer's fields
  private FixedBitSet windowMatches;
  private double[] windowScores;
  private int innerWindowMin;

  // Output buffers (pre-allocated to max size, like MaxScoreBulkScorer reuses them)
  private int[] outDocs;
  private double[] outScores;
  private int outSize;

  // Pre-computed match positions and scores for deterministic setup
  private int[] matchPositions;
  private double[] matchScoreValues;

  @Setup(Level.Trial)
  public void setupTrial() {
    windowMatches = new FixedBitSet(INNER_WINDOW_SIZE);
    windowScores = new double[INNER_WINDOW_SIZE];
    // +1 for denseWord2Array sentinel slot
    outDocs = new int[INNER_WINDOW_SIZE + 1];
    outScores = new double[INNER_WINDOW_SIZE + 1];
    outSize = 0;
    innerWindowMin = 100_000; // arbitrary base doc ID

    // Pre-compute random match positions
    matchPositions = new int[matchCount];
    matchScoreValues = new double[matchCount];
    FixedBitSet temp = new FixedBitSet(INNER_WINDOW_SIZE);
    int count = 0;
    while (count < matchCount) {
      int pos = random.nextInt(INNER_WINDOW_SIZE);
      if (!temp.get(pos)) {
        temp.set(pos);
        matchPositions[count] = pos;
        matchScoreValues[count] = random.nextDouble() * 10.0;
        count++;
      }
    }
    Arrays.sort(matchPositions);
  }

  /** Populate the bitset and windowScores — simulates what the essential clause collection does. */
  private void populateWindow() {
    for (int i = 0; i < matchPositions.length; i++) {
      int pos = matchPositions[i];
      windowMatches.set(pos);
      windowScores[pos] = matchScoreValues[i];
    }
  }

  /**
   * ORIGINAL: cardinality() + forEach(lambda) + clear(). This is what the code did before any
   * optimization — 3 passes over the bitset.
   */
  @Benchmark
  public int oldCardinalityForEach(Blackhole bh) throws IOException {
    populateWindow();
    int innerWindowSize = INNER_WINDOW_SIZE;

    // Pass 1: count bits to pre-size buffer
    int card = windowMatches.cardinality(0, innerWindowSize);
    // In original code: docAndScoreAccBuffer.growNoCopy(card)
    // We simulate with pre-allocated buffer, but cardinality() cost is still measured

    // Pass 2: forEach with lambda to extract docs + scores + zero scores
    outSize = 0;
    windowMatches.forEach(
        0,
        innerWindowSize,
        0,
        (IOIntConsumer)
            index -> {
              outDocs[outSize] = innerWindowMin + index;
              outScores[outSize] = windowScores[index];
              outSize++;
              windowScores[index] = 0d;
            });

    // Pass 3: clear the bitset
    windowMatches.clear(0, innerWindowSize);

    bh.consume(card);
    bh.consume(outScores);
    return outSize;
  }

  /**
   * OPTIMIZED: forEach(lambda) + clear() with pre-allocated buffer. Eliminates the cardinality()
   * pass — 2 passes over the bitset. This is the current implementation.
   */
  @Benchmark
  public int newForEachNoCardinality(Blackhole bh) throws IOException {
    populateWindow();
    int innerWindowSize = INNER_WINDOW_SIZE;

    // No cardinality pass needed — buffer pre-allocated to INNER_WINDOW_SIZE

    // Single extraction pass: forEach with lambda
    outSize = 0;
    windowMatches.forEach(
        0,
        innerWindowSize,
        0,
        (IOIntConsumer)
            index -> {
              outDocs[outSize] = innerWindowMin + index;
              outScores[outSize] = windowScores[index];
              outSize++;
              windowScores[index] = 0d;
            });

    // Clear the bitset
    windowMatches.clear(0, innerWindowSize);

    bh.consume(outScores);
    return outSize;
  }

  /**
   * ALTERNATIVE: intoArray() + score gather loop + clear(). Uses the optimized branchless
   * denseWord2Array for bit extraction — best for dense windows.
   */
  @Benchmark
  public int newIntoArray(Blackhole bh) {
    populateWindow();
    int innerWindowSize = INNER_WINDOW_SIZE;

    // Single pass: extract doc IDs and get count
    int count = windowMatches.intoArray(0, innerWindowSize, innerWindowMin, outDocs);

    // Gather scores using extracted indices + zero used entries
    for (int i = 0; i < count; ++i) {
      int index = outDocs[i] - innerWindowMin;
      outScores[i] = windowScores[index];
      windowScores[index] = 0d;
    }

    // Clear the bitset
    windowMatches.clear(0, innerWindowSize);

    bh.consume(outScores);
    return count;
  }
}
