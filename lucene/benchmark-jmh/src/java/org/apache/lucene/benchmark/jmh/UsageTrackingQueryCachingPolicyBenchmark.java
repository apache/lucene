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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures throughput of {@link UsageTrackingQueryCachingPolicy#onUse(Query)} and its internal
 * frequency lookup ({@code shouldCache}) under concurrent load.
 *
 * <p>Goal: demonstrate the contention removed by replacing the single-monitor {@code
 * FrequencyTrackingRingBuffer} field with a striped tracker. Run this benchmark on the baseline
 * branch and the candidate branch separately and compare ops/sec at each thread count.
 *
 * <p>We use {@link TermInSetQuery} as the canonical "costly" filter (it bypasses {@code
 * shouldNeverCache} and exercises the full ring-buffer add path).
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(
    value = 3,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class UsageTrackingQueryCachingPolicyBenchmark {

  private static final int NUM_QUERIES = 4096;

  private UsageTrackingQueryCachingPolicy policy;
  private Query[] queries;
  private int[] zipfKeys;

  @Setup
  public void setup() {
    policy = new UsageTrackingQueryCachingPolicy();
    queries = new Query[NUM_QUERIES];
    for (int i = 0; i < NUM_QUERIES; i++) {
      // TermInSetQuery is treated as "costly" so it passes shouldNeverCache and reaches the
      // tracker on every onUse() call.
      BytesRef term = new BytesRef(UUID.randomUUID().toString());
      queries[i] = new TermInSetQuery("field" + (i & 31), Collections.singletonList(term));
    }
    ZipfDistribution zipf = new ZipfDistribution(NUM_QUERIES, 1.2);
    zipfKeys = new int[100_000];
    for (int i = 0; i < zipfKeys.length; i++) {
      zipfKeys[i] = zipf.sample() - 1;
    }
    // Prime the policy so we are measuring steady-state contention, not the cold-cache path.
    for (int i = 0; i < NUM_QUERIES * 4; i++) {
      policy.onUse(queries[i % NUM_QUERIES]);
    }
  }

  // ---- single-threaded baseline ------------------------------------------------------------

  @Benchmark
  public void singleThread_onUse_uniform() {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    policy.onUse(queries[idx]);
  }

  @Benchmark
  public boolean singleThread_shouldCache_uniform() throws IOException {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    return policy.shouldCache(queries[idx]);
  }

  // ---- concurrent: uniform query distribution (worst case for any policy) ------------------

  @Benchmark
  @Group("concurrent_uniform_4t")
  @GroupThreads(4)
  public void concurrent_uniform_4t_onUse() {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    policy.onUse(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_uniform_8t")
  @GroupThreads(8)
  public void concurrent_uniform_8t_onUse() {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    policy.onUse(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_uniform_16t")
  @GroupThreads(16)
  public void concurrent_uniform_16t_onUse() {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    policy.onUse(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_uniform_32t")
  @GroupThreads(32)
  public void concurrent_uniform_32t_onUse() {
    int idx = ThreadLocalRandom.current().nextInt(NUM_QUERIES);
    policy.onUse(queries[idx]);
  }

  // ---- concurrent: realistic mixed read/write traffic --------------------------------------
  // Mirrors real search-node behavior: every search calls onUse + shouldCache. The ratio of
  // 2 readers per writer matches what IndexSearcher does (shouldCache called more often than
  // onUse advances a stripe).

  @Benchmark
  @Group("concurrent_mixed_zipf_16t")
  @GroupThreads(6)
  public void concurrent_mixed_zipf_16t_onUse() {
    int idx = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    policy.onUse(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_mixed_zipf_16t")
  @GroupThreads(10)
  public boolean concurrent_mixed_zipf_16t_shouldCache() throws IOException {
    int idx = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    return policy.shouldCache(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_mixed_zipf_32t")
  @GroupThreads(12)
  public void concurrent_mixed_zipf_32t_onUse() {
    int idx = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    policy.onUse(queries[idx]);
  }

  @Benchmark
  @Group("concurrent_mixed_zipf_32t")
  @GroupThreads(20)
  public boolean concurrent_mixed_zipf_32t_shouldCache() throws IOException {
    int idx = zipfKeys[ThreadLocalRandom.current().nextInt(zipfKeys.length)];
    return policy.shouldCache(queries[idx]);
  }
}
