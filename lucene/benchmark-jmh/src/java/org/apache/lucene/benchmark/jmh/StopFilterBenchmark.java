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

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.CharArraySet;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StopFilterBenchmark {

  private static final List<String> ENGLISH_STOP_WORDS =
      Arrays.asList(
          "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
          "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
          "these", "they", "this", "to", "was", "will", "with");

  private static final String[] CONTENT_WORDS = {
    "lucene", "search", "index", "query", "document", "field", "segment", "merge", "filter",
    "score", "boost", "term", "token", "analyzer", "reader", "writer", "directory", "codec",
    "postings", "collection", "virtual"
  };

  @Param({"english", "technical"})
  String textType;

  private CharArraySet baseline;
  private CharArraySet packed;
  private char[][] lookupTokens;
  private int[] lookupLengths;

  @Setup(Level.Trial)
  public void setup() {
    baseline = new CharArraySet(ENGLISH_STOP_WORDS, true, 0);
    packed = new CharArraySet(ENGLISH_STOP_WORDS, true);

    Random rng = new Random(42);
    int tokenCount = 2000;
    lookupTokens = new char[tokenCount][];
    lookupLengths = new int[tokenCount];
    double stopWordRatio = textType.equals("english") ? 0.50 : 0.20;

    for (int i = 0; i < tokenCount; i++) {
      if (rng.nextDouble() < stopWordRatio) {
        String sw = ENGLISH_STOP_WORDS.get(rng.nextInt(ENGLISH_STOP_WORDS.size()));
        lookupTokens[i] = sw.toCharArray();
      } else {
        String cw = CONTENT_WORDS[rng.nextInt(CONTENT_WORDS.length)];
        lookupTokens[i] = cw.toCharArray();
      }
      lookupLengths[i] = lookupTokens[i].length;
    }
  }

  @Benchmark
  public void baseline(Blackhole bh) {
    for (int i = 0; i < lookupTokens.length; i++) {
      bh.consume(baseline.contains(lookupTokens[i], 0, lookupLengths[i]));
    }
  }

  @Benchmark
  public void packed(Blackhole bh) {
    for (int i = 0; i < lookupTokens.length; i++) {
      bh.consume(packed.contains(lookupTokens[i], 0, lookupLengths[i]));
    }
  }
}
