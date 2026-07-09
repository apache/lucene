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
import java.io.StringReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Benchmark for {@link SnowballFilter} comparing stemming throughput with and without caching. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
public class SnowballStemBenchmark {

  @Param({"0", "1024", "4096", "8192"})
  int cacheSize;

  @Param({"English", "German"})
  String language;

  @Param({"500", "5000", "50000"})
  int vocabSize;

  private String corpus;
  private Analyzer analyzer;

  @Setup(Level.Trial)
  public void setup() {
    corpus = buildZipfianCorpus(vocabSize, 10_000, 42);
    analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new WhitespaceTokenizer();
            return new TokenStreamComponents(
                tokenizer, new SnowballFilter(tokenizer, language, cacheSize));
          }
        };
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (analyzer != null) {
      analyzer.close();
    }
  }

  @Benchmark
  public int stem() throws IOException {
    int count = 0;
    try (TokenStream ts = analyzer.tokenStream("field", new StringReader(corpus))) {
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        count += termAtt.length();
      }
      ts.end();
    }
    return count;
  }

  private static String buildZipfianCorpus(int uniqueWords, int totalTokens, long seed) {
    Random rng = new Random(seed);
    String[] vocabulary = generateVocabulary(uniqueWords, rng);

    double[] weights = new double[uniqueWords];
    double totalWeight = 0;
    for (int i = 0; i < uniqueWords; i++) {
      weights[i] = 1.0 / (i + 1);
      totalWeight += weights[i];
    }

    double[] cumulative = new double[uniqueWords];
    cumulative[0] = weights[0] / totalWeight;
    for (int i = 1; i < uniqueWords; i++) {
      cumulative[i] = cumulative[i - 1] + weights[i] / totalWeight;
    }

    StringBuilder sb = new StringBuilder(totalTokens * 8);
    for (int t = 0; t < totalTokens; t++) {
      if (t > 0) {
        sb.append(' ');
      }
      double r = rng.nextDouble();
      int idx = findBucket(cumulative, r);
      sb.append(vocabulary[idx]);
    }
    return sb.toString();
  }

  private static int findBucket(double[] cumulative, double r) {
    int lo = 0;
    int hi = cumulative.length - 1;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (cumulative[mid] < r) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private static String[] generateVocabulary(int count, Random rng) {
    String[] suffixes = {
      "ing", "tion", "ness", "ment", "able", "ible", "ful", "less", "ous", "ive", "ity", "ence",
      "ance", "ly", "er", "ed", "es", "al", "ism", "ist"
    };
    String[] roots = {
      "act",
      "run",
      "walk",
      "play",
      "work",
      "think",
      "build",
      "creat",
      "develop",
      "establish",
      "manag",
      "process",
      "produc",
      "communic",
      "determin",
      "recommend",
      "understand",
      "perform",
      "consider",
      "represent",
      "organiz",
      "recogniz",
      "transform",
      "implement",
      "invest",
      "increas",
      "reduc",
      "improv",
      "measur",
      "distribut",
      "compar",
      "contribut",
      "demonstrat",
      "environ",
      "experienc",
      "gener",
      "govern",
      "individu",
      "interpret",
      "legislat"
    };

    String[] words = new String[count];
    for (int i = 0; i < count; i++) {
      String root = roots[rng.nextInt(roots.length)];
      if (rng.nextDouble() < 0.6) {
        words[i] = root + suffixes[rng.nextInt(suffixes.length)];
      } else {
        words[i] = root;
      }
    }
    return words;
  }
}
