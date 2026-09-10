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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
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

  @Param({"0", "64", "128", "256", "512"})
  int cacheSize;

  @Param({"Romanian", "Turkish"})
  String language;

  @Param({"5000", "50000"})
  int vocabSize;

  @Param({"true", "false"})
  String stopFilter;

  @Param({"1", "4"})
  int numAnalyzers;

  private String[] documents;
  private Analyzer[] analyzers;

  @Setup(Level.Trial)
  public void setup() {
    boolean useStopFilter = Boolean.parseBoolean(stopFilter);
    CharArraySet stopWords =
        switch (language) {
          case "Romanian" -> RomanianAnalyzer.getDefaultStopSet();
          case "Turkish" -> TurkishAnalyzer.getDefaultStopSet();
          default -> CharArraySet.EMPTY_SET;
        };

    List<String> stopWordList = extractStopWords(stopWords);
    String corpus = buildZipfianCorpus(vocabSize, 10_000, 42, useStopFilter ? stopWordList : null);
    documents = splitIntoDocuments(corpus, 500);

    analyzers = new Analyzer[numAnalyzers];
    for (int i = 0; i < numAnalyzers; i++) {
      analyzers[i] = createAnalyzer(language, cacheSize, useStopFilter, stopWords);
    }
  }

  private static Analyzer createAnalyzer(
      String language, int cacheSize, boolean useStopFilter, CharArraySet stopWords) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new WhitespaceTokenizer();
        TokenStream stream = tokenizer;
        if (useStopFilter) {
          stream = new StopFilter(stream, stopWords);
        }
        stream = new SnowballFilter(stream, language, cacheSize);
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    for (Analyzer a : analyzers) {
      if (a != null) {
        a.close();
      }
    }
  }

  @Benchmark
  public int stem() throws IOException {
    int count = 0;
    for (Analyzer analyzer : analyzers) {
      for (String doc : documents) {
        try (TokenStream ts = analyzer.tokenStream("field", new StringReader(doc))) {
          CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
          ts.reset();
          while (ts.incrementToken()) {
            count += termAtt.length();
          }
          ts.end();
        }
      }
    }
    return count;
  }

  private static String[] splitIntoDocuments(String corpus, int tokensPerDoc) {
    String[] allTokens = corpus.split(" ");
    int numDocs = (allTokens.length + tokensPerDoc - 1) / tokensPerDoc;
    String[] docs = new String[numDocs];
    for (int i = 0; i < numDocs; i++) {
      int start = i * tokensPerDoc;
      int end = Math.min(start + tokensPerDoc, allTokens.length);
      docs[i] = String.join(" ", java.util.Arrays.copyOfRange(allTokens, start, end));
    }
    return docs;
  }

  private static List<String> extractStopWords(CharArraySet stopWords) {
    List<String> words = new ArrayList<>();
    for (Object obj : stopWords) {
      if (obj instanceof char[] chars) {
        words.add(new String(chars));
      }
    }
    return words;
  }

  private static String buildZipfianCorpus(
      int uniqueWords, int totalTokens, long seed, List<String> stopWords) {
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
      if (stopWords != null && !stopWords.isEmpty() && rng.nextDouble() < 0.45) {
        sb.append(stopWords.get(rng.nextInt(stopWords.size())));
      } else {
        double r = rng.nextDouble();
        int idx = findBucket(cumulative, r);
        sb.append(vocabulary[idx]);
      }
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
