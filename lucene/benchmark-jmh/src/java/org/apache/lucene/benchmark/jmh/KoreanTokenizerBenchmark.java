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
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.KoreanAnalyzer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
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

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
public class KoreanTokenizerBenchmark {

  @Param({"SHORT", "LONG"})
  public String textSize;

  @Param({"false", "true"})
  public boolean userDictionary;

  private Analyzer analyzer;
  private String text;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    UserDictionary userDict = userDictionary ? openUserDictionary() : null;
    analyzer = new KoreanAnalyzer(userDict, KoreanTokenizer.DecompoundMode.DISCARD, null, false);
    text =
        switch (textSize) {
          case "SHORT" -> "한국은 대단한 나라입니다.";
          case "LONG" ->
              """
              서울특별시는 대한민국의 수도이자 최대 도시이다. 한강을 중심으로 발전해 왔으며 \
              정치, 경제, 사회, 문화의 중심지 역할을 한다. 많은 기업과 연구 기관이 \
              집중되어 있어 정보 기술과 금융 산업이 발달했다.""";
          default -> throw new IllegalStateException("unknown textSize: " + textSize);
        };
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (analyzer != null) {
      analyzer.close();
    }
  }

  @Benchmark
  public int tokenizeDocument() throws IOException {
    int tokenCount = 0;
    try (TokenStream stream = analyzer.tokenStream("field", text)) {
      stream.reset();
      while (stream.incrementToken()) {
        tokenCount++;
      }
      stream.end();
    }
    return tokenCount;
  }

  private static UserDictionary openUserDictionary() throws IOException {
    return UserDictionary.open(new StringReader("세종시\n세종 시"));
  }
}
