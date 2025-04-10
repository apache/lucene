/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;

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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class BinningIndexingLatencyBenchmark {

  private Directory directory;
  private IndexWriter writer;

  @Param({"1000"})
  private int docCount;

  @Param({"true", "false"})
  private boolean binningEnabled;

  private FieldType fieldType;
  private final Random random = new Random(42);

  @Setup(Level.Trial)
  public void setup() throws Exception {
    Path tempDir = Files.createTempDirectory("indexing-benchmark");
    directory = new MMapDirectory(tempDir);
    IndexWriterConfig config = new IndexWriterConfig(new SyntheticAnalyzer());
    config.setCodec(new Lucene101Codec());
    config.setUseCompoundFile(false);
    writer = new IndexWriter(directory, config);

    fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    if (binningEnabled) {
      fieldType.putAttribute("postingsFormat", "Lucene101");
      fieldType.putAttribute("doBinning", "true");
      fieldType.putAttribute("bin.count", "4");
    }
    fieldType.freeze();
  }

  @Benchmark
  public void indexBatch() throws IOException {
    for (int i = 0; i < docCount; i++) {
      Document doc = new Document();
      doc.add(new Field("field", generateRandomText(), fieldType));
      writer.addDocument(doc);
    }
    writer.commit();
  }

  private String generateRandomText() {
    int tokens = 10 + random.nextInt(10); // 10-20 words
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tokens; i++) {
      sb.append(randomWord());
      sb.append(' ');
    }
    return sb.toString().trim();
  }

  private String randomWord() {
    char[] chars = new char[3 + random.nextInt(5)];
    for (int i = 0; i < chars.length; i++) {
      chars[i] = (char) ('a' + random.nextInt(26));
    }
    return new String(chars).toLowerCase(Locale.ROOT);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    writer.close();
    directory.close();
  }

  /**
   * Analyzer that emits a fixed synthetic stream of lowercase tokens.
   */
  private static final class SyntheticAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new Tokenizer() {
        private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
        private boolean emitted = false;

        @Override
        public boolean incrementToken() {
          if (emitted) {
            return false;
          }
          clearAttributes();
          termAttr.append("synthetic");
          emitted = true;
          return true;
        }

        @Override
        public void reset() throws IOException {
          super.reset();
          emitted = false;
        }
      };
      return new TokenStreamComponents(tokenizer);
    }
  }
}