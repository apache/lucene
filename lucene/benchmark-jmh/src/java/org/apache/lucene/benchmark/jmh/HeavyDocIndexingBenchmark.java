/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
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
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class HeavyDocIndexingBenchmark {

  @Param({"100000"})
  private int docCount;

  @Param({"baseline", "binning-approx", "binning-exact"})
  private String mode;

  private Path tempDir;
  private FieldType contentType;
  private FieldType titleType;
  private FieldType tagType;
  private FieldType idType;
  private Random random;

  @Setup(Level.Trial)
  public void setup() {
    this.random = new Random(42);
    this.contentType = new FieldType(TextField.TYPE_NOT_STORED);
    this.contentType.setTokenized(true);
    this.contentType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

    this.titleType = new FieldType(TextField.TYPE_STORED);
    this.titleType.setTokenized(true);
    this.titleType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);

    this.tagType = new FieldType(TextField.TYPE_NOT_STORED);
    this.tagType.setTokenized(true);
    this.tagType.setIndexOptions(IndexOptions.DOCS);

    this.idType = new FieldType(StringField.TYPE_STORED);

    if (mode.startsWith("binning")) {
      String builder = mode.equals("binning-exact") ? "exact" : "approx";
      this.contentType.putAttribute("postingsFormat", "Lucene101");
      this.contentType.putAttribute("doBinning", "true");
      this.contentType.putAttribute("bin.count", "4");
      this.contentType.putAttribute("bin.builder", builder);
    }

    this.contentType.freeze();
    this.titleType.freeze();
    this.tagType.freeze();
    this.idType.freeze();
  }

  @Benchmark
  public void indexDocuments() throws IOException {
    this.tempDir = Files.createTempDirectory("lucene-ext-benchmark");
    try (Directory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig config = new IndexWriterConfig(new HeavyDocAnalyzer());
      config.setCodec(new Lucene103Codec());
      config.setUseCompoundFile(false);

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docCount; i++) {
          Document doc = new Document();

          String id = "doc-" + i;
          String title = "Lucene indexing benchmark test title " + i;
          String content = generateRandomParagraph(i);
          String tags = "benchmark binning lucene";

          doc.add(new Field("id", id, idType));
          doc.add(new Field("title", title, titleType));
          doc.add(new Field("content", content, contentType));
          doc.add(new Field("tags", tags, tagType));

          writer.addDocument(doc);
        }
        writer.commit();
      }
    } finally {
      Files.walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException ignore) {
                }
              });
    }
  }

  private String generateRandomParagraph(int seed) {
    StringBuilder sb = new StringBuilder();
    int sentenceCount = 5 + random.nextInt(10);
    for (int i = 0; i < sentenceCount; i++) {
      sb.append("This is sentence ").append(i).append(" from document ").append(seed).append(". ");
    }
    return sb.toString();
  }

  /** Analyzer that returns a single token per field for fast benchmarks. */
  private static final class HeavyDocAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute attr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() {
              if (emitted) {
                return false;
              }
              clearAttributes();
              attr.append("lucene");
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
