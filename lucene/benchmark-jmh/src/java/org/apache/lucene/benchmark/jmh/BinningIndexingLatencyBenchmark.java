/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class BinningIndexingLatencyBenchmark {

  @Param({"1000000"})
  private int docCount;

  private Path tempDir;

  @Benchmark
  public void indexBaseline() throws IOException {
    FieldType fieldType = getFieldTypeBaseline();
    indexDocuments(fieldType);
  }

  @Benchmark
  public void indexWithExactBinning() throws IOException {
    FieldType fieldType = getFieldTypeWithBinning("exact");
    indexDocuments(fieldType);
  }

  @Benchmark
  public void indexWithApproxBinning() throws IOException {
    FieldType fieldType = getFieldTypeWithBinning("approx");
    indexDocuments(fieldType);
  }

  @Benchmark
  public void indexWithAutoBinning() throws IOException {
    FieldType fieldType = getFieldTypeWithBinning("auto");
    indexDocuments(fieldType);
  }

  private void indexDocuments(FieldType fieldType) throws IOException {
    tempDir = Files.createTempDirectory("lucene-binning-benchmark");
    try (Directory dir = new MMapDirectory(tempDir)) {
      IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
      config.setCodec(new Lucene103Codec());
      config.setUseCompoundFile(false);

      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docCount; i++) {
          Document doc = new Document();
          String content = (i % 200 == 0) ? "lucene relevant content" : "noise filler text " + i;
          doc.add(new Field("field", content, fieldType));
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
                } catch (IOException e) {
                  assert e != null;
                }
              });
    }
  }

  private FieldType getFieldTypeBaseline() {
    FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.freeze();
    return type;
  }

  private FieldType getFieldTypeWithBinning(String builder) {
    FieldType type = new FieldType(TextField.TYPE_NOT_STORED);
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.putAttribute("postingsFormat", "Lucene101");
    type.putAttribute("doBinning", "true");
    type.putAttribute("bin.count", "4");
    type.putAttribute("graph.builder", builder);
    type.freeze();
    return type;
  }

  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute attr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() throws IOException {
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

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      return reader;
    }
  }
}
