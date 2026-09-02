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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Bits;
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

/**
 * Measures the cost of wrapping a reader with {@link SoftDeletesDirectoryReaderWrapper}, which
 * calls {@link org.apache.lucene.util.FixedBitSet#copyOf(Bits)} on each leaf's live docs. Since
 * #15413, live docs are {@link org.apache.lucene.util.DenseLiveDocs} or {@link
 * org.apache.lucene.util.SparseLiveDocs} which fall through to the per-bit generic loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsPrepend = "--add-modules=jdk.incubator.vector")
public class SoftDeletesReaderBenchmark {

  private static final String SOFT_DELETE_FIELD = "__soft_delete";

  @Param({"200000", "1000000"})
  int numDocs;

  @Param({"0.01", "0.05"})
  double softDeleteRate;

  private Directory dir;
  private DirectoryReader baseReader;
  private Path tempDir;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    tempDir = Path.of(System.getProperty("java.io.tmpdir"), "softdel-bench-" + System.nanoTime());
    dir = MMapDirectory.open(tempDir);

    IndexWriterConfig config =
        new IndexWriterConfig()
            .setSoftDeletesField(SOFT_DELETE_FIELD)
            .setRAMBufferSizeMB(256);
    try (IndexWriter w = new IndexWriter(dir, config)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        doc.add(new NumericDocValuesField("val", i));
        w.addDocument(doc);
      }
      w.flush();
      int softDeleted = (int) (numDocs * softDeleteRate);
      for (int i = 0; i < softDeleted; i++) {
        int docId = (int) (((long) i * 7919) % numDocs);
        Document replacement = new Document();
        replacement.add(new StringField("id", Integer.toString(docId), Field.Store.NO));
        replacement.add(new NumericDocValuesField("val", -1));
        w.softUpdateDocument(
            new Term("id", Integer.toString(docId)),
            replacement,
            new NumericDocValuesField(SOFT_DELETE_FIELD, 1));
      }
      int hardDeleted = Math.max(1, (int) (numDocs * 0.01));
      for (int i = 0; i < hardDeleted; i++) {
        int docId = (int) (((long) (i + softDeleted) * 6971) % numDocs);
        w.deleteDocuments(new Term("id", Integer.toString(docId)));
      }
      w.commit();
    }
    baseReader = DirectoryReader.open(dir);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    baseReader.close();
    dir.close();
    for (var f : tempDir.toFile().listFiles()) {
      f.delete();
    }
    tempDir.toFile().delete();
  }

  @Benchmark
  public int wrapSoftDeletes() throws IOException {
    DirectoryReader wrapped =
        new SoftDeletesDirectoryReaderWrapper(baseReader, SOFT_DELETE_FIELD);
    int count = 0;
    for (LeafReaderContext ctx : wrapped.leaves()) {
      Bits liveDocs = ctx.reader().getLiveDocs();
      if (liveDocs != null) {
        count += liveDocs.length();
      }
    }
    return count;
  }
}
