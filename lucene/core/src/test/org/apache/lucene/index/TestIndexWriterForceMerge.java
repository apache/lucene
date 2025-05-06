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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestIndexWriterForceMerge extends LuceneTestCase {
  public void testPartialMerge() throws IOException {

    Directory dir = newDirectory();

    final Document doc = new Document();
    doc.add(newStringField("content", "aaa", Field.Store.NO));
    final int incrMin = TEST_NIGHTLY ? 15 : 40;
    for (int numDocs = 10;
        numDocs < 500;
        numDocs += TestUtil.nextInt(random(), incrMin, 5 * incrMin)) {
      LogDocMergePolicy ldmp = new LogDocMergePolicy();
      ldmp.setMinMergeDocs(1);
      ldmp.setMergeFactor(5);
      IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig(new MockAnalyzer(random()))
                  .setOpenMode(OpenMode.CREATE)
                  .setMaxBufferedDocs(2)
                  .setMergePolicy(ldmp));
      for (int j = 0; j < numDocs; j++) writer.addDocument(doc);
      writer.close();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      final int segCount = sis.size();

      ldmp = new LogDocMergePolicy();
      ldmp.setMergeFactor(5);
      writer =
          new IndexWriter(
              dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(ldmp));
      writer.forceMerge(3);
      writer.close();

      sis = SegmentInfos.readLatestCommit(dir);
      final int optSegCount = sis.size();

      if (segCount < 3) assertEquals(segCount, optSegCount);
      else assertEquals(3, optSegCount);
    }
    dir.close();
  }

  public void testMaxNumSegments2() throws IOException {
    Directory dir = newDirectory();

    final Document doc = new Document();
    doc.add(newStringField("content", "aaa", Field.Store.NO));

    LogDocMergePolicy ldmp = new LogDocMergePolicy();
    ldmp.setMinMergeDocs(1);
    ldmp.setMergeFactor(4);
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMaxBufferedDocs(2)
                .setMergePolicy(ldmp)
                .setMergeScheduler(new ConcurrentMergeScheduler()));

    for (int iter = 0; iter < 10; iter++) {
      for (int i = 0; i < 19; i++) writer.addDocument(doc);

      writer.commit();
      writer.waitForMerges();
      writer.commit();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);

      final int segCount = sis.size();
      writer.forceMerge(7);
      writer.commit();
      writer.waitForMerges();

      sis = SegmentInfos.readLatestCommit(dir);
      final int optSegCount = sis.size();

      if (segCount < 7) assertEquals(segCount, optSegCount);
      else assertEquals("seg: " + segCount, 7, optSegCount);
    }
    writer.close();
    dir.close();
  }

  /**
   * Make sure forceMerge doesn't use any more than 1X starting index size as its temporary free
   * space required.
   */
  public void testForceMergeTempSpaceUsage() throws IOException {

    final MockDirectoryWrapper dir = newMockDirectory();
    // don't use MockAnalyzer, variable length payloads can cause merge to make things bigger,
    // since things are optimized for fixed length case. this is a problem for MemoryPF's encoding.
    // (it might have other problems too)
    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, true));
          }
        };
    IndexWriter writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(analyzer)
                .setMaxBufferedDocs(10)
                .setMergePolicy(newLogMergePolicy()));

    if (VERBOSE) {
      System.out.println("TEST: config1=" + writer.getConfig());
    }

    for (int j = 0; j < 500; j++) {
      TestIndexWriter.addDocWithIndex(writer, j);
    }
    // force one extra segment w/ different doc store so
    // we see the doc stores get merged
    writer.commit();
    TestIndexWriter.addDocWithIndex(writer, 500);
    writer.close();

    long startDiskUsage = 0;
    for (String f : dir.listAll()) {
      startDiskUsage += dir.fileLength(f);
      if (VERBOSE) {
        System.out.println(f + ": " + dir.fileLength(f));
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: start disk usage = " + startDiskUsage);
    }
    String startListing = listFiles(dir);

    dir.resetMaxUsedSizeInBytes();
    dir.setTrackDiskUsage(true);

    writer =
        new IndexWriter(
            dir,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setOpenMode(OpenMode.APPEND)
                .setMergePolicy(newLogMergePolicy()));

    if (VERBOSE) {
      System.out.println("TEST: config2=" + writer.getConfig());
    }

    writer.forceMerge(1);
    writer.close();

    long finalDiskUsage = 0;
    for (String f : dir.listAll()) {
      finalDiskUsage += dir.fileLength(f);
      if (VERBOSE) {
        System.out.println(f + ": " + dir.fileLength(f));
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: final disk usage = " + finalDiskUsage);
    }

    // The result of the merged index is often smaller, but sometimes it could
    // be bigger (compression slightly changes, Codec changes etc.). Therefore
    // we compare the temp space used to the max of the initial and final index
    // size
    long maxStartFinalDiskUsage = Math.max(startDiskUsage, finalDiskUsage);
    long maxDiskUsage = dir.getMaxUsedSizeInBytes();
    assertTrue(
        "forceMerge used too much temporary space: starting usage was "
            + startDiskUsage
            + " bytes; final usage was "
            + finalDiskUsage
            + " bytes; max temp usage was "
            + maxDiskUsage
            + " but should have been at most "
            + (4 * maxStartFinalDiskUsage)
            + " (= 4X starting usage), BEFORE="
            + startListing
            + "AFTER="
            + listFiles(dir),
        maxDiskUsage <= 4 * maxStartFinalDiskUsage);
    dir.close();
  }

  // print out listing of files and sizes, but recurse into CFS to debug nested files there.
  private String listFiles(Directory dir) throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    StringBuilder sb = new StringBuilder();
    sb.append(System.lineSeparator());
    for (SegmentCommitInfo info : infos) {
      for (String file : info.files()) {
        sb.append(String.format(Locale.ROOT, "%-20s%d%n", file, dir.fileLength(file)));
      }
      if (info.info.getUseCompoundFile()) {
        try (Directory cfs =
            info.info.getCodec().compoundFormat().getCompoundReader(dir, info.info)) {
          for (String file : cfs.listAll()) {
            sb.append(
                String.format(
                    Locale.ROOT,
                    " |- (inside compound file) %-20s%d%n",
                    file,
                    cfs.fileLength(file)));
          }
        }
      }
    }
    sb.append(System.lineSeparator());
    return sb.toString();
  }

  // Test calling forceMerge(1, false) whereby forceMerge is kicked
  // off but we don't wait for it to finish (but
  // writer.close()) does wait
  public void testBackgroundForceMerge() throws IOException {

    Directory dir = newDirectory();
    for (int pass = 0; pass < 2; pass++) {
      IndexWriter writer =
          new IndexWriter(
              dir,
              newIndexWriterConfig(new MockAnalyzer(random()))
                  .setOpenMode(OpenMode.CREATE)
                  .setMaxBufferedDocs(2)
                  .setMergePolicy(newLogMergePolicy(51)));
      Document doc = new Document();
      doc.add(newStringField("field", "aaa", Field.Store.NO));
      for (int i = 0; i < 100; i++) {
        writer.addDocument(doc);
      }
      writer.forceMerge(1, false);

      if (0 == pass) {
        writer.close();
        DirectoryReader reader = DirectoryReader.open(dir);
        assertEquals(1, reader.leaves().size());
        reader.close();
      } else {
        // Get another segment to flush so we can verify it is
        // NOT included in the merging
        writer.addDocument(doc);
        writer.addDocument(doc);
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        assertTrue(reader.leaves().size() > 1);
        reader.close();

        SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
        assertEquals(2, infos.size());
      }
    }

    dir.close();
  }

  @AwaitsFix(bugUrl = "https://github.com/apache/lucene/issues/13478")
  public void testMergePerField() throws IOException {
    IndexWriterConfig config = new IndexWriterConfig();
    ConcurrentMergeScheduler mergeScheduler =
        new ConcurrentMergeScheduler() {
          @Override
          public Executor getIntraMergeExecutor(MergePolicy.OneMerge merge) {
            // always enable parallel merges
            return intraMergeExecutor;
          }
        };
    mergeScheduler.setMaxMergesAndThreads(4, 4);
    config.setMergeScheduler(mergeScheduler);
    Codec codec = TestUtil.getDefaultCodec();
    CyclicBarrier barrier = new CyclicBarrier(2);
    config.setCodec(
        new FilterCodec(codec.getName(), codec) {
          @Override
          public PostingsFormat postingsFormat() {
            return new PerFieldPostingsFormat() {
              @Override
              public PostingsFormat getPostingsFormatForField(String field) {
                return new BlockingOnMergePostingsFormat(
                    TestUtil.getDefaultPostingsFormat(), barrier);
              }
            };
          }

          @Override
          public DocValuesFormat docValuesFormat() {
            return new PerFieldDocValuesFormat() {
              @Override
              public DocValuesFormat getDocValuesFormatForField(String field) {
                return new BlockingOnMergeDocValuesFormat(
                    TestUtil.getDefaultDocValuesFormat(), barrier);
              }
            };
          }
        });
    try (Directory directory = newDirectory();
        IndexWriter writer = new IndexWriter(directory, config)) {
      int numDocs = 50 + random().nextInt(100);
      int numFields = 5 + random().nextInt(5);
      for (int d = 0; d < numDocs; d++) {
        Document doc = new Document();
        for (int f = 0; f < numFields * 2; f++) {
          String field = "f" + f;
          String value = "v-" + random().nextInt(100);
          if (f % 2 == 0) {
            doc.add(new StringField(field, value, Field.Store.NO));
          } else {
            doc.add(new BinaryDocValuesField(field, new BytesRef(value)));
          }
          doc.add(new LongPoint("p" + f, random().nextInt(10000)));
        }
        writer.addDocument(doc);
        if (random().nextInt(100) < 10) {
          writer.flush();
        }
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertEquals(numDocs, reader.numDocs());
      }
    }
  }

  static class BlockingOnMergePostingsFormat extends PostingsFormat {
    private final PostingsFormat postingsFormat;
    private final CyclicBarrier barrier;

    BlockingOnMergePostingsFormat(PostingsFormat postingsFormat, CyclicBarrier barrier) {
      super(postingsFormat.getName());
      this.postingsFormat = postingsFormat;
      this.barrier = barrier;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      var in = postingsFormat.fieldsConsumer(state);
      return new FieldsConsumer() {
        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
          in.write(fields, norms);
        }

        @Override
        public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
          try {
            barrier.await(1, TimeUnit.SECONDS);
          } catch (Exception e) {
            throw new AssertionError("broken barrier", e);
          }
          in.merge(mergeState, norms);
        }

        @Override
        public void close() throws IOException {
          in.close();
        }
      };
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
      return postingsFormat.fieldsProducer(state);
    }
  }

  static class BlockingOnMergeDocValuesFormat extends DocValuesFormat {
    private final DocValuesFormat docValuesFormat;
    private final CyclicBarrier barrier;

    BlockingOnMergeDocValuesFormat(DocValuesFormat docValuesFormat, CyclicBarrier barrier) {
      super(docValuesFormat.getName());
      this.docValuesFormat = docValuesFormat;
      this.barrier = barrier;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      DocValuesConsumer in = docValuesFormat.fieldsConsumer(state);
      return new DocValuesConsumer() {
        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
            throws IOException {
          in.addNumericField(field, valuesProducer);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer)
            throws IOException {
          in.addBinaryField(field, valuesProducer);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer)
            throws IOException {
          in.addSortedField(field, valuesProducer);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
            throws IOException {
          in.addSortedNumericField(field, valuesProducer);
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
            throws IOException {
          in.addSortedSetField(field, valuesProducer);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
          try {
            barrier.await(1, TimeUnit.SECONDS);
          } catch (Exception e) {
            throw new AssertionError("broken barrier", e);
          }
          in.merge(mergeState);
        }

        @Override
        public void close() throws IOException {
          in.close();
        }
      };
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
      return docValuesFormat.fieldsProducer(state);
    }
  }
}
