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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.tests.codecs.compressing.FastCompressingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestCompressingStoredFieldsFormat extends BaseStoredFieldsFormatTestCase {

  static final long SECOND = 1000L;
  static final long HOUR = 60 * 60 * SECOND;
  static final long DAY = 24 * HOUR;

  @Override
  protected Codec getCodec() {
    if (TEST_NIGHTLY) {
      return CompressingCodec.randomInstance(random());
    } else {
      return CompressingCodec.reasonableInstance(random());
    }
  }

  public void testZFloat() throws Exception {
    byte[] buffer = new byte[5]; // we never need more than 5 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      float f = (float) i;
      Lucene90CompressingStoredFieldsWriter.writeZFloat(out, f);
      in.reset(buffer, 0, out.getPosition());
      float g = Lucene90CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));

      // check that compression actually works
      if (i >= -1 && i <= 123) {
        assertEquals(1, out.getPosition()); // single byte compression
      }
      out.reset(buffer);
    }

    // round-trip special values
    float[] special = {
      -0.0f,
      +0.0f,
      Float.NEGATIVE_INFINITY,
      Float.POSITIVE_INFINITY,
      Float.MIN_VALUE,
      Float.MAX_VALUE,
      Float.NaN,
    };

    for (float f : special) {
      Lucene90CompressingStoredFieldsWriter.writeZFloat(out, f);
      in.reset(buffer, 0, out.getPosition());
      float g = Lucene90CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));
      out.reset(buffer);
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      float f = r.nextFloat() * (random().nextInt(100) - 50);
      Lucene90CompressingStoredFieldsWriter.writeZFloat(out, f);
      assertTrue(
          "length=" + out.getPosition() + ", f=" + f,
          out.getPosition() <= ((Float.floatToIntBits(f) >>> 31) == 1 ? 5 : 4));
      in.reset(buffer, 0, out.getPosition());
      float g = Lucene90CompressingStoredFieldsReader.readZFloat(in);
      assertTrue(in.eof());
      assertEquals(Float.floatToIntBits(f), Float.floatToIntBits(g));
      out.reset(buffer);
    }
  }

  public void testZDouble() throws Exception {
    byte[] buffer = new byte[9]; // we never need more than 9 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      double x = (double) i;
      Lucene90CompressingStoredFieldsWriter.writeZDouble(out, x);
      in.reset(buffer, 0, out.getPosition());
      double y = Lucene90CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));

      // check that compression actually works
      if (i >= -1 && i <= 124) {
        assertEquals(1, out.getPosition()); // single byte compression
      }
      out.reset(buffer);
    }

    // round-trip special values
    double[] special = {
      -0.0d,
      +0.0d,
      Double.NEGATIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.MIN_VALUE,
      Double.MAX_VALUE,
      Double.NaN
    };

    for (double x : special) {
      Lucene90CompressingStoredFieldsWriter.writeZDouble(out, x);
      in.reset(buffer, 0, out.getPosition());
      double y = Lucene90CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      double x = r.nextDouble() * (random().nextInt(100) - 50);
      Lucene90CompressingStoredFieldsWriter.writeZDouble(out, x);
      assertTrue("length=" + out.getPosition() + ", d=" + x, out.getPosition() <= (x < 0 ? 9 : 8));
      in.reset(buffer, 0, out.getPosition());
      double y = Lucene90CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }

    // same with floats
    for (int i = 0; i < 100000; i++) {
      double x = (double) (r.nextFloat() * (random().nextInt(100) - 50));
      Lucene90CompressingStoredFieldsWriter.writeZDouble(out, x);
      assertTrue("length=" + out.getPosition() + ", d=" + x, out.getPosition() <= 5);
      in.reset(buffer, 0, out.getPosition());
      double y = Lucene90CompressingStoredFieldsReader.readZDouble(in);
      assertTrue(in.eof());
      assertEquals(Double.doubleToLongBits(x), Double.doubleToLongBits(y));
      out.reset(buffer);
    }
  }

  public void testTLong() throws Exception {
    byte[] buffer = new byte[10]; // we never need more than 10 bytes
    ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
    ByteArrayDataInput in = new ByteArrayDataInput(buffer);

    // round-trip small integer values
    for (int i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++) {
      for (long mul : new long[] {SECOND, HOUR, DAY}) {
        long l1 = (long) i * mul;
        Lucene90CompressingStoredFieldsWriter.writeTLong(out, l1);
        in.reset(buffer, 0, out.getPosition());
        long l2 = Lucene90CompressingStoredFieldsReader.readTLong(in);
        assertTrue(in.eof());
        assertEquals(l1, l2);

        // check that compression actually works
        if (i >= -16 && i <= 15) {
          assertEquals(1, out.getPosition()); // single byte compression
        }
        out.reset(buffer);
      }
    }

    // round-trip random values
    Random r = random();
    for (int i = 0; i < 100000; i++) {
      final int numBits = r.nextInt(65);
      long l1 = r.nextLong() & ((1L << numBits) - 1);
      switch (r.nextInt(4)) {
        case 0:
          l1 *= SECOND;
          break;
        case 1:
          l1 *= HOUR;
          break;
        case 2:
          l1 *= DAY;
          break;
        default:
          break;
      }
      Lucene90CompressingStoredFieldsWriter.writeTLong(out, l1);
      in.reset(buffer, 0, out.getPosition());
      long l2 = Lucene90CompressingStoredFieldsReader.readTLong(in);
      assertTrue(in.eof());
      assertEquals(l1, l2);
      out.reset(buffer);
    }
  }

  /**
   * writes some tiny segments with incomplete compressed blocks, and ensures merge recompresses
   * them.
   */
  public void testChunkCleanup() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMergePolicy(NoMergePolicy.INSTANCE);

    // we have to enforce certain things like maxDocsPerChunk to cause dirty chunks to be created
    // by this test.
    iwConf.setCodec(CompressingCodec.randomInstance(random(), 4 * 1024, 4, false, 8));
    IndexWriter iw = new IndexWriter(dir, iwConf);
    DirectoryReader ir = DirectoryReader.open(iw);
    for (int i = 0; i < 5; i++) {
      Document doc = new Document();
      doc.add(new StoredField("text", "not very long at all"));
      iw.addDocument(doc);
      // force flush
      DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
      assertNotNull(ir2);
      ir.close();
      ir = ir2;
      // examine dirty counts:
      for (LeafReaderContext leaf : ir2.leaves()) {
        CodecReader sr = (CodecReader) leaf.reader();
        Lucene90CompressingStoredFieldsReader reader =
            (Lucene90CompressingStoredFieldsReader) sr.getFieldsReader();
        assertTrue(reader.getNumDirtyDocs() > 0);
        assertTrue(reader.getNumDirtyDocs() < 100); // can't be gte the number of docs per chunk
        assertEquals(1, reader.getNumDirtyChunks());
      }
    }
    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    // add a single doc and merge again
    Document doc = new Document();
    doc.add(new StoredField("text", "not very long at all"));
    iw.addDocument(doc);
    iw.forceMerge(1);
    DirectoryReader ir2 = DirectoryReader.openIfChanged(ir);
    assertNotNull(ir2);
    ir.close();
    ir = ir2;
    CodecReader sr = (CodecReader) getOnlyLeafReader(ir);
    Lucene90CompressingStoredFieldsReader reader =
        (Lucene90CompressingStoredFieldsReader) sr.getFieldsReader();
    // at most 2: the 5 chunks from 5 doc segment will be collapsed into a single chunk
    assertTrue(reader.getNumDirtyChunks() <= 2);
    ir.close();
    iw.close();
    dir.close();
  }

  //
  // Tests for the HYBRID merge strategy, which copies compressed chunks directly for chunks whose
  // documents are all live, and falls back to doc-by-doc copy for chunks containing deletions.
  //
  // These tests use a large chunk size so that the maxDocsPerChunk threshold drives chunk
  // boundaries, making the chunk layout fully predictable: N docs written in one flush produce
  // N / maxDocsPerChunk full chunks.
  //

  /** Zero-padded id so that lexicographic order matches numeric order (for index sorting). */
  private static String hybridId(int id) {
    return String.format(Locale.ROOT, "%05d", id);
  }

  private static Document hybridTestDoc(int id, int size) {
    Document doc = new Document();
    doc.add(new StringField("id", hybridId(id), Field.Store.YES));
    // SORTED doc values so that the id can be used as an index sort key
    doc.add(new SortedDocValuesField("id", new BytesRef(hybridId(id))));
    StringBuilder text = new StringBuilder();
    while (text.length() < size) {
      text.append("not very long at all ");
    }
    doc.add(new StoredField("text", text.toString()));
    return doc;
  }

  private static void addSegment(IndexWriter iw, int startId, int numDocs) throws IOException {
    // add docs one by one: document blocks (addDocuments) don't support index sorting
    for (int i = 0; i < numDocs; ++i) {
      iw.addDocument(hybridTestDoc(startId + i, 100));
    }
    iw.commit();
  }

  private static void deleteDocs(IndexWriter iw, int... docIds) throws IOException {
    for (int docId : docIds) {
      iw.deleteDocuments(new Term("id", hybridId(docId)));
    }
    iw.commit();
  }

  private static List<Integer> readIds(DirectoryReader ir) throws IOException {
    List<Integer> ids = new ArrayList<>();
    for (LeafReaderContext leaf : ir.leaves()) {
      for (int docID = 0; docID < leaf.reader().maxDoc(); ++docID) {
        ids.add(Integer.valueOf(leaf.reader().storedFields().document(docID).get("id")));
      }
    }
    return ids;
  }

  private static List<Integer> idRange(int startInclusive, int endExclusive) {
    List<Integer> ids = new ArrayList<>();
    for (int i = startInclusive; i < endExclusive; ++i) {
      ids.add(i);
    }
    return ids;
  }

  private static Lucene90CompressingStoredFieldsReader getOnlyFieldsReader(DirectoryReader ir)
      throws IOException {
    CodecReader sr = (CodecReader) getOnlyLeafReader(ir);
    return (Lucene90CompressingStoredFieldsReader) sr.getFieldsReader();
  }

  /** Creates a writer config with a fixed large maxBufferedDocs, for predictable segments. */
  private IndexWriterConfig hybridWriterConfig() {
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMergePolicy(NoMergePolicy.INSTANCE);
    // disable the random flush settings of newIndexWriterConfig, which would split segments
    iwConf.setMaxBufferedDocs(10000);
    iwConf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    // large chunk size so that maxDocsPerChunk=10 drives chunk boundaries
    iwConf.setCodec(new FastCompressingCodec(64 * 1024, 10, false, 8));
    return iwConf;
  }

  /**
   * Clustered deletions enable the hybrid strategy: chunks whose docs are all live are copied
   * directly, keeping the source chunk layout, while docs around the deletion are copied doc by doc
   * and flushed as a dirty chunk before the next chunk copied directly.
   */
  public void testHybridMergeClusteredDeletions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 100);
    addSegment(iw, 100, 20);
    deleteDocs(iw, 50, 51, 52, 53, 54);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // segment A: chunks 0-4 copied directly, docs 55-59 flushed as a dirty chunk, chunks 6-9
      // copied directly; segment B: chunks 0-1 copied directly.
      assertEquals(12, reader.getNumChunks());
      assertEquals(1, reader.getNumDirtyChunks());
      assertEquals(5, reader.getNumDirtyDocs());

      List<Integer> expectedIds = new ArrayList<>();
      expectedIds.addAll(idRange(0, 50));
      expectedIds.addAll(idRange(55, 120));
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * Uniform deletions disable the hybrid strategy: every deletion touches a different chunk, so
   * there is no chunk worth copying directly and merging falls back to recompressing everything
   * doc, which produces full chunks only.
   *
   * <p>Layout: segment A has 100 docs = 10 full chunks, deleting one doc every 10 docs touches all
   * 10 chunks, so the hybrid gate rejects segment A and it is merged doc by doc.
   */
  public void testHybridMergeUniformDeletionsFallsBackToDocCopy() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 100);
    addSegment(iw, 100, 20);
    deleteDocs(iw, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // everything is recompressed doc by doc: 110 docs / 10 docs per chunk = 11 full chunks
      assertEquals(11, reader.getNumChunks());
      assertEquals(0, reader.getNumDirtyChunks());

      List<Integer> expectedIds = new ArrayList<>();
      for (int i = 0; i < 120; ++i) {
        if (i % 10 == 5 && i < 100) {
          continue;
        }
        expectedIds.add(i);
      }
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * Multiple deletion runs within the same chunk are counted once by the hybrid gate, so the hybrid
   * strategy is still used and alternating deletions are handled correctly: docs 51, 53, 55, 57 and
   * 59 are buffered and flushed as a single dirty chunk.
   */
  public void testHybridMergeAlternatingDeletionsWithinChunk() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 100);
    addSegment(iw, 100, 20);
    deleteDocs(iw, 50, 52, 54, 56, 58);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // segment A: chunks 0-4 copied directly, the 5 live docs of chunk 5 buffered and flushed as a
      // single dirty chunk, chunks 6-9 copied directly; segment B: chunks 0-1 copied directly.
      assertEquals(12, reader.getNumChunks());
      assertEquals(1, reader.getNumDirtyChunks());
      assertEquals(5, reader.getNumDirtyDocs());

      List<Integer> expectedIds = new ArrayList<>();
      expectedIds.addAll(idRange(0, 50));
      for (int i = 51; i < 60; i += 2) {
        expectedIds.add(i);
      }
      expectedIds.addAll(idRange(60, 120));
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * A single-chunk segment with deletions is not worth the hybrid strategy (its only chunk is
   * dirty), so it is merged doc by doc.
   */
  public void testHybridMergeSingleChunkSegmentWithDeletions() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 5);
    addSegment(iw, 100, 20);
    deleteDocs(iw, 1, 3);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // segment A is merged doc by doc: its 3 live docs stay in the buffer (too few to make a
      // full chunk); segment B copies its 2 chunks directly, flushing the 3 buffered docs as a
      // dirty chunk first.
      assertEquals(3, reader.getNumChunks());
      assertEquals(1, reader.getNumDirtyChunks());
      assertEquals(3, reader.getNumDirtyDocs());

      List<Integer> expectedIds = new ArrayList<>(Arrays.asList(0, 2, 4));
      expectedIds.addAll(idRange(100, 120));
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * Deletions at the very end of a segment: the last live range ends in the middle of chunk 8 (doc
   * 89 is deleted, docs 90-99 remove the last chunk entirely).
   */
  public void testHybridMergeDeletionsAtSegmentEnd() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 100);
    addSegment(iw, 100, 20);
    int[] deleted = new int[11];
    for (int i = 0; i < 10; ++i) {
      deleted[i] = 90 + i;
    }
    deleted[10] = 89;
    deleteDocs(iw, deleted);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // segment A: chunks 0-7 copied directly, docs 80-88 (9 docs, prefix of chunk 8) buffered and
      // flushed as a dirty chunk before segment B is copied directly.
      assertEquals(11, reader.getNumChunks());
      assertEquals(1, reader.getNumDirtyChunks());
      assertEquals(9, reader.getNumDirtyDocs());

      List<Integer> expectedIds = new ArrayList<>();
      expectedIds.addAll(idRange(0, 89));
      expectedIds.addAll(idRange(100, 120));
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * The hybrid strategy works with an index sort, where the docID merger interleaves docs from
   * multiple readers and live doc ranges get broken up.
   *
   * <p>Layout: two segments of 30 docs = 3 full chunks each, sorted by id. Each segment has a
   * deletion run covering the first half of its middle chunk.
   */
  public void testHybridMergeWithIndexSort() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = hybridWriterConfig();
    iwConf.setIndexSort(new Sort(new SortField("id", SortField.Type.STRING)));
    IndexWriter iw = new IndexWriter(dir, iwConf);

    addSegment(iw, 0, 30);
    addSegment(iw, 30, 30);
    deleteDocs(iw, 10, 11, 12, 13, 14, 40, 41, 42, 43, 44);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      Lucene90CompressingStoredFieldsReader reader = getOnlyFieldsReader(ir);
      // each segment contributes: chunk 0 copied directly, 5 buffered docs flushed as a dirty
      // chunk, chunk 2 copied directly.
      assertEquals(6, reader.getNumChunks());
      assertEquals(2, reader.getNumDirtyChunks());
      assertEquals(10, reader.getNumDirtyDocs());

      // docs are sorted by id
      List<Integer> expectedIds = new ArrayList<>();
      for (int i = 0; i < 60; ++i) {
        if ((i >= 10 && i < 15) || (i >= 40 && i < 45)) {
          continue;
        }
        expectedIds.add(i);
      }
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /**
   * Heterogeneous doc sizes make chunks split on the byte threshold rather than on the doc-count
   * threshold, exercising chunk boundary detection with non-trivial boundaries. Only correctness of
   * the merge output is asserted (the chunk layout is too complex to predict).
   */
  public void testHybridMergeHeterogeneousDocSizes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMergePolicy(NoMergePolicy.INSTANCE);
    // disable the random flush settings of newIndexWriterConfig, which would split segments
    iwConf.setMaxBufferedDocs(10000);
    iwConf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    // small chunk size: big docs flush chunks on the byte threshold, small docs on the doc count
    iwConf.setCodec(new FastCompressingCodec(1024, 8, false, 8));
    IndexWriter iw = new IndexWriter(dir, iwConf);

    // segment A: 40 small docs followed by 20 big docs, deletions clustered in the big-doc area
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 40; ++i) {
      docs.add(hybridTestDoc(i, 30));
    }
    for (int i = 40; i < 60; ++i) {
      docs.add(hybridTestDoc(i, 2048));
    }
    iw.addDocuments(docs);
    iw.commit();
    addSegment(iw, 100, 20);
    deleteDocs(iw, 42, 43, 44, 45, 46);

    iw.getConfig().setMergePolicy(newLogMergePolicy());
    iw.forceMerge(1);
    iw.commit();

    try (DirectoryReader ir = DirectoryReader.open(iw)) {
      List<Integer> expectedIds = new ArrayList<>();
      for (int i = 0; i < 60; ++i) {
        if (i >= 42 && i <= 46) {
          continue;
        }
        expectedIds.add(i);
      }
      expectedIds.addAll(idRange(100, 120));
      assertEquals(expectedIds, readIds(ir));
    }
    iw.close();
    dir.close();
  }

  /** Randomized hybrid merges: random segments, deletion patterns and multiple merge rounds. */
  public void testHybridMergeRandom() throws IOException {
    int numIters = atLeast(3);
    for (int iter = 0; iter < numIters; ++iter) {
      Directory dir = newDirectory();
      IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
      iwConf.setMergePolicy(NoMergePolicy.INSTANCE);
      // the random maxBufferedDocs / ram buffer size set by newIndexWriterConfig could split
      // segments
      iwConf.setMaxBufferedDocs(10000);
      iwConf.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      int chunkDocs = TestUtil.nextInt(random(), 2, 10);
      iwConf.setCodec(new FastCompressingCodec(64 * 1024, chunkDocs, false, 8));
      if (random().nextBoolean()) {
        iwConf.setIndexSort(new Sort(new SortField("id", SortField.Type.STRING)));
      }
      IndexWriter iw = new IndexWriter(dir, iwConf);

      int nextId = 0;
      int numSegments = TestUtil.nextInt(random(), 2, 4);
      for (int seg = 0; seg < numSegments; ++seg) {
        int numDocs = TestUtil.nextInt(random(), 1, 30);
        addSegment(iw, nextId, numDocs);
        nextId += numDocs;
      }

      int numRounds = TestUtil.nextInt(random(), 1, 3);
      for (int round = 0; round < numRounds; ++round) {
        // deletions: either clustered (one run) or scattered (individual docs)
        int numDeletions = TestUtil.nextInt(random(), 0, 5);
        if (numDeletions > 0) {
          if (random().nextBoolean() && nextId > numDeletions) {
            // clustered: delete a run of consecutive ids
            int start = TestUtil.nextInt(random(), 0, nextId - numDeletions);
            for (int i = 0; i < numDeletions; ++i) {
              iw.deleteDocuments(new Term("id", hybridId(start + i)));
            }
          } else {
            // scattered: delete individual random docs (may not all exist anymore, which is fine)
            for (int i = 0; i < numDeletions; ++i) {
              int id = random().nextInt(nextId);
              iw.deleteDocuments(new Term("id", hybridId(id)));
            }
          }
          iw.commit();
        }

        iw.getConfig().setMergePolicy(newLogMergePolicy());
        iw.forceMerge(1);
        iw.commit();

        // verify that all live docs are still readable with their original content
        try (DirectoryReader ir = DirectoryReader.open(iw)) {
          int previous = -1;
          for (LeafReaderContext leaf : ir.leaves()) {
            for (int docID = 0; docID < leaf.reader().maxDoc(); ++docID) {
              Document doc = leaf.reader().storedFields().document(docID);
              int id = Integer.parseInt(doc.get("id"));
              // docs come out sorted by id (index order or index sort)
              assertTrue(id > previous);
              previous = id;
              // each doc must still carry its stored text field
              assertNotNull(doc.get("text"));
              assertTrue(doc.get("text").length() >= 20);
            }
          }
        }
      }
      iw.close();
      dir.close();
    }
  }
}
