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

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
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

  public void testDeletePartiallyWrittenFilesIfAbort() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwConf = newIndexWriterConfig(new MockAnalyzer(random()));
    iwConf.setMaxBufferedDocs(RandomNumbers.randomIntBetween(random(), 2, 30));
    iwConf.setCodec(getCodec());
    // disable CFS because this test checks file names
    iwConf.setMergePolicy(newLogMergePolicy(false));
    iwConf.setUseCompoundFile(false);

    // Cannot use RIW because this test wants CFS to stay off:
    IndexWriter iw = new IndexWriter(dir, iwConf);

    final Document validDoc = new Document();
    validDoc.add(new IntPoint("id", 0));
    validDoc.add(new StoredField("id", 0));
    iw.addDocument(validDoc);
    iw.commit();

    // make sure that #writeField will fail to trigger an abort
    final Document invalidDoc = new Document();
    FieldType fieldType = new FieldType();
    fieldType.setStored(true);
    invalidDoc.add(
        new Field("invalid", fieldType) {

          @Override
          public String stringValue() {
            // TODO: really bad & scary that this causes IW to
            // abort the segment!!  We should fix this.
            return null;
          }
        });

    try {
      iw.addDocument(invalidDoc);
      iw.commit();
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals(iae, iw.getTragicException());
    }
    // Writer should be closed by tragedy
    assertFalse(iw.isOpen());
    dir.close();
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

  public void testSortedSetVariableLengthBigStoredFields() throws Exception {
    int numIterations = atLeast(1);
    for (int i = 0; i < numIterations; i++) {
      int numDocs = atLeast(200);
      doTestSortedSetVsStoredFields(numDocs, 32766, 32766, 100, 100);
    }
  }

  protected void doTestSortedSetVsStoredFields(
      int numDocs, int minLength, int maxLength, int maxValuesPerDoc, int maxUniqueValues)
      throws Exception {
    Directory dir = newFSDirectory(createTempDir("dvduel"));
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
    conf.setCodec(getCodec());
    Set<String> valueSet = new HashSet<String>();
    for (int i = 0; i < 10000 && valueSet.size() < maxUniqueValues; ++i) {
      final int length = TestUtil.nextInt(random(), minLength, maxLength);
      valueSet.add(TestUtil.randomSimpleString(random(), length));
    }
    String[] uniqueValues = valueSet.toArray(new String[0]);

    // index some docs
    if (VERBOSE) {
      System.out.println("\nTEST: now add numDocs=" + numDocs);
    }
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
      doc.add(idField);
      int numValues = TestUtil.nextInt(random(), 0, maxValuesPerDoc);
      // create a random set of strings
      Set<String> values = new TreeSet<>();
      for (int v = 0; v < numValues; v++) {
        values.add(RandomPicks.randomFrom(random(), uniqueValues));
      }

      // add ordered to the stored field
      for (String v : values) {
        doc.add(new StoredField("stored", v));
      }

      // add in any order to the dv field
      ArrayList<String> unordered = new ArrayList<>(values);
      Collections.shuffle(unordered, random());
      for (String v : unordered) {
        doc.add(new SortedSetDocValuesField("dv", newBytesRef(v)));
      }

      writer.addDocument(doc);
      if (random().nextInt(31) == 0) {
        writer.commit();
      }
    }

    writer.flush();

    // delete some docs
    int numDeletions = random().nextInt(numDocs / 10);
    if (VERBOSE) {
      System.out.println("\nTEST: now delete " + numDeletions + " docs");
    }
    for (int i = 0; i < numDeletions; i++) {
      int id = random().nextInt(numDocs);
      writer.deleteDocuments(new Term("id", Integer.toString(id)));
    }

    // compare
    if (VERBOSE) {
      System.out.println("\nTEST: now get reader");
    }
    DirectoryReader ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        String[] stringValues = r.document(i).getValues("stored");
        if (docValues != null) {
          if (docValues.docID() < i) {
            docValues.nextDoc();
          }
        }
        if (docValues != null && stringValues.length > 0) {
          assertEquals(i, docValues.docID());
          for (int j = 0; j < stringValues.length; j++) {
            assert docValues != null;
            long ord = docValues.nextOrd();
            assert ord != NO_MORE_ORDS;
            BytesRef scratch = docValues.lookupOrd(ord);
            assertEquals(stringValues[j], scratch.utf8ToString());
          }
          assertEquals(NO_MORE_ORDS, docValues.nextOrd());
        }
      }
    }
    if (VERBOSE) {
      System.out.println("\nTEST: now close reader");
    }
    ir.close();
    if (VERBOSE) {
      System.out.println("TEST: force merge");
    }
    writer.forceMerge(1);

    // compare again
    ir = writer.getReader();
    TestUtil.checkReader(ir);
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
      for (int i = 0; i < r.maxDoc(); i++) {
        String[] stringValues = r.document(i).getValues("stored");
        if (docValues.docID() < i) {
          docValues.nextDoc();
        }
        if (stringValues.length > 0) {
          assertEquals(i, docValues.docID());
          for (int j = 0; j < stringValues.length; j++) {
            assert docValues != null;
            long ord = docValues.nextOrd();
            assert ord != NO_MORE_ORDS;
            BytesRef scratch = docValues.lookupOrd(ord);
            assertEquals(stringValues[j], scratch.utf8ToString());
          }
          assertEquals(NO_MORE_ORDS, docValues.nextOrd());
        }
      }
    }
    if (VERBOSE) {
      System.out.println("TEST: close reader");
    }
    ir.close();
    if (VERBOSE) {
      System.out.println("TEST: close writer");
    }
    writer.close();
    if (VERBOSE) {
      System.out.println("TEST: close dir");
    }
    dir.close();
  }

  static byte[] randomArray(Random random) {
    int bigsize = 10 * 1024 * 1024;
    final int max = 255;
    final int length = bigsize;
    return randomArray(random, length, max);
  }

  static byte[] randomArray(Random random, int length, int max) {
    final byte[] arr = new byte[length];
    for (int i = 0; i < arr.length; ++i) {
      arr[i] = (byte) RandomNumbers.randomIntBetween(random, 0, max);
    }
    return arr;
  }
}
