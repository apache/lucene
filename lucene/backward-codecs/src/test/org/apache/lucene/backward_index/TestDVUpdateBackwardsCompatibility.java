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
package org.apache.lucene.backward_index;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class TestDVUpdateBackwardsCompatibility extends BackwardsCompatibilityTestBase {

  static final String INDEX_NAME = "dvupdates";
  static final String SUFFIX = "";

  public TestDVUpdateBackwardsCompatibility(Version version, String pattern) {
    super(version, pattern);
  }

  /** Provides the initial release of the previous major to the test-framework */
  @ParametersFactory(argumentFormatting = "Lucene-Version:%1$s; Pattern: %2$s")
  public static Iterable<Object[]> testVersionsFactory() {
    List<Object[]> params = new ArrayList<>();
    // TODO - WHY ONLY on the first major version?
    params.add(new Object[] {Version.LUCENE_9_0_0, createPattern(INDEX_NAME, SUFFIX)});
    return params;
  }

  @Override
  protected void createIndex(Directory directory) throws IOException {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setCodec(TestUtil.getDefaultCodec())
            .setUseCompoundFile(false)
            .setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(directory, conf);
    // create an index w/ few doc-values fields, some with updates and some without
    for (int i = 0; i < 30; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "" + i, Field.Store.NO));
      doc.add(new NumericDocValuesField("ndv1", i));
      doc.add(new NumericDocValuesField("ndv1_c", i * 2));
      doc.add(new NumericDocValuesField("ndv2", i * 3));
      doc.add(new NumericDocValuesField("ndv2_c", i * 6));
      doc.add(new BinaryDocValuesField("bdv1", toBytes(i)));
      doc.add(new BinaryDocValuesField("bdv1_c", toBytes(i * 2)));
      doc.add(new BinaryDocValuesField("bdv2", toBytes(i * 3)));
      doc.add(new BinaryDocValuesField("bdv2_c", toBytes(i * 6)));
      writer.addDocument(doc);
      if ((i + 1) % 10 == 0) {
        writer.commit(); // flush every 10 docs
      }
    }

    // first segment: no updates

    // second segment: update two fields, same gen
    updateNumeric(writer, "10", "ndv1", "ndv1_c", 100L);
    updateBinary(writer, "11", "bdv1", "bdv1_c", 100L);
    writer.commit();

    // third segment: update few fields, different gens, few docs
    updateNumeric(writer, "20", "ndv1", "ndv1_c", 100L);
    updateBinary(writer, "21", "bdv1", "bdv1_c", 100L);
    writer.commit();
    updateNumeric(writer, "22", "ndv1", "ndv1_c", 200L); // update the field again
    writer.close();
  }

  public void testDocValuesUpdates() throws Exception {
    searchDocValuesUpdatesIndex(directory);
  }

  public void testDeletes() throws Exception {
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(directory, conf);

    int maxDoc = writer.getDocStats().maxDoc;
    writer.deleteDocuments(new Term("id", "1"));
    if (random().nextBoolean()) {
      writer.commit();
    }

    writer.forceMerge(1);
    writer.commit();
    assertEquals(maxDoc - 1, writer.getDocStats().maxDoc);

    writer.close();
  }

  public void testSoftDeletes() throws Exception {
    IndexWriterConfig conf =
        new IndexWriterConfig(new MockAnalyzer(random())).setSoftDeletesField("__soft_delete");
    IndexWriter writer = new IndexWriter(directory, conf);
    int maxDoc = writer.getDocStats().maxDoc;
    writer.updateDocValues(new Term("id", "1"), new NumericDocValuesField("__soft_delete", 1));

    if (random().nextBoolean()) {
      writer.commit();
    }
    writer.forceMerge(1);
    writer.commit();
    assertEquals(maxDoc - 1, writer.getDocStats().maxDoc);
    writer.close();
  }

  public void testDocValuesUpdatesWithNewField() throws Exception {
    // update fields and verify index
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(directory, conf);
    // introduce a new field that we later update
    writer.addDocument(
        Arrays.asList(
            new StringField("id", "" + Integer.MAX_VALUE, Field.Store.NO),
            new NumericDocValuesField("new_numeric", 1),
            new BinaryDocValuesField("new_binary", toBytes(1))));
    writer.updateNumericDocValue(new Term("id", "1"), "new_numeric", 1);
    writer.updateBinaryDocValue(new Term("id", "1"), "new_binary", toBytes(1));

    writer.commit();
    Runnable assertDV =
        () -> {
          boolean found = false;
          try (DirectoryReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext ctx : reader.leaves()) {
              LeafReader leafReader = ctx.reader();
              TermsEnum id = leafReader.terms("id").iterator();
              if (id.seekExact(new BytesRef("1"))) {
                PostingsEnum postings = id.postings(null, PostingsEnum.NONE);
                NumericDocValues numericDocValues = leafReader.getNumericDocValues("new_numeric");
                BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues("new_binary");
                int doc;
                while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  found = true;
                  assertTrue(binaryDocValues.advanceExact(doc));
                  assertTrue(numericDocValues.advanceExact(doc));
                  assertEquals(1, numericDocValues.longValue());
                  assertEquals(toBytes(1), binaryDocValues.binaryValue());
                }
              }
            }
          } catch (IOException e) {
            throw new AssertionError(e);
          }
          assertTrue(found);
        };
    assertDV.run();
    // merge all segments
    writer.forceMerge(1);
    writer.commit();
    assertDV.run();
    writer.close();
  }

  private void assertNumericDocValues(LeafReader r, String f, String cf) throws IOException {
    NumericDocValues ndvf = r.getNumericDocValues(f);
    NumericDocValues ndvcf = r.getNumericDocValues(cf);
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, ndvcf.nextDoc());
      assertEquals(i, ndvf.nextDoc());
      assertEquals(ndvcf.longValue(), ndvf.longValue() * 2);
    }
  }

  private void assertBinaryDocValues(LeafReader r, String f, String cf) throws IOException {
    BinaryDocValues bdvf = r.getBinaryDocValues(f);
    BinaryDocValues bdvcf = r.getBinaryDocValues(cf);
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(i, bdvf.nextDoc());
      assertEquals(i, bdvcf.nextDoc());
      assertEquals(getValue(bdvcf), getValue(bdvf) * 2);
    }
  }

  static long getValue(BinaryDocValues bdv) throws IOException {
    BytesRef term = bdv.binaryValue();
    int idx = term.offset;
    byte b = term.bytes[idx++];
    long value = b & 0x7FL;
    for (int shift = 7; (b & 0x80L) != 0; shift += 7) {
      b = term.bytes[idx++];
      value |= (b & 0x7FL) << shift;
    }
    return value;
  }

  private void verifyDocValues(Directory dir) throws IOException {
    DirectoryReader reader = DirectoryReader.open(dir);
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader r = context.reader();
      assertNumericDocValues(r, "ndv1", "ndv1_c");
      assertNumericDocValues(r, "ndv2", "ndv2_c");
      assertBinaryDocValues(r, "bdv1", "bdv1_c");
      assertBinaryDocValues(r, "bdv2", "bdv2_c");
    }
    reader.close();
  }

  private void searchDocValuesUpdatesIndex(Directory dir) throws IOException {
    verifyUsesDefaultCodec(dir, indexName(version));
    verifyDocValues(dir);

    // update fields and verify index
    IndexWriterConfig conf = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    updateNumeric(writer, "1", "ndv1", "ndv1_c", 300L);
    updateNumeric(writer, "1", "ndv2", "ndv2_c", 300L);
    updateBinary(writer, "1", "bdv1", "bdv1_c", 300L);
    updateBinary(writer, "1", "bdv2", "bdv2_c", 300L);

    writer.commit();
    verifyDocValues(dir);

    // merge all segments
    writer.forceMerge(1);
    writer.commit();
    verifyDocValues(dir);

    writer.close();
  }

  private void updateNumeric(IndexWriter writer, String id, String f, String cf, long value)
      throws IOException {
    writer.updateNumericDocValue(new Term("id", id), f, value);
    writer.updateNumericDocValue(new Term("id", id), cf, value * 2);
  }

  private void updateBinary(IndexWriter writer, String id, String f, String cf, long value)
      throws IOException {
    writer.updateBinaryDocValue(new Term("id", id), f, toBytes(value));
    writer.updateBinaryDocValue(new Term("id", id), cf, toBytes(value * 2));
  }
}
