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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.BaseTestCheckIndex;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.VectorUtil;
import org.junit.Test;

public class TestCheckIndex extends BaseTestCheckIndex {
  private Directory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
  }

  @Override
  public void tearDown() throws Exception {
    directory.close();
    super.tearDown();
  }

  @Test
  public void testDeletedDocs() throws IOException {
    testDeletedDocs(directory);
  }

  @Test
  public void testChecksumsOnly() throws IOException {
    testChecksumsOnly(directory);
  }

  @Test
  public void testChecksumsOnlyVerbose() throws IOException {
    testChecksumsOnlyVerbose(directory);
  }

  @Test
  public void testObtainsLock() throws IOException {
    testObtainsLock(directory);
  }

  @Test
  public void testCheckIndexAllValid() throws Exception {
    try (Directory dir = newDirectory()) {
      int liveDocCount = 1 + random().nextInt(10);
      IndexWriterConfig config = newIndexWriterConfig();
      config.setIndexSort(new Sort(new SortField("sort_field", SortField.Type.INT, true)));
      config.setSoftDeletesField("soft_delete");
      // preserves soft-deletes across merges
      config.setMergePolicy(
          new SoftDeletesRetentionMergePolicy(
              "soft_delete", MatchAllDocsQuery::new, config.getMergePolicy()));
      try (IndexWriter w = new IndexWriter(dir, config)) {
        for (int i = 0; i < liveDocCount; i++) {
          Document doc = new Document();

          // stored field
          doc.add(new StringField("id", Integer.toString(random().nextInt()), Field.Store.YES));
          doc.add(new StoredField("field", "value" + TestUtil.randomSimpleString(random())));

          // vector
          doc.add(new KnnFloatVectorField("v1", randomVector(3)));
          doc.add(new KnnFloatVectorField("v2", randomVector(3)));

          // doc value
          doc.add(new NumericDocValuesField("dv", random().nextLong()));

          // point value
          byte[] point = new byte[4];
          NumericUtils.intToSortableBytes(random().nextInt(), point, 0);
          doc.add(new BinaryPoint("point", point));

          // term vector
          Token token1 =
              new Token("bar", 0, 3) {
                {
                  setPayload(new BytesRef("pay1"));
                }
              };
          Token token2 =
              new Token("bar", 4, 8) {
                {
                  setPayload(new BytesRef("pay2"));
                }
              };
          FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
          ft.setStoreTermVectors(true);
          ft.setStoreTermVectorPositions(true);
          ft.setStoreTermVectorPayloads(true);
          doc.add(new Field("termvector", new CannedTokenStream(token1, token2), ft));

          w.addDocument(doc);
        }

        Document tombstone = new Document();
        tombstone.add(new NumericDocValuesField("soft_delete", 1));
        w.softUpdateDocument(
            new Term("id", "1"), tombstone, new NumericDocValuesField("soft_delete", 1));
        w.forceMerge(1);
      }

      ByteArrayOutputStream output = new ByteArrayOutputStream();
      CheckIndex.Status status = TestUtil.checkIndex(dir, false, true, true, output);

      assertEquals(1, status.segmentInfos.size());

      CheckIndex.Status.SegmentInfoStatus segStatus = status.segmentInfos.get(0);

      // confirm live docs testing status
      assertEquals(0, segStatus.liveDocStatus.numDeleted);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: check live docs"));
      assertNull(segStatus.liveDocStatus.error);

      // confirm field infos testing status
      assertEquals(8, segStatus.fieldInfoStatus.totFields);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: field infos"));
      assertNull(segStatus.fieldInfoStatus.error);

      // confirm field norm (from term vector) testing status
      assertEquals(1, segStatus.fieldNormStatus.totFields);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: field norms"));
      assertNull(segStatus.fieldNormStatus.error);

      // confirm term index testing status
      assertTrue(segStatus.termIndexStatus.termCount > 0);
      assertTrue(segStatus.termIndexStatus.totFreq > 0);
      assertTrue(segStatus.termIndexStatus.totPos > 0);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: terms, freq, prox"));
      assertNull(segStatus.termIndexStatus.error);

      // confirm stored field testing status
      // add storedField from tombstone doc
      assertEquals(liveDocCount + 1, segStatus.storedFieldStatus.docCount);
      assertEquals(2 * liveDocCount, segStatus.storedFieldStatus.totFields);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: stored fields"));
      assertNull(segStatus.storedFieldStatus.error);

      // confirm term vector testing status
      assertEquals(liveDocCount, segStatus.termVectorStatus.docCount);
      assertEquals(liveDocCount, segStatus.termVectorStatus.totVectors);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: term vectors"));
      assertNull(segStatus.termVectorStatus.error);

      // confirm doc values testing status
      assertEquals(2, segStatus.docValuesStatus.totalNumericFields);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: docvalues"));
      assertNull(segStatus.docValuesStatus.error);

      // confirm point values testing status
      assertEquals(1, segStatus.pointsStatus.totalValueFields);
      assertEquals(liveDocCount, segStatus.pointsStatus.totalValuePoints);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: points"));
      assertNull(segStatus.pointsStatus.error);

      // confirm vector testing status
      assertEquals(2 * liveDocCount, segStatus.vectorValuesStatus.totalVectorValues);
      assertEquals(2, segStatus.vectorValuesStatus.totalKnnVectorFields);
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: vectors"));
      assertNull(segStatus.vectorValuesStatus.error);

      // confirm index sort testing status
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: index sort"));
      assertNull(segStatus.indexSortStatus.error);

      // confirm soft deletes testing status
      assertTrue(output.toString(IOUtils.UTF_8).contains("test: check soft deletes"));
      assertNull(segStatus.softDeletesStatus.error);
    }
  }

  public void testInvalidThreadCountArgument() {
    String[] args = new String[] {"-threadCount", "0"};
    expectThrows(IllegalArgumentException.class, () -> CheckIndex.parseOptions(args));
  }

  private float[] randomVector(int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = random().nextFloat();
    }
    VectorUtil.l2normalize(v);
    return v;
  }

  // Never deletes any commit points!  Do not use in production!!
  private static class DeleteNothingIndexDeletionPolicy extends IndexDeletionPolicy {

    public static final IndexDeletionPolicy INSTANCE = new DeleteNothingIndexDeletionPolicy();

    private DeleteNothingIndexDeletionPolicy() {
      // NO
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) {}

    @Override
    public void onCommit(List<? extends IndexCommit> commits) {}
  }

  // https://github.com/apache/lucene/issues/7820 -- when the most recent commit point in
  // the index is OK, but older commit points are broken, CheckIndex fails to detect and
  // correct that, while opening an IndexWriter on the index will fail since IndexWriter
  // loads all commit points on init
  public void testPriorBrokenCommitPoint() throws Exception {

    try (MockDirectoryWrapper dir = newMockDirectory()) {

      // disable this normally useful test infra feature since this test intentionally leaves broken
      // indices:
      dir.setCheckIndexOnClose(false);

      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setIndexDeletionPolicy(DeleteNothingIndexDeletionPolicy.INSTANCE);

      try (IndexWriter iw = new IndexWriter(dir, iwc)) {

        // create first segment, and commit point referencing only segment 0
        Document doc = new Document();
        doc.add(new StringField("id", "a", Field.Store.NO));
        iw.addDocument(doc);
        iw.commit();

        // NOTE: we are (illegally) relying on precise file naming here -- if Codec or IW's
        // behaviour changes, this may need fixing:
        assertTrue(slowFileExists(dir, "_0.si"));

        // create second segment, and another commit point referencing only segment 1
        doc.add(new StringField("id", "a", Field.Store.NO));
        iw.updateDocument(new Term("id", "a"), doc);
        iw.commit();

        // NOTE: we are (illegally) relying on precise file naming here -- if Codec or IW's
        // behaviour changes, this may need fixing:
        assertTrue(slowFileExists(dir, "_0.si"));
        assertTrue(slowFileExists(dir, "_1.si"));
      }

      try (CheckIndex checkers = new CheckIndex(dir)) {
        CheckIndex.Status checkIndexStatus = checkers.checkIndex();
        assertTrue(checkIndexStatus.clean);
      }

      // now corrupt segment 0, which is referenced by only the first commit point, by removing its
      // .si file (_0.si)
      dir.deleteFile("_0.si");

      try (CheckIndex checkers = new CheckIndex(dir)) {
        CheckIndex.Status checkIndexStatus = checkers.checkIndex();
        assertFalse(checkIndexStatus.clean);
      }
    }
  }
}
