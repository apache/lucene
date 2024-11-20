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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * This test creates the following index:
 *
 * <p>segment 0: (ord = 0, live = true), (ord = 1, live = false)
 *
 * <p>segment 1: (ord = 2, live = true), (ord = 3, live = false)
 *
 * <p>segment 4: (ord = 4, live = true), (ord = 5, live = false)
 *
 * <p>It also creates 3 selectors:
 *
 * <p>{@link LiveToLiveSelector} selects for ord in {0, 2}
 *
 * <p>{@link DeleteToLiveSelector} selects for ord in {3, 5}
 *
 * <p>{@link DeleteSelector} selects for ord in {1, 2, 4, 5}
 */
public class TestIndexRearranger extends LuceneTestCase {
  /**
   * Use {@link LiveToLiveSelector} and {@link DeleteToLiveSelector} to rearrange the index into 2
   * segments:
   *
   * <p>segment 0: (ord = 0, live = true), (ord = 2, live = true)
   *
   * <p>segment 1: (ord = 3, live = true), (ord = 5, live = true)
   *
   * <p>The documents with ord 1 and 4 have now been lost, since they were not selected to be in the
   * rearranged index. All documents that were selected are now considered live.
   *
   * <p>Next, we apply the deletions specified by {@link DeleteSelector}:
   *
   * <p>segment 0: (ord = 0, live = true), (ord = 2, live = false)
   *
   * <p>segment 1: (ord = 3, live = true), (ord = 5, live = false)
   *
   * <p>The documents with ord 2 and 5 have been marked for deletion. Documents 1 and 4 would have
   * also been marked if they were present.
   */
  public void testLiveDeleteCombinations() throws Exception {
    Directory inputDir = newDirectory();
    createIndex(inputDir);

    Directory outputDir = newDirectory();
    IndexRearranger rearranger =
        new IndexRearranger(
            inputDir,
            outputDir,
            getIndexWriterConfig(),
            List.of(
                new TestIndexRearranger.LiveToLiveSelector(),
                new TestIndexRearranger.DeleteToLiveSelector()),
            new TestIndexRearranger.DeleteSelector());
    rearranger.execute();

    IndexReader reader = DirectoryReader.open(outputDir);
    assertEquals(2, reader.leaves().size());

    LeafReader segment1 = reader.leaves().get(0).reader();
    assertEquals(1, segment1.numDocs());
    assertEquals(2, segment1.maxDoc());
    Bits liveDocs = segment1.getLiveDocs();
    assertNotNull(liveDocs);
    NumericDocValues numericDocValues = segment1.getNumericDocValues("ord");
    assertNotNull(numericDocValues);
    assertTrue(numericDocValues.advanceExact(0));
    assertTrue(liveDocs.get(0));
    assertEquals(0, numericDocValues.longValue());
    assertTrue(numericDocValues.advanceExact(1));
    assertFalse(liveDocs.get(1));
    assertEquals(2, numericDocValues.longValue());

    LeafReader segment2 = reader.leaves().get(1).reader();
    assertEquals(1, segment2.numDocs());
    assertEquals(2, segment2.maxDoc());
    liveDocs = segment1.getLiveDocs();
    assertNotNull(liveDocs);
    numericDocValues = segment2.getNumericDocValues("ord");
    assertNotNull(numericDocValues);
    assertTrue(liveDocs.get(0));
    assertTrue(numericDocValues.advanceExact(0));
    assertEquals(3, numericDocValues.longValue());
    assertFalse(liveDocs.get(1));
    assertTrue(numericDocValues.advanceExact(1));
    assertEquals(5, numericDocValues.longValue());

    reader.close();
    inputDir.close();
    outputDir.close();
  }

  /**
   * This test arrives at an empty rearranged index by using the same selector for creating segments
   * and applying deletes.
   */
  public void testDeleteEverything() throws Exception {
    Directory inputDir = newDirectory();
    createIndex(inputDir);

    Directory outputDir = newDirectory();
    IndexRearranger rearranger =
        new IndexRearranger(
            inputDir,
            outputDir,
            getIndexWriterConfig(),
            List.of(new TestIndexRearranger.LiveToLiveSelector()),
            new TestIndexRearranger.LiveToLiveSelector());
    rearranger.execute();

    IndexReader reader = DirectoryReader.open(outputDir);
    assertEquals(0, reader.leaves().size());

    reader.close();
    inputDir.close();
    outputDir.close();
  }

  /** Don't pass a deletes selector, all selected docs will be live. */
  public void testDeleteNothing() throws Exception {
    Directory inputDir = newDirectory();
    createIndex(inputDir);

    Directory outputDir = newDirectory();
    IndexRearranger rearranger =
        new IndexRearranger(
            inputDir,
            outputDir,
            getIndexWriterConfig(),
            List.of(
                new TestIndexRearranger.LiveToLiveSelector(),
                new TestIndexRearranger.DeleteToLiveSelector()));
    rearranger.execute();

    IndexReader reader = DirectoryReader.open(outputDir);
    assertEquals(2, reader.leaves().size());

    LeafReader segment1 = reader.leaves().get(0).reader();
    assertEquals(2, segment1.numDocs());
    assertEquals(2, segment1.maxDoc());
    Bits liveDocs = segment1.getLiveDocs();
    assertNull(liveDocs);
    NumericDocValues numericDocValues = segment1.getNumericDocValues("ord");
    assertNotNull(numericDocValues);
    assertTrue(numericDocValues.advanceExact(0));
    assertEquals(0, numericDocValues.longValue());
    assertTrue(numericDocValues.advanceExact(1));
    assertEquals(2, numericDocValues.longValue());

    LeafReader segment2 = reader.leaves().get(1).reader();
    assertEquals(2, segment2.numDocs());
    assertEquals(2, segment2.maxDoc());
    liveDocs = segment1.getLiveDocs();
    assertNull(liveDocs);
    numericDocValues = segment2.getNumericDocValues("ord");
    assertNotNull(numericDocValues);
    assertTrue(numericDocValues.advanceExact(0));
    assertEquals(3, numericDocValues.longValue());
    assertTrue(numericDocValues.advanceExact(1));
    assertEquals(5, numericDocValues.longValue());

    reader.close();
    inputDir.close();
    outputDir.close();
  }

  private static IndexWriterConfig getIndexWriterConfig() {
    return new IndexWriterConfig(null)
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setIndexSort(new Sort(new SortField("ord", SortField.Type.INT)));
  }

  private static void createIndex(Directory dir) throws IOException {
    IndexWriter w = new IndexWriter(dir, getIndexWriterConfig());
    for (int i = 0; i < 6; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("ord", i));
      if (i % 2 == 1) {
        doc.add(new StringField("delete", new BytesRef("yes"), Field.Store.YES));
      }
      w.addDocument(doc);
      if (i % 2 == 1) {
        w.deleteDocuments(new Term("delete", "yes"));
        w.commit();
      }
    }

    assertCreatedIndex(dir);
    w.close();
  }

  private static void assertCreatedIndex(Directory dir) throws IOException {
    IndexReader reader = DirectoryReader.open(dir);
    assertEquals(3, reader.leaves().size());

    for (int i = 0; i < 3; i++) {
      LeafReader segmentReader = reader.leaves().get(i).reader();
      assertEquals(1, segmentReader.numDocs());
      assertEquals(2, segmentReader.maxDoc());

      Bits liveDocs = segmentReader.getLiveDocs();
      assertNotNull(liveDocs);
      assertTrue(liveDocs.get(0));
      assertFalse(liveDocs.get(1));

      NumericDocValues ord = segmentReader.getNumericDocValues("ord");
      assertNotNull(ord);
      assertTrue(ord.advanceExact(0));
      assertEquals(2 * i, ord.longValue());
      assertTrue(ord.advanceExact(1));
      assertEquals(2 * i + 1, ord.longValue());
    }

    reader.close();
  }

  private static class LiveToLiveSelector implements IndexRearranger.DocumentSelector {
    @Override
    public BitSet getFilteredDocs(CodecReader reader) throws IOException {
      FixedBitSet filteredSet = new FixedBitSet(reader.maxDoc());
      NumericDocValues numericDocValues = reader.getNumericDocValues("ord");
      assert numericDocValues != null;
      for (int docid = 0; docid < reader.maxDoc(); docid++) {
        if (numericDocValues.advanceExact(docid)
            && Arrays.asList(0, 2).contains((int) numericDocValues.longValue())) {
          filteredSet.set(docid);
        }
      }
      return filteredSet;
    }
  }

  private static class DeleteToLiveSelector implements IndexRearranger.DocumentSelector {
    @Override
    public BitSet getFilteredDocs(CodecReader reader) throws IOException {
      FixedBitSet filteredSet = new FixedBitSet(reader.maxDoc());
      NumericDocValues numericDocValues = reader.getNumericDocValues("ord");
      assert numericDocValues != null;
      for (int docid = 0; docid < reader.maxDoc(); docid++) {
        if (numericDocValues.advanceExact(docid)
            && Arrays.asList(3, 5).contains((int) numericDocValues.longValue())) {
          filteredSet.set(docid);
        }
      }
      return filteredSet;
    }
  }

  private static class DeleteSelector implements IndexRearranger.DocumentSelector {
    @Override
    public BitSet getFilteredDocs(CodecReader reader) throws IOException {
      FixedBitSet filteredSet = new FixedBitSet(reader.maxDoc());
      NumericDocValues numericDocValues = reader.getNumericDocValues("ord");
      assert numericDocValues != null;
      for (int docid = 0; docid < reader.maxDoc(); docid++) {
        if (numericDocValues.advanceExact(docid)
            && Arrays.asList(1, 2, 4, 5).contains((int) numericDocValues.longValue())) {
          filteredSet.set(docid);
        }
      }
      return filteredSet;
    }
  }
}
