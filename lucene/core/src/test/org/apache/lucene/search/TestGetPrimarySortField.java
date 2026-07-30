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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests for {@link Sort#getPrimarySortField(LeafReader)}, which returns the effective primary sort
 * field for a segment, skipping fields that are no-ops because they have no values in the segment
 * or have only a single distinct value according to their skip index.
 */
public class TestGetPrimarySortField extends LuceneTestCase {

  /** A reader with no index sort configured always returns null. */
  public void testNoIndexSort() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig());
    Document doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field", 42));
    iw.addDocument(doc);
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    assertNull(Sort.getPrimarySortField(leafReader));
    reader.close();
    dir.close();
  }

  /**
   * When the sort field has no DocValues skip index (plain DV field), there is no skipper to detect
   * a single-value no-op, but the field exists in FieldInfos so it is returned as-is.
   */
  public void testSortFieldWithoutSkipperIsReturned() throws IOException {
    Directory dir = newDirectory();
    SortField sortField = new SortField("field", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(sortField)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("field", i)); // plain DV, no skip index
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field", result.getField());
    reader.close();
    dir.close();
  }

  /**
   * A sort field with a skip index and multiple distinct values is returned as-is: it is not a
   * no-op because the skipper's min and max values differ.
   */
  public void testSortFieldWithMultipleDistinctValuesIsReturned() throws IOException {
    Directory dir = newDirectory();
    SortField sortField = new SortField("field", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(sortField)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field", i)); // indexed DV, skipper present
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field", result.getField());
    reader.close();
    dir.close();
  }

  /**
   * A sort field with a skip index and one distinct value but with documents with no value is
   * returned as-is: it is not a no-op because documents with no value will group together and form
   * an implicitly-valued second group.
   */
  public void testSparseSortFieldValuesIsReturned() throws IOException {
    Directory dir = newDirectory();
    SortField primary = new SortField("field1", SortField.Type.LONG);
    SortField secondary = new SortField("field2", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(primary, secondary)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      if (i != 7) {
        doc.add(NumericDocValuesField.indexedField("field1", 42)); // constant → no-op
      }
      doc.add(NumericDocValuesField.indexedField("field2", i));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field1", result.getField());
    reader.close();
    dir.close();
  }

  /**
   * When the primary sort field has no values at all in a segment, its FieldInfo is absent and it
   * is treated as a no-op. The next sort field becomes the effective primary.
   */
  public void testPrimaryWithNoValuesInSegmentIsSkipped() throws IOException {
    Directory dir = newDirectory();
    SortField primary = new SortField("field1", SortField.Type.LONG);
    SortField secondary = new SortField("field2", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(primary, secondary)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      // field1: no doc values → FieldInfos has no entry for "field1" → skipped
      doc.add(NumericDocValuesField.indexedField("field2", i));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field2", result.getField());
    reader.close();
    dir.close();
  }

  /**
   * When the primary sort field has a skip index with a single distinct value (skipper min == max),
   * it is a no-op and the next sort field becomes the effective primary.
   */
  public void testSingleValuePrimaryIsSkippedToSecondary() throws IOException {
    Directory dir = newDirectory();
    SortField primary = new SortField("field1", SortField.Type.LONG);
    SortField secondary = new SortField("field2", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(primary, secondary)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field1", 42)); // constant → no-op
      doc.add(NumericDocValuesField.indexedField("field2", i)); // multiple distinct values
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field2", result.getField());
    reader.close();
    dir.close();
  }

  /**
   * When all sort fields have only a single distinct value in the segment, every field is a no-op
   * and null is returned.
   */
  public void testAllSingleValueFieldsReturnNull() throws IOException {
    Directory dir = newDirectory();
    SortField primary = new SortField("field1", SortField.Type.LONG);
    SortField secondary = new SortField("field2", SortField.Type.LONG);
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(primary, secondary)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field1", 42)); // constant
      doc.add(NumericDocValuesField.indexedField("field2", 7)); // constant
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    assertNull(Sort.getPrimarySortField(leafReader));
    reader.close();
    dir.close();
  }

  /**
   * With NoMergePolicy ensuring a fixed three-segment layout, each leaf independently returns its
   * own effective primary sort field: segment 0 has a real primary, segment 1 has an absent primary
   * (no FieldInfo), and segment 2 has a constant primary (single distinct value).
   */
  public void testPerSegmentEffectivePrimaryWithNoMergePolicy() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setIndexSort(
        new Sort(
            new SortField("field1", SortField.Type.LONG), // primary
            new SortField("field2", SortField.Type.LONG))); // secondary
    IndexWriter iw = new IndexWriter(dir, iwc);

    // Segment 0: field1 has two distinct values → effective primary is field1
    Document doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field1", 1));
    doc.add(NumericDocValuesField.indexedField("field2", 10));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field1", 2));
    doc.add(NumericDocValuesField.indexedField("field2", 20));
    iw.addDocument(doc);
    iw.commit();

    // Segment 1: field1 absent (no FieldInfo) → effective primary is field2
    doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field2", 30));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field2", 31));
    iw.addDocument(doc);
    iw.commit();

    // Segment 2: field1 is constant (skipper min == max) → no-op, effective primary is field2
    doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field1", 42));
    doc.add(NumericDocValuesField.indexedField("field2", 40));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(NumericDocValuesField.indexedField("field1", 42));
    doc.add(NumericDocValuesField.indexedField("field2", 50));
    iw.addDocument(doc);
    iw.commit();

    iw.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    assertEquals(3, reader.leaves().size());

    // Segment 0: field1 has multiple distinct values → effective primary is field1
    LeafReader leaf0 = reader.leaves().get(0).reader();
    SortField sf0 = Sort.getPrimarySortField(leaf0);
    assertNotNull(sf0);
    assertEquals("field1", sf0.getField());

    // Segment 1: field1 absent → effective primary is field2
    LeafReader leaf1 = reader.leaves().get(1).reader();
    SortField sf1 = Sort.getPrimarySortField(leaf1);
    assertNotNull(sf1);
    assertEquals("field2", sf1.getField());

    // Segment 2: field1 constant → no-op, effective primary is field2
    LeafReader leaf2 = reader.leaves().get(2).reader();
    SortField sf2 = Sort.getPrimarySortField(leaf2);
    assertNotNull(sf2);
    assertEquals("field2", sf2.getField());

    reader.close();
    dir.close();
  }

  /**
   * The reverse attribute of the effective primary sort field is correctly preserved when the
   * initial primary sort field is skipped as a no-op.
   */
  public void testSecondaryReverseAttributeIsPreserved() throws IOException {
    Directory dir = newDirectory();
    SortField primary = new SortField("field1", SortField.Type.LONG, false); // ascending
    SortField secondary = new SortField("field2", SortField.Type.LONG, true); // descending
    IndexWriter iw =
        new IndexWriter(dir, new IndexWriterConfig().setIndexSort(new Sort(primary, secondary)));
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("field1", 42)); // constant → no-op
      doc.add(NumericDocValuesField.indexedField("field2", i));
      iw.addDocument(doc);
    }
    iw.forceMerge(1);
    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    LeafReader leafReader = reader.leaves().get(0).reader();
    SortField result = Sort.getPrimarySortField(leafReader);
    assertNotNull(result);
    assertEquals("field2", result.getField());
    assertTrue("secondary sort field should be reversed", result.getReverse());
    reader.close();
    dir.close();
  }
}
