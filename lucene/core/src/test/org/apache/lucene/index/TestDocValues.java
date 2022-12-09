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
import org.apache.lucene.document.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** Tests helper methods in DocValues */
public class TestDocValues extends LuceneTestCase {

  /**
   * If the field doesn't exist, we return empty instances: it can easily happen that a segment just
   * doesn't have any docs with the field.
   */
  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    iw.addDocument(new Document());
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getBinary(r, "bogus"));
    assertNotNull(DocValues.getNumeric(r, "bogus"));
    assertNotNull(DocValues.getSorted(r, "bogus"));
    assertNotNull(DocValues.getSortedSet(r, "bogus"));
    assertNotNull(DocValues.getSortedNumeric(r, "bogus"));

    dr.close();
    iw.close();
    dir.close();
  }

  /** field just doesnt have any docvalues at all: exception */
  public void testMisconfiguredField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getBinary(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getNumeric(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSorted(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedSet(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedNumeric(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  /** field with numeric docvalues */
  public void testNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getBinary(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSorted(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedSet(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  /** field with binary docvalues */
  public void testBinaryField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getBinary(r, "foo"));

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getNumeric(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSorted(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedSet(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedNumeric(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  /** field with sorted docvalues */
  public void testSortedField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getSorted(r, "foo"));
    assertNotNull(DocValues.getSortedSet(r, "foo"));

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getBinary(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getNumeric(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedNumeric(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  /** field with sortedset docvalues */
  public void testSortedSetField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getSortedSet(r, "foo"));

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getBinary(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getNumeric(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSorted(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedNumeric(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  /** field with sortednumeric docvalues */
  public void testSortedNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    // ok
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    // errors
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getBinary(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getNumeric(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSorted(r, "foo");
        });
    expectThrows(
        IllegalStateException.class,
        () -> {
          DocValues.getSortedSet(r, "foo");
        });

    dr.close();
    iw.close();
    dir.close();
  }

  public void testAddNullNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    if (random().nextBoolean()) {
      doc.add(new NumericDocValuesField("foo", null));
    } else {
      doc.add(new BinaryDocValuesField("foo", null));
    }
    IllegalArgumentException iae =
        expectThrows(IllegalArgumentException.class, () -> iw.addDocument(doc));
    assertEquals("field=\"foo\": null value not allowed", iae.getMessage());
    IOUtils.close(iw, dir);
  }

  public void testIntField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new IntField("foo", 10, false));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testSortedIntField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new IntField("foo", 10, true));
    doc.add(new IntField("foo", 128, true));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getNumeric(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testLongField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new LongField("foo", 10, false));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testSortedLongField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new LongField("foo", 10, true));
    doc.add(new LongField("foo", 128, true));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getNumeric(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testFloatField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new FloatField("foo", 10.8f, false));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testMultiValuedFloatField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new FloatField("foo", 10.8f));
    doc.add(new FloatField("foo", 128.8f));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getNumeric(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testDoubleField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new DoubleField("foo", 10.8, false));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }

  public void testMultiValuedDoubleField() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new DoubleField("foo", 10.8));
    doc.add(new DoubleField("foo", 128.8));
    iw.addDocument(doc);

    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);

    assertNotNull(DocValues.getSortedNumeric(r, "foo"));

    expectThrows(IllegalStateException.class, () -> DocValues.getNumeric(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getBinary(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSorted(r, "foo"));
    expectThrows(IllegalStateException.class, () -> DocValues.getSortedSet(r, "foo"));

    IOUtils.close(dr, iw, dir);
  }
}
