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
package org.apache.lucene.document;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

// sanity check some basics of fields
public class TestField extends LuceneTestCase {

  public void testDoublePoint() throws Exception {
    Field field = new DoublePoint("foo", 5d);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    field.setDoubleValue(6d); // ok
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6d, field.numericValue().doubleValue(), 0.0d);
    assertEquals("DoublePoint <foo:6.0>", field.toString());
  }

  public void testDoublePoint2D() throws Exception {
    DoublePoint field = new DoublePoint("foo", 5d, 4d);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    field.setDoubleValues(6d, 7d); // ok
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    IllegalStateException expected =
        expectThrows(
            IllegalStateException.class,
            () -> {
              field.numericValue();
            });
    assertTrue(expected.getMessage().contains("cannot convert to a single numeric value"));
    assertEquals("DoublePoint <foo:6.0,7.0>", field.toString());
  }

  public void testDoubleDocValuesField() throws Exception {
    DoubleDocValuesField field = new DoubleDocValuesField("foo", 5d);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    field.setDoubleValue(6d); // ok
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6d, Double.longBitsToDouble(field.numericValue().longValue()), 0.0d);
  }

  public void testFloatDocValuesField() throws Exception {
    FloatDocValuesField field = new FloatDocValuesField("foo", 5f);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setFloatValue(6f); // ok
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6f, Float.intBitsToFloat(field.numericValue().intValue()), 0.0f);
  }

  public void testFloatPoint() throws Exception {
    Field field = new FloatPoint("foo", 5f);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setFloatValue(6f); // ok
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6f, field.numericValue().floatValue(), 0.0f);
    assertEquals("FloatPoint <foo:6.0>", field.toString());
  }

  public void testFloatPoint2D() throws Exception {
    FloatPoint field = new FloatPoint("foo", 5f, 4f);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setFloatValues(6f, 7f); // ok
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    IllegalStateException expected =
        expectThrows(
            IllegalStateException.class,
            () -> {
              field.numericValue();
            });
    assertTrue(expected.getMessage().contains("cannot convert to a single numeric value"));
    assertEquals("FloatPoint <foo:6.0,7.0>", field.toString());
  }

  public void testIntPoint() throws Exception {
    Field field = new IntPoint("foo", 5);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    field.setIntValue(6); // ok
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6, field.numericValue().intValue());
    assertEquals("IntPoint <foo:6>", field.toString());
  }

  public void testIntPoint2D() throws Exception {
    IntPoint field = new IntPoint("foo", 5, 4);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setIntValues(6, 7); // ok
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    IllegalStateException expected =
        expectThrows(
            IllegalStateException.class,
            () -> {
              field.numericValue();
            });
    assertTrue(expected.getMessage().contains("cannot convert to a single numeric value"));
    assertEquals("IntPoint <foo:6,7>", field.toString());
  }

  public void testIntField() throws Exception {
    Field[] fields =
        new Field[] {
          new IntField("foo", 12, Field.Store.NO), new IntField("foo", 12, Field.Store.YES),
        };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      field.setIntValue(6);
      trySetLongValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);

      assertEquals(6, field.numericValue().intValue());
      assertEquals(6, NumericUtils.sortableBytesToInt(field.binaryValue().bytes, 0));
      assertEquals("IntField <foo:6>", field.toString());
      if (field.fieldType().stored()) {
        assertEquals(6, field.storedValue().getIntValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testLongField() throws Exception {
    Field[] fields =
        new Field[] {
          new LongField("foo", 12, Field.Store.NO), new LongField("foo", 12, Field.Store.YES),
        };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      field.setLongValue(6);
      trySetFloatValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);

      assertEquals(6L, field.numericValue().longValue());
      assertEquals(6L, NumericUtils.sortableBytesToLong(field.binaryValue().bytes, 0));
      assertEquals("LongField <foo:6>", field.toString());
      if (field.fieldType().stored()) {
        assertEquals(6, field.storedValue().getLongValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testFloatField() throws Exception {
    Field[] fields =
        new Field[] {
          new FloatField("foo", 12.6f, Field.Store.NO),
          new FloatField("foo", 12.6f, Field.Store.YES),
        };

    for (Field field : fields) {
      assertEquals(12.6f, NumericUtils.sortableIntToFloat(field.numericValue().intValue()), 0.0f);
      assertEquals(12.6f, FloatPoint.decodeDimension(field.binaryValue().bytes, 0), 0.0f);
      assertEquals("FloatField <foo:12.6>", field.toString());

      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetLongValue(field);
      field.setFloatValue(-28.8f);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);

      assertEquals(-28.8f, NumericUtils.sortableIntToFloat(field.numericValue().intValue()), 0.0f);
      assertEquals(-28.8f, FloatPoint.decodeDimension(field.binaryValue().bytes, 0), 0.0f);
      assertEquals("FloatField <foo:-28.8>", field.toString());
      if (field.fieldType().stored()) {
        assertEquals(-28.8f, field.storedValue().getFloatValue(), 0f);
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testDoubleField() throws Exception {
    Field[] fields =
        new Field[] {
          new DoubleField("foo", 12.7, Field.Store.NO),
          new DoubleField("foo", 12.7, Field.Store.YES),
        };

    for (Field field : fields) {
      assertEquals(12.7, NumericUtils.sortableLongToDouble(field.numericValue().longValue()), 0.0f);
      assertEquals(12.7, DoublePoint.decodeDimension(field.binaryValue().bytes, 0), 0.0f);
      assertEquals("DoubleField <foo:12.7>", field.toString());

      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetIntValue(field);
      trySetLongValue(field);
      trySetFloatValue(field);
      field.setDoubleValue(-28.8);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);

      assertEquals(
          -28.8, NumericUtils.sortableLongToDouble(field.numericValue().longValue()), 0.0f);
      assertEquals(-28.8, DoublePoint.decodeDimension(field.binaryValue().bytes, 0), 0.0f);
      assertEquals("DoubleField <foo:-28.8>", field.toString());
      if (field.fieldType().stored()) {
        assertEquals(-28.8, field.storedValue().getDoubleValue(), 0d);
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testNumericDocValuesField() throws Exception {
    NumericDocValuesField field = new NumericDocValuesField("foo", 5L);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setLongValue(6); // ok
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6L, field.numericValue().longValue());
  }

  public void testLongPoint() throws Exception {
    Field field = new LongPoint("foo", 5);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setLongValue(6); // ok
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(6, field.numericValue().intValue());
    assertEquals("LongPoint <foo:6>", field.toString());
  }

  public void testLongPoint2D() throws Exception {
    LongPoint field = new LongPoint("foo", 5, 4);

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    field.setLongValues(6, 7); // ok
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    IllegalStateException expected =
        expectThrows(
            IllegalStateException.class,
            () -> {
              field.numericValue();
            });
    assertTrue(expected.getMessage().contains("cannot convert to a single numeric value"));
    assertEquals("LongPoint <foo:6,7>", field.toString());
  }

  public void testSortedBytesDocValuesField() throws Exception {
    SortedDocValuesField field = new SortedDocValuesField("foo", newBytesRef("bar"));

    trySetByteValue(field);
    field.setBytesValue("fubar".getBytes(StandardCharsets.UTF_8));
    field.setBytesValue(newBytesRef("baz"));
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(newBytesRef("baz"), field.binaryValue());
  }

  public void testBinaryDocValuesField() throws Exception {
    BinaryDocValuesField field = new BinaryDocValuesField("foo", newBytesRef("bar"));

    trySetByteValue(field);
    field.setBytesValue("fubar".getBytes(StandardCharsets.UTF_8));
    field.setBytesValue(newBytesRef("baz"));
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(newBytesRef("baz"), field.binaryValue());
  }

  public void testStringField() throws Exception {
    Field[] fields =
        new Field[] {
          new StringField("foo", "bar", Field.Store.NO),
          new StringField("foo", "bar", Field.Store.YES)
        };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      field.setStringValue("baz");
      trySetTokenStreamValue(field);

      assertEquals("baz", field.stringValue());
      if (field.fieldType().stored()) {
        assertEquals("baz", field.storedValue().getStringValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testTextFieldString() throws Exception {
    Field[] fields =
        new Field[] {
          new TextField("foo", "bar", Field.Store.NO), new TextField("foo", "bar", Field.Store.YES)
        };

    for (Field field : fields) {
      trySetByteValue(field);
      trySetBytesValue(field);
      trySetBytesRefValue(field);
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      field.setStringValue("baz");
      field.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));

      assertEquals("baz", field.stringValue());
      if (field.fieldType().stored()) {
        assertEquals("baz", field.storedValue().getStringValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testTextFieldReader() throws Exception {
    Field field = new TextField("foo", new StringReader("bar"));

    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    field.setReaderValue(new StringReader("foobar"));
    trySetShortValue(field);
    trySetStringValue(field);
    field.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));

    assertNotNull(field.readerValue());
    assertNull(field.storedValue());
  }

  /* TODO: this is pretty expert and crazy
   * see if we can fix it up later
  public void testTextFieldTokenStream() throws Exception {
  }
  */

  public void testStoredFieldBytes() throws Exception {
    Field[] fields =
        new Field[] {
          new StoredField("foo", "bar".getBytes(StandardCharsets.UTF_8)),
          new StoredField("foo", "bar".getBytes(StandardCharsets.UTF_8), 0, 3),
          new StoredField("foo", newBytesRef("bar")),
        };

    for (Field field : fields) {
      trySetByteValue(field);
      field.setBytesValue("baz".getBytes(StandardCharsets.UTF_8));
      field.setBytesValue(newBytesRef("baz"));
      trySetDoubleValue(field);
      trySetIntValue(field);
      trySetFloatValue(field);
      trySetLongValue(field);
      trySetReaderValue(field);
      trySetShortValue(field);
      trySetStringValue(field);
      trySetTokenStreamValue(field);

      assertEquals(newBytesRef("baz"), field.binaryValue());
    }
  }

  public void testStoredFieldString() throws Exception {
    Field field = new StoredField("foo", "bar");
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    field.setStringValue("baz");
    trySetTokenStreamValue(field);

    assertEquals("baz", field.stringValue());
  }

  public void testStoredFieldInt() throws Exception {
    Field field = new StoredField("foo", 1);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    field.setIntValue(5);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(5, field.numericValue().intValue());
  }

  public void testStoredFieldDouble() throws Exception {
    Field field = new StoredField("foo", 1D);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    field.setDoubleValue(5D);
    trySetIntValue(field);
    trySetFloatValue(field);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(5D, field.numericValue().doubleValue(), 0.0D);
  }

  public void testStoredFieldFloat() throws Exception {
    Field field = new StoredField("foo", 1F);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    field.setFloatValue(5f);
    trySetLongValue(field);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(5f, field.numericValue().floatValue(), 0.0f);
  }

  public void testStoredFieldLong() throws Exception {
    Field field = new StoredField("foo", 1L);
    trySetByteValue(field);
    trySetBytesValue(field);
    trySetBytesRefValue(field);
    trySetDoubleValue(field);
    trySetIntValue(field);
    trySetFloatValue(field);
    field.setLongValue(5);
    trySetReaderValue(field);
    trySetShortValue(field);
    trySetStringValue(field);
    trySetTokenStreamValue(field);

    assertEquals(5L, field.numericValue().longValue());
  }

  public void testIndexedBinaryField() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    BytesRef br = newBytesRef(new byte[5]);
    Field field = new StringField("binary", br, Field.Store.YES);
    assertEquals(br, field.binaryValue());
    doc.add(field);
    w.addDocument(doc);
    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new TermQuery(new Term("binary", br)), 1);
    assertEquals(1, hits.totalHits.value);
    Document storedDoc = s.storedFields().document(hits.scoreDocs[0].doc);
    assertEquals(br, storedDoc.getField("binary").binaryValue());

    r.close();
    w.close();
    dir.close();
  }

  public void testKnnVectorField() throws Exception {
    if (Codec.getDefault().getName().equals("SimpleText")) {
      return;
    }
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      byte[] b = new byte[5];
      KnnByteVectorField field =
          new KnnByteVectorField("binary", b, VectorSimilarityFunction.EUCLIDEAN);
      assertNull(field.binaryValue());
      assertArrayEquals(b, field.vectorValue());
      expectThrows(
          IllegalArgumentException.class,
          () -> new KnnFloatVectorField("bogus", new float[] {1}, (FieldType) field.fieldType()));
      float[] vector = new float[] {1, 2};
      Field field2 = new KnnFloatVectorField("float", vector);
      assertNull(field2.binaryValue());
      doc.add(field);
      doc.add(field2);
      w.addDocument(doc);
      try (IndexReader r = DirectoryReader.open(w)) {
        ByteVectorValues binary = r.leaves().get(0).reader().getByteVectorValues("binary");
        assertEquals(1, binary.size());
        assertNotEquals(NO_MORE_DOCS, binary.nextDoc());
        assertNotNull(binary.vectorValue());
        assertArrayEquals(b, binary.vectorValue());
        assertEquals(NO_MORE_DOCS, binary.nextDoc());

        FloatVectorValues floatValues = r.leaves().get(0).reader().getFloatVectorValues("float");
        assertEquals(1, floatValues.size());
        assertNotEquals(NO_MORE_DOCS, floatValues.nextDoc());
        assertEquals(vector.length, floatValues.vectorValue().length);
        assertEquals(vector[0], floatValues.vectorValue()[0], 0);
        assertEquals(NO_MORE_DOCS, floatValues.nextDoc());
      }
    }
  }

  private void trySetByteValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setByteValue((byte) 10);
        });
  }

  private void trySetBytesValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setBytesValue(new byte[] {5, 5});
        });
  }

  private void trySetBytesRefValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setBytesValue(newBytesRef("bogus"));
        });
  }

  private void trySetDoubleValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setDoubleValue(Double.MAX_VALUE);
        });
  }

  private void trySetIntValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setIntValue(Integer.MAX_VALUE);
        });
  }

  private void trySetLongValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setLongValue(Long.MAX_VALUE);
        });
  }

  private void trySetFloatValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setFloatValue(Float.MAX_VALUE);
        });
  }

  private void trySetReaderValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setReaderValue(new StringReader("BOO!"));
        });
  }

  private void trySetShortValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setShortValue(Short.MAX_VALUE);
        });
  }

  private void trySetStringValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setStringValue("BOO!");
        });
  }

  private void trySetTokenStreamValue(Field f) {
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          f.setTokenStream(new CannedTokenStream(new Token("foo", 0, 3)));
        });
  }
}
