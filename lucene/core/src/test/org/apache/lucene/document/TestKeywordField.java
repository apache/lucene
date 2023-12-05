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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestKeywordField extends LuceneTestCase {

  public void testSetBytesValue() {
    Field[] fields =
        new Field[] {
          new KeywordField("name", newBytesRef("value"), Field.Store.NO),
          new KeywordField("name", newBytesRef("value"), Field.Store.YES)
        };
    for (Field field : fields) {
      assertEquals(newBytesRef("value"), field.binaryValue());
      assertNull(field.stringValue());
      if (field.fieldType().stored()) {
        assertEquals(newBytesRef("value"), field.storedValue().getBinaryValue());
      } else {
        assertNull(field.storedValue());
      }
      assertThrows(
          IllegalArgumentException.class, () -> field.setBytesValue(newBytesRef("value2")));
      assertEquals(newBytesRef("value"), field.binaryValue());
      assertNull(field.stringValue());
      if (field.fieldType().stored()) {
        assertEquals(newBytesRef("value"), field.storedValue().getBinaryValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testSetStringValue() {
    Field[] fields =
        new Field[] {
          new KeywordField("name", "value", Field.Store.NO),
          new KeywordField("name", "value", Field.Store.YES)
        };
    for (Field field : fields) {
      assertEquals("value", field.stringValue());
      assertEquals(newBytesRef("value"), field.binaryValue());
      if (field.fieldType().stored()) {
        assertEquals("value", field.storedValue().getStringValue());
      } else {
        assertNull(field.storedValue());
      }
      field.setStringValue("value2");
      assertEquals("value2", field.stringValue());
      assertEquals(newBytesRef("value2"), field.binaryValue());
      if (field.fieldType().stored()) {
        assertEquals("value2", field.storedValue().getStringValue());
      } else {
        assertNull(field.storedValue());
      }
    }
  }

  public void testIndexBytesValue() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(
        Collections.singleton(new KeywordField("field", newBytesRef("value"), Field.Store.YES)));
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leaf = getOnlyLeafReader(reader);
    TermsEnum terms = leaf.terms("field").iterator();
    assertEquals(new BytesRef("value"), terms.next());
    assertNull(terms.next());
    SortedSetDocValues values = leaf.getSortedSetDocValues("field");
    assertTrue(values.advanceExact(0));
    assertEquals(1, values.docValueCount());
    assertEquals(0L, values.nextOrd());
    assertEquals(new BytesRef("value"), values.lookupOrd(0));
    Document storedDoc = leaf.storedFields().document(0);
    assertEquals(new BytesRef("value"), storedDoc.getBinaryValue("field"));
    reader.close();
    dir.close();
  }

  public void testIndexStringValue() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(Collections.singleton(new KeywordField("field", "value", Field.Store.YES)));
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    LeafReader leaf = getOnlyLeafReader(reader);
    TermsEnum terms = leaf.terms("field").iterator();
    assertEquals(new BytesRef("value"), terms.next());
    assertNull(terms.next());
    SortedSetDocValues values = leaf.getSortedSetDocValues("field");
    assertTrue(values.advanceExact(0));
    assertEquals(1, values.docValueCount());
    assertEquals(0L, values.nextOrd());
    assertEquals(new BytesRef("value"), values.lookupOrd(0));
    Document storedDoc = leaf.storedFields().document(0);
    assertEquals("value", storedDoc.get("field"));
    reader.close();
    dir.close();
  }

  public void testValueClone() {
    List<BytesRef> values = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      String s = TestUtil.randomSimpleString(random(), 10, 20);
      values.add(new BytesRef(s));
    }

    // Make sure we don't modify the input values array.
    List<BytesRef> expected = new ArrayList<>(values);
    KeywordField.newSetQuery("f", values);
    assertEquals(expected, values);
  }
}
