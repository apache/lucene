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
package org.apache.lucene.codecs.lucene90;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;

public class TestLucene90StoredFieldsFormatBestSpeed extends BaseStoredFieldsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return new Lucene94Codec(Lucene94Codec.Mode.BEST_SPEED);
  }

  public void testBestSpeedSkipDecompression() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(new Lucene94Codec(Lucene94Codec.Mode.BEST_SPEED));
    IndexWriter iw = new IndexWriter(dir, iwc);

    final int numDocs = atLeast(100);
    StringBuilder longValueBuilder = new StringBuilder();
    String shortValue = "value";
    longValueBuilder.append(shortValue.repeat(20 * 1024));
    String longValue = longValueBuilder.toString();

    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(new StoredField("field1", "value1_1"));
      doc.add(new StoredField("field2", longValue));
      doc.add(new StoredField("field1", "value1_2"));
      doc.add(new StoredField("field3", "value3"));
      iw.addDocument(doc);
    }

    iw.commit();
    iw.close();

    DirectoryReader ir = DirectoryReader.open(dir);
    assertEquals(numDocs, ir.numDocs());

    {
      Set<String> fields = new HashSet<>();
      fields.add("field1");
      for (int i = 0; i < numDocs; i++) {
        Document doc = ir.document(i, fields);
        assertEquals(2, doc.getFields("field1").length);
        assertEquals("value1_1", doc.getFields("field1")[0].stringValue());
        assertEquals("value1_2", doc.getFields("field1")[1].stringValue());
        assertEquals(null, doc.get("field2"));
        assertEquals(null, doc.get("field3"));
      }
    }

    {
      Set<String> fields = new HashSet<>();
      fields.add("field1");
      fields.add("field2");
      for (int i = 0; i < numDocs; i++) {
        Document doc = ir.document(i, fields);
        assertEquals(2, doc.getValues("field1").length);
        assertEquals("value1_1", doc.getFields("field1")[0].stringValue());
        assertEquals("value1_2", doc.getFields("field1")[1].stringValue());
        assertEquals(longValue, doc.get("field2"));
        assertEquals(null, doc.get("field3"));
      }
    }

    {
      Set<String> fields = new HashSet<>();
      fields.add("field1");
      fields.add("field3");
      for (int i = 0; i < numDocs; i++) {
        Document doc = ir.document(i, fields);
        assertEquals(2, doc.getValues("field1").length);
        assertEquals("value1_1", doc.getFields("field1")[0].stringValue());
        assertEquals("value1_2", doc.getFields("field1")[1].stringValue());
        assertEquals(null, doc.get("field2"));
        assertEquals("value3", doc.get("field3"));
      }
    }

    {
      Set<String> fields = new HashSet<>();
      fields.add("field3");
      for (int i = 0; i < numDocs; i++) {
        Document doc = ir.document(i, fields);
        assertEquals(0, doc.getValues("field1").length);
        assertEquals(null, doc.get("field2"));
        assertEquals("value3", doc.get("field3"));
      }
    }
    ir.close();
    // checkindex
    dir.close();
  }
}
