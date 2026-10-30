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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestNonPruningSortField extends LuceneTestCase {

  public void testEqualsHashCodeToString() {
    SortField plain = new SortField("f", SortField.Type.LONG, true);
    SortField wrapped = SortField.withoutPruning(plain);
    assertEquals(wrapped, SortField.withoutPruning(new SortField("f", SortField.Type.LONG, true)));
    assertEquals(wrapped.hashCode(), SortField.withoutPruning(plain).hashCode());
    assertNotEquals(wrapped, plain);
    assertNotEquals(plain, wrapped);
    assertSame(wrapped, SortField.withoutPruning(wrapped));
    assertTrue(wrapped.toString().contains("no pruning"));
    assertEquals("f", wrapped.getField());
    assertEquals(SortField.Type.LONG, wrapped.getType());
    assertTrue(wrapped.getReverse());
  }

  public void testCannotBeIndexSort() throws IOException {
    SortField plain = new SortField("f", SortField.Type.LONG);
    SortField wrapped = SortField.withoutPruning(plain);
    assertNotNull(plain.getIndexSorter());
    expectThrows(UnsupportedOperationException.class, wrapped::getIndexSorter);
    expectThrows(
        UnsupportedOperationException.class,
        () -> new IndexWriterConfig().setIndexSort(new Sort(wrapped)));
  }

  public void testRewrite() throws IOException {
    SortField plain = new SortField("f", SortField.Type.LONG);
    SortField wrapped = SortField.withoutPruning(plain);
    assertSame(wrapped, wrapped.rewrite(null));
  }

  public void testSortsSameAsDelegate() throws IOException {
    Directory dir = newDirectory();
    try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, new IndexWriterConfig())) {
      for (int i = 0; i < 200; i++) {
        Document doc = new Document();
        doc.add(NumericDocValuesField.indexedField("f", random().nextInt(50)));
        w.addDocument(doc);
      }
    }
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      SortField plain = new SortField("f", SortField.Type.LONG);
      TopDocs expected = searcher.search(new MatchAllDocsQuery(), 10, new Sort(plain));
      TopDocs actual =
          searcher.search(new MatchAllDocsQuery(), 10, new Sort(SortField.withoutPruning(plain)));
      assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
      for (int i = 0; i < expected.scoreDocs.length; i++) {
        assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
      }
      // merging per-shard results works with the wrapper
      TopFieldDocs shard =
          searcher.search(new MatchAllDocsQuery(), 10, new Sort(SortField.withoutPruning(plain)));
      TopDocs merged =
          TopDocs.merge(new Sort(SortField.withoutPruning(plain)), 10, new TopFieldDocs[] {shard});
      assertEquals(10, merged.scoreDocs.length);
    }
    IOUtils.close(dir);
  }
}
