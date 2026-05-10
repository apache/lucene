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
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;

/**
 * Tests primary-sort FILTER optimization with {@link SortedSetDocValuesField#newSlowRangeQuery} on
 * a primary {@link SortedSetSortField}.
 */
public class TestPrimarySortFilterWithSlowSortedSetRange extends BasePrimarySortFilterTestCase {

  @Override
  protected Sort buildIndexSort() {
    return new Sort(
        KeywordField.newSortField(
            "category", false, SortedSetSelector.Type.MIN, SortField.STRING_LAST));
  }

  @Override
  protected void addDocument(IndexWriter writer, int i) throws IOException {
    Document doc = new Document();
    String category;
    if (i < 30) {
      category = "articles";
    } else if (i < 50) {
      category = "books";
    } else {
      category = "music";
    }
    doc.add(new KeywordField("category", category, org.apache.lucene.document.Field.Store.NO));
    writer.addDocument(doc);
  }

  @Override
  protected Query buildFilterQuery() {
    // Range [articles, books] inclusive — matches docs 0..49
    return SortedSetDocValuesField.newSlowRangeQuery(
        "category", new BytesRef("articles"), new BytesRef("books"), true, true);
  }

  @Override
  protected int expectedFilteredHitCount() {
    return 50;
  }
}
