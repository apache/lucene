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
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexWriter;

/** Tests primary-sort FILTER optimization with {@link LongField#newRangeQuery}. */
public class TestPrimarySortFilterWithLongRange extends BasePrimarySortFilterTestCase {

  @Override
  protected DensePrimarySortBulkChecks densePrimarySortBulkChecksOrNull() {
    return new DensePrimarySortBulkChecks(40, 60, 20, 20L);
  }

  @Override
  protected Sort buildIndexSort() {
    return new Sort(
        LongField.newSortField("sort", false, SortedNumericSelector.Type.MIN));
  }

  @Override
  protected void addDocument(IndexWriter writer, int i) throws IOException {
    Document doc = new Document();
    doc.add(new LongField("sort", i, org.apache.lucene.document.Field.Store.NO));
    writer.addDocument(doc);
  }

  @Override
  protected Query buildFilterQuery() {
    return LongField.newRangeQuery("sort", 40, 59);
  }

  @Override
  protected int expectedFilteredHitCount() {
    return 20;
  }

  @Override
  protected Query buildWiderFilterQuery() {
    return LongField.newRangeQuery("sort", 20, 79);
  }
}