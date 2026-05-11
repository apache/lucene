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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests primary-sort FILTER optimization with {@link LongPoint#newRangeQuery} (standalone point
 * range, not wrapped in {@link IndexSortSortedNumericDocValuesRangeQuery}).
 *
 * <p>SimpleText is suppressed because its {@code PointTree} traversal does not support the BKD
 * in-order navigation used by {@code IndexSortSortedNumericDocValuesRangeQuery}, causing the
 * optimization to produce an empty range even when matches exist.
 */
@LuceneTestCase.SuppressCodecs("SimpleText")
public class TestPrimarySortFilterWithPointRange extends BasePrimarySortFilterTestCase {

  private long filterLo;
  private long filterHi;
  private long widerFilterLo;
  private long widerFilterHi;

  @Override
  public void setUp() throws Exception {
    super.setUp(); // sets numDocs
    filterLo = TestUtil.nextInt(random(), 1, numDocs / 2);
    filterHi = TestUtil.nextInt(random(), (int) filterLo + 1, numDocs - 2);
    widerFilterLo = TestUtil.nextInt(random(), 0, (int) filterLo);
    widerFilterHi = TestUtil.nextInt(random(), (int) filterHi, numDocs - 1);
  }

  @Override
  protected DensePrimarySortBulkChecks densePrimarySortBulkChecksOrNull() {
    return new DensePrimarySortBulkChecks(
        (int) filterLo, (int) filterHi + 1, (int) (filterHi - filterLo + 1));
  }

  @Override
  protected Sort buildIndexSort() {
    return new Sort(LongField.newSortField("sort", false, SortedNumericSelector.Type.MIN));
  }

  @Override
  protected void addDocument(IndexWriter writer, int i) throws IOException {
    Document doc = new Document();
    doc.add(new LongField("sort", i, org.apache.lucene.document.Field.Store.NO));
    writer.addDocument(doc);
  }

  @Override
  protected Query buildFilterQuery() {
    return LongPoint.newRangeQuery("sort", filterLo, filterHi);
  }

  @Override
  protected int expectedFilteredHitCount() {
    return (int) (filterHi - filterLo + 1);
  }

  @Override
  protected Query buildWiderFilterQuery() {
    return LongPoint.newRangeQuery("sort", widerFilterLo, widerFilterHi);
  }
}
