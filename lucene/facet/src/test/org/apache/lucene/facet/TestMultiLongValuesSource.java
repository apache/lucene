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
package org.apache.lucene.facet;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

public class TestMultiLongValuesSource extends MultiValuesSourceTestCase {

  public void testRandom() throws Exception {
    MultiLongValuesSource valuesSource;

    valuesSource = MultiLongValuesSource.fromIntField("single_int");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_int");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiLongValuesSource.fromLongField("single_long");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_long");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiLongValuesSource.fromIntField("multi_int");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_int");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiLongValuesSource.fromLongField("multi_long");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_long");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }
  }

  public void testFromSingleValued() throws Exception {
    MultiLongValuesSource valuesSource;
    LongValuesSource singleton;

    valuesSource =
        MultiLongValuesSource.fromSingleValued(LongValuesSource.fromIntField("single_int"));
    singleton = MultiLongValuesSource.unwrapSingleton(valuesSource);
    assertNotNull(singleton);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_int");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());

      NumericDocValues singletonDv = DocValues.getNumeric(ctx.reader(), "single_int");
      LongValues singletonVals = singleton.getValues(ctx, null);
      validateFieldBasedSource(singletonDv, singletonVals, ctx.reader().maxDoc());
    }

    valuesSource =
        MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("single_long"));
    singleton = MultiLongValuesSource.unwrapSingleton(valuesSource);
    assertNotNull(singleton);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_long");
      MultiLongValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());

      NumericDocValues singletonDv = DocValues.getNumeric(ctx.reader(), "single_long");
      LongValues singletonVals = singleton.getValues(ctx, null);
      validateFieldBasedSource(singletonDv, singletonVals, ctx.reader().maxDoc());
    }
  }

  public void testToDouble() throws Exception {
    MultiDoubleValuesSource valuesSource =
        MultiLongValuesSource.fromLongField("multi_long").toMultiDoubleValuesSource();
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_long");
      MultiDoubleValues values = valuesSource.getValues(ctx);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }
  }

  public void testCacheable() {
    MultiLongValuesSource valuesSource = MultiLongValuesSource.fromLongField("multi_long");
    for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
      assertEquals(DocValues.isCacheable(ctx, "multi_long"), valuesSource.isCacheable(ctx));
    }
  }

  public void testEqualsAndHashcode() {
    MultiLongValuesSource valuesSource1 = MultiLongValuesSource.fromLongField("multi_long");
    MultiLongValuesSource valuesSource2 = MultiLongValuesSource.fromLongField("multi_long");
    MultiLongValuesSource valuesSource3 = MultiLongValuesSource.fromLongField("multi_int");
    assertEquals(valuesSource1, valuesSource2);
    assertNotEquals(valuesSource1, valuesSource3);
    assertEquals(valuesSource1.hashCode(), valuesSource2.hashCode());
    assertNotEquals(valuesSource1.hashCode(), valuesSource3.hashCode());

    valuesSource1 =
        MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("single_long"));
    valuesSource2 =
        MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("single_long"));
    valuesSource3 =
        MultiLongValuesSource.fromSingleValued(LongValuesSource.fromLongField("single_int"));
    assertEquals(valuesSource1, valuesSource2);
    assertNotEquals(valuesSource1, valuesSource3);
    assertEquals(valuesSource1.hashCode(), valuesSource2.hashCode());
    assertNotEquals(valuesSource1.hashCode(), valuesSource3.hashCode());

    LongValuesSource singleton1 = MultiLongValuesSource.unwrapSingleton(valuesSource1);
    LongValuesSource singleton2 = MultiLongValuesSource.unwrapSingleton(valuesSource2);
    LongValuesSource singleton3 = MultiLongValuesSource.unwrapSingleton(valuesSource3);
    assertEquals(singleton1, singleton2);
    assertNotEquals(singleton1, singleton3);
    assertEquals(singleton1.hashCode(), singleton2.hashCode());
    assertNotEquals(singleton1.hashCode(), singleton3.hashCode());

    MultiDoubleValuesSource doubleValuesSource1 = valuesSource1.toMultiDoubleValuesSource();
    MultiDoubleValuesSource doubleValuesSource2 = valuesSource2.toMultiDoubleValuesSource();
    MultiDoubleValuesSource doubleValuesSource3 = valuesSource3.toMultiDoubleValuesSource();
    assertEquals(doubleValuesSource1, doubleValuesSource2);
    assertNotEquals(doubleValuesSource1, doubleValuesSource3);
    assertEquals(doubleValuesSource1.hashCode(), doubleValuesSource2.hashCode());
    assertNotEquals(doubleValuesSource1.hashCode(), doubleValuesSource3.hashCode());
  }
}
