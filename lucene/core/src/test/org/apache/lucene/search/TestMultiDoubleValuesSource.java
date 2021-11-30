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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;

public class TestMultiDoubleValuesSource extends MultiValuesSourceTestCase {

  public void testRandom() throws Exception {
    MultiDoubleValuesSource valuesSource;

    valuesSource = MultiDoubleValuesSource.fromIntField("single_int");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_int");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiDoubleValuesSource.fromLongField("single_long");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_long");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiDoubleValuesSource.fromIntField("multi_int");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_int");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiDoubleValuesSource.fromLongField("multi_long");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_long");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc());
    }

    valuesSource = MultiDoubleValuesSource.fromFloatField("single_float");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_float");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), false);
    }

    valuesSource = MultiDoubleValuesSource.fromDoubleField("single_double");
    assertNotNull(valuesSource);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_double");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), true);
    }

    valuesSource = MultiDoubleValuesSource.fromFloatField("multi_float");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_float");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), false);
    }

    valuesSource = MultiDoubleValuesSource.fromDoubleField("multi_double");
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "multi_double");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), true);
    }
  }

  public void testFromSingleValued() throws Exception {
    MultiDoubleValuesSource valuesSource;
    DoubleValuesSource singleton;

    valuesSource =
        MultiDoubleValuesSource.fromSingleValued(DoubleValuesSource.fromFloatField("single_float"));
    singleton = MultiDoubleValuesSource.unwrapSingleton(valuesSource);
    assertNotNull(singleton);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_float");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), false);

      NumericDocValues singletonDv = DocValues.getNumeric(ctx.reader(), "single_float");
      DoubleValues singletonVals = singleton.getValues(ctx, null);
      validateFieldBasedSource(singletonDv, singletonVals, ctx.reader().maxDoc(), false);
    }

    valuesSource =
        MultiDoubleValuesSource.fromSingleValued(
            DoubleValuesSource.fromDoubleField("single_double"));
    singleton = MultiDoubleValuesSource.unwrapSingleton(valuesSource);
    assertNotNull(singleton);
    for (LeafReaderContext ctx : reader.leaves()) {
      SortedNumericDocValues docValues = DocValues.getSortedNumeric(ctx.reader(), "single_double");
      MultiDoubleValues values = valuesSource.getValues(ctx, null);
      validateFieldBasedSource(docValues, values, ctx.reader().maxDoc(), true);

      NumericDocValues singletonDv = DocValues.getNumeric(ctx.reader(), "single_double");
      DoubleValues singletonVals = singleton.getValues(ctx, null);
      validateFieldBasedSource(singletonDv, singletonVals, ctx.reader().maxDoc(), true);
    }
  }

  public void testNoScoreNeed() throws Exception {
    MultiDoubleValuesSource valuesSource = MultiDoubleValuesSource.fromDoubleField("multi_double");
    // field-backed instances shouldn't need scores:
    assertFalse(valuesSource.needsScores());
  }

  public void testRewriteSame() throws Exception {
    MultiDoubleValuesSource valuesSource = MultiDoubleValuesSource.fromDoubleField("multi_double");
    MultiDoubleValuesSource rewritten = valuesSource.rewrite(searcher);
    // field-backed instances shouldn't do anything interesting when rewritten:
    assertSame(valuesSource, rewritten);
  }

  public void testRewriteDifferent() throws Exception {
    DoubleValuesSource rewritingSingleton =
        new TestMultiDoubleValuesSource.RewritingDoubleValuesSource();
    MultiDoubleValuesSource valuesSource =
        MultiDoubleValuesSource.fromSingleValued(rewritingSingleton);
    MultiDoubleValuesSource rewritten = valuesSource.rewrite(searcher);
    assertNotSame(valuesSource, rewritten);

    DoubleValuesSource unwrappedOriginal = MultiDoubleValuesSource.unwrapSingleton(valuesSource);
    DoubleValuesSource unwrappedRewritten = MultiDoubleValuesSource.unwrapSingleton(rewritten);
    assertNotSame(unwrappedOriginal, unwrappedRewritten);
  }

  public void testCacheable() throws Exception {
    MultiDoubleValuesSource valuesSource = MultiDoubleValuesSource.fromDoubleField("multi_double");
    for (LeafReaderContext ctx : searcher.leafContexts) {
      assertEquals(DocValues.isCacheable(ctx, "multi_double"), valuesSource.isCacheable(ctx));
    }
  }

  public void testEqualsAndHashcode() throws Exception {
    MultiDoubleValuesSource valuesSource1 = MultiDoubleValuesSource.fromLongField("multi_long");
    MultiDoubleValuesSource valuesSource2 = MultiDoubleValuesSource.fromLongField("multi_long");
    MultiDoubleValuesSource valuesSource3 = MultiDoubleValuesSource.fromLongField("multi_int");
    assertEquals(valuesSource1, valuesSource2);
    assertNotEquals(valuesSource1, valuesSource3);
    assertEquals(valuesSource1.hashCode(), valuesSource2.hashCode());
    assertNotEquals(valuesSource1.hashCode(), valuesSource3.hashCode());

    valuesSource1 =
        MultiDoubleValuesSource.fromSingleValued(DoubleValuesSource.fromLongField("single_long"));
    valuesSource2 =
        MultiDoubleValuesSource.fromSingleValued(DoubleValuesSource.fromLongField("single_long"));
    valuesSource3 =
        MultiDoubleValuesSource.fromSingleValued(DoubleValuesSource.fromLongField("single_int"));
    assertEquals(valuesSource1, valuesSource2);
    assertNotEquals(valuesSource1, valuesSource3);
    assertEquals(valuesSource1.hashCode(), valuesSource2.hashCode());
    assertNotEquals(valuesSource1.hashCode(), valuesSource3.hashCode());

    DoubleValuesSource singleton1 = MultiDoubleValuesSource.unwrapSingleton(valuesSource1);
    DoubleValuesSource singleton2 = MultiDoubleValuesSource.unwrapSingleton(valuesSource2);
    DoubleValuesSource singleton3 = MultiDoubleValuesSource.unwrapSingleton(valuesSource3);
    assertEquals(singleton1, singleton2);
    assertNotEquals(singleton1, singleton3);
    assertEquals(singleton1.hashCode(), singleton2.hashCode());
    assertNotEquals(singleton1.hashCode(), singleton3.hashCode());

    valuesSource1 = MultiDoubleValuesSource.fromField("single_long", Long::valueOf);
    valuesSource2 = MultiDoubleValuesSource.fromField("single_long", v -> -1 * v);
    valuesSource3 = MultiDoubleValuesSource.fromField("single_int", Long::valueOf);
    assertNotEquals(valuesSource1, valuesSource2);
    assertNotEquals(valuesSource1, valuesSource3);
    assertNotEquals(valuesSource1.hashCode(), valuesSource2.hashCode());
    assertNotEquals(valuesSource1.hashCode(), valuesSource3.hashCode());
  }

  private static class RewritingDoubleValuesSource extends DoubleValuesSource {

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      return null;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public String toString() {
      return null;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
      return new RewritingDoubleValuesSource();
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }
}
