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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public abstract class MultiValuesSourceTestCase extends LuceneTestCase {
  protected Directory dir;
  protected IndexReader reader;
  protected IndexSearcher searcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    int numDocs = RandomNumbers.randomIntBetween(random(), 100, 1000);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();

      if (random().nextInt(10) < 8) {
        doc.add(new NumericDocValuesField("single_int", random().nextInt()));
      }
      if (random().nextInt(10) < 8) {
        doc.add(new NumericDocValuesField("single_long", random().nextLong()));
      }
      if (random().nextInt(10) < 8) {
        doc.add(new FloatDocValuesField("single_float", random().nextFloat()));
      }
      if (random().nextInt(10) < 8) {
        doc.add(new DoubleDocValuesField("single_double", random().nextDouble()));
      }

      int limit = RandomNumbers.randomIntBetween(random(), 0, 100);
      for (int j = 0; j < limit; j++) {
        doc.add(new SortedNumericDocValuesField("multi_int", random().nextInt()));
      }
      limit = RandomNumbers.randomIntBetween(random(), 0, 100);
      for (int j = 0; j < limit; j++) {
        doc.add(new SortedNumericDocValuesField("multi_long", random().nextLong()));
      }
      limit = RandomNumbers.randomIntBetween(random(), 0, 100);
      for (int j = 0; j < limit; j++) {
        doc.add(
            new SortedNumericDocValuesField(
                "multi_float", Float.floatToRawIntBits(random().nextFloat())));
      }
      limit = RandomNumbers.randomIntBetween(random(), 0, 100);
      for (int j = 0; j < limit; j++) {
        doc.add(
            new SortedNumericDocValuesField(
                "multi_double", Double.doubleToRawLongBits(random().nextDouble())));
      }

      iw.addDocument(doc);

      if (i % 100 == 0 && random().nextBoolean()) {
        iw.commit();
      }
    }

    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  protected void validateFieldBasedSource(NumericDocValues docValues, LongValues values, int maxDoc)
      throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        assertEquals(docValues.longValue(), values.longValue());
      }
    }
  }

  protected void validateFieldBasedSource(
      SortedNumericDocValues docValues, MultiLongValues values, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        int valueCount = docValues.docValueCount();
        assertEquals(valueCount, values.getValueCount());
        for (int i = 0; i < valueCount; i++) {
          assertEquals(docValues.nextValue(), values.nextValue());
        }
      }
    }
  }

  protected void validateFieldBasedSource(
      NumericDocValues docValues, DoubleValues values, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        assertEquals((double) docValues.longValue(), values.doubleValue(), 0.00001);
      }
    }
  }

  protected void validateFieldBasedSource(
      SortedNumericDocValues docValues, MultiDoubleValues values, int maxDoc) throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        int valueCount = docValues.docValueCount();
        assertEquals(valueCount, values.getValueCount());
        for (int i = 0; i < valueCount; i++) {
          assertEquals((double) docValues.nextValue(), values.nextValue(), 0.00001);
        }
      }
    }
  }

  protected void validateFieldBasedSource(
      NumericDocValues docValues, DoubleValues values, int maxDoc, boolean useDoublePrecision)
      throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        if (useDoublePrecision) {
          long asLong = Double.doubleToRawLongBits(values.doubleValue());
          assertEquals(docValues.longValue(), asLong);
        } else {
          int asInt = Float.floatToRawIntBits((float) values.doubleValue());
          assertEquals((int) docValues.longValue(), asInt);
        }
      }
    }
  }

  protected void validateFieldBasedSource(
      SortedNumericDocValues docValues,
      MultiDoubleValues values,
      int maxDoc,
      boolean useDoublePrecision)
      throws IOException {
    for (int doc = 0; doc < maxDoc; doc++) {
      boolean hasValues = docValues.advanceExact(doc);
      assertEquals(hasValues, values.advanceExact(doc));
      if (hasValues) {
        int valueCount = docValues.docValueCount();
        assertEquals(valueCount, values.getValueCount());
        for (int i = 0; i < valueCount; i++) {
          if (useDoublePrecision) {
            long asLong = Double.doubleToRawLongBits(values.nextValue());
            assertEquals(docValues.nextValue(), asLong);
          } else {
            int asInt = Float.floatToRawIntBits((float) values.nextValue());
            assertEquals(docValues.nextValue(), asInt);
          }
        }
      }
    }
  }
}
