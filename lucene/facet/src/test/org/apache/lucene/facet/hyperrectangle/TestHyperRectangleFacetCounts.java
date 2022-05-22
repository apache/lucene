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
package org.apache.lucene.facet.hyperrectangle;

import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePointDocValuesField;
import org.apache.lucene.document.LongPointDocValuesField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;

public class TestHyperRectangleFacetCounts extends FacetTestCase {

  public void testBasicLong() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    for (long l = 0; l < 100; l++) {
      Document doc = new Document();
      LongPointDocValuesField field = new LongPointDocValuesField("field", l, l + 1L, l + 2L);
      doc.add(field);
      w.addDocument(doc);
    }

    // Also add point with Long.MAX_VALUE
    Document doc = new Document();
    LongPointDocValuesField field =
        new LongPointDocValuesField(
            "field", Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new HyperRectangleFacetCounts(
            "field",
            fc,
            new LongHyperRectangle(
                "less than (10, 11, 12)",
                new HyperRectangle.LongRangePair(0L, true, 10L, false),
                new HyperRectangle.LongRangePair(0L, true, 11L, false),
                new HyperRectangle.LongRangePair(0L, true, 12L, false)),
            new LongHyperRectangle(
                "less than or equal to (10, 11, 12)",
                new HyperRectangle.LongRangePair(0L, true, 10L, true),
                new HyperRectangle.LongRangePair(0L, true, 11L, true),
                new HyperRectangle.LongRangePair(0L, true, 12L, true)),
            new LongHyperRectangle(
                "over (90, 91, 92)",
                new HyperRectangle.LongRangePair(90L, false, 100L, false),
                new HyperRectangle.LongRangePair(91L, false, 101L, false),
                new HyperRectangle.LongRangePair(92L, false, 102L, false)),
            new LongHyperRectangle(
                "(90, 91, 92) or above",
                new HyperRectangle.LongRangePair(90L, true, 100L, false),
                new HyperRectangle.LongRangePair(91L, true, 101L, false),
                new HyperRectangle.LongRangePair(92L, true, 102L, false)),
            new LongHyperRectangle(
                "over (1000, 1000, 1000)",
                new HyperRectangle.LongRangePair(1000L, false, Long.MAX_VALUE - 2L, true),
                new HyperRectangle.LongRangePair(1000L, false, Long.MAX_VALUE - 1L, true),
                new HyperRectangle.LongRangePair(1000L, false, Long.MAX_VALUE, true)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(22, result.value);
    assertEquals(5, result.childCount);

    LabelAndValue[] expectedLabelsAndValues = new LabelAndValue[5];
    expectedLabelsAndValues[0] = new LabelAndValue("less than (10, 11, 12)", 10);
    expectedLabelsAndValues[1] = new LabelAndValue("less than or equal to (10, 11, 12)", 11);
    expectedLabelsAndValues[2] = new LabelAndValue("over (90, 91, 92)", 9);
    expectedLabelsAndValues[3] = new LabelAndValue("(90, 91, 92) or above", 10);
    expectedLabelsAndValues[4] = new LabelAndValue("over (1000, 1000, 1000)", 1);

    r.close();
    d.close();
  }

  public void testBasicDouble() throws Exception {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    for (double l = 0; l < 100; l++) {
      Document doc = new Document();
      DoublePointDocValuesField field = new DoublePointDocValuesField("field", l, l + 1.0, l + 2.0);
      doc.add(field);
      w.addDocument(doc);
    }

    // Also add point with Long.MAX_VALUE
    Document doc = new Document();
    DoublePointDocValuesField field =
        new DoublePointDocValuesField(
            "field", Double.MAX_VALUE - 2.0, Double.MAX_VALUE - 1.0, Double.MAX_VALUE);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new HyperRectangleFacetCounts(
            "field",
            fc,
            new DoubleHyperRectangle(
                "less than (10, 11, 12)",
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 10.0, false),
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 11.0, false),
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 12.0, false)),
            new DoubleHyperRectangle(
                "less than or equal to (10, 11, 12)",
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 10.0, true),
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 11.0, true),
                new DoubleHyperRectangle.DoubleRangePair(0.0, true, 12.0, true)),
            new DoubleHyperRectangle(
                "over (90, 91, 92)",
                new DoubleHyperRectangle.DoubleRangePair(90.0, false, 100.0, false),
                new DoubleHyperRectangle.DoubleRangePair(91.0, false, 101.0, false),
                new DoubleHyperRectangle.DoubleRangePair(92.0, false, 102.0, false)),
            new DoubleHyperRectangle(
                "(90, 91, 92) or above",
                new DoubleHyperRectangle.DoubleRangePair(90.0, true, 100.0, false),
                new DoubleHyperRectangle.DoubleRangePair(91.0, true, 101.0, false),
                new DoubleHyperRectangle.DoubleRangePair(92.0, true, 102.0, false)),
            new DoubleHyperRectangle(
                "over (1000, 1000, 1000)",
                new DoubleHyperRectangle.DoubleRangePair(
                    1000.0, false, Double.MAX_VALUE - 2.0, true),
                new DoubleHyperRectangle.DoubleRangePair(
                    1000.0, false, Double.MAX_VALUE - 1.0, true),
                new DoubleHyperRectangle.DoubleRangePair(1000.0, false, Double.MAX_VALUE, true)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(22, result.value);
    assertEquals(5, result.childCount);

    LabelAndValue[] expectedLabelsAndValues = new LabelAndValue[5];
    expectedLabelsAndValues[0] = new LabelAndValue("less than (10, 11, 12)", 10);
    expectedLabelsAndValues[1] = new LabelAndValue("less than or equal to (10, 11, 12)", 11);
    expectedLabelsAndValues[2] = new LabelAndValue("over (90, 91, 92)", 9);
    expectedLabelsAndValues[3] = new LabelAndValue("(90, 91, 92) or above", 10);
    expectedLabelsAndValues[4] = new LabelAndValue("over (1000, 1000, 1000)", 1);

    assertArrayEquals(expectedLabelsAndValues, result.labelValues);

    r.close();
    d.close();
  }

  public void testNegativeLong() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    for (long l = -99; l <= 0; l++) {
      Document doc = new Document();
      LongPointDocValuesField field = new LongPointDocValuesField("field", l, l - 1L, l - 2L);
      doc.add(field);
      w.addDocument(doc);
    }

    // Also add point with Long.MIN_VALUE
    Document doc = new Document();
    LongPointDocValuesField field =
        new LongPointDocValuesField(
            "field", Long.MIN_VALUE + 2L, Long.MIN_VALUE + 1L, Long.MIN_VALUE);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new HyperRectangleFacetCounts(
            "field",
            fc,
            new LongHyperRectangle(
                "greater than (-10, -11, -12)",
                new HyperRectangle.LongRangePair(-10L, false, 0L, true),
                new HyperRectangle.LongRangePair(-11L, false, 0L, true),
                new HyperRectangle.LongRangePair(-12L, false, 0L, true)),
            new LongHyperRectangle(
                "greater than or equal to (-10, -11, -12)",
                new HyperRectangle.LongRangePair(-10L, true, 0L, true),
                new HyperRectangle.LongRangePair(-11L, true, 0L, true),
                new HyperRectangle.LongRangePair(-12L, true, 0L, true)),
            new LongHyperRectangle(
                "under (-90, -91, -92)",
                new HyperRectangle.LongRangePair(-100L, false, -90L, false),
                new HyperRectangle.LongRangePair(-101L, false, -91L, false),
                new HyperRectangle.LongRangePair(-102L, false, -92L, false)),
            new LongHyperRectangle(
                "(90, 91, 92) or below",
                new HyperRectangle.LongRangePair(-100L, false, -90L, true),
                new HyperRectangle.LongRangePair(-101L, false, -91L, true),
                new HyperRectangle.LongRangePair(-102L, false, -92L, true)),
            new LongHyperRectangle(
                "under (1000, 1000, 1000)",
                new HyperRectangle.LongRangePair(Long.MIN_VALUE + 2L, true, -1000L, false),
                new HyperRectangle.LongRangePair(Long.MIN_VALUE + 1L, true, -1000L, false),
                new HyperRectangle.LongRangePair(Long.MIN_VALUE, true, -1000L, false)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(22, result.value);
    assertEquals(5, result.childCount);

    LabelAndValue[] expectedLabelsAndValues = new LabelAndValue[5];
    expectedLabelsAndValues[0] = new LabelAndValue("greater than (-10, -11, -12)", 10);
    expectedLabelsAndValues[1] = new LabelAndValue("greater than or equal to (-10, -11, -12)", 11);
    expectedLabelsAndValues[2] = new LabelAndValue("under (-90, -91, -92)", 9);
    expectedLabelsAndValues[3] = new LabelAndValue("(90, 91, 92) or below", 10);
    expectedLabelsAndValues[4] = new LabelAndValue("under (1000, 1000, 1000)", 1);

    assertArrayEquals(expectedLabelsAndValues, result.labelValues);

    r.close();
    d.close();
  }

  public void testNegativeDouble() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    for (double i = -99; i <= 0; i++) {
      Document doc = new Document();
      DoublePointDocValuesField field = new DoublePointDocValuesField("field", i, i - 1.0, i - 2.0);
      doc.add(field);
      w.addDocument(doc);
    }

    // Also add point with Double.MIN_VALUE
    Document doc = new Document();
    DoublePointDocValuesField field =
        new DoublePointDocValuesField(
            "field",
            Double.NEGATIVE_INFINITY + 2.0,
            Double.NEGATIVE_INFINITY + 1.0,
            Double.NEGATIVE_INFINITY);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new HyperRectangleFacetCounts(
            "field",
            fc,
            new DoubleHyperRectangle(
                "greater than (-10, -11, -12)",
                new DoubleHyperRectangle.DoubleRangePair(-10.0, false, 0.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-11.0, false, 0.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-12.0, false, 0.0, true)),
            new DoubleHyperRectangle(
                "greater than or equal to (-10, -11, -12)",
                new DoubleHyperRectangle.DoubleRangePair(-10.0, true, 0.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-11.0, true, 0.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-12.0, true, 0.0, true)),
            new DoubleHyperRectangle(
                "under (-90, -91, -92)",
                new DoubleHyperRectangle.DoubleRangePair(-100.0, false, -90.0, false),
                new DoubleHyperRectangle.DoubleRangePair(-101.0, false, -91.0, false),
                new DoubleHyperRectangle.DoubleRangePair(-102.0, false, -92.0, false)),
            new DoubleHyperRectangle(
                "(90, 91, 92) or below",
                new DoubleHyperRectangle.DoubleRangePair(-100.0, false, -90.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-101.0, false, -91.0, true),
                new DoubleHyperRectangle.DoubleRangePair(-102.0, false, -92.0, true)),
            new DoubleHyperRectangle(
                "under (1000, 1000, 1000)",
                new DoubleHyperRectangle.DoubleRangePair(
                    Double.NEGATIVE_INFINITY + 2.0, true, -1000.0, false),
                new DoubleHyperRectangle.DoubleRangePair(
                    Double.NEGATIVE_INFINITY + 1.0, true, -1000.0, false),
                new DoubleHyperRectangle.DoubleRangePair(
                    Double.NEGATIVE_INFINITY, true, -1000.0, false)));

    FacetResult result = facets.getTopChildren(10, "field");

    assertEquals("field", result.dim);
    assertEquals(0, result.path.length);
    assertEquals(22, result.value);
    assertEquals(5, result.childCount);

    LabelAndValue[] expectedLabelsAndValues = new LabelAndValue[5];
    expectedLabelsAndValues[0] = new LabelAndValue("greater than (-10, -11, -12)", 10);
    expectedLabelsAndValues[1] = new LabelAndValue("greater than or equal to (-10, -11, -12)", 11);
    expectedLabelsAndValues[2] = new LabelAndValue("under (-90, -91, -92)", 9);
    expectedLabelsAndValues[3] = new LabelAndValue("(90, 91, 92) or below", 10);
    expectedLabelsAndValues[4] = new LabelAndValue("under (1000, 1000, 1000)", 1);

    assertArrayEquals(expectedLabelsAndValues, result.labelValues);

    r.close();
    d.close();
  }

  public void testGetTopChildren() throws IOException {
    Directory d = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), d);

    Document doc = new Document();
    LongPointDocValuesField field = new LongPointDocValuesField("field", 0L);
    doc.add(field);
    w.addDocument(doc);

    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    FacetsCollector fc = s.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new HyperRectangleFacetCounts(
            "field",
            fc,
            new LongHyperRectangle("test", new HyperRectangle.LongRangePair(1, false, 10, false)));

    // test getTopChildren(0, dim)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(0, "field");
        });

    r.close();
    d.close();
  }
}
