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
package org.apache.lucene.sandbox.facet;

import static org.apache.lucene.facet.FacetsConfig.DEFAULT_INDEX_FIELD_NAME;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.labels.OrdLabelBiMap;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;
import org.apache.lucene.sandbox.facet.ordinals.OrdToComparable;
import org.apache.lucene.sandbox.facet.ordinals.OrdinalIterator;
import org.apache.lucene.sandbox.facet.ordinals.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.ordinals.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.MultiFacetsRecorder;
import org.apache.lucene.sandbox.facet.recorders.Reducer;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;

/** Test for {@link FacetRecorder} */
public class TestFacetRecorders extends SandboxFacetTestCase {

  public void testCountAndLongAggregationRecordersBasic() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);
    // TODO: we only set it to true because rollup is not implemented for Long aggregations yet.
    //  let's remove this line once it is implemented!
    config.setMultiValued("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    doc.add(new NumericDocValuesField("Units", 9));
    doc.add(new DoubleDocValuesField("Popularity", 3.5d));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    doc.add(new NumericDocValuesField("Units", 2));
    doc.add(new DoubleDocValuesField("Popularity", 4.1D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    doc.add(new NumericDocValuesField("Units", 5));
    doc.add(new DoubleDocValuesField("Popularity", 3.9D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    doc.add(new NumericDocValuesField("Units", 7));
    doc.add(new DoubleDocValuesField("Popularity", 4D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    doc.add(new NumericDocValuesField("Units", 6));
    doc.add(new DoubleDocValuesField("Popularity", 7.9D));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = new MatchAllDocsQuery();

    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);

    LongValuesSource[] longValuesSources = new LongValuesSource[2];
    Reducer[] reducers = new Reducer[2];
    // popularity:max
    longValuesSources[0] = DoubleValuesSource.fromDoubleField("Popularity").toLongValuesSource();
    reducers[0] = Reducer.MAX;
    // units:sum
    longValuesSources[1] = LongValuesSource.fromLongField("Units");
    reducers[1] = Reducer.SUM;

    LongAggregationsFacetRecorder longAggregationsFacetRecorder =
        new LongAggregationsFacetRecorder(longValuesSources, reducers);

    final CountFacetRecorder countRecorder = new CountFacetRecorder();
    // Compute both counts and aggregations
    MultiFacetsRecorder multiFacetsRecorder =
        new MultiFacetsRecorder(countRecorder, longAggregationsFacetRecorder);

    FacetFieldCollectorManager<MultiFacetsRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultTaxoCutter, multiFacetsRecorder);
    searcher.search(query, collectorManager);

    int[] ordsFromCounts = countRecorder.recordedOrds().toArray();
    Arrays.sort(ordsFromCounts);
    int[] ordsFromAggregations = longAggregationsFacetRecorder.recordedOrds().toArray();
    Arrays.sort(ordsFromAggregations);
    assertArrayEquals(ordsFromCounts, ordsFromAggregations);

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[]\n"
            + "  2010 (2,  agg0=4 agg1=11)\n"
            + "  2012 (2,  agg0=4 agg1=12)\n"
            + "  1999 (1,  agg0=7 agg1=6)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, null, "Publish Date"));
    assertEquals(
        "dim=Author path=[]\n"
            + "  Lisa (2,  agg0=4 agg1=7)\n"
            + "  Bob (1,  agg0=3 agg1=9)\n"
            + "  Susan (1,  agg0=4 agg1=7)\n"
            + "  Frank (1,  agg0=7 agg1=6)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, null, "Author"));

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  /**
   * Test that counts and long aggregations are correct when different index segments have different
   * facet ordinals.
   */
  public void testCountAndLongAggregationRecordersMultipleSegments() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);
    // TODO: we only set it to true because rollup is not implemented for Long aggregations yet.
    //  let's remove this line once it is implemented!
    config.setMultiValued("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    doc.add(new NumericDocValuesField("Units", 9));
    doc.add(new DoubleDocValuesField("Popularity", 3.5d));
    writer.addDocument(config.build(taxoWriter, doc));
    writer.commit();

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "10", "20"));
    doc.add(new NumericDocValuesField("Units", 2));
    doc.add(new DoubleDocValuesField("Popularity", 4.1D));
    writer.addDocument(config.build(taxoWriter, doc));
    writer.commit();

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = new MatchAllDocsQuery();

    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);

    LongValuesSource[] longValuesSources = new LongValuesSource[2];
    Reducer[] reducers = new Reducer[2];
    // popularity:max
    longValuesSources[0] = DoubleValuesSource.fromDoubleField("Popularity").toLongValuesSource();
    reducers[0] = Reducer.MAX;
    // units:sum
    longValuesSources[1] = LongValuesSource.fromLongField("Units");
    reducers[1] = Reducer.SUM;

    LongAggregationsFacetRecorder longAggregationsFacetRecorder =
        new LongAggregationsFacetRecorder(longValuesSources, reducers);

    final CountFacetRecorder countRecorder = new CountFacetRecorder();
    // Compute both counts and aggregations
    MultiFacetsRecorder multiFacetsRecorder =
        new MultiFacetsRecorder(countRecorder, longAggregationsFacetRecorder);

    FacetFieldCollectorManager<MultiFacetsRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultTaxoCutter, multiFacetsRecorder);
    searcher.search(query, collectorManager);

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[]\n"
            + "  2010 (1,  agg0=3 agg1=9)\n"
            + "  2012 (1,  agg0=4 agg1=2)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, null, "Publish Date"));
    assertEquals(
        "dim=Author path=[]\n" + "  Bob (1,  agg0=3 agg1=9)\n" + "  Lisa (1,  agg0=4 agg1=2)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, null, "Author"));

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  public void testSortByLongAggregation() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    // Writes facet ords to a separate directory from the
    // main index:
    DirectoryTaxonomyWriter taxoWriter =
        new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("Publish Date", true);
    // TODO: we only set it to true because rollup is not implemented for Long aggregations yet.
    //  let's remove this line once it is implemented!
    config.setMultiValued("Publish Date", true);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    doc.add(new NumericDocValuesField("Units", 9));
    doc.add(new DoubleDocValuesField("Popularity", 3.5d));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    doc.add(new NumericDocValuesField("Units", 2));
    doc.add(new DoubleDocValuesField("Popularity", 4.1D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    doc.add(new NumericDocValuesField("Units", 5));
    doc.add(new DoubleDocValuesField("Popularity", 3.9D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    doc.add(new NumericDocValuesField("Units", 7));
    doc.add(new DoubleDocValuesField("Popularity", 4D));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    doc.add(new NumericDocValuesField("Units", 6));
    doc.add(new DoubleDocValuesField("Popularity", 7.9D));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = new MatchAllDocsQuery();

    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);

    LongValuesSource[] longValuesSources = new LongValuesSource[2];
    Reducer[] reducers = new Reducer[2];
    // popularity:max
    longValuesSources[0] = DoubleValuesSource.fromDoubleField("Popularity").toLongValuesSource();
    reducers[0] = Reducer.MAX;
    // units:sum
    longValuesSources[1] = LongValuesSource.fromLongField("Units");
    reducers[1] = Reducer.SUM;

    LongAggregationsFacetRecorder longAggregationsFacetRecorder =
        new LongAggregationsFacetRecorder(longValuesSources, reducers);

    final CountFacetRecorder countRecorder = new CountFacetRecorder();
    // Compute both counts and aggregations
    MultiFacetsRecorder multiFacetsRecorder =
        new MultiFacetsRecorder(countRecorder, longAggregationsFacetRecorder);

    FacetFieldCollectorManager<MultiFacetsRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultTaxoCutter, multiFacetsRecorder);
    searcher.search(query, collectorManager);

    // Retrieve & verify results:
    assertEquals(
        "dim=Publish Date path=[]\n"
            + "  2012 (2,  agg0=4 agg1=12)\n"
            + "  2010 (2,  agg0=4 agg1=11)\n"
            + "  1999 (1,  agg0=7 agg1=6)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, 1, "Publish Date"));
    assertEquals(
        "dim=Author path=[]\n"
            + "  Frank (1,  agg0=7 agg1=6)\n"
            + "  Lisa (2,  agg0=4 agg1=7)\n"
            + "  Susan (1,  agg0=4 agg1=7)\n"
            + "  Bob (1,  agg0=3 agg1=9)\n",
        getTopChildrenWithLongAggregations(
            countRecorder, taxoReader, 10, 2, longAggregationsFacetRecorder, 0, "Author"));

    writer.close();
    IOUtils.close(taxoWriter, searcher.getIndexReader(), taxoReader, taxoDir, dir);
  }

  private String getTopChildrenWithLongAggregations(
      CountFacetRecorder countFacetRecorder,
      TaxonomyReader taxoReader,
      int topN,
      int numOfAggregations,
      LongAggregationsFacetRecorder longAggregationsFacetRecorder,
      Integer sortByLongAggregationId,
      String dimension,
      String... path)
      throws IOException {
    StringBuilder resultBuilder = new StringBuilder();
    resultBuilder.append("dim=");
    resultBuilder.append(dimension);
    resultBuilder.append(" path=");
    resultBuilder.append(Arrays.toString(path));
    resultBuilder.append('\n');

    OrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    OrdinalIterator childrenIternator =
        new TaxonomyChildrenOrdinalIterator(
            countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            ordLabels.getOrd(parentLabel));
    final int[] resultOrdinals;
    if (sortByLongAggregationId != null) {
      OrdToComparable<ComparableUtils.ComparableLongIntOrd> ordToComparable =
          ComparableUtils.ordToComparableRankCountOrd(
              countFacetRecorder, longAggregationsFacetRecorder, sortByLongAggregationId);
      OrdinalIterator topByCountOrds =
          new TopnOrdinalIterator<>(childrenIternator, ordToComparable, topN);
      resultOrdinals = topByCountOrds.toArray();
    } else {
      OrdToComparable<ComparableUtils.ComparableIntOrd> countComparable =
          ComparableUtils.ordToComparableCountOrd(countFacetRecorder);
      OrdinalIterator topByCountOrds =
          new TopnOrdinalIterator<>(childrenIternator, countComparable, topN);
      resultOrdinals = topByCountOrds.toArray();
    }

    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    for (int i = 0; i < resultOrdinals.length; i++) {
      int facetOrdinal = resultOrdinals[i];
      int count = countFacetRecorder.getCount(facetOrdinal);
      resultBuilder.append("  ");
      resultBuilder.append(labels[i].lastComponent());
      resultBuilder.append(" (");
      resultBuilder.append(count);
      resultBuilder.append(", ");
      for (int a = 0; a < numOfAggregations; a++) {
        resultBuilder.append(" agg");
        resultBuilder.append(a);
        resultBuilder.append("=");
        resultBuilder.append(longAggregationsFacetRecorder.getRecordedValue(facetOrdinal, a));
      }
      resultBuilder.append(")");
      resultBuilder.append("\n");
    }
    // int value = countFacetRecorder.getCount(parentOrdinal);
    return resultBuilder.toString();
  }
}
