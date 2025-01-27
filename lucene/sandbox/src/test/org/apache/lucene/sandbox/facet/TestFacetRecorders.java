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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.iterators.CandidateSetOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.MultiFacetsRecorder;
import org.apache.lucene.sandbox.facet.recorders.Reducer;
import org.apache.lucene.sandbox.facet.utils.ComparableUtils;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
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
    config.setMultiValued("Publish Date", random().nextBoolean());

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    doc.add(new NumericDocValuesField("Units", 9));
    doc.add(new DoubleDocValuesField("Popularity", 3.5d));
    doc.add(new StringField("Availability", "yes", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010"));
    doc.add(new NumericDocValuesField("Units", 2));
    doc.add(new DoubleDocValuesField("Popularity", 4.1D));
    doc.add(new StringField("Availability", "yes", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    doc.add(new NumericDocValuesField("Units", 5));
    doc.add(new DoubleDocValuesField("Popularity", 3.9D));
    doc.add(new StringField("Availability", "yes", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    doc.add(new NumericDocValuesField("Units", 7));
    doc.add(new DoubleDocValuesField("Popularity", 4D));
    doc.add(new StringField("Availability", "yes", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    doc.add(new NumericDocValuesField("Units", 6));
    doc.add(new DoubleDocValuesField("Popularity", 7.9D));
    doc.add(new StringField("Availability", "yes", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    // Add a document that is not returned by a query
    doc = new Document();
    doc.add(new FacetField("Author", "John"));
    doc.add(new FacetField("Publish Date", "2024", "11", "12"));
    doc.add(new NumericDocValuesField("Units", 200));
    doc.add(new DoubleDocValuesField("Popularity", 13D));
    doc.add(new StringField("Availability", "no", Field.Store.NO));
    writer.addDocument(config.build(taxoWriter, doc));

    // NRT open
    IndexSearcher searcher = newSearcher(writer.getReader());

    // NRT open
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);

    Query query = new TermQuery(new Term("Availability", "yes"));

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
        new FacetFieldCollectorManager<>(defaultTaxoCutter, multiFacetsRecorder);
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

    assertArrayEquals(
        new long[] {11, 6},
        getAggregationForRecordedCandidates(
            longAggregationsFacetRecorder,
            1,
            taxoReader,
            new FacetLabel[] {
              new FacetLabel("Publish Date", "2010"),
              // Not in the index - skipped
              new FacetLabel("Publish Date", "2025"),
              // Not matched by the query - skipped
              new FacetLabel("Publish Date", "2024"),
              new FacetLabel("Publish Date", "1999"),
            }));

    assertArrayEquals(
        new long[] {7, 6},
        getAggregationForRecordedCandidates(
            longAggregationsFacetRecorder,
            1,
            taxoReader,
            new FacetLabel[] {
              new FacetLabel("Author", "Lisa"),
              // Not in the index - skipped
              new FacetLabel("Author", "Christofer"),
              // Not matched by the query - skipped
              new FacetLabel("Author", "John"),
              new FacetLabel("Author", "Frank"),
            }));

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
    config.setMultiValued("Publish Date", random().nextBoolean());

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
        new FacetFieldCollectorManager<>(defaultTaxoCutter, multiFacetsRecorder);
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
    config.setMultiValued("Publish Date", random().nextBoolean());

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
        new FacetFieldCollectorManager<>(defaultTaxoCutter, multiFacetsRecorder);
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

    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    FacetLabel parentLabel = new FacetLabel(dimension, path);
    OrdinalIterator childrenIternator =
        new TaxonomyChildrenOrdinalIterator(
            countFacetRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            ordLabels.getOrd(parentLabel));
    final int[] resultOrdinals;
    if (sortByLongAggregationId != null) {
      ComparableSupplier<ComparableUtils.ByAggregatedValueComparable> comparableSupplier =
          ComparableUtils.byAggregatedValue(
              countFacetRecorder, longAggregationsFacetRecorder, sortByLongAggregationId);
      OrdinalIterator topByCountOrds =
          new TopnOrdinalIterator<>(childrenIternator, comparableSupplier, topN);
      resultOrdinals = topByCountOrds.toArray();
    } else {
      ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
          ComparableUtils.byCount(countFacetRecorder);
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

  long[] getAggregationForRecordedCandidates(
      LongAggregationsFacetRecorder aggregationsRecorder,
      int aggregationId,
      TaxonomyReader taxoReader,
      FacetLabel[] candidates)
      throws IOException {
    int[] resultOrds =
        new CandidateSetOrdinalIterator(
                aggregationsRecorder, candidates, new TaxonomyOrdLabelBiMap(taxoReader))
            .toArray();
    long[] result = new long[resultOrds.length];
    for (int i = 0; i < resultOrds.length; i++) {
      result[i] = aggregationsRecorder.getRecordedValue(resultOrds[i], aggregationId);
    }
    return result;
  }
}
