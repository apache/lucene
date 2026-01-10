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
package org.apache.lucene.demo.facet;

import static org.apache.lucene.facet.FacetsConfig.DEFAULT_INDEX_FIELD_NAME;
import static org.apache.lucene.sandbox.facet.utils.ComparableUtils.byAggregatedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.cutters.TaxonomyFacetsCutter;
import org.apache.lucene.sandbox.facet.cutters.ranges.LongRangeFacetCutter;
import org.apache.lucene.sandbox.facet.iterators.ComparableSupplier;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.sandbox.facet.iterators.TopnOrdinalIterator;
import org.apache.lucene.sandbox.facet.labels.RangeOrdToLabel;
import org.apache.lucene.sandbox.facet.labels.TaxonomyOrdLabelBiMap;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.LongAggregationsFacetRecorder;
import org.apache.lucene.sandbox.facet.recorders.MultiFacetsRecorder;
import org.apache.lucene.sandbox.facet.recorders.Reducer;
import org.apache.lucene.sandbox.facet.utils.ComparableUtils;
import org.apache.lucene.sandbox.facet.utils.DrillSidewaysFacetOrchestrator;
import org.apache.lucene.sandbox.facet.utils.FacetBuilder;
import org.apache.lucene.sandbox.facet.utils.FacetOrchestrator;
import org.apache.lucene.sandbox.facet.utils.RangeFacetBuilderFactory;
import org.apache.lucene.sandbox.facet.utils.TaxonomyFacetBuilder;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

/** Demo for sandbox faceting. */
public class SandboxFacetsExample {

  private final Directory indexDir = new ByteBuffersDirectory();
  private final Directory taxoDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();

  private SandboxFacetsExample() {
    config.setHierarchical("Publish Date", true);
    config.setHierarchical("Author", false);
  }

  /** Build the example index. */
  void index() throws IOException {
    IndexWriter indexWriter =
        new IndexWriter(
            indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(OpenMode.CREATE));

    // Writes facet ords to a separate directory from the main index
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    Document doc = new Document();
    doc.add(new FacetField("Author", "Bob"));
    doc.add(new FacetField("Publish Date", "2010", "10", "15"));
    doc.add(new NumericDocValuesField("Price", 10));
    doc.add(new NumericDocValuesField("Units", 9));
    doc.add(new DoubleDocValuesField("Popularity", 3.5d));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    doc.add(new NumericDocValuesField("Price", 4));
    doc.add(new NumericDocValuesField("Units", 2));
    doc.add(new DoubleDocValuesField("Popularity", 4.1D));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    doc.add(new NumericDocValuesField("Price", 3));
    doc.add(new NumericDocValuesField("Units", 5));
    doc.add(new DoubleDocValuesField("Popularity", 3.9D));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    doc.add(new NumericDocValuesField("Price", 8));
    doc.add(new NumericDocValuesField("Units", 7));
    doc.add(new DoubleDocValuesField("Popularity", 4D));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    doc.add(new NumericDocValuesField("Price", 9));
    doc.add(new NumericDocValuesField("Units", 6));
    doc.add(new DoubleDocValuesField("Popularity", 4.9D));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    IOUtils.close(indexWriter, taxoWriter);
  }

  /**
   * Example for {@link FacetBuilder} usage - simple API that provides results in a format very
   * similar to classic facets module. It doesn't give all flexibility available with {@link
   * org.apache.lucene.sandbox.facet.cutters.FacetCutter} and {@link
   * org.apache.lucene.sandbox.facet.recorders.FacetRecorder} though, see below for lower level API
   * usage examples.
   */
  private List<FacetResult> simpleFacetsWithSearch() throws IOException {
    //// init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// build facets requests
    FacetBuilder authorFacetBuilder =
        new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
    FacetBuilder priceFacetBuilder =
        RangeFacetBuilderFactory.forLongRanges(
            "Price",
            new LongRange("0-10", 0, true, 10, true),
            new LongRange("10-20", 10, true, 20, true));

    //// Main hits collector
    TopScoreDocCollectorManager hitsCollectorManager =
        new TopScoreDocCollectorManager(2, Integer.MAX_VALUE);

    //// Search and collect
    TopDocs topDocs =
        new FacetOrchestrator()
            .addBuilder(authorFacetBuilder)
            .addBuilder(priceFacetBuilder)
            .collect(MatchAllDocsQuery.INSTANCE, searcher, hitsCollectorManager);
    System.out.println(
        "Search results: totalHits: "
            + topDocs.totalHits
            + ", collected hits: "
            + topDocs.scoreDocs.length);

    //// Results
    FacetResult authorResults = authorFacetBuilder.getResult();
    FacetResult rangeResults = priceFacetBuilder.getResult();

    IOUtils.close(indexReader, taxoReader);

    return List.of(authorResults, rangeResults);
  }

  /** Example for {@link FacetBuilder} usage with {@link DrillSideways}. */
  private List<FacetResult> simpleFacetsWithDrillSideways() throws IOException {
    //// init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    DrillSideways ds = new DrillSideways(searcher, config, taxoReader);

    //// build facets requests
    FacetBuilder authorFacetBuilder =
        new TaxonomyFacetBuilder(config, taxoReader, "Author").withTopN(10);
    FacetBuilder priceFacetBuilder =
        RangeFacetBuilderFactory.forLongRanges(
            "Price",
            new LongRange("0-10", 0, true, 10, true),
            new LongRange("10-20", 10, true, 20, true));

    //// Build query and collect
    DrillDownQuery query = new DrillDownQuery(config);
    query.add("Author", "Lisa");

    new DrillSidewaysFacetOrchestrator()
        .addDrillDownBuilder(priceFacetBuilder)
        .addDrillSidewaysBuilder("Author", authorFacetBuilder)
        .collect(query, ds);

    //// Results
    FacetResult authorResults = authorFacetBuilder.getResult();
    FacetResult rangeResults = priceFacetBuilder.getResult();

    IOUtils.close(indexReader, taxoReader);

    return List.of(authorResults, rangeResults);
  }

  /** User runs a query and counts facets only without collecting the matching documents. */
  List<FacetResult> facetsOnly() throws IOException {
    //// (1) init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collector
    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountFacetRecorder defaultRecorder = new CountFacetRecorder();

    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultRecorder);

    // (2.1) if we need to collect data using multiple different collectors, e.g. taxonomy and
    // ranges, or even two taxonomy facets that use different Category List Field, we can
    // use MultiCollectorManager, e.g.:
    //
    // TODO: add a demo for it.
    // TaxonomyFacetsCutter publishDateCutter = new
    // TaxonomyFacetsCutter(config.getDimConfig("Publish Date"), taxoReader);
    // CountFacetRecorder publishDateRecorder = new CountFacetRecorder(false);
    // FacetFieldCollectorManager<CountFacetRecorder> publishDateCollectorManager = new
    // FacetFieldCollectorManager<>(publishDateCutter, publishDateRecorder);
    // MultiCollectorManager drillDownCollectorManager = new
    // MultiCollectorManager(authorCollectorManager, publishDateCollectorManager);
    // Object[] results = searcher.search(MatchAllDocsQuery.INSTANCE, drillDownCollectorManager);

    //// (3) search
    // Search returns the same Recorder we created - so we can ignore results
    searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(defaultRecorder);
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    List<FacetResult> results = new ArrayList<>(2);
    // This object provides labels for ordinals.
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    for (String dimension : List.of("Author", "Publish Date")) {
      //// (4.1) Chain two ordinal iterators to get top N children
      int dimOrdinal = ordLabels.getOrd(new FacetLabel(dimension));
      OrdinalIterator childrenIterator =
          new TaxonomyChildrenOrdinalIterator(
              defaultRecorder.recordedOrds(),
              taxoReader.getParallelTaxonomyArrays().parents(),
              dimOrdinal);
      OrdinalIterator topByCountOrds =
          new TopnOrdinalIterator<>(childrenIterator, countComparable, 10);
      // Get array of final ordinals - we need to use all of them to get labels first, and then to
      // get counts,
      // but OrdinalIterator only allows reading ordinals once.
      int[] resultOrdinals = topByCountOrds.toArray();

      //// (4.2) Use faceting results
      FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
      List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
      for (int i = 0; i < resultOrdinals.length; i++) {
        labelsAndValues.add(
            new LabelAndValue(
                labels[i].lastComponent(), defaultRecorder.getCount(resultOrdinals[i])));
      }
      int dimensionValue = defaultRecorder.getCount(dimOrdinal);
      results.add(
          new FacetResult(
              dimension,
              new String[0],
              dimensionValue,
              labelsAndValues.toArray(new LabelAndValue[0]),
              labelsAndValues.size()));
    }

    IOUtils.close(indexReader, taxoReader);
    return results;
  }

  /**
   * User runs a query and counts facets for exclusive ranges without collecting the matching
   * documents
   */
  List<FacetResult> exclusiveRangesCountFacetsOnly() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    MultiLongValuesSource valuesSource = MultiLongValuesSource.fromLongField("Price");

    // Exclusive ranges example
    LongRange[] inputRanges = new LongRange[2];
    inputRanges[0] = new LongRange("0-5", 0, true, 5, true);
    inputRanges[1] = new LongRange("5-10", 5, false, 10, true);

    LongRangeFacetCutter longRangeFacetCutter =
        LongRangeFacetCutter.create(valuesSource, inputRanges);
    CountFacetRecorder countRecorder = new CountFacetRecorder();

    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longRangeFacetCutter, countRecorder);
    searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);
    RangeOrdToLabel ordToLabels = new RangeOrdToLabel(inputRanges);

    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(countRecorder);
    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(countRecorder.recordedOrds(), countComparable, 10);

    List<FacetResult> results = new ArrayList<>(2);

    int[] resultOrdinals = topByCountOrds.toArray();
    FacetLabel[] labels = ordToLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(labels[i].lastComponent(), countRecorder.getCount(resultOrdinals[i])));
    }

    results.add(
        new FacetResult(
            "Price", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    System.out.println("Computed counts");
    IOUtils.close(indexReader);
    return results;
  }

  List<FacetResult> overlappingRangesCountFacetsOnly() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    MultiLongValuesSource valuesSource = MultiLongValuesSource.fromLongField("Price");

    // overlapping ranges example
    LongRange[] inputRanges = new LongRange[2];
    inputRanges[0] = new LongRange("0-5", 0, true, 5, true);
    inputRanges[1] = new LongRange("0-10", 0, true, 10, true);

    LongRangeFacetCutter longRangeFacetCutter =
        LongRangeFacetCutter.create(valuesSource, inputRanges);
    CountFacetRecorder countRecorder = new CountFacetRecorder();

    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longRangeFacetCutter, countRecorder);
    searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);
    RangeOrdToLabel ordToLabels = new RangeOrdToLabel(inputRanges);

    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(countRecorder);
    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(countRecorder.recordedOrds(), countComparable, 10);

    List<FacetResult> results = new ArrayList<>(2);

    int[] resultOrdinals = topByCountOrds.toArray();
    FacetLabel[] labels = ordToLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(labels[i].lastComponent(), countRecorder.getCount(resultOrdinals[i])));
    }

    results.add(
        new FacetResult(
            "Price", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    System.out.println("Computed counts");
    IOUtils.close(indexReader);
    return results;
  }

  List<FacetResult> exclusiveRangesAggregationFacets() throws IOException {
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);

    MultiLongValuesSource valuesSource = MultiLongValuesSource.fromLongField("Price");

    // Exclusive ranges example
    LongRange[] inputRanges = new LongRange[2];
    inputRanges[0] = new LongRange("0-5", 0, true, 5, true);
    inputRanges[1] = new LongRange("5-10", 5, false, 10, true);

    LongRangeFacetCutter longRangeFacetCutter =
        LongRangeFacetCutter.create(valuesSource, inputRanges);

    // initialise the aggregations to be computed - a values source + reducer
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

    CountFacetRecorder countRecorder = new CountFacetRecorder();

    // Compute both counts and aggregations
    MultiFacetsRecorder multiFacetsRecorder =
        new MultiFacetsRecorder(countRecorder, longAggregationsFacetRecorder);

    FacetFieldCollectorManager<MultiFacetsRecorder> collectorManager =
        new FacetFieldCollectorManager<>(longRangeFacetCutter, multiFacetsRecorder);
    searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);
    RangeOrdToLabel ordToLabels = new RangeOrdToLabel(inputRanges);

    // Get recorded ords - use either count/aggregations recorder
    OrdinalIterator recordedOrds = longAggregationsFacetRecorder.recordedOrds();

    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    List<FacetResult> results = new ArrayList<>(2);
    ComparableSupplier<ComparableUtils.ByAggregatedValueComparable> comparableSupplier;
    OrdinalIterator topOrds;
    int[] resultOrdinals;
    FacetLabel[] labels;
    List<LabelAndValue> labelsAndValues;

    // Sort results by units:sum and tie-break by count
    comparableSupplier = byAggregatedValue(countRecorder, longAggregationsFacetRecorder, 1);
    topOrds = new TopnOrdinalIterator<>(recordedOrds, comparableSupplier, 10);

    resultOrdinals = topOrds.toArray();
    labels = ordToLabels.getLabels(resultOrdinals);
    labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(
              labels[i].lastComponent(),
              longAggregationsFacetRecorder.getRecordedValue(resultOrdinals[i], 1)));
    }
    results.add(
        new FacetResult(
            "Price", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    // note: previous ordinal iterator was exhausted
    recordedOrds = longAggregationsFacetRecorder.recordedOrds();
    // Sort results by popularity:max and tie-break by count
    comparableSupplier = byAggregatedValue(countRecorder, longAggregationsFacetRecorder, 0);
    topOrds = new TopnOrdinalIterator<>(recordedOrds, comparableSupplier, 10);
    resultOrdinals = topOrds.toArray();
    labels = ordToLabels.getLabels(resultOrdinals);
    labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(
              labels[i].lastComponent(),
              longAggregationsFacetRecorder.getRecordedValue(resultOrdinals[i], 0)));
    }
    results.add(
        new FacetResult(
            "Price", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    return results;
  }

  /** User runs a query and counts facets. */
  private List<FacetResult> facetsWithSearch() throws IOException {
    //// (1) init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collectors
    // Facet collectors
    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountFacetRecorder defaultRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> taxoFacetsCollectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultRecorder);
    // Hits collector
    TopScoreDocCollectorManager hitsCollectorManager =
        new TopScoreDocCollectorManager(2, Integer.MAX_VALUE);
    // Now wrap them with MultiCollectorManager to collect both hits and facets.
    MultiCollectorManager collectorManager =
        new MultiCollectorManager(hitsCollectorManager, taxoFacetsCollectorManager);

    //// (3) search
    Object[] results = searcher.search(MatchAllDocsQuery.INSTANCE, collectorManager);
    TopDocs topDocs = (TopDocs) results[0];
    System.out.println(
        "Search results: totalHits: "
            + topDocs.totalHits
            + ", collected hits: "
            + topDocs.scoreDocs.length);
    // FacetFieldCollectorManager returns the same Recorder it gets - so we can ignore read the
    // results from original recorder
    // and ignore this value.
    // CountFacetRecorder defaultRecorder = (CountFacetRecorder) results[1];

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(defaultRecorder);
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    List<FacetResult> facetResults = new ArrayList<>(2);
    // This object provides labels for ordinals.
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    for (String dimension : List.of("Author", "Publish Date")) {
      int dimensionOrdinal = ordLabels.getOrd(new FacetLabel(dimension));
      //// (4.1) Chain two ordinal iterators to get top N children
      OrdinalIterator childrenIterator =
          new TaxonomyChildrenOrdinalIterator(
              defaultRecorder.recordedOrds(),
              taxoReader.getParallelTaxonomyArrays().parents(),
              dimensionOrdinal);
      OrdinalIterator topByCountOrds =
          new TopnOrdinalIterator<>(childrenIterator, countComparable, 10);
      // Get array of final ordinals - we need to use all of them to get labels first, and then to
      // get counts,
      // but OrdinalIterator only allows reading ordinals once.
      int[] resultOrdinals = topByCountOrds.toArray();

      //// (4.2) Use faceting results
      FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
      List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
      for (int i = 0; i < resultOrdinals.length; i++) {
        labelsAndValues.add(
            new LabelAndValue(
                labels[i].lastComponent(), defaultRecorder.getCount(resultOrdinals[i])));
      }
      int dimensionValue = defaultRecorder.getCount(dimensionOrdinal);
      facetResults.add(
          new FacetResult(
              dimension,
              new String[0],
              dimensionValue,
              labelsAndValues.toArray(new LabelAndValue[0]),
              labelsAndValues.size()));
    }

    IOUtils.close(indexReader, taxoReader);
    return facetResults;
  }

  /** User drills down on 'Publish Date/2010', and we return facets for 'Author' */
  FacetResult drillDown() throws IOException {
    //// (1) init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collector
    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountFacetRecorder defaultRecorder = new CountFacetRecorder();

    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultRecorder);

    DrillDownQuery q = new DrillDownQuery(config);
    q.add("Publish Date", "2010");

    //// (3) search
    // Right now we return the same Recorder we created - so we can ignore results
    searcher.search(q, collectorManager);

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(defaultRecorder);

    // This object provides labels for ordinals.
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    String dimension = "Author";
    //// (4.1) Chain two ordinal iterators to get top N children
    int dimOrdinal = ordLabels.getOrd(new FacetLabel(dimension));
    OrdinalIterator childrenIterator =
        new TaxonomyChildrenOrdinalIterator(
            defaultRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            dimOrdinal);
    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(childrenIterator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get
    // counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(
              labels[i].lastComponent(), defaultRecorder.getCount(resultOrdinals[i])));
    }

    IOUtils.close(indexReader, taxoReader);
    int dimensionValue = defaultRecorder.getCount(dimOrdinal);
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    return new FacetResult(
        dimension,
        new String[0],
        dimensionValue,
        labelsAndValues.toArray(new LabelAndValue[0]),
        labelsAndValues.size());
  }

  /**
   * User drills down on 'Publish Date/2010', and we return facets for both 'Publish Date' and
   * 'Author', using DrillSideways.
   */
  private List<FacetResult> drillSideways() throws IOException {
    //// (1) init readers and searcher
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init drill down query and collectors
    TaxonomyFacetsCutter defaultTaxoCutter =
        new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountFacetRecorder drillDownRecorder = new CountFacetRecorder();
    FacetFieldCollectorManager<CountFacetRecorder> drillDownCollectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, drillDownRecorder);

    DrillDownQuery q = new DrillDownQuery(config);

    //// (2.1) add query and collector dimensions
    q.add("Publish Date", "2010");
    CountFacetRecorder publishDayDimensionRecorder = new CountFacetRecorder();
    // Note that it is safe to use the same FacetsCutter here because we create Leaf cutter for each
    // leaf for each
    // FacetFieldCollectorManager anyway, and leaf cutter are not merged or anything like that.
    FacetFieldCollectorManager<CountFacetRecorder> publishDayDimensionCollectorManager =
        new FacetFieldCollectorManager<>(defaultTaxoCutter, publishDayDimensionRecorder);
    List<FacetFieldCollectorManager<CountFacetRecorder>> drillSidewaysManagers =
        List.of(publishDayDimensionCollectorManager);

    //// (3) search
    // Right now we return the same Recorder we created - so we can ignore results
    DrillSideways ds = new DrillSideways(searcher, config, taxoReader);
    ds.search(q, drillDownCollectorManager, drillSidewaysManagers);

    //// (4) Get top 10 results by count for Author
    List<FacetResult> facetResults = new ArrayList<>(2);
    // This object provides labels for ordinals.
    TaxonomyOrdLabelBiMap ordLabels = new TaxonomyOrdLabelBiMap(taxoReader);
    // This object is used to get topN results by count
    ComparableSupplier<ComparableUtils.ByCountComparable> countComparable =
        ComparableUtils.byCount(drillDownRecorder);
    //// (4.1) Chain two ordinal iterators to get top N children
    int dimOrdinal = ordLabels.getOrd(new FacetLabel("Author"));
    OrdinalIterator childrenIterator =
        new TaxonomyChildrenOrdinalIterator(
            drillDownRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            dimOrdinal);
    OrdinalIterator topByCountOrds =
        new TopnOrdinalIterator<>(childrenIterator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get
    // counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(
              labels[i].lastComponent(), drillDownRecorder.getCount(resultOrdinals[i])));
    }
    int dimensionValue = drillDownRecorder.getCount(dimOrdinal);
    facetResults.add(
        new FacetResult(
            "Author",
            new String[0],
            dimensionValue,
            labelsAndValues.toArray(new LabelAndValue[0]),
            labelsAndValues.size()));

    //// (5) Same process, but for Publish Date drill sideways dimension
    countComparable = ComparableUtils.byCount(publishDayDimensionRecorder);
    //// (4.1) Chain two ordinal iterators to get top N children
    dimOrdinal = ordLabels.getOrd(new FacetLabel("Publish Date"));
    childrenIterator =
        new TaxonomyChildrenOrdinalIterator(
            publishDayDimensionRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(),
            dimOrdinal);
    topByCountOrds = new TopnOrdinalIterator<>(childrenIterator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get
    // counts,
    // but OrdinalIterator only allows reading ordinals once.
    resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    labels = ordLabels.getLabels(resultOrdinals);
    labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(
          new LabelAndValue(
              labels[i].lastComponent(), publishDayDimensionRecorder.getCount(resultOrdinals[i])));
    }
    dimensionValue = publishDayDimensionRecorder.getCount(dimOrdinal);
    facetResults.add(
        new FacetResult(
            "Publish Date",
            new String[0],
            dimensionValue,
            labelsAndValues.toArray(new LabelAndValue[0]),
            labelsAndValues.size()));

    IOUtils.close(indexReader, taxoReader);
    return facetResults;
  }

  /** Runs the simple search example. */
  public List<FacetResult> runSimpleFacetsWithSearch() throws IOException {
    index();
    return simpleFacetsWithSearch();
  }

  /** Runs the simple drill sideways example. */
  public List<FacetResult> runSimpleFacetsWithDrillSideways() throws IOException {
    index();
    return simpleFacetsWithDrillSideways();
  }

  /** Runs the search example. */
  public List<FacetResult> runFacetOnly() throws IOException {
    index();
    return facetsOnly();
  }

  /** Runs the search example. */
  public List<FacetResult> runSearch() throws IOException {
    index();
    return facetsWithSearch();
  }

  /** Runs the drill-down example. */
  public FacetResult runDrillDown() throws IOException {
    index();
    return drillDown();
  }

  /** Runs the drill-sideways example. */
  public List<FacetResult> runDrillSideways() throws IOException {
    index();
    return drillSideways();
  }

  /** Runs the example of non overlapping range facets */
  public List<FacetResult> runNonOverlappingRangesCountFacetsOnly() throws IOException {
    index();
    return exclusiveRangesCountFacetsOnly();
  }

  /** Runs the example of overlapping range facets */
  public List<FacetResult> runOverlappingRangesCountFacetsOnly() throws IOException {
    index();
    return overlappingRangesCountFacetsOnly();
  }

  /** Runs the example of collecting long aggregations for non overlapping range facets. */
  public List<FacetResult> runNonOverlappingRangesAggregationFacets() throws IOException {
    index();
    return exclusiveRangesAggregationFacets();
  }

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    SandboxFacetsExample example = new SandboxFacetsExample();

    System.out.println("Simple facet counting example:");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runSimpleFacetsWithSearch()) {
      System.out.println(result);
    }

    System.out.println("Simple facet counting for drill sideways example:");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runSimpleFacetsWithDrillSideways()) {
      System.out.println(result);
    }

    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    List<FacetResult> results1 = example.runFacetOnly();
    System.out.println("Author: " + results1.get(0));
    System.out.println("Publish Date: " + results1.get(1));

    System.out.println("Facet counting example (combined facets and search):");
    System.out.println("-----------------------");
    List<FacetResult> results = example.runSearch();
    System.out.println("Author: " + results.get(0));
    System.out.println("Publish Date: " + results.get(1));

    System.out.println("Facet drill-down example (Publish Date/2010):");
    System.out.println("---------------------------------------------");
    System.out.println("Author: " + example.runDrillDown());

    System.out.println("Facet drill-sideways example (Publish Date/2010):");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runDrillSideways()) {
      System.out.println(result);
    }

    System.out.println("Facet counting example with exclusive ranges:");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runNonOverlappingRangesCountFacetsOnly()) {
      System.out.println(result);
    }

    System.out.println("Facet counting example with overlapping ranges:");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runOverlappingRangesCountFacetsOnly()) {
      System.out.println(result);
    }

    System.out.println("Facet aggregation example with exclusive ranges:");
    System.out.println("---------------------------------------------");
    for (FacetResult result : example.runNonOverlappingRangesAggregationFacets()) {
      System.out.println(result);
    }
  }
}
