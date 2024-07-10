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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sandbox.FacetFieldCollector;
import org.apache.lucene.facet.sandbox.FacetFieldCollectorManager;
import org.apache.lucene.facet.sandbox.abstracts.OrdToComparable;
import org.apache.lucene.facet.sandbox.abstracts.OrdToLabels;
import org.apache.lucene.facet.sandbox.abstracts.OrdinalIterator;
import org.apache.lucene.facet.sandbox.aggregations.CountRecorder;
import org.apache.lucene.facet.sandbox.aggregations.OrdRank;
import org.apache.lucene.facet.sandbox.aggregations.SortOrdinalIterator;
import org.apache.lucene.facet.sandbox.taxonomy.TaxonomyChildrenOrdinalIterator;
import org.apache.lucene.facet.sandbox.taxonomy.TaxonomyFacetsCutter;
import org.apache.lucene.facet.sandbox.taxonomy.TaxonomyOrdLabels;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.CollectorOwner;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.facet.FacetsConfig.DEFAULT_INDEX_FIELD_NAME;

/** Demo for sandbox faceting. */
public class SandboxFacetsExample {

  private final Directory indexDir = new ByteBuffersDirectory();
  private final Directory taxoDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();

  /** Empty constructor */
  public SandboxFacetsExample() {
    config.setHierarchical("Publish Date", true);
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
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2010", "10", "20"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Lisa"));
    doc.add(new FacetField("Publish Date", "2012", "1", "1"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Susan"));
    doc.add(new FacetField("Publish Date", "2012", "1", "7"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    doc = new Document();
    doc.add(new FacetField("Author", "Frank"));
    doc.add(new FacetField("Publish Date", "1999", "5", "5"));
    indexWriter.addDocument(config.build(taxoWriter, doc));

    IOUtils.close(indexWriter, taxoWriter);
  }

  /** User runs a query and counts facets only without collecting the matching documents. */
  List<FacetResult> facetsOnly() throws IOException {
    //// (1) init readers
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collector
    TaxonomyFacetsCutter defaultTaxoCutter = new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountRecorder defaultRecorder = new CountRecorder();

    FacetFieldCollectorManager<CountRecorder> collectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultTaxoCutter, defaultRecorder);

    //// (2.1) if we need to collect data using multiple different collectors, e.g. taxonomy and ranges,
    ////       or even two taxonomy facets that use different Category List Field, we can use MultiCollectorManager, e.g.:
    // TODO: add a demo for it.
    // TaxonomyFacetsCutter publishDateCutter = new TaxonomyFacetsCutter(config.getDimConfig("Publish Date"), taxoReader);
    // CountRecorder publishDateRecorder = new CountRecorder();
    // FacetFieldCollectorManager<CountRecorder> publishDateCollectorManager = new FacetFieldCollectorManager<>(publishDateCutter, publishDateRecorder);
    // MultiCollectorManager drillDownCollectorManager = new MultiCollectorManager(authorCollectorManager, publishDateCollectorManager);
    // Object[] results = searcher.search(new MatchAllDocsQuery(), drillDownCollectorManager);

    //// (3) search
    // Right now we return the same Recorder we created - so we can ignore results
    CountRecorder searchResults = searcher.search(new MatchAllDocsQuery(), collectorManager);

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    OrdToComparable<OrdRank> countComparable = new OrdRank.Comparable(defaultRecorder);
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    List<FacetResult> results = new ArrayList<>(2);
    // This object provides labels for ordinals.
    OrdToLabels ordLabels = new TaxonomyOrdLabels(taxoReader);
    for (String dimension: List.of("Author", "Publish Date")) {
      //// (4.1) Chain two ordinal iterators to get top N children
      OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(defaultRecorder.recordedOrds(), taxoReader.getParallelTaxonomyArrays()
              .parents(), taxoReader.getOrdinal(dimension));
      OrdinalIterator topByCountOrds = new SortOrdinalIterator<>(childrenIternator, countComparable, 10);
      // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
      // but OrdinalIterator only allows reading ordinals once.
      int[] resultOrdinals = topByCountOrds.toArray();

      //// (4.2) Use faceting results
      FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
      List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
      for (int i = 0; i < resultOrdinals.length; i++) {
        labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), defaultRecorder.getCount(resultOrdinals[i])));
      }
      // TODO fix value and childCount
      results.add(new FacetResult(dimension, new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));
    }

    IOUtils.close(indexReader, taxoReader);
    return results;
  }

  /** User runs a query and counts facets.*/
  private List<FacetResult> facetsWithSearch() throws IOException {
    //// (1) init readers
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collectors
    // Facet collectors
    TaxonomyFacetsCutter defaultTaxoCutter = new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountRecorder defaultRecorder = new CountRecorder();
    FacetFieldCollectorManager<CountRecorder> taxoFacetsCollectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter, defaultTaxoCutter, defaultRecorder);
    // Hits collector
    TopScoreDocCollectorManager hitsCollectorManager = new TopScoreDocCollectorManager(2, Integer.MAX_VALUE);
    // Now wrap them with MultiCollectorManager to collect both hits and facets.
    MultiCollectorManager collectorManager = new MultiCollectorManager(hitsCollectorManager, taxoFacetsCollectorManager);

    //// (3) search
    Object[] results = searcher.search(new MatchAllDocsQuery(), collectorManager);
    TopDocs topDocs = (TopDocs) results[0];
    System.out.println("Search results: totalHits: " + topDocs.totalHits + ", collected hits: " + topDocs.scoreDocs.length);
    // FacetFieldCollectorManager returns the same Recorder it gets - so we can ignore read the results from original recorder
    // and ignore this value.
    //CountRecorder defaultRecorder = (CountRecorder) results[1];

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    OrdToComparable<OrdRank> countComparable = new OrdRank.Comparable(defaultRecorder);
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    List<FacetResult> facetResults = new ArrayList<>(2);
    // This object provides labels for ordinals.
    OrdToLabels ordLabels = new TaxonomyOrdLabels(taxoReader);
    for (String dimension: List.of("Author", "Publish Date")) {
      //// (4.1) Chain two ordinal iterators to get top N children
      OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(defaultRecorder.recordedOrds(), taxoReader.getParallelTaxonomyArrays()
              .parents(), taxoReader.getOrdinal(dimension));
      OrdinalIterator topByCountOrds = new SortOrdinalIterator<>(childrenIternator, countComparable, 10);
      // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
      // but OrdinalIterator only allows reading ordinals once.
      int[] resultOrdinals = topByCountOrds.toArray();

      //// (4.2) Use faceting results
      FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
      List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
      for (int i = 0; i < resultOrdinals.length; i++) {
        labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), defaultRecorder.getCount(resultOrdinals[i])));
      }
      // TODO fix value and childCount
      facetResults.add(new FacetResult(dimension, new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));
    }

    IOUtils.close(indexReader, taxoReader);
    return facetResults;
  }

  /** User drills down on 'Publish Date/2010', and we return facets for 'Author' */
  FacetResult drillDown() throws IOException {
    //// (1) init readers
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init collector
    TaxonomyFacetsCutter defaultTaxoCutter = new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountRecorder defaultRecorder = new CountRecorder();

    FacetFieldCollectorManager<CountRecorder> collectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter,
            defaultTaxoCutter, defaultRecorder);

    DrillDownQuery q = new DrillDownQuery(config);
    q.add("Publish Date", "2010");

    //// (3) search
    // Right now we return the same Recorder we created - so we can ignore results
    CountRecorder searchResults = searcher.search(q, collectorManager);

    //// (4) Get top 10 results by count for Author and Publish Date
    // This object is used to get topN results by count
    OrdToComparable<OrdRank> countComparable = new OrdRank.Comparable(defaultRecorder);

    // This object provides labels for ordinals.
    OrdToLabels ordLabels = new TaxonomyOrdLabels(taxoReader);
    String dimension = "Author";
    //// (4.1) Chain two ordinal iterators to get top N children
    OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(defaultRecorder.recordedOrds(),
            taxoReader.getParallelTaxonomyArrays().parents(), taxoReader.getOrdinal(dimension));
    OrdinalIterator topByCountOrds = new SortOrdinalIterator<>(childrenIternator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), defaultRecorder.getCount(resultOrdinals[i])));
    }

    IOUtils.close(indexReader, taxoReader);
    // TODO fix value and childCount
    // We don't actually need to use FacetResult, it is up to client what to do with the results.
    // Here we just want to demo that we can still do FacetResult as well
    return new FacetResult(dimension, new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0);
  }

  /**
   * User drills down on 'Publish Date/2010', and we return facets for both 'Publish Date' and
   * 'Author', using DrillSideways.
   */
  private List<FacetResult> drillSideways() throws IOException {
    //// (1) init readers
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

    //// (2) init drill down query and collectors
    TaxonomyFacetsCutter defaultTaxoCutter = new TaxonomyFacetsCutter(DEFAULT_INDEX_FIELD_NAME, config, taxoReader);
    CountRecorder drillDownRecorder = new CountRecorder();
    FacetFieldCollectorManager<CountRecorder> drillDownCollectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter,
            defaultTaxoCutter, drillDownRecorder);

    DrillDownQuery q = new DrillDownQuery(config);

    //// (2.1) add query and collector dimensions
    q.add("Publish Date", "2010");
    CountRecorder publishDayDimensionRecorder = new CountRecorder();
    // Note that it is safe to use the same FacetsCutter here because collection for all dimensions
    // is synconized, i.e. we never collect doc ID that is less than current doc ID across all dimensions.
    FacetFieldCollectorManager<CountRecorder> publishDayDimensionCollectorManager = new FacetFieldCollectorManager<>(defaultTaxoCutter,
            defaultTaxoCutter, publishDayDimensionRecorder);
    List<CollectorOwner<FacetFieldCollector, CountRecorder>> drillSidewaysOwners = List.of(CollectorOwner.hire(
            publishDayDimensionCollectorManager));

    //// (3) search
    // Right now we return the same Recorder we created - so we can ignore results
    DrillSideways ds = new DrillSideways(searcher, config, taxoReader);
    // We must wrap list of drill sideways owner with unmodifiableList to make generics work.
    ds.search(q, CollectorOwner.hire(drillDownCollectorManager), Collections.unmodifiableList(drillSidewaysOwners), true);

    //// (4) Get top 10 results by count for Author
    List<FacetResult> facetResults = new ArrayList<>(2);
    // This object provides labels for ordinals.
    OrdToLabels ordLabels = new TaxonomyOrdLabels(taxoReader);
    // This object is used to get topN results by count
    OrdToComparable<OrdRank> countComparable = new OrdRank.Comparable(drillDownRecorder);
    //// (4.1) Chain two ordinal iterators to get top N children
    OrdinalIterator childrenIternator = new TaxonomyChildrenOrdinalIterator(drillDownRecorder.recordedOrds(), taxoReader.getParallelTaxonomyArrays()
            .parents(), taxoReader.getOrdinal("Author"));
    OrdinalIterator topByCountOrds = new SortOrdinalIterator<>(childrenIternator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
    // but OrdinalIterator only allows reading ordinals once.
    int[] resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    FacetLabel[] labels = ordLabels.getLabels(resultOrdinals);
    List<LabelAndValue> labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), drillDownRecorder.getCount(resultOrdinals[i])));
    }
    // TODO fix value and childCount
    facetResults.add(new FacetResult("Author", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    //// (5) Same process, but for Publish Date drill sideways dimension
    countComparable = new OrdRank.Comparable(publishDayDimensionRecorder);
    //// (4.1) Chain two ordinal iterators to get top N children
    childrenIternator = new TaxonomyChildrenOrdinalIterator(publishDayDimensionRecorder.recordedOrds(), taxoReader.getParallelTaxonomyArrays()
            .parents(), taxoReader.getOrdinal("Publish Date"));
    topByCountOrds = new SortOrdinalIterator<>(childrenIternator, countComparable, 10);
    // Get array of final ordinals - we need to use all of them to get labels first, and then to get counts,
    // but OrdinalIterator only allows reading ordinals once.
    resultOrdinals = topByCountOrds.toArray();

    //// (4.2) Use faceting results
    labels = ordLabels.getLabels(resultOrdinals);
    labelsAndValues = new ArrayList<>(labels.length);
    for (int i = 0; i < resultOrdinals.length; i++) {
      labelsAndValues.add(new LabelAndValue(labels[i].getLeaf(), publishDayDimensionRecorder.getCount(resultOrdinals[i])));
    }
    // TODO fix value and childCount
    facetResults.add(new FacetResult("Publish Date", new String[0], 0, labelsAndValues.toArray(new LabelAndValue[0]), 0));

    IOUtils.close(indexReader, taxoReader);
    return facetResults;
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

  /** Runs the search and drill-down examples and prints the results. */
  public static void main(String[] args) throws Exception {
    System.out.println("Facet counting example:");
    System.out.println("-----------------------");
    SandboxFacetsExample example = new SandboxFacetsExample();
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
  }
}
