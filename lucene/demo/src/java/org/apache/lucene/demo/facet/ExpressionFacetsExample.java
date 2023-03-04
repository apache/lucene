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
import java.text.ParseException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;
import org.apache.lucene.facet.taxonomy.ExpressionFacets;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetFloatAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/** Example of expression faceting */
public class ExpressionFacetsExample {
  private final Directory indexDir = new ByteBuffersDirectory();
  private final Directory taxoDir = new ByteBuffersDirectory();
  private final FacetsConfig config = new FacetsConfig();
  private final SimpleBindings bindings = new SimpleBindings();

  public static void main(String[] args) throws Exception {
    // Seattle as demo user location:
    float userLat = 47.6062f;
    float userLon = -122.3321f;

    // Assign each facet label a weight based on inverse average distance of all its cities and
    // average population of all its cities.
    String facetExpression =
        "1000 * ln(1 + (1 / (val_distance_sum / val_ONE_sum))) + (val_population_sum / val_ONE_sum)";

    ExpressionFacetsExample example = new ExpressionFacetsExample();
    example.index();
    example.setupBindings(userLat, userLon);

    System.out.println("Facet by count:");
    System.out.println("-----------------------");
    printFacetResults(example.facetByCount());
    System.out.println();

    System.out.println("Facet by SUM(distance):");
    System.out.println("-----------------------");
    printFacetResults(example.facetBySumDistance());
    System.out.println();

    System.out.println("Facet by SUM(population):");
    System.out.println("-----------------------");
    printFacetResults(example.facetBySumPopulation());
    System.out.println();

    System.out.println("Facet by " + facetExpression + ":");
    System.out.println("-----------------------");
    printFacetResults(example.facetByExpression(facetExpression));
    System.out.println();
  }

  private static void printFacetResults(FacetResult facetResult) {
    for (LabelAndValue e : facetResult.labelValues) {
      System.out.println(e);
    }
  }

  private void setupBindings(float userLat, float userLon) throws ParseException {
    // Simple dv fields:
    bindings.add("population", DoubleValuesSource.fromLongField("population"));
    bindings.add("latitude", DoubleValuesSource.fromFloatField("latitude"));
    bindings.add("longitude", DoubleValuesSource.fromFloatField("longitude"));

    // Constant value used for counting:
    bindings.add("ONE", DoubleValuesSource.constant(1d));

    // Computed distance from user position to doc:
    bindings.add(
        "distance",
        JavascriptCompiler.compile(
            String.format("haversin(%f,%f,latitude,longitude)", userLat, userLon)));
  }

  private FacetResult facetByCount() throws IOException {
    try (IndexReader indexReader = DirectoryReader.open(indexDir);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Facets facets =
          new FastTaxonomyFacetCounts(
              FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
      return facets.getTopChildren(10, "country");
    }
  }

  private FacetResult facetBySumDistance() throws IOException {
    try (IndexReader indexReader = DirectoryReader.open(indexDir);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      DoubleValuesSource distance = bindings.getDoubleValuesSource("distance");
      Facets facets =
          new TaxonomyFacetFloatAssociations(
              FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
              taxoReader,
              config,
              fc,
              AssociationAggregationFunction.SUM,
              distance);
      return facets.getTopChildren(10, "country");
    }
  }

  private FacetResult facetBySumPopulation() throws IOException {
    try (IndexReader indexReader = DirectoryReader.open(indexDir);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      DoubleValuesSource population = bindings.getDoubleValuesSource("population");
      Facets facets =
          new TaxonomyFacetFloatAssociations(
              FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
              taxoReader,
              config,
              fc,
              AssociationAggregationFunction.SUM,
              population);
      return facets.getTopChildren(10, "country");
    }
  }

  private FacetResult facetByExpression(String facetExpression) throws IOException, ParseException {
    try (IndexReader indexReader = DirectoryReader.open(indexDir);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {
      IndexSearcher searcher = new IndexSearcher(indexReader);
      FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
      Facets facets =
          new ExpressionFacets(
              FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
              taxoReader,
              config,
              fc,
              facetExpression,
              bindings);
      return facets.getTopChildren(10, "country");
    }
  }

  private void index() throws IOException {
    IndexWriterConfig writerConfig = new IndexWriterConfig();
    try (IndexWriter indexWriter = new IndexWriter(indexDir, writerConfig);
        TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir)) {
      Document doc = new Document();
      doc.add(new KeywordField("name", "San Francisco", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 815201));
      doc.add(new FloatDocValuesField("latitude", 37.7775f));
      doc.add(new FloatDocValuesField("longitude", -122.4163f));
      doc.add(new FacetField("country", "US"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "New York City", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 8804190));
      doc.add(new FloatDocValuesField("latitude", 40.7127f));
      doc.add(new FloatDocValuesField("longitude", -74.0061f));
      doc.add(new FacetField("country", "US"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Paris", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 2165423));
      doc.add(new FloatDocValuesField("latitude", 48.8566f));
      doc.add(new FloatDocValuesField("longitude", 2.3522f));
      doc.add(new FacetField("country", "FR"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Nice", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 342522));
      doc.add(new FloatDocValuesField("latitude", 43.7034f));
      doc.add(new FloatDocValuesField("longitude", 7.2663f));
      doc.add(new FacetField("country", "FR"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Barcelona", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 1620343));
      doc.add(new FloatDocValuesField("latitude", 41.3833f));
      doc.add(new FloatDocValuesField("longitude", 2.1833f));
      doc.add(new FacetField("country", "ES"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Madrid", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 3223334));
      doc.add(new FloatDocValuesField("latitude", 40.4169f));
      doc.add(new FloatDocValuesField("longitude", -3.7033f));
      doc.add(new FacetField("country", "ES"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Dublin", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 544107));
      doc.add(new FloatDocValuesField("latitude", 53.35f));
      doc.add(new FloatDocValuesField("longitude", -6.2602f));
      doc.add(new FacetField("country", "IE"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Cork", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 124391));
      doc.add(new FloatDocValuesField("latitude", 51.8972f));
      doc.add(new FloatDocValuesField("longitude", -8.47f));
      doc.add(new FacetField("country", "IE"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Toronto", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 2794356));
      doc.add(new FloatDocValuesField("latitude", 43.7416f));
      doc.add(new FloatDocValuesField("longitude", -79.3733f));
      doc.add(new FacetField("country", "CA"));
      indexWriter.addDocument(config.build(taxoWriter, doc));

      doc = new Document();
      doc.add(new KeywordField("name", "Vancouver", Field.Store.YES));
      doc.add(new NumericDocValuesField("population", 675218));
      doc.add(new FloatDocValuesField("latitude", 49.2611f));
      doc.add(new FloatDocValuesField("longitude", -123.1138f));
      doc.add(new FacetField("country", "CA"));
      indexWriter.addDocument(config.build(taxoWriter, doc));
    }
  }
}
