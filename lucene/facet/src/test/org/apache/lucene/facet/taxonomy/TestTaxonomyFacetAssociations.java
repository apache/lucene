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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Test for associations */
public class TestTaxonomyFacetAssociations extends FacetTestCase {

  private static Directory dir;
  private static IndexReader reader;
  private static Directory taxoDir;
  private static TaxonomyReader taxoReader;

  private static FacetsConfig config;

  private static Map<String, List<Integer>> randomIntValues;
  private static Map<String, List<Float>> randomFloatValues;
  private static Map<String, List<Integer>> randomIntSingleValued;
  private static Map<String, List<Float>> randomFloatSingleValued;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dir = newDirectory();
    taxoDir = newDirectory();
    // preparations - index, taxonomy, content

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);

    // Cannot mix ints & floats in the same indexed field:
    config = new FacetsConfig();
    config.setIndexFieldName("int", "$facets.int");
    config.setMultiValued("int", true);
    config.setIndexFieldName("int_random", "$facets.int");
    config.setMultiValued("int_random", true);
    config.setIndexFieldName("int_single_valued", "$facets.int");
    config.setIndexFieldName("float", "$facets.float");
    config.setMultiValued("float", true);
    config.setIndexFieldName("float_random", "$facets.float");
    config.setMultiValued("float_random", true);
    config.setIndexFieldName("float_single_valued", "$facets.float");

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // index documents, 50% have only 'b' and all have 'a'
    for (int i = 0; i < 110; i++) {
      Document doc = new Document();
      // every 11th document is added empty, this used to cause the association
      // aggregators to go into an infinite loop
      if (i % 11 != 0) {
        doc.add(new IntAssociationFacetField(2, "int", "a"));
        doc.add(new FloatAssociationFacetField(0.5f, "float", "a"));
        if (i % 2 == 0) { // 50
          doc.add(new IntAssociationFacetField(3, "int", "b"));
          doc.add(new FloatAssociationFacetField(0.2f, "float", "b"));
        }
      }
      writer.addDocument(config.build(taxoWriter, doc));
    }

    // Also index random content for more random testing:
    String[] paths = new String[] {"a", "b", "c"};
    int count = random().nextInt(1000);
    randomIntValues = new HashMap<>();
    randomFloatValues = new HashMap<>();
    randomIntSingleValued = new HashMap<>();
    randomFloatSingleValued = new HashMap<>();
    for (int i = 0; i < count; i++) {
      Document doc = new Document();

      if (random().nextInt(10) >= 2) { // occasionally don't add any fields
        // Add up to five ordinals + values for each doc. Note that duplicates are totally fine:
        for (int j = 0; j < 5; j++) {
          String path = paths[random().nextInt(3)];
          if (random().nextBoolean()) { // maybe index an int association with the dim
            int nextInt = atLeast(1);
            randomIntValues.computeIfAbsent(path, k -> new ArrayList<>()).add(nextInt);
            doc.add(new IntAssociationFacetField(nextInt, "int_random", path));
          }
          if (random().nextBoolean()) { // maybe index a float association with the dim
            float nextFloat = random().nextFloat() * 10000f;
            randomFloatValues.computeIfAbsent(path, k -> new ArrayList<>()).add(nextFloat);
            doc.add(new FloatAssociationFacetField(nextFloat, "float_random", path));
          }
        }

        // Also, (maybe) add to the single-valued association fields:
        String path = paths[random().nextInt(3)];
        if (random().nextBoolean()) {
          int nextInt = atLeast(1);
          randomIntSingleValued.computeIfAbsent(path, k -> new ArrayList<>()).add(nextInt);
          doc.add(new IntAssociationFacetField(nextInt, "int_single_valued", path));
        }
        if (random().nextBoolean()) {
          float nextFloat = random().nextFloat() * 10000f;
          randomFloatSingleValued.computeIfAbsent(path, k -> new ArrayList<>()).add(nextFloat);
          doc.add(new FloatAssociationFacetField(nextFloat, "float_single_valued", path));
        }
      }

      writer.addDocument(config.build(taxoWriter, doc));
    }

    taxoWriter.close();
    reader = writer.getReader();
    writer.close();
    taxoReader = new DirectoryTaxonomyReader(taxoDir);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    reader = null;
    dir.close();
    dir = null;
    taxoReader.close();
    taxoReader = null;
    taxoDir.close();
    taxoDir = null;
  }

  public void testIntSumAssociation() throws Exception {

    IndexSearcher searcher = newSearcher(reader);
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new TaxonomyFacetIntAssociations(
            "$facets.int", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    assertEquals(
        "dim=int path=[] value=-1 childCount=2\n  a (200)\n  b (150)\n",
        facets.getTopChildren(10, "int").toString());
    assertEquals(
        "Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals(
        "Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  public void testIntAssociationRandom() throws Exception {

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);

    Map<String, Integer> expected;
    Facets facets;

    // SUM:
    facets =
        new TaxonomyFacetIntAssociations(
            "$facets.int", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : randomIntValues.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().reduce(Integer::sum).orElse(0));
    }
    validateInts("int_random", expected, AssociationAggregationFunction.SUM, true, facets);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : randomIntSingleValued.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().reduce(Integer::sum).orElse(0));
    }
    validateInts("int_single_valued", expected, AssociationAggregationFunction.SUM, false, facets);

    // MAX:
    facets =
        new TaxonomyFacetIntAssociations(
            "$facets.int", taxoReader, config, fc, AssociationAggregationFunction.MAX);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : randomIntValues.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().max(Integer::compareTo).orElse(0));
    }
    validateInts("int_random", expected, AssociationAggregationFunction.MAX, true, facets);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Integer>> e : randomIntSingleValued.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().max(Integer::compareTo).orElse(0));
    }
    validateInts("int_single_valued", expected, AssociationAggregationFunction.MAX, false, facets);
  }

  public void testFloatSumAssociation() throws Exception {
    IndexSearcher searcher = newSearcher(reader);
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new TaxonomyFacetFloatAssociations(
            "$facets.float", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    assertEquals(
        "dim=float path=[] value=-1.0 childCount=2\n  a (50.0)\n  b (9.999995)\n",
        facets.getTopChildren(10, "float").toString());
    assertEquals(
        "Wrong count for category 'a'!",
        50f,
        facets.getSpecificValue("float", "a").floatValue(),
        0.00001);
    assertEquals(
        "Wrong count for category 'b'!",
        10f,
        facets.getSpecificValue("float", "b").floatValue(),
        0.00001);
  }

  public void testFloatAssociationRandom() throws Exception {

    FacetsCollector fc = new FacetsCollector();

    IndexSearcher searcher = newSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), fc);

    Map<String, Float> expected;
    Facets facets;

    // SUM:
    facets =
        new TaxonomyFacetFloatAssociations(
            "$facets.float", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Float>> e : randomFloatValues.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().reduce(Float::sum).orElse(0f));
    }
    validateFloats("float_random", expected, AssociationAggregationFunction.SUM, true, facets);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Float>> e : randomFloatSingleValued.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().reduce(Float::sum).orElse(0f));
    }
    validateFloats(
        "float_single_valued", expected, AssociationAggregationFunction.SUM, false, facets);

    // MAX:
    facets =
        new TaxonomyFacetFloatAssociations(
            "$facets.float", taxoReader, config, fc, AssociationAggregationFunction.MAX);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Float>> e : randomFloatValues.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().max(Float::compareTo).orElse(0f));
    }
    validateFloats("float_random", expected, AssociationAggregationFunction.MAX, true, facets);
    expected = new HashMap<>();
    for (Map.Entry<String, List<Float>> e : randomFloatSingleValued.entrySet()) {
      expected.put(e.getKey(), e.getValue().stream().max(Float::compareTo).orElse(0f));
    }
    validateFloats(
        "float_single_valued", expected, AssociationAggregationFunction.MAX, false, facets);
  }

  /**
   * Make sure we can test both int and float assocs in one index, as long as we send each to a
   * different field.
   */
  public void testIntAndFloatAssocation() throws Exception {
    IndexSearcher searcher = newSearcher(reader);
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());

    Facets facets =
        new TaxonomyFacetFloatAssociations(
            "$facets.float", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    assertEquals(
        "Wrong count for category 'a'!",
        50f,
        facets.getSpecificValue("float", "a").floatValue(),
        0.00001);
    assertEquals(
        "Wrong count for category 'b'!",
        10f,
        facets.getSpecificValue("float", "b").floatValue(),
        0.00001);

    facets =
        new TaxonomyFacetIntAssociations(
            "$facets.int", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    assertEquals(
        "Wrong count for category 'a'!", 200, facets.getSpecificValue("int", "a").intValue());
    assertEquals(
        "Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  public void testWrongIndexFieldName() throws Exception {
    IndexSearcher searcher = newSearcher(reader);
    FacetsCollector fc = searcher.search(new MatchAllDocsQuery(), new FacetsCollectorManager());
    Facets facets =
        new TaxonomyFacetFloatAssociations(
            "wrong_field", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getSpecificValue("float");
        });

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          facets.getTopChildren(10, "float");
        });
  }

  public void testMixedTypesInSameIndexField() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    doc.add(new FloatAssociationFacetField(55.0f, "b", "y"));
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          writer.addDocument(config.build(taxoWriter, doc));
        });
    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testNoHierarchy() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setHierarchical("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          writer.addDocument(config.build(taxoWriter, doc));
        });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testRequireDimCount() throws Exception {
    Directory dir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetsConfig config = new FacetsConfig();
    config.setRequireDimCount("a", true);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new IntAssociationFacetField(14, "a", "x"));
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          writer.addDocument(config.build(taxoWriter, doc));
        });

    writer.close();
    IOUtils.close(taxoWriter, dir, taxoDir);
  }

  public void testIntSumAssociationDrillDown() throws Exception {
    IndexSearcher searcher = newSearcher(reader);
    DrillDownQuery q = new DrillDownQuery(config);
    q.add("int", "b");
    FacetsCollector fc = searcher.search(q, new FacetsCollectorManager());

    Facets facets =
        new TaxonomyFacetIntAssociations(
            "$facets.int", taxoReader, config, fc, AssociationAggregationFunction.SUM);
    assertEquals(
        "dim=int path=[] value=-1 childCount=2\n  b (150)\n  a (100)\n",
        facets.getTopChildren(10, "int").toString());
    assertEquals(
        "Wrong count for category 'a'!", 100, facets.getSpecificValue("int", "a").intValue());
    assertEquals(
        "Wrong count for category 'b'!", 150, facets.getSpecificValue("int", "b").intValue());
  }

  private void validateInts(
      String dim,
      Map<String, Integer> expected,
      AssociationAggregationFunction aggregationFunction,
      boolean isMultiValued,
      Facets facets)
      throws IOException {
    int aggregatedValue = 0;
    for (Map.Entry<String, Integer> e : expected.entrySet()) {
      int value = e.getValue();
      assertEquals(value, facets.getSpecificValue(dim, e.getKey()).intValue());
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
    }

    if (isMultiValued) {
      aggregatedValue = -1;
    }

    FacetResult facetResult = facets.getTopChildren(10, dim);
    assertEquals(dim, facetResult.dim);
    assertEquals(aggregatedValue, facetResult.value.intValue());
    assertEquals(expected.size(), facetResult.childCount);
  }

  private void validateFloats(
      String dim,
      Map<String, Float> expected,
      AssociationAggregationFunction aggregationFunction,
      boolean isMultiValued,
      Facets facets)
      throws IOException {
    float aggregatedValue = 0f;
    for (Map.Entry<String, Float> e : expected.entrySet()) {
      float value = e.getValue();
      assertEquals(value, facets.getSpecificValue(dim, e.getKey()).floatValue(), 1);
      aggregatedValue = aggregationFunction.aggregate(aggregatedValue, value);
    }

    if (isMultiValued) {
      aggregatedValue = -1;
    }

    FacetResult facetResult = facets.getTopChildren(10, dim);
    assertEquals(dim, facetResult.dim);
    assertEquals(aggregatedValue, facetResult.value.floatValue(), 1);
    assertEquals(expected.size(), facetResult.childCount);
  }
}
