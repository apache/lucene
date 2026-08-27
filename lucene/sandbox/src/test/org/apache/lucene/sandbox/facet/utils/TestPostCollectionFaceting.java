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
package org.apache.lucene.sandbox.facet.utils;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NamedThreadFactory;

public class TestPostCollectionFaceting extends LuceneTestCase {

  private final List<DirectoryReader> testReaders = new ArrayList<>();
  private final List<Directory> testDirs = new ArrayList<>();

  @Override
  public void tearDown() throws Exception {
    for (DirectoryReader reader : testReaders) {
      reader.close();
    }
    for (Directory dir : testDirs) {
      dir.close();
    }
    super.tearDown();
  }

  public void testBasic() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(2);
    LeafReaderContext ctx0 = contexts.get(0);
    LeafReaderContext ctx1 = contexts.get(1);

    TestFacetsCollector drillDownFacets = createFacetsCollector(ctx0, 2, 3);
    addMatchingDocs(drillDownFacets, ctx1, 5);

    TestFacetsCollector dim1Facets = createFacetsCollector(ctx0, 1, 2, 6);
    addMatchingDocs(dim1Facets, ctx1, 4, 7);

    TestFacetsCollector dim2Facets = createFacetsCollector(ctx0, 2, 3, 8);
    addMatchingDocs(dim2Facets, ctx1, 5, 9);

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", dim1Facets);
    drillSidewaysFacets.put("dim2", dim2Facets);

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());
    drillSidewaysManagers.put("dim2", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                drillDownManager,
                drillSidewaysManagers,
                drillDownFacets,
                drillSidewaysFacets,
                null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();

    assertNotNull(result.drillDownResult());
    assertEquals(3, result.drillDownResult().totalDocs);
    assertEquals(Set.of(2, 3, 5), result.drillDownResult().allDocIds);

    assertNotNull(result.drillSidewaysResults());
    assertEquals(2, result.drillSidewaysResults().size());

    DocCountResult dim1Result = result.drillSidewaysResults().get("dim1");
    assertNotNull(dim1Result);
    assertEquals(5, dim1Result.totalDocs);
    assertEquals(Set.of(1, 2, 6, 4, 7), dim1Result.allDocIds);

    DocCountResult dim2Result = result.drillSidewaysResults().get("dim2");
    assertNotNull(dim2Result);
    assertEquals(5, dim2Result.totalDocs);
    assertEquals(Set.of(2, 3, 8, 5, 9), dim2Result.allDocIds);
  }

  public void testEmptySlices() throws IOException {
    TestFacetsCollector drillDownFacets = new TestFacetsCollector();
    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", new TestFacetsCollector());

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                drillDownManager,
                drillSidewaysManagers,
                drillDownFacets,
                drillSidewaysFacets,
                null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();

    assertNotNull(result.drillDownResult());
    assertEquals(0, result.drillDownResult().totalDocs);
    assertEquals(1, result.drillSidewaysResults().size());
    assertEquals(0, result.drillSidewaysResults().get("dim1").totalDocs);
  }

  public void testScoreCollector() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(1);
    TestFacetsCollector drillDownFacets =
        createFacetsCollectorWithScores(
            contexts.get(0), new int[] {0, 1}, new float[] {1.0f, 2.0f});

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put(
        "dim1",
        createFacetsCollectorWithScores(contexts.get(0), new int[] {0}, new float[] {1.5f}));

    ScoreCollectorManager drillDownManager = new ScoreCollectorManager();
    Map<String, ScoreCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new ScoreCollectorManager());

    PostCollectionFaceting<ScoreCollector, Float, ScoreCollector, Float> faceting =
        new PostCollectionFaceting<>(
            drillDownManager, drillSidewaysManagers, drillDownFacets, drillSidewaysFacets, null);

    PostCollectionFaceting.Result<Float, Float> result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(3.0f, result.drillDownResult(), 0.001f);
    assertEquals(1.5f, result.drillSidewaysResults().get("dim1"), 0.001f);
  }

  public void testScoreCollectorWithoutScores() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(1);
    TestFacetsCollector drillDownFacets = createFacetsCollector(contexts.get(0), 1, 2);

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", createFacetsCollector(contexts.get(0), 1));

    ScoreCollectorManager drillDownManager = new ScoreCollectorManager();
    Map<String, ScoreCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new ScoreCollectorManager());

    PostCollectionFaceting<ScoreCollector, Float, ScoreCollector, Float> faceting =
        new PostCollectionFaceting<>(
            drillDownManager, drillSidewaysManagers, drillDownFacets, drillSidewaysFacets, null);

    try {
      faceting.collect();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Collector requires scores"));
    }
  }

  public void testMultipleSlices() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(3);

    TestFacetsCollector drillDownFacets =
        createLargeFacetsCollector(contexts, PostCollectionFaceting.MIN_DOCS_PER_SLICE);
    TestFacetsCollector dim1Facets =
        createLargeFacetsCollector(contexts, PostCollectionFaceting.MIN_DOCS_PER_SLICE * 2);

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", dim1Facets);

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                drillDownManager,
                drillSidewaysManagers,
                drillDownFacets,
                drillSidewaysFacets,
                null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(PostCollectionFaceting.MIN_DOCS_PER_SLICE, result.drillDownResult().totalDocs);
    assertEquals(
        PostCollectionFaceting.MIN_DOCS_PER_SLICE * 2,
        result.drillSidewaysResults().get("dim1").totalDocs);
  }

  public void testNullDrillSideways() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(1);
    TestFacetsCollector drillDownFacets = createFacetsCollector(contexts.get(0), 1, 2);

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting = new PostCollectionFaceting<>(drillDownManager, drillDownFacets, null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(2, result.drillDownResult().totalDocs);
    assertEquals(Set.of(1, 2), result.drillDownResult().allDocIds);
    assertNotNull(result.drillSidewaysResults());
    assertEquals(0, result.drillSidewaysResults().size());
  }

  public void testNullDrillDownManager() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(1);
    TestFacetsCollector drillDownFacets = createFacetsCollector(contexts.get(0), 1, 2);
    TestFacetsCollector dim1Facets = createFacetsCollector(contexts.get(0), 1);

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", dim1Facets);

    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                null, drillSidewaysManagers, drillDownFacets, drillSidewaysFacets, null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();
    assertNull(result.drillDownResult());
    assertNotNull(result.drillSidewaysResults());
    assertEquals(1, result.drillSidewaysResults().size());
    assertEquals(1, result.drillSidewaysResults().get("dim1").totalDocs);
  }

  public void testNullMatchingDocs() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(2);
    LeafReaderContext ctx0 = contexts.get(0);
    LeafReaderContext ctx1 = contexts.get(1);

    // Create drill down facets with docs in both contexts
    TestFacetsCollector drillDownFacets = createFacetsCollector(ctx0, 1, 2);
    addMatchingDocs(drillDownFacets, ctx1, 3);

    // Create drill sideways facets with docs only in ctx0, leaving ctx1 with null matching docs
    TestFacetsCollector dim1Facets = createFacetsCollector(ctx0, 1);
    // Intentionally not adding matching docs for ctx1 to create null scenario

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", dim1Facets);

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                drillDownManager,
                drillSidewaysManagers,
                drillDownFacets,
                drillSidewaysFacets,
                null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(3, result.drillDownResult().totalDocs);
    assertEquals(1, result.drillSidewaysResults().get("dim1").totalDocs);
  }

  public void testMismatchedDimensionMaps() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(1);

    // Scenario 1: FacetsCollector is missing for a dimension
    TestFacetsCollector drillDownFacets = createFacetsCollector(contexts.get(0), 1, 2);

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", createFacetsCollector(contexts.get(0), 1));

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());
    drillSidewaysManagers.put("dim2", new DocCountCollectorManager());

    PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
        faceting =
            new PostCollectionFaceting<>(
                drillDownManager,
                drillSidewaysManagers,
                drillDownFacets,
                drillSidewaysFacets,
                null);

    PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(2, result.drillDownResult().totalDocs);
    assertEquals(1, result.drillSidewaysResults().size());
    assertTrue(result.drillSidewaysResults().containsKey("dim1"));
    assertFalse(result.drillSidewaysResults().containsKey("dim2"));

    // Scenario 1: collector manager is missing for a dimension
    drillDownFacets = createFacetsCollector(contexts.get(0), 1, 2);

    drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", createFacetsCollector(contexts.get(0), 1));
    drillSidewaysFacets.put("dim2", createFacetsCollector(contexts.get(0), 1, 2));

    drillDownManager = new DocCountCollectorManager();
    drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    faceting =
        new PostCollectionFaceting<>(
            drillDownManager, drillSidewaysManagers, drillDownFacets, drillSidewaysFacets, null);

    result = faceting.collect();
    assertNotNull(result.drillDownResult());
    assertEquals(2, result.drillDownResult().totalDocs);
    assertEquals(1, result.drillSidewaysResults().size());
    assertTrue(result.drillSidewaysResults().containsKey("dim1"));
    assertFalse(result.drillSidewaysResults().containsKey("dim2"));
  }

  public void testRandomConcurrentExecution() throws IOException {
    List<LeafReaderContext> contexts = createLeafContexts(4);

    // Ensure enough docs to create 3 slices (threshold is MIN_DOCS_PER_SLICE)
    int docsPerLeaf = PostCollectionFaceting.MIN_DOCS_PER_SLICE * 3 / contexts.size() + 10;

    var drillDownResult = createRandomFacetsCollector(contexts, docsPerLeaf);
    TestFacetsCollector drillDownFacets = drillDownResult.collector();
    Set<Integer> expectedDrillDownDocs = drillDownResult.allDocIds();

    var dim1Result = createRandomFacetsCollector(contexts, 5);
    TestFacetsCollector dim1Facets = dim1Result.collector();
    Set<Integer> expectedDim1Docs = dim1Result.allDocIds();

    Map<String, FacetsCollector> drillSidewaysFacets = new HashMap<>();
    drillSidewaysFacets.put("dim1", dim1Facets);

    DocCountCollectorManager drillDownManager = new DocCountCollectorManager();
    Map<String, DocCountCollectorManager> drillSidewaysManagers = new HashMap<>();
    drillSidewaysManagers.put("dim1", new DocCountCollectorManager());

    // Use multi-threaded executor
    ExecutorService executor =
        newFixedThreadPool(3, new NamedThreadFactory("TestPostCollectionFaceting"));
    try {
      PostCollectionFaceting<DocCountCollector, DocCountResult, DocCountCollector, DocCountResult>
          faceting =
              new PostCollectionFaceting<>(
                  drillDownManager,
                  drillSidewaysManagers,
                  drillDownFacets,
                  drillSidewaysFacets,
                  executor);

      PostCollectionFaceting.Result<DocCountResult, DocCountResult> result = faceting.collect();

      // Assert exact doc IDs match
      assertEquals(expectedDrillDownDocs.size(), result.drillDownResult().totalDocs);
      assertEquals(expectedDrillDownDocs, result.drillDownResult().allDocIds);

      assertEquals(expectedDim1Docs.size(), result.drillSidewaysResults().get("dim1").totalDocs);
      assertEquals(expectedDim1Docs, result.drillSidewaysResults().get("dim1").allDocIds);
    } finally {
      executor.shutdown();
    }
  }

  // Helper methods

  private record FacetsCollectorData(TestFacetsCollector collector, Set<Integer> allDocIds) {}

  private FacetsCollectorData createRandomFacetsCollector(
      List<LeafReaderContext> contexts, int docsPerLeaf) {
    Set<Integer> allDocIds = new HashSet<>();
    TestFacetsCollector collector = new TestFacetsCollector();

    for (LeafReaderContext context : contexts) {
      Set<Integer> leafDocs = new HashSet<>();
      while (leafDocs.size() < docsPerLeaf) {
        int docId;
        do {
          docId = random().nextInt();
        } while (docId < 0 || docId == Integer.MAX_VALUE || allDocIds.contains(docId));
        leafDocs.add(docId);
        allDocIds.add(docId);
      }
      int[] docIds = leafDocs.stream().mapToInt(Integer::intValue).toArray();
      addMatchingDocs(collector, context, docIds);
    }

    return new FacetsCollectorData(collector, allDocIds);
  }

  private TestFacetsCollector createFacetsCollectorWithScores(
      LeafReaderContext context, int[] docIds, float[] scores) {
    TestFacetsCollector fc = new TestFacetsCollector();
    addMatchingDocs(fc, context, docIds, scores);
    return fc;
  }

  private TestFacetsCollector createLargeFacetsCollector(
      List<LeafReaderContext> contexts, int totalDocs) {
    TestFacetsCollector fc = new TestFacetsCollector();
    int docsPerContext = totalDocs / contexts.size();
    int remainder = totalDocs % contexts.size();

    for (int i = 0; i < contexts.size(); i++) {
      int docsInThisContext = docsPerContext;
      if (i < remainder) {
        docsInThisContext++;
      }
      int[] docIds = new int[docsInThisContext];
      for (int j = 0; j < docsInThisContext; j++) {
        docIds[j] = j;
      }
      addMatchingDocs(fc, contexts.get(i), docIds);
    }
    return fc;
  }

  private List<LeafReaderContext> createLeafContexts(int count) throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer =
        new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));

    for (int i = 0; i < count; i++) {
      writer.addDocument(new Document());
      writer.commit();
    }
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    List<LeafReaderContext> contexts = reader.leaves();
    assertEquals(count, contexts.size());

    testReaders.add(reader);
    testDirs.add(dir);

    return contexts;
  }

  private TestFacetsCollector createFacetsCollector(LeafReaderContext context, int... docIds) {
    TestFacetsCollector fc = new TestFacetsCollector();
    addMatchingDocs(fc, context, docIds);
    return fc;
  }

  private void addMatchingDocs(TestFacetsCollector fc, LeafReaderContext context, int... docIds) {
    addMatchingDocs(fc, context, docIds, null);
  }

  private void addMatchingDocs(
      TestFacetsCollector fc, LeafReaderContext context, int[] docIds, float[] scores) {
    int maxDocId = Arrays.stream(docIds).max().orElse(0);
    DocIdSetBuilder builder = new DocIdSetBuilder(maxDocId + 1);
    DocIdSetBuilder.BulkAdder adder = builder.grow(docIds.length);
    for (int docId : docIds) {
      adder.add(docId);
    }

    FacetsCollector.MatchingDocs matchingDocs =
        new FacetsCollector.MatchingDocs(context, builder.build(), docIds.length, scores);
    fc.addMatchingDocs(matchingDocs);
  }

  // Test helper classes

  private static final class DocCountCollector extends SimpleCollector {
    private int docCount = 0;
    private final Set<Integer> allDocIds = new HashSet<>();

    @Override
    public void collect(int doc) {
      docCount++;
      allDocIds.add(doc);
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    public int getDocCount() {
      return docCount;
    }

    public Set<Integer> getAllDocIds() {
      return allDocIds;
    }
  }

  private record DocCountResult(int totalDocs, Set<Integer> allDocIds) {}

  private static final class DocCountCollectorManager
      implements CollectorManager<DocCountCollector, DocCountResult> {
    @Override
    public DocCountCollector newCollector() {
      return new DocCountCollector();
    }

    @Override
    public DocCountResult reduce(Collection<DocCountCollector> collectors) {
      int total = 0;
      Set<Integer> allDocs = new HashSet<>();
      for (DocCountCollector c : collectors) {
        total += c.getDocCount();
        allDocs.addAll(c.getAllDocIds());
      }
      return new DocCountResult(total, allDocs);
    }
  }

  private static final class TestFacetsCollector extends FacetsCollector {
    private final List<MatchingDocs> testMatchingDocs = new ArrayList<>();

    void addMatchingDocs(MatchingDocs docs) {
      testMatchingDocs.add(docs);
    }

    @Override
    public List<MatchingDocs> getMatchingDocs() {
      return testMatchingDocs;
    }
  }

  private static final class ScoreCollector extends SimpleCollector {
    private float totalScore = 0;
    private Scorable scorer;

    @Override
    public void collect(int doc) throws IOException {
      totalScore += scorer.score();
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    public float getTotalScore() {
      return totalScore;
    }
  }

  private static final class ScoreCollectorManager
      implements CollectorManager<ScoreCollector, Float> {
    @Override
    public ScoreCollector newCollector() {
      return new ScoreCollector();
    }

    @Override
    public Float reduce(Collection<ScoreCollector> collectors) {
      float total = 0;
      for (ScoreCollector c : collectors) {
        total += c.getTotalScore();
      }
      return total;
    }
  }
}
