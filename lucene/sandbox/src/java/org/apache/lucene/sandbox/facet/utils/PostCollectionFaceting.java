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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.ArrayUtil;

/**
 * Performs post-collection faceting by replaying collected documents through drill-down and
 * drill-sideways collectors. This enables parallel facet computation after initial document
 * collection.
 *
 * <p>Normally, users can collect facets directly during search without needing FacetsCollector to
 * store doc IDs. However, this class implements the second step of two-step collection: iterating
 * over doc IDs already collected in FacetsCollector to compute facet results. This approach is
 * useful when all matches must be known before computing facets, or when reusing the same matching
 * documents to run faceting multiple times.
 *
 * @param <C> drill-down collector type
 * @param <T> drill-down result type
 * @param <K> drill-sideways collector type
 * @param <R> drill-sideways result type
 */
public final class PostCollectionFaceting<C extends Collector, T, K extends Collector, R> {
  // TODO: is there more optimal slicing method or docs per slice limit?
  static final int MIN_DOCS_PER_SLICE = 100;

  private final CollectorManager<C, T> drillDownCollectorManager;
  private final List<CollectorManager<K, R>> drillSidewaysCollectorManagers;
  private final FacetsCollector drillDownFacetsCollector;
  private final Map<String, FacetsCollector> drillSidewaysFacetsCollectors;
  private final TaskExecutor taskExecutor;
  private final Map<String, Integer> dimToIndexMap;
  private final int numOfIndexLeafs;

  /**
   * Creates a new PostCollectionFaceting instance.
   *
   * @param drillDownCollectorManager collector manager for drill-down results
   * @param drillSidewaysCollectorManagers map of dimension names to collector managers for
   *     drill-sideways results
   * @param drillDownFacetsCollector facets collector containing drill-down matching documents
   * @param drillSidewaysFacetsCollectors map of dimension names to facets collectors for
   *     drill-sideways
   * @param executor executor for parallel processing, or null for sequential execution
   */
  public PostCollectionFaceting(
      CollectorManager<C, T> drillDownCollectorManager,
      Map<String, ? extends CollectorManager<K, R>> drillSidewaysCollectorManagers,
      FacetsCollector drillDownFacetsCollector,
      Map<String, FacetsCollector> drillSidewaysFacetsCollectors,
      Executor executor) {
    this.drillDownCollectorManager = drillDownCollectorManager;
    this.drillDownFacetsCollector = drillDownFacetsCollector;
    if (drillSidewaysFacetsCollectors == null) {
      this.drillSidewaysFacetsCollectors = Map.of();
    } else {
      this.drillSidewaysFacetsCollectors = drillSidewaysFacetsCollectors;
    }
    if (executor == null) {
      this.taskExecutor = new TaskExecutor(Runnable::run);
    } else {
      this.taskExecutor = new TaskExecutor(executor);
    }
    this.numOfIndexLeafs = calculateNumOfIndexLeafs();
    this.dimToIndexMap = new HashMap<>();
    int ind = 0;
    if (drillSidewaysCollectorManagers == null) {
      // assert this.drillSidewaysFacetsCollectors.isEmpty();
      this.drillSidewaysCollectorManagers = List.of();
    } else {
      // Ignore dimensions that don't exist in either one of the maps.
      this.drillSidewaysCollectorManagers = new ArrayList<>(drillSidewaysCollectorManagers.size());
      for (Map.Entry<String, ? extends CollectorManager<K, R>> entry :
          drillSidewaysCollectorManagers.entrySet()) {
        if (this.drillSidewaysFacetsCollectors.containsKey(entry.getKey())) {
          dimToIndexMap.put(entry.getKey(), ind++);
          this.drillSidewaysCollectorManagers.add(entry.getValue());
        }
      }
    }
  }

  /**
   * Creates a new PostCollectionFaceting instance without drill-sideways collectors.
   *
   * @param drillDownCollectorManager collector manager for drill-down results
   * @param drillDownFacetsCollector facets collector containing drill-down matching documents
   * @param executor executor for parallel processing, or null for sequential execution
   */
  public PostCollectionFaceting(
      CollectorManager<C, T> drillDownCollectorManager,
      FacetsCollector drillDownFacetsCollector,
      Executor executor) {
    this(drillDownCollectorManager, null, drillDownFacetsCollector, null, executor);
  }

  private int calculateNumOfIndexLeafs() {
    int maxOrd = -1;
    for (FacetsCollector.MatchingDocs matchingDocs : drillDownFacetsCollector.getMatchingDocs()) {
      maxOrd = Math.max(maxOrd, matchingDocs.context().ord);
    }
    for (FacetsCollector facetsCollector : drillSidewaysFacetsCollectors.values()) {
      for (FacetsCollector.MatchingDocs matchingDocs : facetsCollector.getMatchingDocs()) {
        maxOrd = Math.max(maxOrd, matchingDocs.context().ord);
      }
    }
    return maxOrd + 1;
  }

  /**
   * Organizes matching documents into a 2D array indexed by leaf ordinal and dimension ordinal.
   *
   * @return leaf ord -> dim ord -> matching docs, where dim ordinal is from dimToIndexMap
   *     <p>Example: For an index with 3 segments and 2 drill-sideways dimensions ("brand",
   *     "color"):
   *     <pre>
   * // dimToIndexMap: {"brand" -> 0, "color" -> 1}
   * // Result array structure:
   * result[0][0] = drill-down docs for segment 0
   * result[0][1] = "brand" drill-sideways docs for segment 0
   * result[0][2] = "color" drill-sideways docs for segment 0
   * result[1][0] = drill-down docs for segment 1
   * result[1][1] = "brand" drill-sideways docs for segment 1
   * ...
   * </pre>
   *     <p>Note: {@link FacetsCollector#getMatchingDocs()} returns one MatchingDocs per visited
   *     segment, so the number of MatchingDocs is never greater than the number of index segments,
   *     even if intra-segment concurrency was used to collect data.
   */
  private FacetsCollector.MatchingDocs[][] getPerLeafMatchingDocs() {
    // Max dim
    FacetsCollector.MatchingDocs[][] perLeafMatchingDocs =
        new FacetsCollector.MatchingDocs[numOfIndexLeafs]
            [drillSidewaysCollectorManagers.size() + 1];
    for (FacetsCollector.MatchingDocs drillDownMatchingDocs :
        drillDownFacetsCollector.getMatchingDocs()) {
      perLeafMatchingDocs[drillDownMatchingDocs.context().ord][0] = drillDownMatchingDocs;
    }
    for (Map.Entry<String, Integer> entry : dimToIndexMap.entrySet()) {
      for (FacetsCollector.MatchingDocs matchingDocs :
          drillSidewaysFacetsCollectors.get(entry.getKey()).getMatchingDocs()) {
        perLeafMatchingDocs[matchingDocs.context().ord][entry.getValue() + 1] = matchingDocs;
      }
    }
    return perLeafMatchingDocs;
  }

  private record Slice(FacetsCollector.MatchingDocs[][] leafMatchingDocs) {}

  /**
   * Partitions matching documents into slices for parallel processing.
   *
   * <p>Slicing enables parallel facet computation by distributing work across multiple threads.
   * Each slice contains a subset of index segments with enough documents to justify the overhead of
   * parallel execution, improving throughput for large result sets.
   *
   * @param minDocsPerSlice minimum number of documents per slice to balance parallelization
   *     overhead
   * @param perLeafMatchingDocs matching documents organized by leaf ordinal and dimension ordinal
   * @return list of slices, each containing a subset of segments for independent processing
   */
  private List<Slice> getSlices(
      int minDocsPerSlice, FacetsCollector.MatchingDocs[][] perLeafMatchingDocs) {
    List<Slice> slices = new ArrayList<>();

    int currentSliceSize = 0;
    int lastSliceEnd = -1;
    for (int leafOrd = 0; leafOrd < perLeafMatchingDocs.length; leafOrd++) {
      for (int dimOrd = 0; dimOrd < perLeafMatchingDocs[leafOrd].length; dimOrd++) {
        if (perLeafMatchingDocs[leafOrd][dimOrd] != null) {
          currentSliceSize += perLeafMatchingDocs[leafOrd][dimOrd].totalHits();
        }
      }
      if (currentSliceSize >= minDocsPerSlice) {
        slices.add(
            new Slice(
                ArrayUtil.copyOfSubArray(perLeafMatchingDocs, lastSliceEnd + 1, leafOrd + 1)));
        currentSliceSize = 0;
        lastSliceEnd = leafOrd;
      }
    }
    // add final slice
    if (currentSliceSize > 0) {
      slices.add(
          new Slice(
              ArrayUtil.copyOfSubArray(
                  perLeafMatchingDocs, lastSliceEnd + 1, perLeafMatchingDocs.length)));
    }
    return slices;
  }

  /**
   * Collects facet results by replaying documents through collectors in parallel slices.
   *
   * @return result containing drill-down and drill-sideways facet results
   * @throws IOException if an I/O error occurs during collection
   */
  public Result<T, R> collect() throws IOException {
    FacetsCollector.MatchingDocs[][] perLeafMatchingDocs = getPerLeafMatchingDocs();
    final List<Slice> leafSlices = getSlices(MIN_DOCS_PER_SLICE, perLeafMatchingDocs);

    if (leafSlices.size() == 0) {
      return getEmptyResult();
    } else {
      final List<C> drillDownCollectors;
      if (drillDownCollectorManager != null) {
        drillDownCollectors = new ArrayList<>(leafSlices.size());
      } else {
        drillDownCollectors = null;
      }
      final List<List<K>> drillSidewaysCollectors = new ArrayList<>(leafSlices.size());
      final List<Callable<Void>> listTasks = new ArrayList<>(leafSlices.size());
      for (int i = 0; i < leafSlices.size(); ++i) {
        final Slice slice = leafSlices.get(i);
        // drill down collector
        final C drillDownCollector;
        if (drillDownCollectorManager != null) {
          drillDownCollector = drillDownCollectorManager.newCollector();
          drillDownCollectors.add(drillDownCollector);
        } else {
          drillDownCollector = null;
        }

        // drill sideways collectors
        List<K> drillSidewaysSliceCollectors =
            new ArrayList<>(drillSidewaysCollectorManagers.size());
        for (CollectorManager<K, R> manager : drillSidewaysCollectorManagers) {
          drillSidewaysSliceCollectors.add(manager.newCollector());
        }
        drillSidewaysCollectors.add(drillSidewaysSliceCollectors);
        listTasks.add(() -> collectSlice(slice, drillDownCollector, drillSidewaysSliceCollectors));
      }
      taskExecutor.invokeAll(listTasks);
      Map<String, R> drillSidewaysResults = new HashMap<>(drillSidewaysCollectorManagers.size());
      for (Map.Entry<String, Integer> entry : dimToIndexMap.entrySet()) {
        List<K> collectors =
            drillSidewaysCollectors.stream()
                .map(list -> list.get(dimToIndexMap.get(entry.getKey())))
                .toList();
        CollectorManager<K, R> collectorManager =
            drillSidewaysCollectorManagers.get(entry.getValue());
        drillSidewaysResults.put(entry.getKey(), collectorManager.reduce(collectors));
      }
      T drillDownResult;
      if (drillDownCollectorManager != null) {
        drillDownResult = drillDownCollectorManager.reduce(drillDownCollectors);
      } else {
        drillDownResult = null;
      }
      return new Result<>(drillDownResult, drillSidewaysResults);
    }
  }

  private Result<T, R> getEmptyResult() throws IOException {
    // there are no segments, nothing to offload to the executor, but we do need to call reduce to
    // create some kind of empty result
    Map<String, R> emptyResults = new HashMap<>();
    for (Map.Entry<String, Integer> entry : dimToIndexMap.entrySet()) {
      emptyResults.put(
          entry.getKey(), drillSidewaysCollectorManagers.get(entry.getValue()).reduce(List.of()));
    }
    T drillDownResult = null;
    if (drillDownCollectorManager != null) {
      drillDownResult = drillDownCollectorManager.reduce(List.of());
    }
    return new Result<>(drillDownResult, emptyResults);
  }

  /**
   * The result. It is very similar to DrillSideways.Result, but it uses Map instead of List for
   * drill sideways results. See also to-do comment in DrillSidewaysFacetOrchestrator.
   *
   * @param <T> drill down result
   * @param <R> drill sideways result
   * @param drillDownResult the drill down result
   * @param drillSidewaysResults the drill sideways results
   */
  public record Result<T, R>(T drillDownResult, Map<String, R> drillSidewaysResults) {}

  private static class MatchingDocsScorable extends Scorable {
    private final FacetsCollector.MatchingDocs matchingDocs;
    private int currentDocId = -1;

    MatchingDocsScorable(FacetsCollector.MatchingDocs matchingDocs) {
      this.matchingDocs = matchingDocs;
    }

    void setCurrentDocId(int docId) {
      this.currentDocId = docId;
    }

    @Override
    public float score() throws IOException {
      assert currentDocId >= 0 : "setCurrentDocId() must be called before score()";
      assert matchingDocs.scores().length > currentDocId
          : "scores array is indexed by doc ID (see FacetsCollector.MatchingDocs), so length must be greater"
              + " than currentDocId";
      return matchingDocs.scores()[currentDocId];
    }
  }

  private static LeafCollector getLeafCollector(
      FacetsCollector.MatchingDocs matchingDocs, Collector collector) throws IOException {
    if (matchingDocs == null || collector == null) {
      return null;
    }
    return collector.getLeafCollector(matchingDocs.context());
  }

  private static MatchingDocsScorable createScorer(
      FacetsCollector.MatchingDocs matchingDocs, Collector collector) {
    if (matchingDocs == null || collector == null) {
      return null;
    }
    if (collector.scoreMode().needsScores()) {
      if (matchingDocs.scores() == null) {
        throw new IllegalStateException(
            "Collector requires scores, but FacetCollector doesn't have them.");
      } else {
        return new MatchingDocsScorable(matchingDocs);
      }
    }
    return null;
  }

  private Void collectSlice(Slice slice, C drillDownCollector, List<K> drillSidewaysCollectors)
      throws IOException {
    LeafCollector[] leafCollectors = new LeafCollector[drillSidewaysCollectors.size() + 1];
    // Init lazily as it is not often needed.
    MatchingDocsScorable[] scorables = null;
    for (FacetsCollector.MatchingDocs[] leafMatchingDocs : slice.leafMatchingDocs()) {
      leafCollectors[0] = getLeafCollector(leafMatchingDocs[0], drillDownCollector);
      // TODO dedup scorer code for drill down and sideways
      MatchingDocsScorable scorer = createScorer(leafMatchingDocs[0], drillDownCollector);
      if (scorer != null) {
        if (scorables == null) {
          scorables = new MatchingDocsScorable[leafCollectors.length];
        }
        scorables[0] = scorer;
        leafCollectors[0].setScorer(scorer);
      }

      for (int i = 0; i < drillSidewaysCollectors.size(); i++) {
        leafCollectors[i + 1] =
            getLeafCollector(leafMatchingDocs[i + 1], drillSidewaysCollectors.get(i));
        scorer = createScorer(leafMatchingDocs[i + 1], drillSidewaysCollectors.get(i));
        if (scorer != null) {
          if (scorables == null) {
            scorables = new MatchingDocsScorable[leafCollectors.length];
          }
          scorables[i + 1] = scorer;
          leafCollectors[i + 1].setScorer(scorer);
        }
      }
      collectLeaf(leafMatchingDocs, leafCollectors, scorables);
    }
    return null;
  }

  private static void collectLeaf(
      FacetsCollector.MatchingDocs[] matchingDocs,
      LeafCollector[] leafCollectors,
      MatchingDocsScorable[] scorables)
      throws IOException {
    assert matchingDocs.length == leafCollectors.length;
    // init
    int currentDocToCollect = NO_MORE_DOCS;
    // TODO: can move iterators out of this method, pass instead of matchingDocs?
    DocIdSetIterator[] iterators = new DocIdSetIterator[matchingDocs.length];
    for (int i = 0; i < matchingDocs.length; i++) {
      if (matchingDocs[i] != null && leafCollectors[i] != null) {
        iterators[i] = matchingDocs[i].bits().iterator();
        int firstDoc = iterators[i].nextDoc();
        if (firstDoc != NO_MORE_DOCS
            && (currentDocToCollect == NO_MORE_DOCS || currentDocToCollect > firstDoc)) {
          currentDocToCollect = firstDoc;
        }
      }
    }
    // collection
    int nextDocToCollect;
    while (currentDocToCollect < NO_MORE_DOCS) {
      nextDocToCollect = Integer.MAX_VALUE;
      for (int i = 0; i < iterators.length; i++) {
        if (iterators[i] == null) {
          continue;
        }
        if (iterators[i].docID() == currentDocToCollect) {
          assert leafCollectors[i] != null
              : "leafCollectors[" + i + "] is null but the iterator is not null";
          if (scorables != null && scorables[i] != null) {
            scorables[i].setCurrentDocId(currentDocToCollect);
          }
          leafCollectors[i].collect(currentDocToCollect);
          int nextDoc = iterators[i].nextDoc();
          if (nextDoc == NO_MORE_DOCS) {
            iterators[i] = null;
          } else if (nextDocToCollect > nextDoc) {
            nextDocToCollect = nextDoc;
          }
        } else {
          assert iterators[i].docID() > currentDocToCollect
              : "currentDocToCollect ("
                  + currentDocToCollect
                  + ") should always be greater than iterators[i].docID() ("
                  + iterators[i].docID()
                  + ")";

          if (nextDocToCollect > iterators[i].docID()) {
            nextDocToCollect = iterators[i].docID();
          }
        }
      }
      assert nextDocToCollect > currentDocToCollect;
      currentDocToCollect = nextDocToCollect;
    }
    // finish
    for (LeafCollector leafCollector : leafCollectors) {
      if (leafCollector != null) {
        leafCollector.finish();
      }
    }
  }
}
