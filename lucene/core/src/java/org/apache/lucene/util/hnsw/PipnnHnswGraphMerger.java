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
package org.apache.lucene.util.hnsw;

import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;

/**
 * An {@link HnswGraphMerger} that rebuilds the merged graph from scratch with {@link
 * PipnnGraphBuilder} unless one of the segments being merged brings a graph that already covers
 * most of the merged vectors, in which case it reuses that graph the way {@link
 * ConcurrentHnswMerger} does and inserts the remaining vectors into it.
 *
 * <p>Rebuilding is what the incremental mergers also do when no source graph can be reused, e.g.
 * when every source segment has deletes, and PiPNN makes that case cheap: the build cost is
 * dominated by dense, cache friendly scoring inside small leaves instead of graph searches. When a
 * large graph can be reused, inserting the few remaining vectors is cheaper than any rebuild.
 *
 * @lucene.experimental
 */
public class PipnnHnswGraphMerger extends ConcurrentHnswMerger {

  /**
   * The merged graph is initialized from an existing graph, instead of rebuilt with PiPNN, when the
   * largest reusable graph holds at least this share of the merged vectors. The incremental merge
   * then inserts the remaining share by graph search, at roughly that share of a full HNSW build; a
   * PiPNN rebuild costs about half a full HNSW build (1M x 1024d, 16 workers), so the two cross
   * near one half.
   */
  static final double REUSE_FRACTION = 0.5;

  private final PipnnParams params;
  private final TaskExecutor taskExecutor;
  private final int numWorkers;

  /**
   * @param fieldInfo FieldInfo for the field being merged
   * @param scorerSupplier scorer over the merged vectors
   * @param M max connections per node on the upper levels
   * @param beamWidth beam width used for the upper levels
   * @param params PiPNN parameters
   * @param taskExecutor executor for the parallel parts of the build, or null for a single thread
   * @param numWorkers number of parallel workers, ignored when {@code taskExecutor} is null
   * @param abortCheck optional check that aborts the build when the merge has been aborted, or null
   */
  public PipnnHnswGraphMerger(
      FieldInfo fieldInfo,
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      PipnnParams params,
      TaskExecutor taskExecutor,
      int numWorkers,
      IORunnable abortCheck) {
    super(fieldInfo, scorerSupplier, M, beamWidth, taskExecutor, numWorkers, abortCheck);
    this.params = params;
    this.taskExecutor = taskExecutor;
    this.numWorkers = numWorkers;
  }

  @Override
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    if (largestGraphReader != null
        && largestGraphReader.liveVectorCount() >= REUSE_FRACTION * maxOrd) {
      if (taskExecutor != null) {
        return super.createBuilder(mergedVectorValues, maxOrd);
      }
      // single threaded reuse, as IncrementalHnswGraphMerger does it
      HnswGraph[] graphs = orderGraphReaders();
      BitSet initializedNodes = new FixedBitSet(maxOrd);
      int[][] ordMaps = getNewOrdMapping(mergedVectorValues, initializedNodes);
      return MergingHnswGraphBuilder.fromGraphs(
          scorerSupplier,
          beamWidth,
          HnswGraphBuilder.randSeed,
          graphs,
          ordMaps,
          maxOrd,
          initializedNodes,
          abortCheck);
    }
    return PipnnGraphBuilder.create(
        scorerSupplier,
        mergedVectorValues,
        fieldInfo.getVectorSimilarityFunction(),
        M,
        beamWidth,
        HnswGraphBuilder.randSeed,
        maxOrd,
        params,
        taskExecutor,
        numWorkers);
  }
}
