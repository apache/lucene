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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOSupplier;

/** This merger merges graph in a concurrent manner, by using {@link HnswConcurrentMergeBuilder} */
public class ConcurrentHnswMerger extends IncrementalHnswGraphMerger {

  private final TaskExecutor taskExecutor;
  private final int numWorker;

  /**
   * @param fieldInfo FieldInfo for the field being merged
   */
  public ConcurrentHnswMerger(
      FieldInfo fieldInfo,
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      TaskExecutor taskExecutor,
      int numWorker) {
    this(fieldInfo, scorerSupplier, M, beamWidth, taskExecutor, numWorker, null);
  }

  /**
   * @param fieldInfo FieldInfo for the field being merged
   * @param abortCheck optional check invoked before every node insertion during graph construction;
   *     may throw {@link org.apache.lucene.index.MergePolicy.MergeAbortedException} to abort the
   *     build when the surrounding merge has been aborted, or null
   */
  public ConcurrentHnswMerger(
      FieldInfo fieldInfo,
      RandomVectorScorerSupplier scorerSupplier,
      int M,
      int beamWidth,
      TaskExecutor taskExecutor,
      int numWorker,
      IORunnable abortCheck) {
    super(fieldInfo, scorerSupplier, M, beamWidth, abortCheck);
    this.taskExecutor = taskExecutor;
    this.numWorker = numWorker;
  }

  @Override
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    if (largestGraphReader == null) {
      return new HnswConcurrentMergeBuilder(
          taskExecutor, numWorker, scorerSupplier, beamWidth, new OnHeapHnswGraph(M, maxOrd), null);
    }
    HnswGraph[] graphs = orderGraphReaders();
    BitSet initializedNodes = new FixedBitSet(maxOrd);
    int[][] ordMaps = getNewOrdMapping(mergedVectorValues, initializedNodes);
    // only prune the base graph here: if it had deletes, the builder repairs it with all workers
    InitializedHnswGraphBuilder.PrunedGraph prunedGraph =
        InitializedHnswGraphBuilder.pruneGraph(
            scorerSupplier, beamWidth, graphs[0], ordMaps[0], maxOrd, abortCheck);
    // the remaining graphs have no deletions; join them into the base graph rather than inserting
    // their nodes from scratch. Each call to getGraph returns a new instance, which is how every
    // worker gets its own.
    List<IOSupplier<HnswGraph>> otherGraphs = new ArrayList<>(graphs.length - 1);
    for (int i = 1; i < graphReaders.size(); i++) {
      HnswGraphProvider provider = (HnswGraphProvider) graphReaders.get(i).reader();
      otherGraphs.add(() -> provider.getGraph(fieldInfo.name));
    }
    return new HnswConcurrentMergeBuilder(
        taskExecutor,
        numWorker,
        scorerSupplier,
        beamWidth,
        prunedGraph.graph(),
        initializedNodes,
        prunedGraph.hasDeletes() ? prunedGraph : null,
        otherGraphs,
        ArrayUtil.copyOfSubArray(ordMaps, 1, ordMaps.length));
  }
}
