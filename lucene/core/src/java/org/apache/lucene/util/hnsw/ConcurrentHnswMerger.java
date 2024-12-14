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
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;

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
    super(fieldInfo, scorerSupplier, M, beamWidth);
    this.taskExecutor = taskExecutor;
    this.numWorker = numWorker;
  }

  @Override
  protected HnswBuilder createBuilder(KnnVectorValues mergedVectorValues, int maxOrd)
      throws IOException {
    OnHeapHnswGraph graph;
    BitSet initializedNodes = null;

    if (initReader == null) {
      graph = new OnHeapHnswGraph(M, maxOrd);
    } else {
      HnswGraph initializerGraph = ((HnswGraphProvider) initReader).getGraph(fieldInfo.name);
      if (initializerGraph.size() == 0) {
        graph = new OnHeapHnswGraph(M, maxOrd);
      } else {
        initializedNodes = new FixedBitSet(maxOrd);
        int[] oldToNewOrdinalMap = getNewOrdMapping(mergedVectorValues, initializedNodes);
        graph =
            InitializedHnswGraphBuilder.initGraph(M, initializerGraph, oldToNewOrdinalMap, maxOrd);
      }
    }
    return new HnswConcurrentMergeBuilder(
        taskExecutor, numWorker, scorerSupplier, M, beamWidth, graph, initializedNodes);
  }
}
