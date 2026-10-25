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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.InfoStream;

/**
 * An {@link HnswGraphMerger} that ignores the graphs of the segments being merged and rebuilds the
 * merged graph from scratch with {@link PipnnGraphBuilder}.
 *
 * <p>Rebuilding is what the incremental mergers also do when no source graph can be reused, e.g.
 * when every source segment has deletes, and PiPNN makes that case cheap: the build cost is
 * dominated by dense, cache friendly scoring inside small leaves instead of graph searches.
 *
 * @lucene.experimental
 */
public class PipnnHnswGraphMerger implements HnswGraphMerger {

  private final FieldInfo fieldInfo;
  private final RandomVectorScorerSupplier scorerSupplier;
  private final int M;
  private final int beamWidth;
  private final PipnnParams params;
  private final TaskExecutor taskExecutor;
  private final int numWorkers;
  private final IORunnable abortCheck;

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
    this.fieldInfo = fieldInfo;
    this.scorerSupplier = scorerSupplier;
    this.M = M;
    this.beamWidth = beamWidth;
    this.params = params;
    this.taskExecutor = taskExecutor;
    this.numWorkers = numWorkers;
    this.abortCheck = abortCheck;
  }

  /** The source graphs are not reused, so readers are accepted but ignored. */
  @Override
  public PipnnHnswGraphMerger addReader(
      KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) {
    return this;
  }

  @Override
  public OnHeapHnswGraph merge(
      KnnVectorValues mergedVectorValues, InfoStream infoStream, int maxOrd) throws IOException {
    PipnnGraphBuilder builder =
        PipnnGraphBuilder.create(
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
    builder.setInfoStream(infoStream);
    if (abortCheck != null) {
      builder.setAbortCheck(abortCheck);
    }
    return builder.build(maxOrd);
  }
}
