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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.util.hnsw.HnswGraphSearcher.expectedVisitedNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.HnswConcurrentMergeBuilder;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraph.NodesIterator;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Writes a de-duplication-aware HNSW graph on top of a de-duplicating flat vectors format.
 *
 * <p>Unlike {@link org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter}, which builds one
 * HNSW node per document, this writer builds a single graph node per <b>distinct</b> vector (group
 * ordinal). When many documents share the same vector, the graph is built once over the distinct
 * vectors, saving construction time and index size. To recover the documents at search time, the
 * writer also stores a {@code DistinctVectorPostings} list that maps each group ordinal to the
 * field ordinals (per-document ordinals) that reference it; {@link DedupHnswVectorsReader} expands
 * a matched group node back to those documents.
 *
 * <h2>.vdhd (dedup HNSW data) file</h2>
 *
 * <p>Per field: the graph neighbor lists (delta-encoded, in group-ordinal space, laid out exactly
 * as {@link org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat}'s vector index), followed
 * by a {@code DistinctVectorPostings}, which maps each individual distinct vector to the field
 * ordinals that reference it.
 *
 * <h2>.vdhm (dedup HNSW metadata) file</h2>
 *
 * <p>Per field: field number, group count, field-ordinal (document) count, graph offset/length,
 * {@code M}, per-level node lists, node offsets metadata, and the postings offsets/data locations.
 *
 * @lucene.experimental
 */
final class DedupHnswVectorsWriter extends KnnVectorsWriter {

  static final String META_CODEC_NAME = "DedupHnswVectorsFormatMeta";
  static final String DATA_CODEC_NAME = "DedupHnswVectorsFormatData";
  static final String META_EXTENSION = "vdhm";
  static final String DATA_EXTENSION = "vdhd";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private final SegmentWriteState segmentWriteState;
  private final int M;
  private final int beamWidth;
  private final int tinySegmentsThreshold;
  private final int hybridGroupThreshold;
  private final int numMergeWorkers;
  // Non-null only when the format was given an explicit merge ExecutorService. Otherwise, the merge
  // scheduler's intra-merge executor (from MergeState) is used when available.
  private final TaskExecutor mergeExec;
  // Set for the duration of a single mergeOneField build so maybeBuildGraph can pick a concurrent
  // builder. null during flush (flush always builds single-threaded, like Lucene99).
  private TaskExecutor activeMergeExecutor;
  private int activeMergeWorkers = 1;
  private final FlatVectorsFormat flatVectorsFormat;
  private final FlatVectorsWriter flatVectorWriter;

  private final IndexOutput meta;
  private final IndexOutput graphData;

  private FlatVectorsReader flatVectorsReader;
  private boolean flatWriterClosed = false;
  private boolean finished = false;

  private final List<FieldInfo> fields = new ArrayList<>();

  DedupHnswVectorsWriter(
      SegmentWriteState state,
      int M,
      int beamWidth,
      int tinySegmentsThreshold,
      int hybridGroupThreshold,
      int numMergeWorkers,
      ExecutorService mergeExec,
      FlatVectorsFormat flatVectorsFormat,
      FlatVectorsWriter flatVectorWriter)
      throws IOException {
    this.segmentWriteState = state;
    this.M = M;
    this.beamWidth = beamWidth;
    this.tinySegmentsThreshold = tinySegmentsThreshold;
    this.hybridGroupThreshold = hybridGroupThreshold;
    this.numMergeWorkers = numMergeWorkers;
    this.mergeExec = mergeExec == null ? null : new TaskExecutor(mergeExec);
    this.flatVectorsFormat = flatVectorsFormat;
    this.flatVectorWriter = flatVectorWriter;

    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    String dataFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DATA_EXTENSION);
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      graphData = state.directory.createOutput(dataFileName, state.context);
      CodecUtil.writeIndexHeader(
          meta, META_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          graphData,
          DATA_CODEC_NAME,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    // The graph is built after flat storage is written, so we simply track the delegate field
    // writer, which buffers the vectors and de-duplication state
    fields.add(fieldInfo);
    return flatVectorWriter.addField(fieldInfo);
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    // Persist the de-duplicated flat vectors (raw + quantized + fieldOrdToGroupOrd + ordToDoc).
    flatVectorWriter.flush(maxDoc, sortMap);
    // Open a reader over what was just written and build the group graph for each field.
    ensureFlatReaderOpen();
    for (FieldInfo fieldInfo : fields) {
      if (fieldInfo.hasVectorValues()) {
        buildAndWriteGraph(fieldInfo);
      }
    }
  }

  @Override
  public IORunnable mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    // Delegate the flat merge; it re-de-duplicates across the merged segments.
    fields.add(fieldInfo);
    flatVectorWriter.mergeOneFlatVectorField(fieldInfo, mergeState);
    // Choose the executor for concurrent graph building during this merge, mirroring
    // Lucene99HnswVectorsWriter: prefer the format's explicit mergeExec; otherwise fall back to the
    // merge scheduler's intra-merge executor. Captured here (on the merge thread) and consumed by
    // buildAndWriteGraph -> maybeBuildGraph in the returned IORunnable.
    final TaskExecutor chosenExec;
    final int chosenWorkers;
    if (mergeExec != null) {
      chosenExec = mergeExec;
      chosenWorkers = numMergeWorkers;
    } else if (mergeState.intraMergeTaskExecutor != null) {
      chosenExec = new TaskExecutor(mergeState.intraMergeTaskExecutor);
      // numMergeWorkers defaults to 1 unless the format was configured otherwise; use it to bound
      // the intra-merge parallelism (matching Lucene99's numMergeWorkers argument).
      chosenWorkers = numMergeWorkers;
    } else {
      chosenExec = null;
      chosenWorkers = 1;
    }
    return () -> {
      mergeState.checkAborted();
      ensureFlatReaderOpen();
      activeMergeExecutor = chosenExec;
      activeMergeWorkers = chosenWorkers;
      try {
        buildAndWriteGraph(fieldInfo);
      } finally {
        activeMergeExecutor = null;
        activeMergeWorkers = 1;
      }
    };
  }

  /** Builds and writes the HNSW graph for a field, choosing a per-field layout. */
  private void buildAndWriteGraph(FieldInfo fieldInfo) throws IOException {
    // TODO: build incrementally as new distinct vectors are introduced, instead of all at once
    //  here. Insert a node the first time each distinct vector appears (keyed by its first-seen
    //  index) via HnswGraphBuilder#addGraphNode, then on flush remap first-seen ids to the final
    //  group ordinals when writing neighbor lists and postings. This should smoothen the peak
    //  memory and flush latency.
    DedupVectorValues dedupValues = getDedupVectorValues(fieldInfo);
    if (dedupValues == null || dedupValues.getGroupView().size() == 0) {
      writeEmptyField(fieldInfo);
      return;
    }

    int groupCount = dedupValues.getGroupView().size();
    int fieldOrdCount = ((KnnVectorValues) dedupValues).size();

    // Number of DISTINCT groups actually referenced BY THIS FIELD. The group view is shared across
    // all of a segment's dedup fields, so groupCount (its global size) can exceed the groups a
    // single field references (e.g. a filtered sub-field that indexes only a subset of documents).
    // Deciding the layout from groupCount vs fieldOrdCount would then wrongly classify a field with
    // NO within-field duplication as de-duplicated. The correct per-field test is: does this field
    // reference as many distinct groups as it has documents? If so, there is nothing to de-dup.
    int distinctGroupsReferenced =
        countDistinctReferencedGroups(dedupValues.getFieldOrdToGroupOrd(), fieldOrdCount, groupCount);

    // When there is no effective de-duplication (every document references a distinct vector), the
    // group machinery is pure overhead, so build a plain document-space graph; otherwise de-dup.
    DedupLayoutMode mode = selectMode(distinctGroupsReferenced, fieldOrdCount);
    if (mode == DedupLayoutMode.PLAIN) {
      writePlainField(fieldInfo, dedupValues, fieldOrdCount);
    } else if (hybridGroupThreshold > 0) {
      // HYBRID is enabled and the field has some de-duplication: promote only the large groups.
      writeHybridField(fieldInfo, dedupValues, groupCount, fieldOrdCount);
    } else {
      writeDedupField(fieldInfo, dedupValues, groupCount, fieldOrdCount);
    }
  }

  /**
   * Counts the distinct group ordinals referenced by a field's documents. Returns a value in {@code
   * [0, fieldOrdCount]}; when it equals {@code fieldOrdCount} the field has no within-field
   * de-duplication (every document references a unique group). {@code groupCount} bounds the group
   * ordinal space and sizes the membership bitset.
   */
  private static int countDistinctReferencedGroups(
      FieldOrdToGroupOrd fieldOrdToGroupOrd, int fieldOrdCount, int groupCount) {
    if (fieldOrdCount == 0) {
      return 0;
    }
    FixedBitSet seen = new FixedBitSet(groupCount);
    int distinct = 0;
    for (int ord = 0; ord < fieldOrdCount; ord++) {
      int groupOrd = fieldOrdToGroupOrd.get(ord);
      if (seen.getAndSet(groupOrd) == false) {
        distinct++;
        // Early out: once we've seen one distinct group per document, it cannot grow further and
        // the field is already known to be fully distinct.
        if (distinct == fieldOrdCount) {
          break;
        }
      }
    }
    return distinct;
  }

  /**
   * PLAIN layout: one graph node per document (field ordinal), scored at full precision, with no
   * postings. Search then behaves like a vanilla HNSW search.
   */
  private void writePlainField(
      FieldInfo fieldInfo, DedupVectorValues dedupValues, int fieldOrdCount) throws IOException {
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(fieldInfo.name);

    long graphDataOffset = graphData.getFilePointer();
    OnHeapHnswGraph graph =
        maybeBuildGraph(
            fieldOrdCount,
            scorer.getPlainRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(), dedupValues));
    int[][] graphLevelNodeOffsets = writeGraph(graph);
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    writeMeta(
        fieldInfo,
        DedupLayoutMode.PLAIN,
        fieldOrdCount,
        fieldOrdCount,
        fieldOrdCount,
        graphDataOffset,
        graphDataLength,
        graph,
        graphLevelNodeOffsets,
        new int[0],
        new int[0]);
  }

  /**
   * DEDUP layout: one graph node per distinct vector (group ordinal), plus a {@code
   * DistinctVectorPostings} that maps each group back to its documents.
   */
  private void writeDedupField(
      FieldInfo fieldInfo, DedupVectorValues dedupValues, int groupCount, int fieldOrdCount)
      throws IOException {
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(fieldInfo.name);

    long graphDataOffset = graphData.getFilePointer();
    OnHeapHnswGraph graph =
        maybeBuildGraph(
            groupCount,
            scorer.getGroupRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(), dedupValues));
    int[][] graphLevelNodeOffsets = writeGraph(graph);
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    // TODO: the flattened field ordinals are written as plain ints in writeMeta. Store them more
    //  compactly (e.g. bit-packed via DirectWriter, like the forward fieldOrdToGroupOrd map) to
    //  reduce index size. Size only; correctness is unaffected.
    int[] groupOffsets = new int[groupCount + 1];
    int[] flattenedFieldOrds = new int[fieldOrdCount];
    computeDistinctVectorPostings(
        dedupValues.getFieldOrdToGroupOrd(),
        fieldOrdCount,
        groupCount,
        groupOffsets,
        flattenedFieldOrds);

    writeMeta(
        fieldInfo,
        DedupLayoutMode.DEDUP,
        groupCount,
        fieldOrdCount,
        groupCount,
        graphDataOffset,
        graphDataLength,
        graph,
        graphLevelNodeOffsets,
        groupOffsets,
        flattenedFieldOrds);
  }

  /**
   * HYBRID layout: one graph node per <b>large</b> group (a distinct vector referenced by more than
   * {@link #hybridGroupThreshold} documents) and one graph node per document that belongs to a
   * small group.
   *
   * <p>Node ordinals are laid out as:
   *
   * <ul>
   *   <li>{@code [0, numLargeGroups)} — the promoted large groups, in ascending group-ordinal
   *       order. Each carries a {@code DistinctVectorPostings} slice listing all its field ordinals
   *       (documents).
   *   <li>{@code [numLargeGroups, numLargeGroups + numSmallDocs)} — the documents of all small
   *       groups, in ascending field-ordinal order. Each carries exactly one field ordinal.
   * </ul>
   *
   * <p>A {@code nodeToGroupOrd} map (one entry per node) resolves every node to a vector in the
   * group view so the HNSW graph can be built and searched uniformly in group-view scoring space.
   */
  private void writeHybridField(
      FieldInfo fieldInfo, DedupVectorValues dedupValues, int groupCount, int fieldOrdCount)
      throws IOException {
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(fieldInfo.name);
    FieldOrdToGroupOrd fieldOrdToGroupOrd = dedupValues.getFieldOrdToGroupOrd();

    // 1. Count how many documents reference each group.
    int[] groupSizes = new int[groupCount];
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      groupSizes[fieldOrdToGroupOrd.get(fieldOrd)]++;
    }

    // 2. Partition groups: large (promoted to a group-node) vs small (kept as per-doc nodes).
    //    largeGroupNode[g] = node ordinal of group g if promoted, else -1.
    int[] largeGroupNode = new int[groupCount];
    Arrays.fill(largeGroupNode, -1);
    int numLargeGroups = 0;
    for (int g = 0; g < groupCount; g++) {
      if (groupSizes[g] > hybridGroupThreshold) {
        largeGroupNode[g] = numLargeGroups++;
      }
    }

    // Number of documents that remain as individual nodes (belong to small groups).
    int numSmallDocs = 0;
    for (int g = 0; g < groupCount; g++) {
      if (largeGroupNode[g] == -1) {
        numSmallDocs += groupSizes[g];
      }
    }
    int nodeCount = numLargeGroups + numSmallDocs;

    // 3. Build the per-node -> group-view-ordinal map and the per-small-node -> field ordinal map.
    //    Node layout: large-group nodes first (ascending group ord), then small-group docs
    //    (ascending field ord).
    int[] nodeToGroupOrd = new int[nodeCount];
    int[] smallNodeFieldOrd = new int[numSmallDocs];
    // Large-group nodes score against the group's distinct vector.
    for (int g = 0; g < groupCount; g++) {
      if (largeGroupNode[g] != -1) {
        nodeToGroupOrd[largeGroupNode[g]] = g;
      }
    }
    // Small-group doc nodes score against their doc's (shared) distinct vector.
    int smallNode = 0;
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      int g = fieldOrdToGroupOrd.get(fieldOrd);
      if (largeGroupNode[g] == -1) {
        int node = numLargeGroups + smallNode;
        nodeToGroupOrd[node] = g;
        smallNodeFieldOrd[smallNode] = fieldOrd;
        smallNode++;
      }
    }
    assert smallNode == numSmallDocs;

    // 4. Build the graph over the hybrid node space, scoring each node via nodeToGroupOrd.
    long graphDataOffset = graphData.getFilePointer();
    OnHeapHnswGraph graph =
        maybeBuildGraph(
            nodeCount,
            scorer.getHybridRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(), dedupValues, nodeToGroupOrd));
    int[][] graphLevelNodeOffsets = writeGraph(graph);
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    // 5. Postings for the promoted (large) groups only, keyed by large-group node ordinal.
    int[] largeGroupOffsets = new int[numLargeGroups + 1];
    int[] flattenedFieldOrds = new int[fieldOrdCount - numSmallDocs];
    computeLargeGroupPostings(
        fieldOrdToGroupOrd,
        fieldOrdCount,
        largeGroupNode,
        numLargeGroups,
        groupSizes,
        largeGroupOffsets,
        flattenedFieldOrds);

    writeHybridMeta(
        fieldInfo,
        groupCount,
        fieldOrdCount,
        nodeCount,
        numLargeGroups,
        numSmallDocs,
        graphDataOffset,
        graphDataLength,
        graph,
        graphLevelNodeOffsets,
        nodeToGroupOrd,
        largeGroupOffsets,
        flattenedFieldOrds,
        smallNodeFieldOrd);
  }

  /**
   * Flattens the large (promoted) groups' field ordinals into contiguous slices keyed by large-group
   * node ordinal. {@code largeGroupOffsets[n]..largeGroupOffsets[n+1]} delimits the documents of the
   * group promoted to node {@code n}.
   */
  private void computeLargeGroupPostings(
      FieldOrdToGroupOrd fieldOrdToGroupOrd,
      int fieldOrdCount,
      int[] largeGroupNode,
      int numLargeGroups,
      int[] groupSizes,
      int[] offsets,
      int[] flattened) {
    for (int g = 0; g < largeGroupNode.length; g++) {
      int node = largeGroupNode[g];
      if (node != -1) {
        offsets[node + 1] = groupSizes[g];
      }
    }
    for (int n = 0; n < numLargeGroups; n++) {
      offsets[n + 1] += offsets[n];
    }
    int[] cursor = ArrayUtil.copyOfSubArray(offsets, 0, numLargeGroups);
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      int node = largeGroupNode[fieldOrdToGroupOrd.get(fieldOrd)];
      if (node != -1) {
        flattened[cursor[node]++] = fieldOrd;
      }
    }
  }

  /**
   * Builds an HNSW graph of {@code nodeCount} nodes from {@code scorerSupplier}, or returns {@code
   * null} for tiny segments where a full scan is cheaper (mirrors {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter}).
   */
  private OnHeapHnswGraph maybeBuildGraph(int nodeCount, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    if (shouldCreateGraph(tinySegmentsThreshold, nodeCount) == false) {
      return null;
    }
    // During a merge with an executor and more than one worker, build the graph concurrently
    // (matching Lucene99HnswVectorsWriter). Flush, and single-worker merges, build single-threaded.
    if (activeMergeExecutor != null && activeMergeWorkers > 1) {
      // getGraph() returns the empty, pre-sized OnHeapHnswGraph that the concurrent workers fill in.
      OnHeapHnswGraph empty =
          HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, nodeCount)
              .getGraph();
      HnswConcurrentMergeBuilder concurrentBuilder =
          new HnswConcurrentMergeBuilder(
              activeMergeExecutor,
              activeMergeWorkers,
              scorerSupplier,
              beamWidth,
              empty,
              /* initializedNodes= */ null);
      concurrentBuilder.setInfoStream(segmentWriteState.infoStream);
      return concurrentBuilder.build(nodeCount);
    }
    HnswGraphBuilder builder =
        HnswGraphBuilder.create(scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, nodeCount);
    builder.setInfoStream(segmentWriteState.infoStream);
    return builder.build(nodeCount);
  }

  /**
   * Chooses {@link DedupLayoutMode#PLAIN} when the field has no effective de-duplication (one
   * distinct vector per document), otherwise {@link DedupLayoutMode#DEDUP}.
   */
  private static DedupLayoutMode selectMode(int distinctGroupsReferenced, int fieldOrdCount) {
    return distinctGroupsReferenced == fieldOrdCount
        ? DedupLayoutMode.PLAIN
        : DedupLayoutMode.DEDUP;
  }

  /**
   * Whether an HNSW graph is worth building for a segment with {@code numNodes} distinct vectors,
   * given the tiny-segments threshold {@code k}. Mirrors {@code
   * Lucene99HnswVectorsWriter#shouldCreateGraph}: {@code k <= 0} always builds; otherwise a graph
   * is built only if it would visit fewer nodes than a full scan.
   */
  private static boolean shouldCreateGraph(int k, int numNodes) {
    // TODO: k is reused as-is from the per-document Lucene99HnswVectorsWriter, but here nodes are
    //  distinct vectors, not documents. Revisit whether the threshold should scale with the
    //  distinct-vector count (e.g. many docs but few distinct vectors).
    if (k <= 0) {
      return true;
    }
    int expectedVisitedNodes = expectedVisitedNodes(k, numNodes);
    return numNodes > expectedVisitedNodes && expectedVisitedNodes > 0;
  }

  private DedupVectorValues getDedupVectorValues(FieldInfo fieldInfo) throws IOException {
    KnnVectorValues values =
        switch (fieldInfo.getVectorEncoding()) {
          case BYTE -> flatVectorsReader.getByteVectorValues(fieldInfo.name);
          case FLOAT16 -> flatVectorsReader.getFloat16VectorValues(fieldInfo.name);
          case FLOAT32 -> flatVectorsReader.getFloatVectorValues(fieldInfo.name);
        };
    if (values instanceof DedupVectorValues dedupValues) {
      return dedupValues;
    }
    return null;
  }

  /**
   * Flattens the group-to-field-ordinals mapping into {@code DistinctVectorPostings}, grouping the
   * field ordinals that reference each distinct vector contiguously by group ordinal.
   */
  private void computeDistinctVectorPostings(
      FieldOrdToGroupOrd fieldOrdToGroupOrd,
      int fieldOrdCount,
      int groupCount,
      int[] offsets,
      int[] flattened) {
    int[] counts = new int[groupCount];
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      counts[fieldOrdToGroupOrd.get(fieldOrd)]++;
    }
    for (int g = 0; g < groupCount; g++) {
      offsets[g + 1] = offsets[g] + counts[g];
    }
    int[] cursor = ArrayUtil.copyOfSubArray(offsets, 0, groupCount);
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      int g = fieldOrdToGroupOrd.get(fieldOrd);
      flattened[cursor[g]++] = fieldOrd;
    }
  }

  /**
   * Writes the graph's neighbor lists (in group-ordinal space) into {@code graphData} and returns
   * the per-level byte length of each node's list. Neighbors are sorted and delta-encoded exactly
   * as {@link org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter}
   */
  private int[][] writeGraph(OnHeapHnswGraph graph) throws IOException {
    if (graph == null) {
      return new int[0][0];
    }
    int countOnLevel0 = graph.size();
    int[][] offsets = new int[graph.numLevels()][];
    int[] scratch = new int[graph.maxConn() * 2];
    for (int level = 0; level < graph.numLevels(); level++) {
      NodesIterator sortedNodes = graph.getSortedNodes(level);
      offsets[level] = new int[sortedNodes.size()];
      int nodeOffsetId = 0;
      while (sortedNodes.hasNext()) {
        NeighborArray neighbors = graph.getNeighbors(level, sortedNodes.next());
        int size = neighbors.size();
        long offsetStart = graphData.getFilePointer();
        int[] nnodes = neighbors.nodes();
        Arrays.sort(nnodes, 0, size);
        int actualSize = 0;
        if (size > 0) {
          scratch[0] = nnodes[0];
          actualSize = 1;
        }
        for (int i = 1; i < size; i++) {
          assert nnodes[i] < countOnLevel0 : "node too large: " + nnodes[i] + ">=" + countOnLevel0;
          if (nnodes[i - 1] == nnodes[i]) {
            continue;
          }
          scratch[actualSize++] = nnodes[i] - nnodes[i - 1];
        }
        graphData.writeVInt(actualSize);
        graphData.writeGroupVInts(scratch, actualSize);
        offsets[level][nodeOffsetId++] = Math.toIntExact(graphData.getFilePointer() - offsetStart);
      }
    }
    return offsets;
  }

  private void writeEmptyField(FieldInfo fieldInfo) throws IOException {
    // groupCount == 0 signals a field with no graph.
    meta.writeInt(fieldInfo.number);
    meta.writeInt(0); // groupCount
    meta.writeInt(0); // fieldOrdCount
  }

  private void writeMeta(
      FieldInfo field,
      DedupLayoutMode mode,
      int groupCount,
      int fieldOrdCount,
      int graphNodeCount,
      long graphDataOffset,
      long graphDataLength,
      HnswGraph graph,
      int[][] graphLevelNodeOffsets,
      int[] groupOffsets,
      int[] flattenedFieldOrds)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(groupCount);
    meta.writeInt(fieldOrdCount);
    mode.write(meta);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);

    // graph nodes on each level (group-ordinal space for DEDUP, field-ordinal space for PLAIN)
    writeGraphMeta(graph, graphNodeCount, graphLevelNodeOffsets);

    // Postings are only written in DEDUP mode (PLAIN nodes are already documents).
    if (mode == DedupLayoutMode.DEDUP) {
      writePostings(groupCount, groupOffsets, flattenedFieldOrds);
    }
  }

  /**
   * Writes the per-level graph node lists and the monotonic node-offset table into {@code meta}
   * (and the offset data into {@code graphData}). {@code graphNodeCount} is the number of level-0
   * nodes (group ordinals for DEDUP, field ordinals for PLAIN, hybrid nodes for HYBRID).
   */
  private void writeGraphMeta(HnswGraph graph, int graphNodeCount, int[][] graphLevelNodeOffsets)
      throws IOException {
    if (graph == null) {
      meta.writeVInt(M);
      meta.writeVInt(0);
      return;
    }
    meta.writeVInt(graph.maxConn());
    meta.writeVInt(graph.numLevels());
    long valueCount = 0;
    for (int level = 0; level < graph.numLevels(); level++) {
      NodesIterator nodesOnLevel = graph.getNodesOnLevel(level);
      valueCount += nodesOnLevel.size();
      if (level > 0) {
        int[] nol = new int[nodesOnLevel.size()];
        int numberConsumed = nodesOnLevel.consume(nol);
        Arrays.sort(nol);
        assert numberConsumed == nodesOnLevel.size();
        meta.writeVInt(nol.length);
        for (int i = nodesOnLevel.size() - 1; i > 0; --i) {
          nol[i] -= nol[i - 1];
        }
        for (int n : nol) {
          assert n >= 0 : "delta encoding for nodes failed; expected nodes to be sorted";
          meta.writeVInt(n);
        }
      } else {
        assert nodesOnLevel.size() == graphNodeCount : "Level 0 expects to have all graph nodes";
      }
    }
    long start = graphData.getFilePointer();
    meta.writeLong(start);
    meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    DirectMonotonicWriter memoryOffsetsWriter =
        DirectMonotonicWriter.getInstance(meta, graphData, valueCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
    long cumulativeOffsetSum = 0;
    for (int[] levelOffsets : graphLevelNodeOffsets) {
      for (int v : levelOffsets) {
        memoryOffsetsWriter.add(cumulativeOffsetSum);
        cumulativeOffsetSum += v;
      }
    }
    memoryOffsetsWriter.finish();
    meta.writeLong(graphData.getFilePointer() - start);
  }

  /**
   * Writes a {@code DistinctVectorPostings} block: a monotonic offsets table ({@code
   * numEntries + 1}) followed by the flattened field ordinals. Used for DEDUP (keyed by group
   * ordinal) and the large-group portion of HYBRID (keyed by large-group node ordinal).
   */
  private void writePostings(int numEntries, int[] offsets, int[] flattenedFieldOrds)
      throws IOException {
    // Postings offsets (monotonic): numEntries + 1 entries.
    long groupOffsetsStart = graphData.getFilePointer();
    meta.writeLong(groupOffsetsStart);
    meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    DirectMonotonicWriter groupOffsetsWriter =
        DirectMonotonicWriter.getInstance(
            meta, graphData, numEntries + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
    for (int offset : offsets) {
      groupOffsetsWriter.add(offset);
    }
    groupOffsetsWriter.finish();
    meta.writeLong(graphData.getFilePointer() - groupOffsetsStart);

    // Postings flattened field ordinals (dense int).
    long fieldOrdsDataStart = graphData.getFilePointer();
    meta.writeLong(fieldOrdsDataStart);
    for (int fieldOrd : flattenedFieldOrds) {
      graphData.writeInt(fieldOrd);
    }
    meta.writeLong(graphData.getFilePointer() - fieldOrdsDataStart);
  }

  /**
   * Writes HYBRID metadata. Layout in {@code meta} after the common header:
   *
   * <ol>
   *   <li>graph meta (over {@code nodeCount} level-0 nodes)
   *   <li>{@code numLargeGroups}, {@code numSmallDocs}
   *   <li>{@code nodeToGroupOrd} block (dense int in {@code graphData}): maps each node to a
   *       group-view ordinal for scoring
   *   <li>large-group postings block (offsets + flattened field ords), keyed by large-group node
   *   <li>small-node field-ordinal block (dense int in {@code graphData}): one field ordinal per
   *       small-group doc node
   * </ol>
   */
  private void writeHybridMeta(
      FieldInfo field,
      int groupCount,
      int fieldOrdCount,
      int nodeCount,
      int numLargeGroups,
      int numSmallDocs,
      long graphDataOffset,
      long graphDataLength,
      HnswGraph graph,
      int[][] graphLevelNodeOffsets,
      int[] nodeToGroupOrd,
      int[] largeGroupOffsets,
      int[] flattenedFieldOrds,
      int[] smallNodeFieldOrd)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(groupCount);
    meta.writeInt(fieldOrdCount);
    DedupLayoutMode.HYBRID.write(meta);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);

    // numLargeGroups/numSmallDocs come before the graph meta so the reader knows the level-0 node
    // count (numLargeGroups + numSmallDocs) while parsing the graph meta.
    meta.writeInt(numLargeGroups);
    meta.writeInt(numSmallDocs);

    // Graph meta over the hybrid node space (level 0 has nodeCount nodes).
    writeGraphMeta(graph, nodeCount, graphLevelNodeOffsets);

    // nodeToGroupOrd: dense int, nodeCount entries.
    long nodeToGroupStart = graphData.getFilePointer();
    meta.writeLong(nodeToGroupStart);
    for (int groupOrd : nodeToGroupOrd) {
      graphData.writeInt(groupOrd);
    }
    meta.writeLong(graphData.getFilePointer() - nodeToGroupStart);

    // Large-group postings, keyed by large-group node ordinal (first numLargeGroups nodes).
    writePostings(numLargeGroups, largeGroupOffsets, flattenedFieldOrds);

    // Small-node field ordinals: dense int, numSmallDocs entries.
    long smallNodeStart = graphData.getFilePointer();
    meta.writeLong(smallNodeStart);
    for (int fieldOrd : smallNodeFieldOrd) {
      graphData.writeInt(fieldOrd);
    }
    meta.writeLong(graphData.getFilePointer() - smallNodeStart);
  }

  private void ensureFlatReaderOpen() throws IOException {
    if (flatVectorsReader == null) {
      flatVectorWriter.finish();
      flatVectorWriter.close();
      flatWriterClosed = true;
      // During flush, segmentWriteState.fieldInfos may be null; reconstruct from the added fields.
      FieldInfos fieldInfos = segmentWriteState.fieldInfos;
      if (fieldInfos == null) {
        fieldInfos = new FieldInfos(fields.toArray(new FieldInfo[0]));
      }
      SegmentReadState readState =
          new SegmentReadState(
              segmentWriteState.directory,
              segmentWriteState.segmentInfo,
              fieldInfos,
              segmentWriteState.context,
              segmentWriteState.segmentSuffix);
      flatVectorsReader = flatVectorsFormat.fieldsReader(readState);
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    if (flatWriterClosed == false) {
      flatVectorWriter.finish();
    }
    if (meta != null) {
      meta.writeInt(-1); // end of fields
      CodecUtil.writeFooter(meta);
    }
    if (graphData != null) {
      CodecUtil.writeFooter(graphData);
    }
  }

  @Override
  public long ramBytesUsed() {
    return flatVectorWriter.ramBytesUsed();
  }

  @Override
  public void close() throws IOException {
    if (flatWriterClosed) {
      IOUtils.close(meta, graphData, flatVectorsReader);
    } else {
      IOUtils.close(meta, graphData, flatVectorWriter, flatVectorsReader);
    }
  }
}
