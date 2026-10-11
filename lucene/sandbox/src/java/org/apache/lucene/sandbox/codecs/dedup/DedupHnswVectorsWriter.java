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
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Writes a de-duplication-aware HNSW graph on top of a de-duplicating flat vectors format.
 *
 * <p>This writer picks a per-field layout to avoid duplicate nodes when documents share vectors:
 *
 * <ul>
 *   <li>{@link DedupLayoutMode#DEDUP}: one node per distinct vector, plus a {@code
 *       DistinctVectorPostings} mapping each group ordinal to its referencing documents.
 *   <li>{@link DedupLayoutMode#PLAIN}: one node per document (no postings), when a field has no
 *       effective de-duplication.
 *   <li>{@link DedupLayoutMode#HYBRID}: promoted groups become one node each (with postings) while
 *       small-group documents stay as per-document nodes.
 * </ul>
 *
 * <p>{@link DedupHnswVectorsReader} expands each matched node back to its document(s). The graph
 * ({@code .vdhd}) and per-field metadata ({@code .vdhm}) are written in two files; the exact
 * per-layout byte layout is documented on {@link #writeMeta} and {@link #writeHybridMeta}.
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
  // builder. null during flush (flush always builds single-threaded).
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
      FlatVectorsFormat flatVectorsFormat,
      FlatVectorsWriter flatVectorWriter,
      int numMergeWorkers,
      TaskExecutor mergeExec,
      int tinySegmentsThreshold,
      int hybridGroupThreshold)
      throws IOException {
    this.segmentWriteState = state;
    this.M = M;
    this.beamWidth = beamWidth;
    this.tinySegmentsThreshold = tinySegmentsThreshold;
    this.hybridGroupThreshold = hybridGroupThreshold;
    this.numMergeWorkers = numMergeWorkers;
    this.mergeExec = mergeExec;
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
    // Choose the executor for concurrent graph building during this merge: prefer the format's
    // explicit mergeExec; otherwise fall back to the merge scheduler's intra-merge executor.
    // Captured here (on the merge thread) and consumed by buildAndWriteGraph -> maybeBuildGraph in
    // the returned IORunnable.
    final TaskExecutor chosenExec;
    final int chosenWorkers;
    if (mergeExec != null) {
      chosenExec = mergeExec;
      chosenWorkers = numMergeWorkers;
    } else if (mergeState.intraMergeTaskExecutor != null) {
      chosenExec = new TaskExecutor(mergeState.intraMergeTaskExecutor);
      // numMergeWorkers defaults to 1 unless the format was configured otherwise; use it to bound
      // the intra-merge parallelism.
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
    // TODO: build the graph incrementally as distinct vectors appear, instead of all at once here,
    //  to smoothen peak memory and flush latency.
    KnnVectorValues values = getVectorValues(fieldInfo);
    if (values instanceof DedupVectorValues == false) {
      throw new IllegalStateException(
          "DedupHnswVectorsWriter requires a de-duplicating flat vectors format; got "
              + (values == null ? "null" : values.getClass().getName())
              + " for field \""
              + fieldInfo.name
              + "\"");
    }
    DedupVectorValues dedupValues = (DedupVectorValues) values;
    if (dedupValues.getGroupView().size() == 0) {
      writeEmptyField(fieldInfo);
      return;
    }

    int groupCount = dedupValues.getGroupView().size();
    int fieldOrdCount = values.size();

    FieldOrdToGroupOrd fieldOrdToGroupOrd = dedupValues.getFieldOrdToGroupOrd();

    // Count the documents per group, and number of distinct groups this field references to
    // drive the layout choice for graph build
    // TODO: persist the distinct count as metadata at flush/merge time instead of recomputing here.
    GroupSizes groupSizes = computeGroupSizes(fieldOrdToGroupOrd, fieldOrdCount, groupCount);

    LayoutSelection selection = selectMode(groupSizes, fieldOrdCount);
    switch (selection.mode()) {
      case PLAIN -> writePlainField(fieldInfo, values, fieldOrdCount);
      case HYBRID ->
          writeHybridField(
              fieldInfo,
              dedupValues,
              groupSizes.sizes(),
              selection.partition(),
              groupCount,
              fieldOrdCount);
      case DEDUP -> writeDedupField(fieldInfo, dedupValues, groupCount, fieldOrdCount);
    }
  }

  /** Per-group document counts and the number of groups referenced by at least one document. */
  private record GroupSizes(int[] sizes, int distinctGroups) {}

  /**
   * Computes {@link GroupSizes} in a single pass over the field ordinals. {@code distinctGroups ==
   * fieldOrdCount} means the field has no within-field de-duplication.
   */
  private static GroupSizes computeGroupSizes(
      FieldOrdToGroupOrd fieldOrdToGroupOrd, int fieldOrdCount, int groupCount) {
    int[] sizes = new int[groupCount];
    int distinctGroups = 0;
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      if (sizes[fieldOrdToGroupOrd.get(fieldOrd)]++ == 0) {
        distinctGroups++;
      }
    }
    return new GroupSizes(sizes, distinctGroups);
  }

  /**
   * Partitions groups into promoted groups and the rest. {@code promotedGroups} marks the promoted
   * groups; each is assigned a dense ordinal in {@code [0, numPromotedGroups)} held in {@code
   * promotedGroupOrdinals} (only meaningful for groups in {@code promotedGroups}).
   */
  private record PromotedGroupPartition(
      FixedBitSet promotedGroups, int[] promotedGroupOrdinals, int numPromotedGroups) {

    /** Whether group {@code g} was promoted to its own node. */
    boolean isPromoted(int g) {
      return promotedGroups.get(g);
    }

    /** The dense promoted-group ordinal assigned to promoted group {@code g}. */
    int promotedGroupOrdinal(int g) {
      return promotedGroupOrdinals[g];
    }
  }

  /** Promotes groups referenced by more than {@code threshold} documents to their own node. */
  private static PromotedGroupPartition partitionPromotedGroups(int[] groupSizes, int threshold) {
    FixedBitSet promotedGroups = new FixedBitSet(groupSizes.length);
    int[] promotedGroupOrdinals = new int[groupSizes.length];
    int numPromotedGroups = 0;
    for (int g = 0; g < groupSizes.length; g++) {
      if (groupSizes[g] > threshold) {
        promotedGroups.set(g);
        promotedGroupOrdinals[g] = numPromotedGroups++;
      }
    }
    return new PromotedGroupPartition(promotedGroups, promotedGroupOrdinals, numPromotedGroups);
  }

  /**
   * PLAIN layout: one graph node per document (field ordinal), scored at full precision, with no
   * postings. Search then behaves like a vanilla HNSW search.
   */
  private void writePlainField(FieldInfo fieldInfo, KnnVectorValues values, int fieldOrdCount)
      throws IOException {
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(fieldInfo.name);

    long graphDataOffset = graphData.getFilePointer();
    OnHeapHnswGraph graph =
        maybeBuildGraph(
            fieldOrdCount,
            scorer.getPlainRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(), values));
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
   * HYBRID layout: one graph node per <b>promoted</b> group (a distinct vector referenced by more
   * than {@link #hybridGroupThreshold} documents) and one graph node per document that belongs to a
   * non-promoted group.
   *
   * <p>Node ordinals are laid out as:
   *
   * <ul>
   *   <li>{@code [0, numPromotedGroups)} — the promoted groups, in ascending group-ordinal order.
   *       Each carries a {@code DistinctVectorPostings} slice listing all its field ordinals
   *       (documents).
   *   <li>{@code [numPromotedGroups, numPromotedGroups + numSmallDocs)} — the documents of all
   *       small groups, in ascending field-ordinal order. Each carries exactly one field ordinal.
   * </ul>
   *
   * <p>A {@code nodeToGroupOrd} map (one entry per node) resolves every node to a vector in the
   * group view so the HNSW graph can be built and searched uniformly in group-view scoring space.
   */
  private void writeHybridField(
      FieldInfo fieldInfo,
      DedupVectorValues dedupValues,
      int[] groupSizes,
      PromotedGroupPartition partition,
      int groupCount,
      int fieldOrdCount)
      throws IOException {
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(fieldInfo.name);
    FieldOrdToGroupOrd fieldOrdToGroupOrd = dedupValues.getFieldOrdToGroupOrd();
    int numPromotedGroups = partition.numPromotedGroups();

    // Documents that remain as individual nodes (belong to small groups).
    int numSmallDocs = 0;
    for (int g = 0; g < groupCount; g++) {
      if (partition.isPromoted(g) == false) {
        numSmallDocs += groupSizes[g];
      }
    }
    int nodeCount = numPromotedGroups + numSmallDocs;

    // 1. Build the per-node -> group-view-ordinal map and the per-small-node -> field ordinal map.
    //    Node layout: promoted-group nodes first (ascending group ord), then small-group docs
    //    (ascending field ord).
    int[] nodeToGroupOrd = new int[nodeCount];
    int[] smallNodeFieldOrd = new int[numSmallDocs];
    // Promoted-group nodes score against the group's distinct vector.
    for (int g = 0; g < groupCount; g++) {
      if (partition.isPromoted(g)) {
        nodeToGroupOrd[partition.promotedGroupOrdinal(g)] = g;
      }
    }
    // Small-group doc nodes score against their doc's (shared) distinct vector.
    int smallNode = 0;
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      int g = fieldOrdToGroupOrd.get(fieldOrd);
      if (partition.isPromoted(g) == false) {
        int node = numPromotedGroups + smallNode;
        nodeToGroupOrd[node] = g;
        smallNodeFieldOrd[smallNode] = fieldOrd;
        smallNode++;
      }
    }
    assert smallNode == numSmallDocs;

    // 2. Build the graph over the hybrid node space, scoring each node via nodeToGroupOrd.
    long graphDataOffset = graphData.getFilePointer();
    OnHeapHnswGraph graph =
        maybeBuildGraph(
            nodeCount,
            scorer.getHybridRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                dedupValues,
                new NodeToGroupOrd.NodeToGroupOrdArray(nodeToGroupOrd)));
    int[][] graphLevelNodeOffsets = writeGraph(graph);
    long graphDataLength = graphData.getFilePointer() - graphDataOffset;

    // 3. Postings for the promoted groups only, keyed by promoted-group ordinal.
    int[] promotedGroupOffsets = new int[numPromotedGroups + 1];
    int[] flattenedFieldOrds = new int[fieldOrdCount - numSmallDocs];
    computePromotedGroupPostings(
        fieldOrdToGroupOrd,
        fieldOrdCount,
        partition,
        numPromotedGroups,
        groupSizes,
        promotedGroupOffsets,
        flattenedFieldOrds);

    writeHybridMeta(
        fieldInfo,
        groupCount,
        fieldOrdCount,
        nodeCount,
        numPromotedGroups,
        numSmallDocs,
        graphDataOffset,
        graphDataLength,
        graph,
        graphLevelNodeOffsets,
        nodeToGroupOrd,
        promotedGroupOffsets,
        flattenedFieldOrds,
        smallNodeFieldOrd);
  }

  /**
   * Flattens the promoted groups' field ordinals into contiguous slices keyed by promoted-group
   * ordinal. {@code promotedGroupOffsets[n]..promotedGroupOffsets[n+1]} delimits the documents of
   * the group promoted to node {@code n}.
   */
  private void computePromotedGroupPostings(
      FieldOrdToGroupOrd fieldOrdToGroupOrd,
      int fieldOrdCount,
      PromotedGroupPartition partition,
      int numPromotedGroups,
      int[] groupSizes,
      int[] offsets,
      int[] flattened) {
    for (int g = 0; g < groupSizes.length; g++) {
      if (partition.isPromoted(g)) {
        offsets[partition.promotedGroupOrdinal(g) + 1] = groupSizes[g];
      }
    }
    for (int n = 0; n < numPromotedGroups; n++) {
      offsets[n + 1] += offsets[n];
    }
    int[] cursor = ArrayUtil.copyOfSubArray(offsets, 0, numPromotedGroups);
    for (int fieldOrd = 0; fieldOrd < fieldOrdCount; fieldOrd++) {
      int g = fieldOrdToGroupOrd.get(fieldOrd);
      if (partition.isPromoted(g)) {
        flattened[cursor[partition.promotedGroupOrdinal(g)]++] = fieldOrd;
      }
    }
  }

  /**
   * Builds an HNSW graph of {@code nodeCount} nodes from {@code scorerSupplier}, or returns {@code
   * null} for tiny segments where a full scan is cheaper.
   */
  private OnHeapHnswGraph maybeBuildGraph(int nodeCount, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    if (shouldCreateGraph(tinySegmentsThreshold, nodeCount) == false) {
      return null;
    }
    // During a merge with an executor and more than one worker, build the graph concurrently.
    // Flush, and single-worker merges, build single-threaded.
    if (activeMergeExecutor != null && activeMergeWorkers > 1) {
      // getGraph() returns the empty, pre-sized OnHeapHnswGraph that the concurrent workers fill
      // in.
      OnHeapHnswGraph empty =
          HnswGraphBuilder.create(
                  scorerSupplier, M, beamWidth, HnswGraphBuilder.randSeed, nodeCount)
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

  /** The chosen layout and, for HYBRID, the promoted-group partition that drives it. */
  private record LayoutSelection(DedupLayoutMode mode, PromotedGroupPartition partition) {}

  /**
   * Chooses the per-field layout:
   *
   * <ul>
   *   <li>PLAIN when the field has no effective de-duplication (one distinct vector per document),
   *       or when HYBRID is enabled but no group exceeds the threshold (nothing to promote).
   *   <li>HYBRID when enabled (threshold > 0) and at least one group is big enough to promote.
   *   <li>DEDUP otherwise.
   * </ul>
   */
  private LayoutSelection selectMode(GroupSizes groupSizes, int fieldOrdCount) {
    if (groupSizes.distinctGroups() == fieldOrdCount) {
      return new LayoutSelection(DedupLayoutMode.PLAIN, null);
    }
    if (hybridGroupThreshold > 0) {
      PromotedGroupPartition partition =
          partitionPromotedGroups(groupSizes.sizes(), hybridGroupThreshold);
      return partition.numPromotedGroups() == 0
          ? new LayoutSelection(DedupLayoutMode.PLAIN, null)
          : new LayoutSelection(DedupLayoutMode.HYBRID, partition);
    }
    return new LayoutSelection(DedupLayoutMode.DEDUP, null);
  }

  /**
   * Whether an HNSW graph is worth building for a segment with {@code numNodes} distinct vectors,
   * given the tiny-segments threshold {@code k}: {@code k <= 0} always builds; otherwise a graph is
   * built only if it would visit fewer nodes than a full scan.
   */
  private static boolean shouldCreateGraph(int k, int numNodes) {
    // TODO: k is reused as-is from the per-document threshold, but here nodes are distinct vectors,
    //  not documents. Revisit whether the threshold should scale with the distinct-vector count
    //  (e.g. many docs but few distinct vectors).
    if (k <= 0) {
      return true;
    }
    int expectedVisitedNodes = expectedVisitedNodes(k, numNodes);
    return numNodes > expectedVisitedNodes && expectedVisitedNodes > 0;
  }

  private KnnVectorValues getVectorValues(FieldInfo fieldInfo) throws IOException {
    return switch (fieldInfo.getVectorEncoding()) {
      case BYTE -> flatVectorsReader.getByteVectorValues(fieldInfo.name);
      case FLOAT16 -> flatVectorsReader.getFloat16VectorValues(fieldInfo.name);
      case FLOAT32 -> flatVectorsReader.getFloatVectorValues(fieldInfo.name);
    };
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
   * the per-level byte length of each node's list. Neighbors are sorted and delta-encoded.
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

  /**
   * Writes PLAIN/DEDUP metadata: field number, group count, field-ordinal count, layout mode, graph
   * offset/length, graph meta, and (DEDUP only) the {@code DistinctVectorPostings}. PLAIN writes no
   * postings since its nodes are already documents.
   */
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
        DirectMonotonicWriter.getInstance(
            meta, graphData, valueCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
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
   * Writes a {@code DistinctVectorPostings} block: a monotonic offsets table ({@code numEntries +
   * 1}) followed by the flattened field ordinals. Used for DEDUP (keyed by group ordinal) and the
   * promoted-group portion of HYBRID (keyed by promoted-group ordinal).
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
   *   <li>{@code numPromotedGroups}, {@code numSmallDocs}
   *   <li>{@code nodeToGroupOrd} block (dense int in {@code graphData}): maps each node to a
   *       group-view ordinal for scoring
   *   <li>promoted-group postings block (offsets + flattened field ords), keyed by promoted-group
   *       ordinal
   *   <li>small-node field-ordinal block (dense int in {@code graphData}): one field ordinal per
   *       small-group doc node
   * </ol>
   */
  private void writeHybridMeta(
      FieldInfo field,
      int groupCount,
      int fieldOrdCount,
      int nodeCount,
      int numPromotedGroups,
      int numSmallDocs,
      long graphDataOffset,
      long graphDataLength,
      HnswGraph graph,
      int[][] graphLevelNodeOffsets,
      int[] nodeToGroupOrd,
      int[] promotedGroupOffsets,
      int[] flattenedFieldOrds,
      int[] smallNodeFieldOrd)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(groupCount);
    meta.writeInt(fieldOrdCount);
    DedupLayoutMode.HYBRID.write(meta);
    meta.writeVLong(graphDataOffset);
    meta.writeVLong(graphDataLength);

    // numPromotedGroups/numSmallDocs come before the graph meta so the reader knows the level-0
    // node
    // count (numPromotedGroups + numSmallDocs) while parsing the graph meta.
    meta.writeInt(numPromotedGroups);
    meta.writeInt(numSmallDocs);

    // Graph meta over the hybrid node space (level 0 has nodeCount nodes).
    writeGraphMeta(graph, nodeCount, graphLevelNodeOffsets);

    // nodeToGroupOrd: bit-packed, nodeCount entries.
    int maxGroupOrd = 0;
    for (int groupOrd : nodeToGroupOrd) {
      maxGroupOrd = Math.max(maxGroupOrd, groupOrd);
    }
    int nodeToGroupOrdBitsPerValue = DirectWriter.bitsRequired(maxGroupOrd);
    long nodeToGroupStart = graphData.getFilePointer();
    meta.writeLong(nodeToGroupStart);
    DirectWriter nodeToGroupWriter =
        DirectWriter.getInstance(graphData, nodeCount, nodeToGroupOrdBitsPerValue);
    for (int groupOrd : nodeToGroupOrd) {
      nodeToGroupWriter.add(groupOrd);
    }
    nodeToGroupWriter.finish();
    meta.writeLong(graphData.getFilePointer() - nodeToGroupStart);
    meta.writeInt(nodeToGroupOrdBitsPerValue);

    // Promoted-group postings, keyed by promoted-group ordinal (first numPromotedGroups nodes).
    writePostings(numPromotedGroups, promotedGroupOffsets, flattenedFieldOrds);

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
