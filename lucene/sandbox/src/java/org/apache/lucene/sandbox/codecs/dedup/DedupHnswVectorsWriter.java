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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.IOUtils;
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
      FlatVectorsFormat flatVectorsFormat,
      FlatVectorsWriter flatVectorWriter)
      throws IOException {
    this.segmentWriteState = state;
    this.M = M;
    this.beamWidth = beamWidth;
    this.tinySegmentsThreshold = tinySegmentsThreshold;
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
    return () -> {
      mergeState.checkAborted();
      ensureFlatReaderOpen();
      buildAndWriteGraph(fieldInfo);
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

    // When there is no effective de-duplication (every document has a distinct vector), the group
    // machinery is pure overhead, so build a plain document-space graph; otherwise de-duplicate.
    if (selectMode(groupCount, fieldOrdCount) == DedupLayoutMode.PLAIN) {
      writePlainField(fieldInfo, dedupValues, fieldOrdCount);
    } else {
      writeDedupField(fieldInfo, dedupValues, groupCount, fieldOrdCount);
    }
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
   * Builds an HNSW graph of {@code nodeCount} nodes from {@code scorerSupplier}, or returns {@code
   * null} for tiny segments where a full scan is cheaper (mirrors {@link
   * org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter}).
   */
  private OnHeapHnswGraph maybeBuildGraph(int nodeCount, RandomVectorScorerSupplier scorerSupplier)
      throws IOException {
    if (shouldCreateGraph(tinySegmentsThreshold, nodeCount) == false) {
      return null;
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
  private static DedupLayoutMode selectMode(int groupCount, int fieldOrdCount) {
    return groupCount == fieldOrdCount ? DedupLayoutMode.PLAIN : DedupLayoutMode.DEDUP;
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
    if (graph == null) {
      meta.writeVInt(M);
      meta.writeVInt(0);
    } else {
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

    // Postings are only written in DEDUP mode (PLAIN nodes are already documents).
    if (mode == DedupLayoutMode.DEDUP) {
      // Postings offsets (monotonic): groupCount + 1 entries.
      long groupOffsetsStart = graphData.getFilePointer();
      meta.writeLong(groupOffsetsStart);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      DirectMonotonicWriter groupOffsetsWriter =
          DirectMonotonicWriter.getInstance(
              meta, graphData, groupCount + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      for (int offset : groupOffsets) {
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
