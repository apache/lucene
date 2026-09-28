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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Reads the de-duplication-aware HNSW graph written by {@link DedupHnswVectorsWriter}.
 *
 * <p>The graph is stored over <b>distinct</b> vectors (group ordinals). At search time, the query
 * is scored against the group view (one entry per distinct vector), the group graph is traversed,
 * and each matched group node is expanded to all documents that reference it (via the stored {@link
 * GroupToFieldOrds} mapping), respecting per-document accept bits. This returns every document
 * sharing a matched distinct vector, each with that vector's similarity.
 *
 * @lucene.experimental
 */
final class DedupHnswVectorsReader extends KnnVectorsReader implements HnswGraphProvider {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupHnswVectorsReader.class);

  private final FlatVectorsReader flatVectorsReader;
  private final FieldInfos fieldInfos;
  private final Map<String, FieldEntry> fields;
  private final IndexInput vectorIndex;
  private final int version;

  DedupHnswVectorsReader(SegmentReadState state, FlatVectorsReader flatVectorsReader)
      throws IOException {
    this.flatVectorsReader = flatVectorsReader;
    this.fieldInfos = state.fieldInfos;
    this.fields = new HashMap<>();

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, DedupHnswVectorsWriter.META_EXTENSION);
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                DedupHnswVectorsWriter.META_CODEC_NAME,
                DedupHnswVectorsWriter.VERSION_START,
                DedupHnswVectorsWriter.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta);
      } catch (Throwable e) {
        priorE = e;
        throw e;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
      this.version = versionMeta;
      this.vectorIndex =
          openDataInput(
              state,
              versionMeta,
              DedupHnswVectorsWriter.DATA_EXTENSION,
              DedupHnswVectorsWriter.DATA_CODEC_NAME);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  private static IndexInput openDataInput(
      SegmentReadState state, int versionMeta, String fileExtension, String codecName)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IOContext context =
        state.context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);
    IndexInput in = state.directory.openInput(fileName, context);
    try {
      int versionData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              DedupHnswVectorsWriter.VERSION_START,
              DedupHnswVectorsWriter.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta=" + versionMeta + ", " + codecName + "=" + versionData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      return in;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, in);
      throw t;
    }
  }

  private void readFields(ChecksumIndexInput meta) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      fields.put(info.name, FieldEntry.read(meta, info));
    }
  }

  /** The underlying flat vectors reader that stores the de-duplicated (raw/quantized) vectors. */
  public FlatVectorsReader getFlatVectorsReader() {
    return flatVectorsReader;
  }

  private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
    FieldInfo info = fieldInfos.fieldInfo(field);
    FieldEntry entry;
    if (info == null || (entry = fields.get(field)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    if (info.getVectorEncoding() != expectedEncoding) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + info.getVectorEncoding()
              + " expected: "
              + expectedEncoding);
    }
    return entry;
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return flatVectorsReader.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return flatVectorsReader.getByteVectorValues(field);
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
    return flatVectorsReader.getFloat16VectorValues(field);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.FLOAT32);
    DedupVectorValues values = (DedupVectorValues) flatVectorsReader.getFloatVectorValues(field);
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(field);
    RandomVectorScorer groupScorer =
        scorer.getGroupRandomVectorScorer(
            fieldInfos.fieldInfo(field).getVectorSimilarityFunction(), values, target);
    searchGroupGraph(entry, values, groupScorer, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.BYTE);
    DedupVectorValues values = (DedupVectorValues) flatVectorsReader.getByteVectorValues(field);
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(field);
    RandomVectorScorer groupScorer =
        scorer.getGroupRandomVectorScorer(
            fieldInfos.fieldInfo(field).getVectorSimilarityFunction(), values, target);
    searchGroupGraph(entry, values, groupScorer, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, short[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    FieldEntry entry = getFieldEntry(field, VectorEncoding.FLOAT16);
    DedupVectorValues values = (DedupVectorValues) flatVectorsReader.getFloat16VectorValues(field);
    DedupFlatVectorsScorer scorer =
        (DedupFlatVectorsScorer) flatVectorsReader.getFlatVectorScorer(field);
    RandomVectorScorer groupScorer =
        scorer.getGroupRandomVectorScorer(
            fieldInfos.fieldInfo(field).getVectorSimilarityFunction(), values, target);
    searchGroupGraph(entry, values, groupScorer, knnCollector, acceptDocs);
  }

  /**
   * Runs an HNSW search over the group graph, then expands each collected group ordinal to all the
   * documents that reference it. The {@code groupScorer} operates in group-ordinal space (its
   * {@code maxOrd()} is the number of distinct vectors and {@code score(groupOrd)} scores the query
   * against the distinct vector).
   */
  private void searchGroupGraph(
      FieldEntry entry,
      DedupVectorValues values,
      RandomVectorScorer groupScorer,
      KnnCollector knnCollector,
      AcceptDocs acceptDocs)
      throws IOException {
    if (entry.groupCount == 0 || knnCollector.k() == 0) {
      return;
    }

    KnnVectorValues fieldView = (KnnVectorValues) values;
    GroupToFieldOrds groupToFieldOrds = new GroupToFieldOrds(entry, vectorIndex);

    // A field ordinal maps to a document via the flat reader's ordToDoc; build a per-group accept
    // bit set and a per-group->docs expander that respect per-document acceptDocs.
    Bits acceptedDocs = acceptDocs.bits();
    Bits groupAccept = groupAcceptBits(entry, groupToFieldOrds, fieldView, acceptedDocs);

    KnnCollector expandingCollector =
        new DedupExpandingCollector(knnCollector, groupToFieldOrds, fieldView, acceptedDocs);

    HnswGraph graph = getGraph(entry);
    int graphSize = entry.groupCount;
    int filteredGroupCount =
        groupAccept == null ? graphSize : countAccepted(groupAccept, graphSize);
    filteredGroupCount = Math.min(filteredGroupCount, graphSize);

    int numGroups = groupScorer.maxOrd();
    // Only use HNSW when a graph was actually built (tiny segments skip it, see the writer). When
    // present, the graph has one node per group so graph.size() == groupCount.
    boolean hasGraph = entry.vectorIndexLength > 0;
    boolean doHnsw = hasGraph && knnCollector.k() < numGroups;
    int unfilteredVisit = HnswGraphSearcher.expectedVisitedNodes(knnCollector.k(), graphSize);
    if (unfilteredVisit >= filteredGroupCount || graphSize == 0) {
      doHnsw = false;
    }

    if (doHnsw) {
      HnswGraphSearcher.search(
          groupScorer, expandingCollector, graph, groupAccept, filteredGroupCount);
    } else {
      // Exhaustive: score every group and expand.
      for (int g = 0; g < numGroups; g++) {
        if (groupAccept != null && groupAccept.get(g) == false) {
          continue;
        }
        if (knnCollector.earlyTerminated()) {
          break;
        }
        knnCollector.incVisitedCount(1);
        float score = groupScorer.score(g);
        expandingCollector.collect(g, score);
      }
    }
  }

  private static int countAccepted(Bits groupAccept, int groupCount) {
    int count = 0;
    for (int g = 0; g < groupCount; g++) {
      if (groupAccept.get(g)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Builds a group-level accept {@link Bits}: a group is accepted if any field ordinal referencing
   * it maps to an accepted document. Returns {@code null} if all documents are accepted.
   */
  private Bits groupAcceptBits(
      FieldEntry entry,
      GroupToFieldOrds groupToFieldOrds,
      KnnVectorValues fieldView,
      Bits acceptedDocs)
      throws IOException {
    if (acceptedDocs == null) {
      return null;
    }
    boolean[] accepted = new boolean[entry.groupCount];
    for (int g = 0; g < entry.groupCount; g++) {
      int start = groupToFieldOrds.offset(g);
      int end = groupToFieldOrds.offset(g + 1);
      for (int i = start; i < end; i++) {
        int fieldOrd = groupToFieldOrds.fieldOrd(i);
        int docId = fieldView.ordToDoc(fieldOrd);
        if (acceptedDocs.get(docId)) {
          accepted[g] = true;
          break;
        }
      }
    }
    final int len = entry.groupCount;
    return new Bits() {
      @Override
      public boolean get(int index) {
        return accepted[index];
      }

      @Override
      public int length() {
        return len;
      }
    };
  }

  @Override
  public HnswGraph getGraph(String field) throws IOException {
    FieldInfo info = fieldInfos.fieldInfo(field);
    FieldEntry entry;
    if (info == null || (entry = fields.get(field)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    if (entry.vectorIndexLength > 0) {
      return getGraph(entry);
    }
    return HnswGraph.EMPTY;
  }

  private HnswGraph getGraph(FieldEntry entry) throws IOException {
    if (entry.vectorIndexLength == 0) {
      return HnswGraph.EMPTY;
    }
    return new OffHeapHnswGraph(entry, vectorIndex, version);
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    flatVectorsReader.checkIntegrity(merge);
    CodecUtil.checksumEntireFile(vectorIndex, merge);
  }

  /** Approximate heap usage of this reader, including the delegate flat reader. */
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + flatVectorsReader.ramBytesUsed()
        + (long) fields.size() * FieldEntry.SHALLOW_SIZE;
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    FieldEntry entry = fields.get(fieldInfo.name);
    var flat = flatVectorsReader.getOffHeapByteSize(fieldInfo);
    if (entry == null) {
      return flat;
    }
    long graphBytes =
        entry.vectorIndexLength + entry.groupOffsetsLength + entry.fieldOrdsDataLength;
    var graph = Map.of(DedupHnswVectorsWriter.DATA_EXTENSION, graphBytes);
    return KnnVectorsReader.mergeOffHeapByteSizeMaps(flat, graph);
  }

  @Override
  public int getVectorCount(FieldInfo fieldInfo) {
    // The number of documents (field ordinals), not the number of distinct vectors.
    FieldEntry entry = fields.get(fieldInfo.name);
    if (entry == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" not found");
    }
    return entry.fieldOrdCount;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(flatVectorsReader, vectorIndex);
  }

  /**
   * A {@link KnnCollector} decorator that, on {@code collect(groupOrd, score)}, expands the group
   * ordinal to all its referencing documents (via {@link GroupToFieldOrds}) and collects each
   * accepted document with the group's score. Group ordinals map to documents through the flat
   * reader's {@code ordToDoc}.
   */
  private static final class DedupExpandingCollector extends KnnCollector.Decorator {
    private final GroupToFieldOrds groupToFieldOrds;
    private final KnnVectorValues fieldView;
    private final Bits acceptedDocs;

    DedupExpandingCollector(
        KnnCollector collector,
        GroupToFieldOrds groupToFieldOrds,
        KnnVectorValues fieldView,
        Bits acceptedDocs) {
      super(collector);
      this.groupToFieldOrds = groupToFieldOrds;
      this.fieldView = fieldView;
      this.acceptedDocs = acceptedDocs;
    }

    @Override
    public boolean collect(int groupOrd, float similarity) {
      boolean collectedAny = false;
      try {
        int start = groupToFieldOrds.offset(groupOrd);
        int end = groupToFieldOrds.offset(groupOrd + 1);
        for (int i = start; i < end; i++) {
          int fieldOrd = groupToFieldOrds.fieldOrd(i);
          int docId = fieldView.ordToDoc(fieldOrd);
          if (acceptedDocs == null || acceptedDocs.get(docId)) {
            collectedAny |= super.collect(docId, similarity);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return collectedAny;
    }
  }

  /**
   * Random access to the group-ordinal to field-ordinals mapping (the inverse of {@code
   * fieldOrdToGroupOrd}), stored in the data file in CSR form: a monotonic {@code offsets} table
   * plus a flattened array of field ordinals.
   */
  private static final class GroupToFieldOrds {
    private final DirectMonotonicReader offsets;
    private final RandomAccessInput data;

    GroupToFieldOrds(FieldEntry entry, IndexInput vectorIndex) throws IOException {
      RandomAccessInput offsetsSlice =
          vectorIndex.randomAccessSlice(entry.groupOffsetsOffset, entry.groupOffsetsLength);
      this.offsets = DirectMonotonicReader.getInstance(entry.groupOffsetsMeta, offsetsSlice);
      this.data =
          vectorIndex.randomAccessSlice(entry.fieldOrdsDataOffset, entry.fieldOrdsDataLength);
    }

    int offset(int group) {
      return (int) offsets.get(group);
    }

    int fieldOrd(int index) throws IOException {
      return data.readInt((long) index * Integer.BYTES);
    }
  }

  /** Per-field metadata parsed from the {@code .vdhm} file. */
  private record FieldEntry(
      VectorEncoding vectorEncoding,
      int groupCount,
      int fieldOrdCount,
      long vectorIndexOffset,
      long vectorIndexLength,
      long groupOffsetsOffset,
      DirectMonotonicReader.Meta groupOffsetsMeta,
      long groupOffsetsLength,
      long fieldOrdsDataOffset,
      long fieldOrdsDataLength,
      int M,
      int numLevels,
      int[][] nodesByLevel,
      DirectMonotonicReader.Meta offsetsMeta,
      long offsetsOffset,
      int offsetsBlockShift,
      long offsetsLength) {

    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);

    static FieldEntry read(IndexInput input, FieldInfo info) throws IOException {
      int groupCount = input.readInt();
      int fieldOrdCount = input.readInt();
      if (groupCount == 0) {
        // empty field: no graph, no CSR
        return new FieldEntry(
            info.getVectorEncoding(),
            0,
            fieldOrdCount,
            0,
            0,
            0,
            null,
            0,
            0,
            0,
            0,
            0,
            new int[0][],
            null,
            0,
            0,
            0);
      }

      long vectorIndexOffset = input.readVLong();
      long vectorIndexLength = input.readVLong();

      int M = input.readVInt();
      int numLevels = input.readVInt();
      int[][] nodesByLevel = new int[numLevels][];
      long numberOfOffsets = 0;
      for (int level = 0; level < numLevels; level++) {
        if (level > 0) {
          int numNodesOnLevel = input.readVInt();
          numberOfOffsets += numNodesOnLevel;
          nodesByLevel[level] = new int[numNodesOnLevel];
          nodesByLevel[level][0] = input.readVInt();
          for (int i = 1; i < numNodesOnLevel; i++) {
            nodesByLevel[level][i] = nodesByLevel[level][i - 1] + input.readVInt();
          }
        } else {
          numberOfOffsets += groupCount;
        }
      }

      long offsetsOffset;
      int offsetsBlockShift;
      DirectMonotonicReader.Meta offsetsMeta;
      long offsetsLength;
      if (numberOfOffsets > 0) {
        offsetsOffset = input.readLong();
        offsetsBlockShift = input.readVInt();
        offsetsMeta = DirectMonotonicReader.loadMeta(input, numberOfOffsets, offsetsBlockShift);
        offsetsLength = input.readLong();
      } else {
        offsetsOffset = 0;
        offsetsBlockShift = 0;
        offsetsMeta = null;
        offsetsLength = 0;
      }

      // CSR inverse offsets (monotonic, groupCount + 1) then flattened field ordinals.
      long groupOffsetsOffset = input.readLong();
      int groupOffsetsBlockShift = input.readVInt();
      DirectMonotonicReader.Meta groupOffsetsMeta =
          DirectMonotonicReader.loadMeta(input, groupCount + 1L, groupOffsetsBlockShift);
      long groupOffsetsLength = input.readLong();
      long fieldOrdsDataOffset = input.readLong();
      long fieldOrdsDataLength = input.readLong();

      return new FieldEntry(
          info.getVectorEncoding(),
          groupCount,
          fieldOrdCount,
          vectorIndexOffset,
          vectorIndexLength,
          groupOffsetsOffset,
          groupOffsetsMeta,
          groupOffsetsLength,
          fieldOrdsDataOffset,
          fieldOrdsDataLength,
          M,
          numLevels,
          nodesByLevel,
          offsetsMeta,
          offsetsOffset,
          offsetsBlockShift,
          offsetsLength);
    }
  }

  /**
   * Off-heap HNSW graph over group ordinals; a direct adaptation of {@code
   * Lucene99HnswVectorsReader.OffHeapHnswGraph}, reading delta-encoded neighbor lists from the data
   * file.
   */
  private static final class OffHeapHnswGraph extends HnswGraph {
    private final IndexInput dataIn;
    private final int[][] nodesByLevel;
    private final int numLevels;
    private final int entryNode;
    private final int size;
    private final int maxConn;
    private final int version;
    private final DirectMonotonicReader graphLevelNodeOffsets;
    private final long[] graphLevelNodeIndexOffsets;
    private final int[] currentNeighborsBuffer;

    private int arcCount;
    private int arcUpTo;
    private int arc;

    OffHeapHnswGraph(FieldEntry entry, IndexInput vectorIndex, int version) throws IOException {
      this.dataIn =
          vectorIndex.slice("graph-data", entry.vectorIndexOffset, entry.vectorIndexLength);
      this.nodesByLevel = entry.nodesByLevel;
      this.numLevels = entry.numLevels;
      this.entryNode = numLevels > 1 ? nodesByLevel[numLevels - 1][0] : 0;
      this.size = entry.groupCount;
      this.version = version;
      RandomAccessInput addressesData =
          vectorIndex.randomAccessSlice(entry.offsetsOffset, entry.offsetsLength);
      this.graphLevelNodeOffsets =
          DirectMonotonicReader.getInstance(entry.offsetsMeta, addressesData);
      this.currentNeighborsBuffer = new int[entry.M * 2];
      this.maxConn = entry.M;
      this.graphLevelNodeIndexOffsets = new long[numLevels];
      graphLevelNodeIndexOffsets[0] = 0;
      for (int i = 1; i < numLevels; i++) {
        int nodeCount = nodesByLevel[i - 1] == null ? size : nodesByLevel[i - 1].length;
        graphLevelNodeIndexOffsets[i] = graphLevelNodeIndexOffsets[i - 1] + nodeCount;
      }
    }

    @Override
    public void seek(int level, int targetOrd) throws IOException {
      int targetIndex =
          level == 0
              ? targetOrd
              : Arrays.binarySearch(nodesByLevel[level], 0, nodesByLevel[level].length, targetOrd);
      assert targetIndex >= 0
          : "seek level=" + level + " target=" + targetOrd + " not found: " + targetIndex;
      dataIn.seek(graphLevelNodeOffsets.get(targetIndex + graphLevelNodeIndexOffsets[level]));
      arcCount = dataIn.readVInt();
      assert arcCount <= currentNeighborsBuffer.length : "too many neighbors: " + arcCount;
      if (arcCount > 0) {
        int sum = 0;
        GroupVIntUtil.readGroupVInts(dataIn, currentNeighborsBuffer, arcCount);
        for (int i = 0; i < arcCount; i++) {
          sum += currentNeighborsBuffer[i];
          currentNeighborsBuffer[i] = sum;
        }
      }
      arc = -1;
      arcUpTo = 0;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int nextNeighbor() {
      if (arcUpTo >= arcCount) {
        return NO_MORE_DOCS;
      }
      arc = currentNeighborsBuffer[arcUpTo];
      ++arcUpTo;
      return arc;
    }

    @Override
    public int neighborCount() {
      return arcCount;
    }

    @Override
    public int numLevels() {
      return numLevels;
    }

    @Override
    public int maxConn() {
      return maxConn;
    }

    @Override
    public int entryNode() {
      return entryNode;
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      if (level == 0) {
        return new DenseNodesIterator(size());
      } else {
        return new ArrayNodesIterator(nodesByLevel[level]);
      }
    }
  }
}
