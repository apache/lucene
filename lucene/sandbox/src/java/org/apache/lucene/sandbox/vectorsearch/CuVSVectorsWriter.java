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
package org.apache.lucene.sandbox.vectorsearch;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_INDEX_CODEC_NAME;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_INDEX_EXT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_META_CODEC_EXT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_META_CODEC_NAME;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsReader.handleThrowable;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.BruteForceIndexParams;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraIndexParams;
import com.nvidia.cuvs.CagraIndexParams.CagraGraphBuildAlgo;
import com.nvidia.cuvs.CuVSResources;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.Sorter.DocMap;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;

/** KnnVectorsWriter for CuVS, responsible for merge and flush of vectors into GPU */
public class CuVSVectorsWriter extends KnnVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED = shallowSizeOfInstance(CuVSVectorsWriter.class);

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(CuVSVectorsWriter.class.getName());

  /** The name of the CUVS component for the info-stream * */
  public static final String CUVS_COMPONENT = "CUVS";

  // The minimum number of vectors in the dataset required before
  // we attempt to build a Cagra index
  static final int MIN_CAGRA_INDEX_SIZE = 2;

  private final int cuvsWriterThreads;
  private final int intGraphDegree;
  private final int graphDegree;

  private final CuVSResources resources;
  private final IndexType indexType;

  @SuppressWarnings("unused")
  private final MergeStrategy mergeStrategy;

  private final FlatVectorsWriter flatVectorsWriter; // for writing the raw vectors
  private final List<CuVSFieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, cuvsIndex;
  private final InfoStream infoStream;
  private boolean finished;

  /** Merge strategy used for CuVS */
  public enum MergeStrategy {
    TRIVIAL_MERGE,
    NON_TRIVIAL_MERGE
  }

  /** The CuVS index Type. */
  public enum IndexType {
    /** Builds a Cagra index. */
    CAGRA(true, false, false),
    /** Builds a Brute Force index. */
    BRUTE_FORCE(false, true, false),
    /** Builds an HSNW index - suitable for searching on CPU. */
    HNSW(false, false, true),
    /** Builds a Cagra and a Brute Force index. */
    CAGRA_AND_BRUTE_FORCE(true, true, false);
    private final boolean cagra, bruteForce, hnsw;

    IndexType(boolean cagra, boolean bruteForce, boolean hnsw) {
      this.cagra = cagra;
      this.bruteForce = bruteForce;
      this.hnsw = hnsw;
    }

    public boolean cagra() {
      return cagra;
    }

    public boolean bruteForce() {
      return bruteForce;
    }

    public boolean hnsw() {
      return hnsw;
    }
  }

  public CuVSVectorsWriter(
      SegmentWriteState state,
      int cuvsWriterThreads,
      int intGraphDegree,
      int graphDegree,
      MergeStrategy mergeStrategy,
      IndexType indexType,
      CuVSResources resources,
      FlatVectorsWriter flatVectorsWriter)
      throws IOException {
    super();
    this.mergeStrategy = mergeStrategy;
    this.indexType = indexType;
    this.cuvsWriterThreads = cuvsWriterThreads;
    this.intGraphDegree = intGraphDegree;
    this.graphDegree = graphDegree;
    this.resources = resources;
    this.flatVectorsWriter = flatVectorsWriter;
    this.infoStream = state.infoStream;

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, CUVS_META_CODEC_EXT);
    String cagraFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, CUVS_INDEX_EXT);

    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      cuvsIndex = state.directory.createOutput(cagraFileName, state.context);
      CodecUtil.writeIndexHeader(
          meta,
          CUVS_META_CODEC_NAME,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      CodecUtil.writeIndexHeader(
          cuvsIndex,
          CUVS_INDEX_CODEC_NAME,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    var encoding = fieldInfo.getVectorEncoding();
    if (encoding != FLOAT32) {
      throw new IllegalArgumentException("expected float32, got:" + encoding);
    }
    var writer = Objects.requireNonNull(flatVectorsWriter.addField(fieldInfo));
    @SuppressWarnings("unchecked")
    var flatWriter = (FlatFieldVectorsWriter<float[]>) writer;
    var cuvsFieldWriter = new CuVSFieldWriter(fieldInfo, flatWriter);
    fields.add(cuvsFieldWriter);
    return writer;
  }

  static String indexMsg(int size, int... args) {
    StringBuilder sb = new StringBuilder("cagra index params");
    sb.append(": size=").append(size);
    sb.append(", intGraphDegree=").append(args[0]);
    sb.append(", actualIntGraphDegree=").append(args[1]);
    sb.append(", graphDegree=").append(args[2]);
    sb.append(", actualGraphDegree=").append(args[3]);
    return sb.toString();
  }

  private CagraIndexParams cagraIndexParams(int size) {
    if (size < 2) {
      // https://github.com/rapidsai/cuvs/issues/666
      throw new IllegalArgumentException("cagra index must be greater than 2");
    }
    var minIntGraphDegree = Math.min(intGraphDegree, size - 1);
    var minGraphDegree = Math.min(graphDegree, minIntGraphDegree);
    // log.info(indexMsg(size, intGraphDegree, minIntGraphDegree, graphDegree, minGraphDegree));

    return new CagraIndexParams.Builder()
        .withNumWriterThreads(cuvsWriterThreads)
        .withIntermediateGraphDegree(minIntGraphDegree)
        .withGraphDegree(minGraphDegree)
        .withCagraGraphBuildAlgo(CagraGraphBuildAlgo.NN_DESCENT)
        .build();
  }

  static long nanosToMillis(long nanos) {
    return Duration.ofNanos(nanos).toMillis();
  }

  private void info(String msg) {
    if (infoStream.isEnabled(CUVS_COMPONENT)) {
      infoStream.message(CUVS_COMPONENT, msg);
    }
  }

  private void writeCagraIndex(OutputStream os, float[][] vectors) throws Throwable {
    if (vectors.length < 2) {
      throw new IllegalArgumentException(vectors.length + " vectors, less than min [2] required");
    }
    CagraIndexParams params = cagraIndexParams(vectors.length);
    long startTime = System.nanoTime();
    var index =
        CagraIndex.newBuilder(resources).withDataset(vectors).withIndexParams(params).build();
    long elapsedMillis = nanosToMillis(System.nanoTime() - startTime);
    info("Cagra index created in " + elapsedMillis + "ms, with " + vectors.length + " vectors");
    Path tmpFile = Files.createTempFile(resources.tempDirectory(), "tmpindex", "cag");
    index.serialize(os, tmpFile);
    index.destroyIndex();
  }

  private void writeBruteForceIndex(OutputStream os, float[][] vectors) throws Throwable {
    BruteForceIndexParams params =
        new BruteForceIndexParams.Builder()
            .withNumWriterThreads(32) // TODO: Make this configurable later.
            .build();
    long startTime = System.nanoTime();
    var index =
        BruteForceIndex.newBuilder(resources).withIndexParams(params).withDataset(vectors).build();
    long elapsedMillis = nanosToMillis(System.nanoTime() - startTime);
    info("bf index created in " + elapsedMillis + "ms, with " + vectors.length + " vectors");
    index.serialize(os);
    index.destroyIndex();
  }

  private void writeHNSWIndex(OutputStream os, float[][] vectors) throws Throwable {
    if (vectors.length < 2) {
      throw new IllegalArgumentException(vectors.length + " vectors, less than min [2] required");
    }
    CagraIndexParams indexParams = cagraIndexParams(vectors.length);
    long startTime = System.nanoTime();
    var index =
        CagraIndex.newBuilder(resources).withDataset(vectors).withIndexParams(indexParams).build();
    long elapsedMillis = nanosToMillis(System.nanoTime() - startTime);
    info("HNSW index created in " + elapsedMillis + "ms, with " + vectors.length + " vectors");
    Path tmpFile = Files.createTempFile("tmpindex", "hnsw");
    index.serializeToHNSW(os, tmpFile);
    index.destroyIndex();
  }

  @Override
  public void flush(int maxDoc, DocMap sortMap) throws IOException {
    flatVectorsWriter.flush(maxDoc, sortMap);
    for (var field : fields) {
      if (sortMap == null) {
        writeField(field);
      } else {
        writeSortingField(field, sortMap);
      }
    }
  }

  private void writeField(CuVSFieldWriter fieldData) throws IOException {
    // TODO: Argh! https://github.com/rapidsai/cuvs/issues/698
    float[][] vectors = fieldData.getVectors().toArray(float[][]::new);
    writeFieldInternal(fieldData.fieldInfo(), vectors);
  }

  private void writeSortingField(CuVSFieldWriter fieldData, Sorter.DocMap sortMap)
      throws IOException {
    DocsWithFieldSet oldDocsWithFieldSet = fieldData.getDocsWithFieldSet();
    final int[] new2OldOrd = new int[oldDocsWithFieldSet.cardinality()]; // new ord to old ord

    mapOldOrdToNewOrd(oldDocsWithFieldSet, sortMap, null, new2OldOrd, null);

    // TODO: Argh! https://github.com/rapidsai/cuvs/issues/698
    // Also will be replaced with the cuVS merge api
    float[][] oldVectors = fieldData.getVectors().toArray(float[][]::new);
    float[][] newVectors = new float[oldVectors.length][];
    for (int i = 0; i < oldVectors.length; i++) {
      newVectors[i] = oldVectors[new2OldOrd[i]];
    }
    writeFieldInternal(fieldData.fieldInfo(), newVectors);
  }

  private void writeFieldInternal(FieldInfo fieldInfo, float[][] vectors) throws IOException {
    if (vectors.length == 0) {
      writeEmpty(fieldInfo);
      return;
    }
    long cagraIndexOffset, cagraIndexLength = 0L;
    long bruteForceIndexOffset, bruteForceIndexLength = 0L;
    long hnswIndexOffset, hnswIndexLength = 0L;

    // workaround for the minimum number of vectors for Cagra
    IndexType indexType =
        this.indexType.cagra() && vectors.length < MIN_CAGRA_INDEX_SIZE
            ? IndexType.BRUTE_FORCE
            : this.indexType;

    try {
      cagraIndexOffset = cuvsIndex.getFilePointer();
      if (indexType.cagra()) {
        try {
          var cagraIndexOutputStream = new IndexOutputOutputStream(cuvsIndex);
          writeCagraIndex(cagraIndexOutputStream, vectors);
        } catch (Throwable t) {
          handleThrowableWithIgnore(t, CANNOT_GENERATE_CAGRA);
          // workaround for cuVS issue
          indexType = IndexType.BRUTE_FORCE;
        }
        cagraIndexLength = cuvsIndex.getFilePointer() - cagraIndexOffset;
      }

      bruteForceIndexOffset = cuvsIndex.getFilePointer();
      if (indexType.bruteForce()) {
        var bruteForceIndexOutputStream = new IndexOutputOutputStream(cuvsIndex);
        writeBruteForceIndex(bruteForceIndexOutputStream, vectors);
        bruteForceIndexLength = cuvsIndex.getFilePointer() - bruteForceIndexOffset;
      }

      hnswIndexOffset = cuvsIndex.getFilePointer();
      if (indexType.hnsw()) {
        var hnswIndexOutputStream = new IndexOutputOutputStream(cuvsIndex);
        if (vectors.length > MIN_CAGRA_INDEX_SIZE) {
          try {
            writeHNSWIndex(hnswIndexOutputStream, vectors);
          } catch (Throwable t) {
            handleThrowableWithIgnore(t, CANNOT_GENERATE_CAGRA);
          }
        }
        hnswIndexLength = cuvsIndex.getFilePointer() - hnswIndexOffset;
      }

      // StringBuilder sb = new StringBuilder("writeField ");
      // sb.append(": fieldInfo.name=").append(fieldInfo.name);
      // sb.append(", fieldInfo.number=").append(fieldInfo.number);
      // sb.append(", size=").append(vectors.length);
      // sb.append(", cagraIndexLength=").append(cagraIndexLength);
      // sb.append(", bruteForceIndexLength=").append(bruteForceIndexLength);
      // sb.append(", hnswIndexLength=").append(hnswIndexLength);
      // log.info(sb.toString());

      writeMeta(
          fieldInfo,
          vectors.length,
          cagraIndexOffset,
          cagraIndexLength,
          bruteForceIndexOffset,
          bruteForceIndexLength,
          hnswIndexOffset,
          hnswIndexLength);
    } catch (Throwable t) {
      handleThrowable(t);
    }
  }

  private void writeEmpty(FieldInfo fieldInfo) throws IOException {
    writeMeta(fieldInfo, 0, 0L, 0L, 0L, 0L, 0L, 0L);
  }

  private void writeMeta(
      FieldInfo field,
      int count,
      long cagraIndexOffset,
      long cagraIndexLength,
      long bruteForceIndexOffset,
      long bruteForceIndexLength,
      long hnswIndexOffset,
      long hnswIndexLength)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeInt(field.getVectorEncoding().ordinal());
    meta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
    meta.writeInt(field.getVectorDimension());
    meta.writeInt(count);
    meta.writeVLong(cagraIndexOffset);
    meta.writeVLong(cagraIndexLength);
    meta.writeVLong(bruteForceIndexOffset);
    meta.writeVLong(bruteForceIndexLength);
    meta.writeVLong(hnswIndexOffset);
    meta.writeVLong(hnswIndexLength);
  }

  static int distFuncToOrd(VectorSimilarityFunction func) {
    for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
      if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
        return (byte) i;
      }
    }
    throw new IllegalArgumentException("invalid distance function: " + func);
  }

  // We currently ignore this, until cuVS supports tiered indices
  private static final String CANNOT_GENERATE_CAGRA =
      """
      Could not generate an intermediate CAGRA graph because the initial \
      kNN graph contains too many invalid or duplicated neighbor nodes. \
      This error can occur, for example, if too many overflows occur \
      during the norm computation between the dataset vectors\
      """;

  static void handleThrowableWithIgnore(Throwable t, String msg) throws IOException {
    if (t.getMessage().contains(msg)) {
      return;
    }
    handleThrowable(t);
  }

  /** Copies the vector values into dst. Returns the actual number of vectors copied. */
  private static int getVectorData(FloatVectorValues floatVectorValues, float[][] dst)
      throws IOException {
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    int count = 0;
    KnnVectorValues.DocIndexIterator iter = floatVectorValues.iterator();
    for (int docV = iter.nextDoc(); docV != NO_MORE_DOCS; docV = iter.nextDoc()) {
      assert iter.index() == count;
      dst[iter.index()] = floatVectorValues.vectorValue(iter.index());
      docsWithField.add(docV);
      count++;
    }
    return docsWithField.cardinality();
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    flatVectorsWriter.mergeOneField(fieldInfo, mergeState);
    try {
      final FloatVectorValues mergedVectorValues =
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> throw new AssertionError("bytes not supported");
            case FLOAT32 ->
                KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
          };

      float[][] vectors = new float[mergedVectorValues.size()][mergedVectorValues.dimension()];
      int ret = getVectorData(mergedVectorValues, vectors);
      if (ret < vectors.length) {
        vectors = ArrayUtil.copyOfSubArray(vectors, 0, ret);
      }
      writeFieldInternal(fieldInfo, vectors);
    } catch (Throwable t) {
      handleThrowable(t);
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    flatVectorsWriter.finish();

    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (cuvsIndex != null) {
      CodecUtil.writeFooter(cuvsIndex);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, cuvsIndex, flatVectorsWriter);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (var field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }
}
