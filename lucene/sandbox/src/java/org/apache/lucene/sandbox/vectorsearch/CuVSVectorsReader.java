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

import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_INDEX_CODEC_NAME;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_INDEX_EXT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_META_CODEC_EXT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.CUVS_META_CODEC_NAME;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.vectorsearch.CuVSVectorsFormat.VERSION_START;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.BruteForceQuery;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CagraQuery;
import com.nvidia.cuvs.CagraSearchParams;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.HnswIndex;
import com.nvidia.cuvs.HnswIndexParams;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.IntToIntFunction;

/** KnnVectorsReader instance associated with CuVS format */
public class CuVSVectorsReader extends KnnVectorsReader {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(CuVSVectorsReader.class.getName());

  private final CuVSResources resources;
  private final FlatVectorsReader flatVectorsReader; // for reading the raw vectors
  private final FieldInfos fieldInfos;
  private final IntObjectHashMap<FieldEntry> fields;
  private final IntObjectHashMap<CuVSIndex> cuvsIndices;
  private final IndexInput cuvsIndexInput;

  public CuVSVectorsReader(
      SegmentReadState state, CuVSResources resources, FlatVectorsReader flatReader)
      throws IOException {
    this.resources = resources;
    this.flatVectorsReader = flatReader;
    this.fieldInfos = state.fieldInfos;
    this.fields = new IntObjectHashMap<>();

    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, CUVS_META_CODEC_EXT);
    boolean success = false;
    int versionMeta = -1;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorException = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                CUVS_META_CODEC_NAME,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta);
      } catch (Throwable exception) {
        priorException = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorException);
      }
      var ioContext = state.context.withReadAdvice(ReadAdvice.SEQUENTIAL);
      cuvsIndexInput = openCuVSInput(state, versionMeta, ioContext);
      cuvsIndices = loadCuVSIndices();
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private static IndexInput openCuVSInput(
      SegmentReadState state, int versionMeta, IOContext context) throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, CUVS_INDEX_EXT);
    IndexInput in = state.directory.openInput(fileName, context);
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              CUVS_INDEX_CODEC_NAME,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      checkVersion(versionMeta, versionVectorData, in);
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  private void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry.dims()) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dims());
    }
  }

  private void readFields(ChecksumIndexInput meta) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta, info);
      validateFieldEntry(info, fieldEntry);
      fields.put(info.number, fieldEntry);
    }
  }

  // List of vector similarity functions. This list is defined here, in order
  // to avoid an undesirable dependency on the declaration and order of values
  // in VectorSimilarityFunction. The list values and order must be identical
  // to that of {@link o.a.l.c.l.Lucene94FieldInfosFormat#SIMILARITY_FUNCTIONS}.
  static final List<VectorSimilarityFunction> SIMILARITY_FUNCTIONS =
      List.of(
          VectorSimilarityFunction.EUCLIDEAN,
          VectorSimilarityFunction.DOT_PRODUCT,
          VectorSimilarityFunction.COSINE,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);

  static VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
    int i = input.readInt();
    if (i < 0 || i >= SIMILARITY_FUNCTIONS.size()) {
      throw new IllegalArgumentException("invalid distance function: " + i);
    }
    return SIMILARITY_FUNCTIONS.get(i);
  }

  static VectorEncoding readVectorEncoding(DataInput input) throws IOException {
    int encodingId = input.readInt();
    if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
      throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
    }
    return VectorEncoding.values()[encodingId];
  }

  private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    if (similarityFunction != info.getVectorSimilarityFunction()) {
      throw new IllegalStateException(
          "Inconsistent vector similarity function for field=\""
              + info.name
              + "\"; "
              + similarityFunction
              + " != "
              + info.getVectorSimilarityFunction());
    }
    return FieldEntry.readEntry(input, vectorEncoding, info.getVectorSimilarityFunction());
  }

  private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
    final FieldInfo info = fieldInfos.fieldInfo(field);
    final FieldEntry fieldEntry;
    if (info == null || (fieldEntry = fields.get(info.number)) == null) {
      throw new IllegalArgumentException("field=\"" + field + "\" not found");
    }
    if (fieldEntry.vectorEncoding != expectedEncoding) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fieldEntry.vectorEncoding
              + " expected: "
              + expectedEncoding);
    }
    return fieldEntry;
  }

  private IntObjectHashMap<CuVSIndex> loadCuVSIndices() throws IOException {
    var indices = new IntObjectHashMap<CuVSIndex>();
    for (var e : fields) {
      var fieldEntry = e.value;
      int fieldNumber = e.key;
      var cuvsIndex = loadCuVSIndex(fieldEntry);
      indices.put(fieldNumber, cuvsIndex);
    }
    return indices;
  }

  private CuVSIndex loadCuVSIndex(FieldEntry fieldEntry) throws IOException {
    CagraIndex cagraIndex = null;
    BruteForceIndex bruteForceIndex = null;
    HnswIndex hnswIndex = null;

    try {
      long len = fieldEntry.cagraIndexLength();
      if (len > 0) {
        long off = fieldEntry.cagraIndexOffset();
        try (var slice = cuvsIndexInput.slice("cagra index", off, len);
            var in = new IndexInputInputStream(slice)) {
          cagraIndex = CagraIndex.newBuilder(resources).from(in).build();
        }
      }

      len = fieldEntry.bruteForceIndexLength();
      if (len > 0) {
        long off = fieldEntry.bruteForceIndexOffset();
        try (var slice = cuvsIndexInput.slice("bf index", off, len);
            var in = new IndexInputInputStream(slice)) {
          bruteForceIndex = BruteForceIndex.newBuilder(resources).from(in).build();
        }
      }

      len = fieldEntry.hnswIndexLength();
      if (len > 0) {
        long off = fieldEntry.hnswIndexOffset();
        try (var slice = cuvsIndexInput.slice("hnsw index", off, len);
            var in = new IndexInputInputStream(slice)) {
          var params = new HnswIndexParams.Builder().build();
          hnswIndex = HnswIndex.newBuilder(resources).withIndexParams(params).from(in).build();
        }
      }
    } catch (Throwable t) {
      handleThrowable(t);
    }
    return new CuVSIndex(cagraIndex, bruteForceIndex, hnswIndex);
  }

  @Override
  public void close() throws IOException {
    var closeableStream =
        Stream.concat(
            Stream.of(flatVectorsReader, cuvsIndexInput),
            stream(cuvsIndices.values().iterator()).map(cursor -> cursor.value));
    IOUtils.close(closeableStream::iterator);
  }

  static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }

  @Override
  public void checkIntegrity() throws IOException {
    // TODO: Pending implementation
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return flatVectorsReader.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    throw new UnsupportedOperationException("byte vectors not supported");
  }

  /** Native float to float function */
  public interface FloatToFloatFunction {
    float apply(float v);
  }

  static long[] bitsToLongArray(Bits bits) {
    if (bits instanceof FixedBitSet fixedBitSet) {
      return fixedBitSet.getBits();
    } else {
      return FixedBitSet.copyOf(bits).getBits();
    }
  }

  static FloatToFloatFunction getScoreNormalizationFunc(VectorSimilarityFunction sim) {
    // TODO: check for different similarities
    return score -> (1f / (1f + score));
  }

  // This is a hack - https://github.com/rapidsai/cuvs/issues/696
  static final int FILTER_OVER_SAMPLE = 10;

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    var fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
    if (fieldEntry.count() == 0 || knnCollector.k() == 0) {
      return;
    }

    var fieldNumber = fieldInfos.fieldInfo(field).number;
    // log.info("fieldNumber=" + fieldNumber + ", fieldEntry.count()=" + fieldEntry.count());

    CuVSIndex cuvsIndex = cuvsIndices.get(fieldNumber);
    if (cuvsIndex == null) {
      throw new IllegalStateException("not index found for field:" + field);
    }

    int collectorTopK = knnCollector.k();
    if (acceptDocs != null) {
      collectorTopK = knnCollector.k() * FILTER_OVER_SAMPLE;
    }
    final int topK = Math.min(collectorTopK, fieldEntry.count());
    assert topK > 0 : "Expected topK > 0, got:" + topK;

    Map<Integer, Float> result;
    if (knnCollector.k() <= 1024 && cuvsIndex.getCagraIndex() != null) {
      // log.info("searching cagra index");
      CagraSearchParams searchParams =
          new CagraSearchParams.Builder(resources)
              .withItopkSize(topK) // TODO: params
              .withSearchWidth(1)
              .build();

      var query =
          new CagraQuery.Builder()
              .withTopK(topK)
              .withSearchParams(searchParams)
              // we don't use ord to doc mapping, https://github.com/rapidsai/cuvs/issues/699
              .withMapping(null)
              .withQueryVectors(new float[][] {target})
              .build();

      CagraIndex cagraIndex = cuvsIndex.getCagraIndex();
      List<Map<Integer, Float>> searchResult = null;
      try {
        searchResult = cagraIndex.search(query).getResults();
      } catch (Throwable t) {
        handleThrowable(t);
      }
      // List expected to have only one entry because of single query "target".
      assert searchResult.size() == 1;
      result = searchResult.getFirst();
    } else {
      BruteForceIndex bruteforceIndex = cuvsIndex.getBruteforceIndex();
      assert bruteforceIndex != null;
      // log.info("searching brute index, with actual topK=" + topK);
      var queryBuilder =
          new BruteForceQuery.Builder().withQueryVectors(new float[][] {target}).withTopK(topK);
      BruteForceQuery query = queryBuilder.build();

      List<Map<Integer, Float>> searchResult = null;
      try {
        searchResult = bruteforceIndex.search(query).getResults();
      } catch (Throwable t) {
        handleThrowable(t);
      }
      assert searchResult.size() == 1;
      result = searchResult.getFirst();
    }
    assert result != null;

    final var rawValues = flatVectorsReader.getFloatVectorValues(field);
    final Bits acceptedOrds = rawValues.getAcceptOrds(acceptDocs);
    final var ordToDocFunction = (IntToIntFunction) rawValues::ordToDoc;
    final var scoreCorrectionFunction = getScoreNormalizationFunc(fieldEntry.similarityFunction);

    for (var entry : result.entrySet()) {
      int ord = entry.getKey();
      float score = entry.getValue();
      if (acceptedOrds == null || acceptedOrds.get(ord)) {
        if (knnCollector.earlyTerminated()) {
          break;
        }
        assert ord >= 0 : "unexpected ord: " + ord;
        int doc = ordToDocFunction.apply(ord);
        float correctedScore = scoreCorrectionFunction.apply(score);
        knnCollector.incVisitedCount(1);
        knnCollector.collect(doc, correctedScore);
      }
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    throw new UnsupportedOperationException("byte vectors not supported");
  }

  record FieldEntry(
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int dims,
      int count,
      long cagraIndexOffset,
      long cagraIndexLength,
      long bruteForceIndexOffset,
      long bruteForceIndexLength,
      long hnswIndexOffset,
      long hnswIndexLength) {

    static FieldEntry readEntry(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      var dims = input.readInt();
      var count = input.readInt();
      var cagraIndexOffset = input.readVLong();
      var cagraIndexLength = input.readVLong();
      var bruteForceIndexOffset = input.readVLong();
      var bruteForceIndexLength = input.readVLong();
      var hnswIndexOffset = input.readVLong();
      var hnswIndexLength = input.readVLong();
      return new FieldEntry(
          vectorEncoding,
          similarityFunction,
          dims,
          count,
          cagraIndexOffset,
          cagraIndexLength,
          bruteForceIndexOffset,
          bruteForceIndexLength,
          hnswIndexOffset,
          hnswIndexLength);
    }
  }

  static void checkVersion(int versionMeta, int versionVectorData, IndexInput in)
      throws CorruptIndexException {
    if (versionMeta != versionVectorData) {
      throw new CorruptIndexException(
          "Format versions mismatch: meta="
              + versionMeta
              + ", "
              + CUVS_META_CODEC_NAME
              + "="
              + versionVectorData,
          in);
    }
  }

  static void handleThrowable(Throwable t) throws IOException {
    switch (t) {
      case IOException ioe -> throw ioe;
      case Error error -> throw error;
      case RuntimeException re -> throw re;
      case null, default -> throw new RuntimeException("UNEXPECTED: exception type", t);
    }
  }
}
