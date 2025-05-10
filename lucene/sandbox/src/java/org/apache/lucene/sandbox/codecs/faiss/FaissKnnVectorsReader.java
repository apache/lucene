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
package org.apache.lucene.sandbox.codecs.faiss;

import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.DATA_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.META_CODEC_NAME;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.VERSION_START;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexRead;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexSearch;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Read per-segment Faiss indexes and associated metadata.
 *
 * @lucene.experimental
 */
final class FaissKnnVectorsReader extends KnnVectorsReader {
  private final FlatVectorsReader rawVectorsReader;
  private final IndexInput meta, data;
  private final Map<String, IndexEntry> indexMap;
  private final Arena arena;
  private boolean closed;

  public FaissKnnVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader)
      throws IOException {
    this.rawVectorsReader = rawVectorsReader;
    this.indexMap = new HashMap<>();
    this.arena = Arena.ofShared();
    this.closed = false;

    boolean failure = true;
    try {
      meta =
          openInput(
              state,
              META_EXTENSION,
              META_CODEC_NAME,
              VERSION_START,
              VERSION_CURRENT,
              state.context);
      data =
          openInput(
              state,
              DATA_EXTENSION,
              DATA_CODEC_NAME,
              VERSION_START,
              VERSION_CURRENT,
              state.context.withReadAdvice(ReadAdvice.RANDOM));

      Map.Entry<String, IndexEntry> entry;
      while ((entry = parseNextField(state)) != null) {
        this.indexMap.put(entry.getKey(), entry.getValue());
      }

      failure = false;
    } finally {
      if (failure) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  private IndexInput openInput(
      SegmentReadState state,
      String extension,
      String codecName,
      int versionStart,
      int versionEnd,
      IOContext context)
      throws IOException {

    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
    IndexInput input = state.directory.openInput(fileName, context);
    CodecUtil.checkIndexHeader(
        input, codecName, versionStart, versionEnd, state.segmentInfo.getId(), state.segmentSuffix);
    return input;
  }

  private Map.Entry<String, IndexEntry> parseNextField(SegmentReadState state) throws IOException {
    int fieldNumber = meta.readInt();
    if (fieldNumber == -1) {
      return null;
    }

    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
    if (fieldInfo == null) {
      throw new IllegalStateException("Invalid field");
    }

    long dataOffset = meta.readLong();
    long dataLength = meta.readLong();

    // See flags defined in c_api/index_io_c.h
    int ioFlags = 3;

    // Read index into memory
    MemorySegment indexPointer =
        indexRead(data.slice(fieldInfo.name, dataOffset, dataLength), ioFlags)
            // Ensure timely cleanup
            .reinterpret(arena, LibFaissC::freeIndex);

    return Map.entry(
        fieldInfo.name, new IndexEntry(indexPointer, fieldInfo.getVectorSimilarityFunction()));
  }

  @Override
  public void checkIntegrity() throws IOException {
    rawVectorsReader.checkIntegrity();
    CodecUtil.checksumEntireFile(meta);
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return rawVectorsReader.getFloatVectorValues(field);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) {
    // TODO: Support using SQ8 quantization, see:
    //  - https://github.com/opensearch-project/k-NN/pull/2425
    throw new UnsupportedOperationException("Byte vectors not supported");
  }

  @Override
  public void search(String field, float[] vector, KnnCollector knnCollector, Bits acceptDocs) {
    IndexEntry entry = indexMap.get(field);
    if (entry != null) {
      indexSearch(entry.indexPointer, entry.function, vector, knnCollector, acceptDocs);
    }
  }

  @Override
  public void search(String field, byte[] vector, KnnCollector knnCollector, Bits acceptDocs) {
    // TODO: Support using SQ8 quantization, see:
    //  - https://github.com/opensearch-project/k-NN/pull/2425
    throw new UnsupportedOperationException("Byte vectors not supported");
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    // TODO: How to estimate Faiss usage?
    return rawVectorsReader.getOffHeapByteSize(fieldInfo);
  }

  @Override
  public void close() throws IOException {
    if (closed == false) {
      IOUtils.close(rawVectorsReader, arena::close, meta, data);
      closed = true;
    }
  }

  private record IndexEntry(MemorySegment indexPointer, VectorSimilarityFunction function) {}
}
