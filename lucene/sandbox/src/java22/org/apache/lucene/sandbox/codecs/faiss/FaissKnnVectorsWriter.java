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
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.createIndex;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexWrite;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.hnsw.IntToIntFunction;

public final class FaissKnnVectorsWriter extends KnnVectorsWriter {
  private final String description, indexParams;
  private final FlatVectorsWriter rawVectorsWriter;
  private final IndexOutput meta, data;
  private final Map<FieldInfo, FlatFieldVectorsWriter<?>> rawFields;
  private boolean finished;

  public FaissKnnVectorsWriter(
      String description,
      String indexParams,
      SegmentWriteState state,
      FlatVectorsWriter rawVectorsWriter)
      throws IOException {

    this.description = description;
    this.indexParams = indexParams;
    this.rawVectorsWriter = rawVectorsWriter;
    this.rawFields = new HashMap<>();
    this.finished = false;

    boolean failure = true;
    try {
      this.meta = openOutput(state, META_EXTENSION, META_CODEC_NAME);
      this.data = openOutput(state, DATA_EXTENSION, DATA_CODEC_NAME);
      failure = false;
    } finally {
      if (failure) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private IndexOutput openOutput(SegmentWriteState state, String extension, String codecName)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, extension);
    IndexOutput output = state.directory.createOutput(fileName, state.context);
    CodecUtil.writeIndexHeader(
        output, codecName, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    return output;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    rawVectorsWriter.mergeOneField(fieldInfo, mergeState);
    switch (fieldInfo.getVectorEncoding()) {
      case BYTE ->
          // TODO: Support using SQ8 quantization, see
          // https://github.com/opensearch-project/k-NN/pull/2425
          throw new UnsupportedOperationException("Byte vectors not supported");
      case FLOAT32 -> {
        FloatVectorValues merged =
            KnnVectorsWriter.MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
        writeFloatField(fieldInfo, merged, doc -> doc);
      }
    }
  }

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawFieldVectorsWriter = rawVectorsWriter.addField(fieldInfo);
    rawFields.put(fieldInfo, rawFieldVectorsWriter);
    return rawFieldVectorsWriter;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorsWriter.flush(maxDoc, sortMap);
    for (Map.Entry<FieldInfo, FlatFieldVectorsWriter<?>> entry : rawFields.entrySet()) {
      FieldInfo fieldInfo = entry.getKey();
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE ->
            // TODO: Support using SQ8 quantization, see
            // https://github.com/opensearch-project/k-NN/pull/2425
            throw new UnsupportedOperationException("Byte vectors not supported");

        case FLOAT32 -> {
          @SuppressWarnings("unchecked")
          FlatFieldVectorsWriter<float[]> rawWriter =
              (FlatFieldVectorsWriter<float[]>) entry.getValue();

          List<float[]> vectors = rawWriter.getVectors();
          int dimension = fieldInfo.getVectorDimension();
          DocIdSet docIdSet = rawWriter.getDocsWithFieldSet();

          writeFloatField(
              fieldInfo,
              new BufferedFloatVectorValues(vectors, dimension, docIdSet),
              (sortMap != null) ? sortMap::oldToNew : doc -> doc);
        }
      }
    }
  }

  private void writeFloatField(
      FieldInfo fieldInfo, FloatVectorValues floatVectorValues, IntToIntFunction oldToNewDocId)
      throws IOException {
    int number = fieldInfo.number;
    meta.writeInt(number);

    // Write index to temp file and deallocate from memory
    try (Arena temp = Arena.ofConfined()) {
      VectorSimilarityFunction function = fieldInfo.getVectorSimilarityFunction();
      MemorySegment indexPointer =
          createIndex(description, indexParams, function, floatVectorValues, oldToNewDocId)
              // Ensure timely cleanup
              .reinterpret(temp, LibFaissC::freeIndex);

      // See flags defined in c_api/index_io_c.h
      int ioFlags = 3;

      // Write index
      long dataOffset = data.getFilePointer();
      indexWrite(indexPointer, data, ioFlags);
      long dataLength = data.getFilePointer() - dataOffset;

      meta.writeLong(dataOffset);
      meta.writeLong(dataLength);
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("Already finished");
    }
    finished = true;

    rawVectorsWriter.finish();
    meta.writeInt(-1);
    CodecUtil.writeFooter(meta);
    CodecUtil.writeFooter(data);
  }

  @Override
  public void close() throws IOException {
    rawVectorsWriter.close();
    if (meta != null) {
      meta.close();
    }
    if (data != null) {
      data.close();
    }
  }

  @Override
  public long ramBytesUsed() {
    // TODO: How to estimate Faiss usage?
    return rawVectorsWriter.ramBytesUsed();
  }

  private static class BufferedFloatVectorValues extends FloatVectorValues {
    private final List<float[]> floats;
    private final int dimension;
    private final DocIdSet docIdSet;

    public BufferedFloatVectorValues(List<float[]> floats, int dimension, DocIdSet docIdSet) {
      this.floats = floats;
      this.dimension = dimension;
      this.docIdSet = docIdSet;
    }

    @Override
    public float[] vectorValue(int ord) {
      return floats.get(ord);
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return floats.size();
    }

    @Override
    public FloatVectorValues copy() {
      return new BufferedFloatVectorValues(floats, dimension, docIdSet);
    }

    @Override
    public DocIndexIterator iterator() {
      try {
        return fromDISI(docIdSet.iterator());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
