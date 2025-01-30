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
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.NAME;
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.createIndex;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexWrite;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.codecs.BufferingKnnVectorsWriter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.IOUtils;

public final class FaissKnnVectorsWriter extends BufferingKnnVectorsWriter {
  private final String description, indexParams;
  private final KnnVectorsWriter rawVectorsWriter;
  private final IndexOutput meta, data;
  private boolean finished;

  public FaissKnnVectorsWriter(
      String description,
      String indexParams,
      SegmentWriteState state,
      KnnVectorsWriter rawVectorsWriter)
      throws IOException {
    this.description = description;
    this.indexParams = indexParams;
    this.rawVectorsWriter = rawVectorsWriter;
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
  @SuppressWarnings("unchecked")
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    return switch (fieldInfo.getVectorEncoding()) {
      case BYTE ->
          // TODO: Support using SQ8 quantization, see
          // https://github.com/opensearch-project/k-NN/pull/2425
          throw new UnsupportedOperationException("Byte vectors not supported");

      case FLOAT32 ->
          new KnnFieldVectorsWriter<float[]>() {
            private final KnnFieldVectorsWriter<float[]> rawWriter =
                (KnnFieldVectorsWriter<float[]>) rawVectorsWriter.addField(fieldInfo);
            private final KnnFieldVectorsWriter<float[]> writer =
                (KnnFieldVectorsWriter<float[]>) FaissKnnVectorsWriter.super.addField(fieldInfo);

            @Override
            public long ramBytesUsed() {
              return rawWriter.ramBytesUsed() + writer.ramBytesUsed();
            }

            @Override
            public void addValue(int i, float[] floats) throws IOException {
              rawWriter.addValue(i, floats);
              writer.addValue(i, floats);
            }

            @Override
            public float[] copyValue(float[] floats) {
              return floats.clone();
            }
          };
    };
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    rawVectorsWriter.mergeOneField(fieldInfo, mergeState);
    super.mergeOneField(fieldInfo, mergeState);
  }

  @Override
  protected void writeField(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, int maxDoc)
      throws IOException {
    int number = fieldInfo.number;
    meta.writeInt(number);

    // TODO: Non FS-based approach?
    Path tempFile = Files.createTempFile(NAME, fieldInfo.name);

    // Write index to temp file and deallocate from memory
    try (Arena temp = Arena.ofConfined()) {
      VectorSimilarityFunction function = fieldInfo.getVectorSimilarityFunction();
      MemorySegment indexPointer =
          createIndex(description, indexParams, function, floatVectorValues)
              // Assign index to explicit scope for timely cleanup
              .reinterpret(temp, LibFaissC::freeIndex);
      indexWrite(indexPointer, tempFile.toString());
    }

    // Copy temp file to index
    long dataOffset = data.getFilePointer();
    try (InputStreamDataInput input = new InputStreamDataInput(Files.newInputStream(tempFile))) {
      data.copyBytes(input, Files.size(tempFile));
    }
    long dataLength = data.getFilePointer() - dataOffset;

    // Cleanup temp file
    Files.delete(tempFile);

    meta.writeLong(dataOffset);
    meta.writeLong(dataLength);
  }

  @Override
  protected void writeField(FieldInfo fieldInfo, ByteVectorValues byteVectorValues, int maxDoc) {
    // TODO: Support using SQ8 quantization, see
    // https://github.com/opensearch-project/k-NN/pull/2425
    throw new UnsupportedOperationException("Byte vectors not supported");
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    rawVectorsWriter.flush(maxDoc, sortMap);
    super.flush(maxDoc, sortMap);
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
}
