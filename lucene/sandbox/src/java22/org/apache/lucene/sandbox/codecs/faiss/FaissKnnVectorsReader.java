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
import static org.apache.lucene.sandbox.codecs.faiss.FaissKnnVectorsFormat.VERSION_START;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexRead;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexSearch;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

public final class FaissKnnVectorsReader extends KnnVectorsReader {
  private final KnnVectorsReader rawVectorsReader;
  private final IndexInput meta, data;
  private final Map<String, LibFaissC.Index> indexMap;
  private final Arena arena;

  public FaissKnnVectorsReader(SegmentReadState state, KnnVectorsReader rawVectorsReader)
      throws IOException {
    this.rawVectorsReader = rawVectorsReader;
    this.indexMap = new HashMap<>();
    this.arena = Arena.ofConfined();

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

      Map.Entry<String, LibFaissC.Index> entry;
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

  private Map.Entry<String, LibFaissC.Index> parseNextField(SegmentReadState state)
      throws IOException {
    int fieldNumber = meta.readInt();
    if (fieldNumber == -1) {
      return null;
    }

    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
    if (fieldInfo == null) {
      throw new IllegalStateException("invalid field");
    }

    int size = meta.readInt();
    int[] ordToDoc = new int[size];
    for (int i = 0; i < size; i++) {
      ordToDoc[i] = meta.readInt();
    }

    long dataOffset = meta.readLong();
    long dataLength = meta.readLong();

    // Copy index to temp file
    // TODO: Non FS-based approach?
    Path tempFile = Files.createTempFile(NAME, fieldInfo.name);
    try (OutputStreamDataOutput output =
        new OutputStreamDataOutput(Files.newOutputStream(tempFile))) {
      data.seek(dataOffset);
      output.copyBytes(data, dataLength);
    }

    // Read index from temp file into memory
    // See flags defined in c_api/index_io_c.h
    MemorySegment indexPointer =
        indexRead(tempFile.toString(), 3)
            // Assign index to explicit scope for timely cleanup
            .reinterpret(arena, LibFaissC::freeIndex);

    // Cleanup
    Files.delete(tempFile);

    return Map.entry(fieldInfo.name, new LibFaissC.Index(indexPointer, ordToDoc));
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
    throw new UnsupportedOperationException("Byte vectors not supported");
  }

  @Override
  public void search(String field, float[] vector, KnnCollector knnCollector, Bits acceptDocs) {
    LibFaissC.Index entry = indexMap.get(field);
    if (entry != null) {
      indexSearch(entry.indexPointer(), entry.ordToDoc(), vector, knnCollector, acceptDocs);
    }
  }

  @Override
  public void search(String field, byte[] vector, KnnCollector knnCollector, Bits acceptDocs) {
    throw new UnsupportedOperationException("Byte vectors not supported");
  }

  @Override
  public void close() throws IOException {
    rawVectorsReader.close();
    arena.close();
    if (meta != null) {
      meta.close();
    }
    if (data != null) {
      data.close();
    }
  }
}
