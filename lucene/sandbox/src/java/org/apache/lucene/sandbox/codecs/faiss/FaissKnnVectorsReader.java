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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * Read per-segment Faiss indexes and associated metadata.
 *
 * @lucene.experimental
 */
final class FaissKnnVectorsReader extends KnnVectorsReader {
  private final FlatVectorsReader rawVectorsReader;
  private final IndexInput data;
  private final Map<String, FaissLibrary.Index> indexMap;
  private boolean closed;

  public FaissKnnVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader)
      throws IOException {
    this.rawVectorsReader = rawVectorsReader;
    this.indexMap = new HashMap<>();

    List<FieldMeta> fieldMetaList = new ArrayList<>();
    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      int versionMeta = -1;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                META_CODEC_NAME,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);

        FieldMeta fieldMeta;
        while ((fieldMeta = parseNextField(meta, state)) != null) {
          fieldMetaList.add(fieldMeta);
        }
      } catch (Throwable t) {
        priorE = t;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }

      String dataFileName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, DATA_EXTENSION);
      this.data =
          state.directory.openInput(
              dataFileName, state.context.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM));

      int versionData =
          CodecUtil.checkIndexHeader(
              this.data,
              DATA_CODEC_NAME,
              VERSION_START,
              VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionData) {
        throw new CorruptIndexException(
            String.format(
                Locale.ROOT,
                "Format versions mismatch (meta=%d, data=%d)",
                versionMeta,
                versionData),
            data);
      }
      CodecUtil.retrieveChecksum(data);

      for (FieldMeta fieldMeta : fieldMetaList) {
        if (indexMap.containsKey(fieldMeta.name)) {
          throw new CorruptIndexException("Duplicate field: " + fieldMeta.name, meta);
        }
        IndexInput indexInput = data.slice(fieldMeta.name, fieldMeta.offset, fieldMeta.length);
        indexMap.put(fieldMeta.name, FaissLibrary.INSTANCE.readIndex(indexInput));
      }
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  private static FieldMeta parseNextField(IndexInput meta, SegmentReadState state)
      throws IOException {
    int fieldNumber = meta.readInt();
    if (fieldNumber == -1) {
      return null;
    }

    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNumber);
    if (fieldInfo == null) {
      throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
    }

    long dataOffset = meta.readLong();
    long dataLength = meta.readLong();

    return new FieldMeta(fieldInfo.name, dataOffset, dataLength);
  }

  @Override
  public void checkIntegrity() throws IOException {
    rawVectorsReader.checkIntegrity();
    // TODO: Evaluate if we need an explicit check for validity of Faiss indexes
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
    FaissLibrary.Index index = indexMap.get(field);
    if (index != null) {
      index.search(vector, knnCollector, acceptDocs);
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
      // Close all indexes
      for (FaissLibrary.Index index : indexMap.values()) {
        index.close();
      }
      indexMap.clear();

      IOUtils.close(rawVectorsReader, data);
      closed = true;
    }
  }

  private record FieldMeta(String name, long offset, long length) {}
}
