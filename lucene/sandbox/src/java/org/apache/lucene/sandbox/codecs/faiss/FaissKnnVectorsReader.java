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
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.FAISS_IO_FLAG_MMAP;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.FAISS_IO_FLAG_READ_ONLY;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexRead;
import static org.apache.lucene.sandbox.codecs.faiss.LibFaissC.indexSearch;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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
import org.apache.lucene.index.VectorSimilarityFunction;
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
  private final Map<String, IndexEntry> indexMap;
  private final Arena arena;
  private boolean closed;

  public FaissKnnVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader)
      throws IOException {
    this.rawVectorsReader = rawVectorsReader;
    this.indexMap = new HashMap<>();
    this.arena = Arena.ofShared();
    this.closed = false;

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
        if (indexMap.put(fieldMeta.fieldInfo.name, loadField(data, arena, fieldMeta)) != null) {
          throw new CorruptIndexException("Duplicate field: " + fieldMeta.fieldInfo.name, meta);
        }
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

    return new FieldMeta(fieldInfo, dataOffset, dataLength);
  }

  private static IndexEntry loadField(IndexInput data, Arena arena, FieldMeta fieldMeta)
      throws IOException {
    int ioFlags = FAISS_IO_FLAG_MMAP | FAISS_IO_FLAG_READ_ONLY;

    // Read index into memory
    MemorySegment indexPointer =
        indexRead(data.slice(fieldMeta.fieldInfo.name, fieldMeta.offset, fieldMeta.length), ioFlags)
            // Ensure timely cleanup
            .reinterpret(arena, LibFaissC::freeIndex);

    return new IndexEntry(indexPointer, fieldMeta.fieldInfo.getVectorSimilarityFunction());
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
      closed = true;
      IOUtils.close(rawVectorsReader, arena::close, data, indexMap::clear);
    }
  }

  private record FieldMeta(FieldInfo fieldInfo, long offset, long length) {}

  private record IndexEntry(MemorySegment indexPointer, VectorSimilarityFunction function) {}
}
