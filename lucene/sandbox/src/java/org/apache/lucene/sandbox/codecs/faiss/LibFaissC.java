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

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.Locale;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;

/**
 * Utility class to wrap necessary functions of the native C_API of Faiss using <a
 * href="https://openjdk.org/projects/panama">Project Panama</a> (library {@value
 * LibFaissC#LIBRARY_NAME}, version {@value LibFaissC#LIBRARY_VERSION}, build using <a
 * href="https://github.com/facebookresearch/faiss/blob/main/c_api/INSTALL.md">this guide</a> and
 * add to runtime along with all dependencies).
 *
 * @lucene.experimental
 */
final class LibFaissC {
  /*
   * TODO: Requires some changes to Faiss, see:
   *  - https://github.com/facebookresearch/faiss/pull/4158 (merged in main, to be released in v1.11.0)
   *  - https://github.com/facebookresearch/faiss/pull/4167 (merged in main, to be released in v1.11.0)
   *  - https://github.com/facebookresearch/faiss/pull/4180 (in progress)
   */

  public static final String LIBRARY_NAME = "faiss_c";
  public static final String LIBRARY_VERSION = "1.10.0";

  static {
    System.loadLibrary(LIBRARY_NAME);
    checkLibraryVersion();
  }

  private LibFaissC() {}

  @SuppressWarnings("SameParameterValue")
  private static MemorySegment getUpcallStub(
      Arena arena, MethodHandle target, MemoryLayout resLayout, MemoryLayout... argLayouts) {
    return Linker.nativeLinker()
        .upcallStub(target, FunctionDescriptor.of(resLayout, argLayouts), arena);
  }

  private static MethodHandle getDowncallHandle(
      String functionName, MemoryLayout resLayout, MemoryLayout... argLayouts) {
    return Linker.nativeLinker()
        .downcallHandle(
            SymbolLookup.loaderLookup().find(functionName).orElseThrow(),
            FunctionDescriptor.of(resLayout, argLayouts));
  }

  private static void checkLibraryVersion() {
    MethodHandle getVersion = getDowncallHandle("faiss_get_version", ADDRESS);
    String actualVersion = callAndGetString(getVersion);
    if (LIBRARY_VERSION.equals(actualVersion) == false) {
      throw new UnsupportedOperationException(
          String.format(
              Locale.ROOT,
              "Expected Faiss library version %s, found %s",
              LIBRARY_VERSION,
              actualVersion));
    }
  }

  private static final MethodHandle FREE_INDEX =
      getDowncallHandle("faiss_Index_free", JAVA_INT, ADDRESS);

  public static void freeIndex(MemorySegment indexPointer) {
    callAndHandleError(FREE_INDEX, indexPointer);
  }

  private static final MethodHandle FREE_CUSTOM_IO_WRITER =
      getDowncallHandle("faiss_CustomIOWriter_free", JAVA_INT, ADDRESS);

  public static void freeCustomIOWriter(MemorySegment customIOWriterPointer) {
    callAndHandleError(FREE_CUSTOM_IO_WRITER, customIOWriterPointer);
  }

  private static final MethodHandle FREE_CUSTOM_IO_READER =
      getDowncallHandle("faiss_CustomIOReader_free", JAVA_INT, ADDRESS);

  public static void freeCustomIOReader(MemorySegment customIOReaderPointer) {
    callAndHandleError(FREE_CUSTOM_IO_READER, customIOReaderPointer);
  }

  private static final MethodHandle FREE_PARAMETER_SPACE =
      getDowncallHandle("faiss_ParameterSpace_free", JAVA_INT, ADDRESS);

  private static void freeParameterSpace(MemorySegment parameterSpacePointer) {
    callAndHandleError(FREE_PARAMETER_SPACE, parameterSpacePointer);
  }

  private static final MethodHandle FREE_ID_SELECTOR_BITMAP =
      getDowncallHandle("faiss_IDSelectorBitmap_free", JAVA_INT, ADDRESS);

  private static void freeIDSelectorBitmap(MemorySegment idSelectorBitmapPointer) {
    callAndHandleError(FREE_ID_SELECTOR_BITMAP, idSelectorBitmapPointer);
  }

  private static final MethodHandle FREE_SEARCH_PARAMETERS =
      getDowncallHandle("faiss_SearchParameters_free", JAVA_INT, ADDRESS);

  private static void freeSearchParameters(MemorySegment searchParametersPointer) {
    callAndHandleError(FREE_SEARCH_PARAMETERS, searchParametersPointer);
  }

  private static final MethodHandle INDEX_FACTORY =
      getDowncallHandle("faiss_index_factory", JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT);

  private static final MethodHandle PARAMETER_SPACE_NEW =
      getDowncallHandle("faiss_ParameterSpace_new", JAVA_INT, ADDRESS);

  private static final MethodHandle SET_INDEX_PARAMETERS =
      getDowncallHandle(
          "faiss_ParameterSpace_set_index_parameters", JAVA_INT, ADDRESS, ADDRESS, ADDRESS);

  private static final MethodHandle ID_SELECTOR_BITMAP_NEW =
      getDowncallHandle("faiss_IDSelectorBitmap_new", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS);

  private static final MethodHandle SEARCH_PARAMETERS_NEW =
      getDowncallHandle("faiss_SearchParameters_new", JAVA_INT, ADDRESS, ADDRESS);

  private static final MethodHandle INDEX_IS_TRAINED =
      getDowncallHandle("faiss_Index_is_trained", JAVA_INT, ADDRESS);

  private static final MethodHandle INDEX_TRAIN =
      getDowncallHandle("faiss_Index_train", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS);

  private static final MethodHandle INDEX_ADD_WITH_IDS =
      getDowncallHandle("faiss_Index_add_with_ids", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS);

  public static MemorySegment createIndex(
      String description,
      String indexParams,
      VectorSimilarityFunction function,
      FloatVectorValues floatVectorValues,
      IntToIntFunction oldToNewDocId)
      throws IOException {

    try (Arena temp = Arena.ofConfined()) {
      int size = floatVectorValues.size();
      int dimension = floatVectorValues.dimension();

      // Mapped from faiss/MetricType.h
      int metric =
          switch (function) {
            case DOT_PRODUCT -> 0;
            case EUCLIDEAN -> 1;
            case COSINE, MAXIMUM_INNER_PRODUCT ->
                throw new UnsupportedOperationException("Metric type not supported");
          };

      // Create an index
      MemorySegment pointer = temp.allocate(ADDRESS);
      callAndHandleError(INDEX_FACTORY, pointer, dimension, temp.allocateFrom(description), metric);
      MemorySegment indexPointer = pointer.get(ADDRESS, 0);

      // Set index params
      callAndHandleError(PARAMETER_SPACE_NEW, pointer);
      MemorySegment parameterSpacePointer =
          pointer
              .get(ADDRESS, 0)
              // Ensure timely cleanup
              .reinterpret(temp, LibFaissC::freeParameterSpace);

      callAndHandleError(
          SET_INDEX_PARAMETERS,
          parameterSpacePointer,
          indexPointer,
          temp.allocateFrom(indexParams));

      // TODO: Improve memory usage (with a tradeoff in performance) by batched indexing, see:
      //  - https://github.com/opensearch-project/k-NN/issues/1506
      //  - https://github.com/opensearch-project/k-NN/issues/1938

      // Allocate docs in native memory
      MemorySegment docs = temp.allocate(JAVA_FLOAT, (long) size * dimension);
      FloatBuffer docsBuffer = docs.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer();

      // Allocate ids in native memory
      MemorySegment ids = temp.allocate(JAVA_LONG, size);
      LongBuffer idsBuffer = ids.asByteBuffer().order(ByteOrder.nativeOrder()).asLongBuffer();

      KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
      for (int i = iterator.nextDoc(); i != NO_MORE_DOCS; i = iterator.nextDoc()) {
        idsBuffer.put(oldToNewDocId.apply(i));
        docsBuffer.put(floatVectorValues.vectorValue(iterator.index()));
      }

      // Train index
      if (callAndGetInt(INDEX_IS_TRAINED, indexPointer) == 0) {
        callAndHandleError(INDEX_TRAIN, indexPointer, size, docs);
      }

      // Add docs to index
      callAndHandleError(INDEX_ADD_WITH_IDS, indexPointer, size, docs, ids);

      return indexPointer;
    }
  }

  @SuppressWarnings("unused") // called using a MethodHandle
  private static int writeBytes(
      IndexOutput output, MemorySegment inputPointer, int itemSize, int numItems)
      throws IOException {
    // TODO: Can we avoid copying to heap?
    byte[] bytes =
        new byte[(int) (Integer.toUnsignedLong(itemSize) * Integer.toUnsignedLong(numItems))];
    inputPointer.reinterpret(bytes.length).asByteBuffer().order(ByteOrder.nativeOrder()).get(bytes);
    output.writeBytes(bytes, 0, bytes.length);
    return numItems;
  }

  @SuppressWarnings("unused") // called using a MethodHandle
  private static int readBytes(
      IndexInput input, MemorySegment outputPointer, int itemSize, int numItems)
      throws IOException {
    // TODO: Can we avoid copying to heap?
    byte[] bytes =
        new byte[(int) (Integer.toUnsignedLong(itemSize) * Integer.toUnsignedLong(numItems))];
    input.readBytes(bytes, 0, bytes.length);
    outputPointer
        .reinterpret(bytes.length)
        .asByteBuffer()
        .order(ByteOrder.nativeOrder())
        .put(bytes);
    return numItems;
  }

  private static final MethodHandle WRITE_BYTES_HANDLE;
  private static final MethodHandle READ_BYTES_HANDLE;

  static {
    try {
      WRITE_BYTES_HANDLE =
          MethodHandles.lookup()
              .findStatic(
                  LibFaissC.class,
                  "writeBytes",
                  MethodType.methodType(
                      int.class, IndexOutput.class, MemorySegment.class, int.class, int.class));

      READ_BYTES_HANDLE =
          MethodHandles.lookup()
              .findStatic(
                  LibFaissC.class,
                  "readBytes",
                  MethodType.methodType(
                      int.class, IndexInput.class, MemorySegment.class, int.class, int.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static final MethodHandle CUSTOM_IO_WRITER_NEW =
      getDowncallHandle("faiss_CustomIOWriter_new", JAVA_INT, ADDRESS, ADDRESS);

  private static final MethodHandle WRITE_INDEX_CUSTOM =
      getDowncallHandle("faiss_write_index_custom", JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);

  public static void indexWrite(MemorySegment indexPointer, IndexOutput output, int ioFlags) {
    try (Arena temp = Arena.ofConfined()) {
      MethodHandle writerHandle = WRITE_BYTES_HANDLE.bindTo(output);
      MemorySegment writerStub =
          getUpcallStub(temp, writerHandle, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT);

      MemorySegment pointer = temp.allocate(ADDRESS);
      callAndHandleError(CUSTOM_IO_WRITER_NEW, pointer, writerStub);
      MemorySegment customIOWriterPointer =
          pointer
              .get(ADDRESS, 0)
              // Ensure timely cleanup
              .reinterpret(temp, LibFaissC::freeCustomIOWriter);

      callAndHandleError(WRITE_INDEX_CUSTOM, indexPointer, customIOWriterPointer, ioFlags);
    }
  }

  private static final MethodHandle CUSTOM_IO_READER_NEW =
      getDowncallHandle("faiss_CustomIOReader_new", JAVA_INT, ADDRESS, ADDRESS);

  private static final MethodHandle READ_INDEX_CUSTOM =
      getDowncallHandle("faiss_read_index_custom", JAVA_INT, ADDRESS, JAVA_INT, ADDRESS);

  public static MemorySegment indexRead(IndexInput input, int ioFlags) {
    try (Arena temp = Arena.ofConfined()) {
      MethodHandle readerHandle = READ_BYTES_HANDLE.bindTo(input);
      MemorySegment readerStub =
          getUpcallStub(temp, readerHandle, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT);

      MemorySegment pointer = temp.allocate(ADDRESS);
      callAndHandleError(CUSTOM_IO_READER_NEW, pointer, readerStub);
      MemorySegment customIOReaderPointer =
          pointer
              .get(ADDRESS, 0)
              // Ensure timely cleanup
              .reinterpret(temp, LibFaissC::freeCustomIOReader);

      callAndHandleError(READ_INDEX_CUSTOM, customIOReaderPointer, ioFlags, pointer);
      return pointer.get(ADDRESS, 0);
    }
  }

  private static final MethodHandle INDEX_SEARCH =
      getDowncallHandle(
          "faiss_Index_search", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS);

  private static final MethodHandle INDEX_SEARCH_WITH_PARAMS =
      getDowncallHandle(
          "faiss_Index_search_with_params",
          JAVA_INT,
          ADDRESS,
          JAVA_LONG,
          ADDRESS,
          JAVA_LONG,
          ADDRESS,
          ADDRESS,
          ADDRESS);

  public static void indexSearch(
      MemorySegment indexPointer,
      VectorSimilarityFunction function,
      float[] query,
      KnnCollector knnCollector,
      Bits acceptDocs) {

    try (Arena temp = Arena.ofConfined()) {
      FixedBitSet fixedBitSet =
          switch (acceptDocs) {
            case null -> null;
            case FixedBitSet bitSet -> bitSet;
            // TODO: Add optimized case for SparseFixedBitSet
            case Bits bits -> FixedBitSet.copyOf(bits);
          };

      // Allocate queries in native memory
      MemorySegment queries = temp.allocate(JAVA_FLOAT, query.length);
      queries.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer().put(query);

      // Faiss knn search
      int k = knnCollector.k();
      MemorySegment distancesPointer = temp.allocate(JAVA_FLOAT, k);
      MemorySegment idsPointer = temp.allocate(JAVA_LONG, k);

      MemorySegment localIndex = indexPointer.reinterpret(temp, null);
      if (fixedBitSet == null) {
        // Search without runtime filters
        callAndHandleError(INDEX_SEARCH, localIndex, 1, queries, k, distancesPointer, idsPointer);
      } else {
        MemorySegment pointer = temp.allocate(ADDRESS);

        long[] bits = fixedBitSet.getBits();
        MemorySegment nativeBits = temp.allocate(JAVA_LONG, bits.length);

        // Use LITTLE_ENDIAN to convert long[] -> uint8_t*
        nativeBits.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(bits);

        callAndHandleError(ID_SELECTOR_BITMAP_NEW, pointer, fixedBitSet.length(), nativeBits);
        MemorySegment idSelectorBitmapPointer =
            pointer
                .get(ADDRESS, 0)
                // Ensure timely cleanup
                .reinterpret(temp, LibFaissC::freeIDSelectorBitmap);

        callAndHandleError(SEARCH_PARAMETERS_NEW, pointer, idSelectorBitmapPointer);
        MemorySegment searchParametersPointer =
            pointer
                .get(ADDRESS, 0)
                // Ensure timely cleanup
                .reinterpret(temp, LibFaissC::freeSearchParameters);

        // Search with runtime filters
        callAndHandleError(
            INDEX_SEARCH_WITH_PARAMS,
            localIndex,
            1,
            queries,
            k,
            searchParametersPointer,
            distancesPointer,
            idsPointer);
      }

      // Retrieve scores
      float[] distances = new float[k];
      distancesPointer.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer().get(distances);

      // Retrieve ids
      long[] ids = new long[k];
      idsPointer.asByteBuffer().order(ByteOrder.nativeOrder()).asLongBuffer().get(ids);

      // Record hits
      for (int i = 0; i < k; i++) {
        // Not enough results
        if (ids[i] == -1) {
          break;
        }

        // Scale Faiss distances to Lucene scores, see VectorSimilarityFunction.java
        float score =
            switch (function) {
              case DOT_PRODUCT ->
                  // distance in Faiss === dotProduct in Lucene
                  Math.max((1 + distances[i]) / 2, 0);

              case EUCLIDEAN ->
                  // distance in Faiss === squareDistance in Lucene
                  1 / (1 + distances[i]);

              case COSINE, MAXIMUM_INNER_PRODUCT ->
                  throw new UnsupportedOperationException("Metric type not supported");
            };

        knnCollector.collect((int) ids[i], score);
      }
    }
  }

  private static final MethodHandle GET_LAST_ERROR =
      getDowncallHandle("faiss_get_last_error", ADDRESS);

  private static int callAndGetInt(MethodHandle handle, Object... args) {
    try {
      return (int) handle.invokeWithArguments(args);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private static String callAndGetString(MethodHandle handle, Object... args) {
    try {
      MemorySegment segment = (MemorySegment) handle.invokeWithArguments(args);
      return segment.reinterpret(Long.MAX_VALUE).getString(0);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private static void callAndHandleError(MethodHandle handle, Object... args) {
    int returnCode = callAndGetInt(handle, args);
    if (returnCode < 0) {
      String error = callAndGetString(GET_LAST_ERROR);
      throw new FaissException(error);
    }
  }

  /**
   * Exception used to rethrow handled Faiss errors in native code.
   *
   * @lucene.experimental
   */
  public static class FaissException extends RuntimeException {
    public FaissException(String message) {
      super(message);
    }
  }
}
