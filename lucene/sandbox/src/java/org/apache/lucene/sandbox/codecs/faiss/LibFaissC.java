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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
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
 * Utility class to wrap necessary functions of the native <a
 * href="https://github.com/facebookresearch/faiss/blob/v1.11.0/c_api/INSTALL.md">C API</a> of Faiss
 * using <a href="https://openjdk.org/projects/panama">Project Panama</a>.
 *
 * @lucene.experimental
 */
final class LibFaissC {
  // TODO: Use vectorized version where available
  public static final String LIBRARY_NAME = "faiss_c";
  public static final String LIBRARY_VERSION = "1.11.0";

  // See flags defined in c_api/index_io_c.h
  static final int FAISS_IO_FLAG_MMAP = 1;
  static final int FAISS_IO_FLAG_READ_ONLY = 2;

  private static final int BUFFER_SIZE = 256 * 1024 * 1024; // 256 MB

  static {
    System.loadLibrary(LIBRARY_NAME);
    checkLibraryVersion();
  }

  private LibFaissC() {}

  private static MemorySegment getUpcallStub(
      Arena arena, MethodHandle target, FunctionDescriptor descriptor) {
    return Linker.nativeLinker().upcallStub(target, descriptor, arena);
  }

  private static MethodHandle getDowncallHandle(
      String functionName, FunctionDescriptor descriptor) {
    return Linker.nativeLinker()
        .downcallHandle(SymbolLookup.loaderLookup().findOrThrow(functionName), descriptor);
  }

  private static void checkLibraryVersion() {
    MethodHandle getVersion =
        getDowncallHandle("faiss_get_version", FunctionDescriptor.of(ADDRESS));

    MemorySegment nativeString = call(getVersion);
    String actualVersion = nativeString.reinterpret(Long.MAX_VALUE).getString(0);

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
      getDowncallHandle("faiss_Index_free", FunctionDescriptor.ofVoid(ADDRESS));

  public static void freeIndex(MemorySegment indexPointer) {
    call(FREE_INDEX, indexPointer);
  }

  private static final MethodHandle FREE_CUSTOM_IO_WRITER =
      getDowncallHandle("faiss_CustomIOWriter_free", FunctionDescriptor.ofVoid(ADDRESS));

  public static void freeCustomIOWriter(MemorySegment customIOWriterPointer) {
    call(FREE_CUSTOM_IO_WRITER, customIOWriterPointer);
  }

  private static final MethodHandle FREE_CUSTOM_IO_READER =
      getDowncallHandle("faiss_CustomIOReader_free", FunctionDescriptor.ofVoid(ADDRESS));

  public static void freeCustomIOReader(MemorySegment customIOReaderPointer) {
    call(FREE_CUSTOM_IO_READER, customIOReaderPointer);
  }

  private static final MethodHandle FREE_PARAMETER_SPACE =
      getDowncallHandle("faiss_ParameterSpace_free", FunctionDescriptor.ofVoid(ADDRESS));

  private static void freeParameterSpace(MemorySegment parameterSpacePointer) {
    call(FREE_PARAMETER_SPACE, parameterSpacePointer);
  }

  private static final MethodHandle FREE_ID_SELECTOR_BITMAP =
      getDowncallHandle("faiss_IDSelectorBitmap_free", FunctionDescriptor.ofVoid(ADDRESS));

  private static void freeIDSelectorBitmap(MemorySegment idSelectorBitmapPointer) {
    call(FREE_ID_SELECTOR_BITMAP, idSelectorBitmapPointer);
  }

  private static final MethodHandle FREE_SEARCH_PARAMETERS =
      getDowncallHandle("faiss_SearchParameters_free", FunctionDescriptor.ofVoid(ADDRESS));

  private static void freeSearchParameters(MemorySegment searchParametersPointer) {
    call(FREE_SEARCH_PARAMETERS, searchParametersPointer);
  }

  private static final MethodHandle INDEX_FACTORY =
      getDowncallHandle(
          "faiss_index_factory",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));

  private static final MethodHandle PARAMETER_SPACE_NEW =
      getDowncallHandle("faiss_ParameterSpace_new", FunctionDescriptor.of(JAVA_INT, ADDRESS));

  private static final MethodHandle SET_INDEX_PARAMETERS =
      getDowncallHandle(
          "faiss_ParameterSpace_set_index_parameters",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));

  private static final MethodHandle ID_SELECTOR_BITMAP_NEW =
      getDowncallHandle(
          "faiss_IDSelectorBitmap_new",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));

  private static final MethodHandle SEARCH_PARAMETERS_NEW =
      getDowncallHandle(
          "faiss_SearchParameters_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  private static final MethodHandle INDEX_IS_TRAINED =
      getDowncallHandle("faiss_Index_is_trained", FunctionDescriptor.of(JAVA_INT, ADDRESS));

  private static final MethodHandle INDEX_TRAIN =
      getDowncallHandle(
          "faiss_Index_train", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));

  private static final MethodHandle INDEX_ADD_WITH_IDS =
      getDowncallHandle(
          "faiss_Index_add_with_ids",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));

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
      int isTrained = call(INDEX_IS_TRAINED, indexPointer);
      if (isTrained == 0) {
        callAndHandleError(INDEX_TRAIN, indexPointer, size, docs);
      }

      // Add docs to index
      callAndHandleError(INDEX_ADD_WITH_IDS, indexPointer, size, docs, ids);

      return indexPointer;
    }
  }

  @SuppressWarnings("unused") // called using a MethodHandle
  private static long writeBytes(
      IndexOutput output, MemorySegment inputPointer, long itemSize, long numItems)
      throws IOException {
    long size = itemSize * numItems;
    inputPointer = inputPointer.reinterpret(size);

    if (size <= BUFFER_SIZE) { // simple case, avoid buffering
      byte[] bytes = new byte[(int) size];
      inputPointer.asSlice(0, size).asByteBuffer().order(ByteOrder.nativeOrder()).get(bytes);
      output.writeBytes(bytes, bytes.length);
    } else { // copy buffered number of bytes repeatedly
      byte[] bytes = new byte[BUFFER_SIZE];
      for (long offset = 0; offset < size; offset += BUFFER_SIZE) {
        int length = (int) Math.min(size - offset, BUFFER_SIZE);
        inputPointer
            .asSlice(offset, length)
            .asByteBuffer()
            .order(ByteOrder.nativeOrder())
            .get(bytes, 0, length);
        output.writeBytes(bytes, length);
      }
    }
    return numItems;
  }

  @SuppressWarnings("unused") // called using a MethodHandle
  private static long readBytes(
      IndexInput input, MemorySegment outputPointer, long itemSize, long numItems)
      throws IOException {
    long size = itemSize * numItems;
    outputPointer = outputPointer.reinterpret(size);

    if (size <= BUFFER_SIZE) { // simple case, avoid buffering
      byte[] bytes = new byte[(int) size];
      input.readBytes(bytes, 0, bytes.length);
      outputPointer
          .asSlice(0, bytes.length)
          .asByteBuffer()
          .order(ByteOrder.nativeOrder())
          .put(bytes);
    } else { // copy buffered number of bytes repeatedly
      byte[] bytes = new byte[BUFFER_SIZE];
      for (long offset = 0; offset < size; offset += BUFFER_SIZE) {
        int length = (int) Math.min(size - offset, BUFFER_SIZE);
        input.readBytes(bytes, 0, length);
        outputPointer
            .asSlice(offset, length)
            .asByteBuffer()
            .order(ByteOrder.nativeOrder())
            .put(bytes, 0, length);
      }
    }
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
                      long.class, IndexOutput.class, MemorySegment.class, long.class, long.class));

      READ_BYTES_HANDLE =
          MethodHandles.lookup()
              .findStatic(
                  LibFaissC.class,
                  "readBytes",
                  MethodType.methodType(
                      long.class, IndexInput.class, MemorySegment.class, long.class, long.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static final MethodHandle CUSTOM_IO_WRITER_NEW =
      getDowncallHandle(
          "faiss_CustomIOWriter_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  private static final MethodHandle WRITE_INDEX_CUSTOM =
      getDowncallHandle(
          "faiss_write_index_custom", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));

  public static void indexWrite(MemorySegment indexPointer, IndexOutput output, int ioFlags) {
    try (Arena temp = Arena.ofConfined()) {
      MethodHandle writerHandle = WRITE_BYTES_HANDLE.bindTo(output);
      MemorySegment writerStub =
          getUpcallStub(
              temp, writerHandle, FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));

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
      getDowncallHandle(
          "faiss_CustomIOReader_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  private static final MethodHandle READ_INDEX_CUSTOM =
      getDowncallHandle(
          "faiss_read_index_custom", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));

  public static MemorySegment indexRead(IndexInput input, int ioFlags) {
    try (Arena temp = Arena.ofConfined()) {
      MethodHandle readerHandle = READ_BYTES_HANDLE.bindTo(input);
      MemorySegment readerStub =
          getUpcallStub(
              temp, readerHandle, FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));

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
          "faiss_Index_search",
          FunctionDescriptor.of(
              JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));

  private static final MethodHandle INDEX_SEARCH_WITH_PARAMS =
      getDowncallHandle(
          "faiss_Index_search_with_params",
          FunctionDescriptor.of(
              JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS));

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

  @SuppressWarnings("unchecked")
  private static <T> T call(MethodHandle handle, Object... args) {
    try {
      return (T) handle.invokeWithArguments(args);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private static void callAndHandleError(MethodHandle handle, Object... args) {
    int returnCode = call(handle, args);
    if (returnCode < 0) {
      // TODO: Surface actual exception in a thread-safe manner?
      throw new FaissException(returnCode);
    }
  }

  /**
   * Exception used to rethrow handled Faiss errors in native code.
   *
   * @lucene.experimental
   */
  public static class FaissException extends RuntimeException {
    // See error codes defined in c_api/error_c.h
    enum ErrorCode {
      /// No error
      OK(0),
      /// Any exception other than Faiss or standard C++ library exceptions
      UNKNOWN_EXCEPT(-1),
      /// Faiss library exception
      FAISS_EXCEPT(-2),
      /// Standard C++ library exception
      STD_EXCEPT(-4);

      private final int code;

      ErrorCode(int code) {
        this.code = code;
      }

      static ErrorCode fromCode(int code) {
        return Arrays.stream(ErrorCode.values())
            .filter(errorCode -> errorCode.code == code)
            .findFirst()
            .orElseThrow();
      }
    }

    public FaissException(int code) {
      super(String.format(Locale.ROOT, "Faiss library ran into %s", ErrorCode.fromCode(code)));
    }
  }
}
