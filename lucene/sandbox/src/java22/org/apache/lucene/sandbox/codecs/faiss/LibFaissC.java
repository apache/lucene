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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.Locale;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.IntToIntFunction;

public final class LibFaissC {
  public static final String LIBRARY_NAME = "faiss_c";
  public static final String LIBRARY_VERSION = "1.9.0";

  static {
    try {
      System.loadLibrary(LIBRARY_NAME);
    } catch (UnsatisfiedLinkError e) {
      throw new RuntimeException(
          "Shared library not found, build the Faiss C_API from https://github.com/facebookresearch/faiss/blob/main/c_api/INSTALL.md "
              + "and link it (along with all dependencies) to the library path "
              + "(-Djava.library.path JVM argument or $LD_LIBRARY_PATH environment variable)",
          e);
    }
    checkLibraryVersion();
  }

  private LibFaissC() {}

  private static MethodHandle getMethodHandle(
      String functionName, MemoryLayout resLayout, MemoryLayout... argLayouts) {
    return Linker.nativeLinker()
        .downcallHandle(
            SymbolLookup.loaderLookup().find(functionName).orElseThrow(),
            FunctionDescriptor.of(resLayout, argLayouts));
  }

  private static void checkLibraryVersion() {
    MethodHandle getVersion = getMethodHandle("faiss_get_version", ADDRESS);
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
      getMethodHandle("faiss_Index_free", JAVA_INT, ADDRESS);

  public static void freeIndex(MemorySegment indexPointer) {
    callAndHandleError(FREE_INDEX, indexPointer);
  }

  private static final MethodHandle FREE_PARAMETER_SPACE =
      getMethodHandle("faiss_ParameterSpace_free", JAVA_INT, ADDRESS);

  private static void freeParameterSpace(MemorySegment parameterSpacePointer) {
    callAndHandleError(FREE_PARAMETER_SPACE, parameterSpacePointer);
  }

  private static final MethodHandle INDEX_FACTORY =
      getMethodHandle("faiss_index_factory", JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT);

  private static final MethodHandle PARAMETER_SPACE_NEW =
      getMethodHandle("faiss_ParameterSpace_new", JAVA_INT, ADDRESS);

  private static final MethodHandle SET_INDEX_PARAMETERS =
      getMethodHandle(
          "faiss_ParameterSpace_set_index_parameters", JAVA_INT, ADDRESS, ADDRESS, ADDRESS);

  private static final MethodHandle INDEX_IS_TRAINED =
      getMethodHandle("faiss_Index_is_trained", JAVA_INT, ADDRESS);

  private static final MethodHandle INDEX_TRAIN =
      getMethodHandle("faiss_Index_train", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS);

  private static final MethodHandle INDEX_ADD_WITH_IDS =
      getMethodHandle("faiss_Index_add_with_ids", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS);

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
            default -> throw new UnsupportedOperationException("Metric type not supported");
          };

      // Create an index
      MemorySegment pointer = temp.allocate(ADDRESS);
      callAndHandleError(INDEX_FACTORY, pointer, dimension, temp.allocateFrom(description), metric);
      MemorySegment indexPointer = pointer.get(ADDRESS, 0);

      // Set index params
      callAndHandleError(PARAMETER_SPACE_NEW, pointer);
      MemorySegment parameterSpacePointer =
          pointer.get(ADDRESS, 0).reinterpret(temp, LibFaissC::freeParameterSpace);
      callAndHandleError(
          SET_INDEX_PARAMETERS,
          parameterSpacePointer,
          indexPointer,
          temp.allocateFrom(indexParams));

      // Allocate docs in native memory
      MemorySegment docs = temp.allocate(JAVA_FLOAT, (long) size * dimension);
      FloatBuffer docsBuffer = docs.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer();

      // Allocate ids in native memory
      MemorySegment ids = temp.allocate(JAVA_LONG, size);
      LongBuffer idsBuffer = ids.asByteBuffer().order(ByteOrder.nativeOrder()).asLongBuffer();

      KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
      for (int i = 0; i < size; i++) {
        idsBuffer.put(oldToNewDocId.apply(iterator.nextDoc()));
        docsBuffer.put(floatVectorValues.vectorValue(i));
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

  private static final MethodHandle INDEX_WRITE =
      getMethodHandle("faiss_write_index_fname", JAVA_INT, ADDRESS, ADDRESS);

  public static void indexWrite(MemorySegment indexPointer, String fileName) {
    try (Arena temp = Arena.ofConfined()) {
      callAndHandleError(INDEX_WRITE, indexPointer, temp.allocateFrom(fileName));
    }
  }

  private static final MethodHandle INDEX_READ =
      getMethodHandle("faiss_read_index_fname", JAVA_INT, ADDRESS, JAVA_INT, ADDRESS);

  public static MemorySegment indexRead(String fileName, int ioFlags) {
    try (Arena temp = Arena.ofConfined()) {
      MemorySegment pointer = temp.allocate(ADDRESS);
      callAndHandleError(INDEX_READ, temp.allocateFrom(fileName), ioFlags, pointer);
      return pointer.get(ADDRESS, 0);
    }
  }

  private static final MethodHandle INDEX_SEARCH =
      getMethodHandle(
          "faiss_Index_search", JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_INT, ADDRESS, ADDRESS);

  public static void indexSearch(
      MemorySegment indexPointer, float[] query, KnnCollector knnCollector, Bits acceptDocs) {
    try (Arena temp = Arena.ofConfined()) {
      // Allocate queries in native memory
      MemorySegment queries = temp.allocate(JAVA_FLOAT, query.length);
      queries.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer().put(query);

      // Faiss knn search
      int k = knnCollector.k();
      MemorySegment distancesPointer = temp.allocate(JAVA_FLOAT, k);
      MemorySegment idsPointer = temp.allocate(JAVA_LONG, k);

      MemorySegment localIndex = indexPointer.reinterpret(temp, null);
      callAndHandleError(INDEX_SEARCH, localIndex, 1, queries, k, distancesPointer, idsPointer);

      // Retrieve scores
      float[] distances = new float[k];
      distancesPointer.asByteBuffer().order(ByteOrder.nativeOrder()).asFloatBuffer().get(distances);

      // Retrieve ids
      long[] ids = new long[k];
      idsPointer.asByteBuffer().order(ByteOrder.nativeOrder()).asLongBuffer().get(ids);

      // Record hits
      for (int i = 0; i < k; i++) {
        int doc = (int) ids[i];

        // TODO: This is like a post-filter, include at runtime?
        if (acceptDocs == null || acceptDocs.get(doc)) {
          knnCollector.collect(doc, distances[i]);
        }
      }
    }
  }

  private static final MethodHandle GET_LAST_ERROR =
      getMethodHandle("faiss_get_last_error", ADDRESS);

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

  public static class FaissException extends RuntimeException {
    public FaissException(String message) {
      super(message);
    }
  }
}
