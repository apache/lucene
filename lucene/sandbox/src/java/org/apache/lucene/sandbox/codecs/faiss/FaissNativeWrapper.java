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
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Locale;

/**
 * Utility class to wrap necessary functions of the native <a
 * href="https://github.com/facebookresearch/faiss/blob/main/c_api/INSTALL.md">C API</a> of Faiss
 * using <a href="https://openjdk.org/projects/panama">Project Panama</a>.
 *
 * @lucene.experimental
 */
@SuppressWarnings("restricted") // uses unsafe calls
final class FaissNativeWrapper {
  static {
    System.loadLibrary(FaissLibrary.NAME);
  }

  private static MethodHandle getHandle(String functionName, FunctionDescriptor descriptor) {
    return Linker.nativeLinker()
        .downcallHandle(SymbolLookup.loaderLookup().findOrThrow(functionName), descriptor);
  }

  FaissNativeWrapper() {
    // Check Faiss version
    String expectedVersion = FaissLibrary.VERSION;
    String actualVersion = faiss_get_version().reinterpret(Long.MAX_VALUE).getString(0);

    if (expectedVersion.equals(actualVersion) == false) {
      throw new LinkageError(
          String.format(
              Locale.ROOT,
              "Expected Faiss library version %s, found %s",
              expectedVersion,
              actualVersion));
    }
  }

  private final MethodHandle faiss_get_version$MH =
      getHandle("faiss_get_version", FunctionDescriptor.of(ADDRESS));

  MemorySegment faiss_get_version() {
    try {
      return (MemorySegment) faiss_get_version$MH.invokeExact();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_CustomIOReader_free$MH =
      getHandle("faiss_CustomIOReader_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_CustomIOReader_free(MemorySegment customIOReaderPointer) {
    try {
      faiss_CustomIOReader_free$MH.invokeExact(customIOReaderPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_CustomIOReader_new$MH =
      getHandle("faiss_CustomIOReader_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  int faiss_CustomIOReader_new(MemorySegment pointer, MemorySegment readerStub) {
    try {
      return (int) faiss_CustomIOReader_new$MH.invokeExact(pointer, readerStub);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_CustomIOWriter_free$MH =
      getHandle("faiss_CustomIOWriter_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_CustomIOWriter_free(MemorySegment customIOWriterPointer) {
    try {
      faiss_CustomIOWriter_free$MH.invokeExact(customIOWriterPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_CustomIOWriter_new$MH =
      getHandle("faiss_CustomIOWriter_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  int faiss_CustomIOWriter_new(MemorySegment pointer, MemorySegment writerStub) {
    try {
      return (int) faiss_CustomIOWriter_new$MH.invokeExact(pointer, writerStub);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_IDSelectorBitmap_free$MH =
      getHandle("faiss_IDSelectorBitmap_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_IDSelectorBitmap_free(MemorySegment idSelectorBitmapPointer) {
    try {
      faiss_IDSelectorBitmap_free$MH.invokeExact(idSelectorBitmapPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_IDSelectorBitmap_new$MH =
      getHandle(
          "faiss_IDSelectorBitmap_new",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));

  int faiss_IDSelectorBitmap_new(MemorySegment pointer, long length, MemorySegment bitmapPointer) {
    try {
      return (int) faiss_IDSelectorBitmap_new$MH.invokeExact(pointer, length, bitmapPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_add_with_ids$MH =
      getHandle(
          "faiss_Index_add_with_ids",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));

  int faiss_Index_add_with_ids(
      MemorySegment indexPointer, long size, MemorySegment docsPointer, MemorySegment idsPointer) {
    try {
      return (int)
          faiss_Index_add_with_ids$MH.invokeExact(indexPointer, size, docsPointer, idsPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_free$MH =
      getHandle("faiss_Index_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_Index_free(MemorySegment indexPointer) {
    try {
      faiss_Index_free$MH.invokeExact(indexPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_is_trained$MH =
      getHandle("faiss_Index_is_trained", FunctionDescriptor.of(JAVA_INT, ADDRESS));

  int faiss_Index_is_trained(MemorySegment indexPointer) {
    try {
      return (int) faiss_Index_is_trained$MH.invokeExact(indexPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_metric_type$MH =
      getHandle("faiss_Index_metric_type", FunctionDescriptor.of(JAVA_INT, ADDRESS));

  int faiss_Index_metric_type(MemorySegment indexPointer) {
    try {
      return (int) faiss_Index_metric_type$MH.invokeExact(indexPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_search$MH =
      getHandle(
          "faiss_Index_search",
          FunctionDescriptor.of(
              JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));

  int faiss_Index_search(
      MemorySegment indexPointer,
      long numQueries,
      MemorySegment queriesPointer,
      long k,
      MemorySegment distancesPointer,
      MemorySegment idsPointer) {
    try {
      return (int)
          faiss_Index_search$MH.invokeExact(
              indexPointer, numQueries, queriesPointer, k, distancesPointer, idsPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_search_with_params$MH =
      getHandle(
          "faiss_Index_search_with_params",
          FunctionDescriptor.of(
              JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS));

  int faiss_Index_search_with_params(
      MemorySegment indexPointer,
      long numQueries,
      MemorySegment queriesPointer,
      long k,
      MemorySegment searchParametersPointer,
      MemorySegment distancesPointer,
      MemorySegment idsPointer) {
    try {
      return (int)
          faiss_Index_search_with_params$MH.invokeExact(
              indexPointer,
              numQueries,
              queriesPointer,
              k,
              searchParametersPointer,
              distancesPointer,
              idsPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_Index_train$MH =
      getHandle("faiss_Index_train", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));

  int faiss_Index_train(MemorySegment indexPointer, long size, MemorySegment docsPointer) {
    try {
      return (int) faiss_Index_train$MH.invokeExact(indexPointer, size, docsPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_ParameterSpace_free$MH =
      getHandle("faiss_ParameterSpace_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_ParameterSpace_free(MemorySegment parameterSpacePointer) {
    try {
      faiss_ParameterSpace_free$MH.invokeExact(parameterSpacePointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_ParameterSpace_new$MH =
      getHandle("faiss_ParameterSpace_new", FunctionDescriptor.of(JAVA_INT, ADDRESS));

  int faiss_ParameterSpace_new(MemorySegment pointer) {
    try {
      return (int) faiss_ParameterSpace_new$MH.invokeExact(pointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_ParameterSpace_set_index_parameters$MH =
      getHandle(
          "faiss_ParameterSpace_set_index_parameters",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));

  int faiss_ParameterSpace_set_index_parameters(
      MemorySegment parameterSpacePointer,
      MemorySegment indexPointer,
      MemorySegment descriptionPointer) {
    try {
      return (int)
          faiss_ParameterSpace_set_index_parameters$MH.invokeExact(
              parameterSpacePointer, indexPointer, descriptionPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_SearchParameters_free$MH =
      getHandle("faiss_SearchParameters_free", FunctionDescriptor.ofVoid(ADDRESS));

  void faiss_SearchParameters_free(MemorySegment searchParametersPointer) {
    try {
      faiss_SearchParameters_free$MH.invokeExact(searchParametersPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_SearchParameters_new$MH =
      getHandle("faiss_SearchParameters_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));

  int faiss_SearchParameters_new(MemorySegment pointer, MemorySegment idSelectorBitmapPointer) {
    try {
      return (int) faiss_SearchParameters_new$MH.invokeExact(pointer, idSelectorBitmapPointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_index_factory$MH =
      getHandle(
          "faiss_index_factory",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));

  int faiss_index_factory(
      MemorySegment pointer, int dimension, MemorySegment description, int metric) {
    try {
      return (int) faiss_index_factory$MH.invokeExact(pointer, dimension, description, metric);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_read_index_custom$MH =
      getHandle(
          "faiss_read_index_custom", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));

  int faiss_read_index_custom(
      MemorySegment customIOReaderPointer, int ioFlags, MemorySegment pointer) {
    try {
      return (int) faiss_read_index_custom$MH.invokeExact(customIOReaderPointer, ioFlags, pointer);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  private final MethodHandle faiss_write_index_custom$MH =
      getHandle(
          "faiss_write_index_custom", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));

  int faiss_write_index_custom(
      MemorySegment indexPointer, MemorySegment customIOWriterPointer, int ioFlags) {
    try {
      return (int)
          faiss_write_index_custom$MH.invokeExact(indexPointer, customIOWriterPointer, ioFlags);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  /**
   * Exception used to rethrow handled Faiss errors in native code.
   *
   * @lucene.experimental
   */
  static class Exception extends RuntimeException {
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

    private Exception(int code) {
      super(
          String.format(
              Locale.ROOT,
              "%s[%s(%d)]",
              Exception.class.getName(),
              ErrorCode.fromCode(code),
              code));
    }

    static void handleException(int code) {
      if (code < 0) {
        throw new Exception(code);
      }
    }
  }
}
