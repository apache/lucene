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
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.sandbox.codecs.faiss.FaissNativeWrapper.Exception.handleException;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.hnsw.IntToIntFunction;

/**
 * A native implementation of {@link FaissLibrary} using {@link FaissNativeWrapper}.
 *
 * @lucene.experimental
 */
@SuppressWarnings("restricted") // uses unsafe calls
final class FaissLibraryNativeImpl implements FaissLibrary {
  private final FaissNativeWrapper wrapper;

  FaissLibraryNativeImpl() {
    this.wrapper = new FaissNativeWrapper();
  }

  private static MemorySegment getStub(
      Arena arena, MethodHandle target, FunctionDescriptor descriptor) {
    return Linker.nativeLinker().upcallStub(target, descriptor, arena);
  }

  private static final int BUFFER_SIZE = 256 * 1024 * 1024; // 256 MB

  @SuppressWarnings("unused") // called using a MethodHandle
  private static long writeBytes(
      IndexOutput output, MemorySegment inputPointer, long itemSize, long numItems)
      throws IOException {
    long size = itemSize * numItems;
    inputPointer = inputPointer.reinterpret(size);

    if (size <= BUFFER_SIZE) { // simple case, avoid buffering
      output.writeBytes(inputPointer.toArray(JAVA_BYTE), (int) size);
    } else { // copy buffered number of bytes repeatedly
      byte[] bytes = new byte[BUFFER_SIZE];
      for (long offset = 0; offset < size; offset += BUFFER_SIZE) {
        int length = (int) Math.min(size - offset, BUFFER_SIZE);
        MemorySegment.copy(inputPointer, JAVA_BYTE, offset, bytes, 0, length);
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
      MemorySegment.copy(bytes, 0, outputPointer, JAVA_BYTE, 0, bytes.length);
    } else { // copy buffered number of bytes repeatedly
      byte[] bytes = new byte[BUFFER_SIZE];
      for (long offset = 0; offset < size; offset += BUFFER_SIZE) {
        int length = (int) Math.min(size - offset, BUFFER_SIZE);
        input.readBytes(bytes, 0, length);
        MemorySegment.copy(bytes, 0, outputPointer, JAVA_BYTE, offset, length);
      }
    }
    return numItems;
  }

  private static final MethodHandle WRITE_BYTES_HANDLE;
  private static final MethodHandle READ_BYTES_HANDLE;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();

      WRITE_BYTES_HANDLE =
          lookup.findStatic(
              FaissLibraryNativeImpl.class,
              "writeBytes",
              MethodType.methodType(
                  long.class, IndexOutput.class, MemorySegment.class, long.class, long.class));

      READ_BYTES_HANDLE =
          lookup.findStatic(
              FaissLibraryNativeImpl.class,
              "readBytes",
              MethodType.methodType(
                  long.class, IndexInput.class, MemorySegment.class, long.class, long.class));

    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new LinkageError(
          "FaissLibraryNativeImpl reader / writer functions are missing or inaccessible", e);
    }
  }

  private static final Map<VectorSimilarityFunction, Integer> FUNCTION_TO_METRIC =
      Map.of(
          // Mapped from faiss/MetricType.h
          DOT_PRODUCT, 0,
          EUCLIDEAN, 1);

  private static int functionToMetric(VectorSimilarityFunction function) {
    Integer metric = FUNCTION_TO_METRIC.get(function);
    if (metric == null) {
      throw new UnsupportedOperationException("Similarity function not supported: " + function);
    }
    return metric;
  }

  // Invert FUNCTION_TO_METRIC
  private static final Map<Integer, VectorSimilarityFunction> METRIC_TO_FUNCTION =
      FUNCTION_TO_METRIC.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

  private static VectorSimilarityFunction metricToFunction(int metric) {
    VectorSimilarityFunction function = METRIC_TO_FUNCTION.get(metric);
    if (function == null) {
      throw new UnsupportedOperationException("Metric not supported: " + metric);
    }
    return function;
  }

  @Override
  public FaissLibrary.Index createIndex(
      String description,
      String indexParams,
      VectorSimilarityFunction function,
      FloatVectorValues floatVectorValues,
      IntToIntFunction oldToNewDocId) {

    try (Arena temp = Arena.ofConfined()) {
      int size = floatVectorValues.size();
      int dimension = floatVectorValues.dimension();
      int metric = functionToMetric(function);

      // Create an index
      MemorySegment pointer = temp.allocate(ADDRESS);
      handleException(
          wrapper.faiss_index_factory(pointer, dimension, temp.allocateFrom(description), metric));

      MemorySegment indexPointer = pointer.get(ADDRESS, 0);

      // Set index params
      handleException(wrapper.faiss_ParameterSpace_new(pointer));
      MemorySegment parameterSpacePointer =
          pointer
              .get(ADDRESS, 0)
              // Ensure timely cleanup
              .reinterpret(temp, wrapper::faiss_ParameterSpace_free);

      handleException(
          wrapper.faiss_ParameterSpace_set_index_parameters(
              parameterSpacePointer, indexPointer, temp.allocateFrom(indexParams)));

      // TODO: Improve memory usage (with a tradeoff in performance) by batched indexing, see:
      //  - https://github.com/opensearch-project/k-NN/issues/1506
      //  - https://github.com/opensearch-project/k-NN/issues/1938

      // Allocate docs in native memory
      MemorySegment docs = temp.allocate(JAVA_FLOAT, (long) size * dimension);
      long docsOffset = 0;
      long perDocByteSize = dimension * JAVA_FLOAT.byteSize();

      // Allocate ids in native memory
      MemorySegment ids = temp.allocate(JAVA_LONG, size);
      int idsIndex = 0;

      KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
      for (int i = iterator.nextDoc(); i != NO_MORE_DOCS; i = iterator.nextDoc()) {
        int id = oldToNewDocId.apply(i);
        ids.setAtIndex(JAVA_LONG, idsIndex, id);
        idsIndex++;

        float[] vector = floatVectorValues.vectorValue(iterator.index());
        MemorySegment.copy(vector, 0, docs, JAVA_FLOAT, docsOffset, vector.length);
        docsOffset += perDocByteSize;
      }

      // Train index
      int isTrained = wrapper.faiss_Index_is_trained(indexPointer);
      if (isTrained == 0) {
        handleException(wrapper.faiss_Index_train(indexPointer, size, docs));
      }

      // Add docs to index
      handleException(wrapper.faiss_Index_add_with_ids(indexPointer, size, docs, ids));

      return new Index(indexPointer);

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // See flags defined in c_api/index_io_c.h
  private static final int FAISS_IO_FLAG_MMAP = 1;
  private static final int FAISS_IO_FLAG_READ_ONLY = 2;

  @Override
  public FaissLibrary.Index readIndex(IndexInput input) {
    try (Arena temp = Arena.ofConfined()) {
      MethodHandle readerHandle = READ_BYTES_HANDLE.bindTo(input);
      MemorySegment readerStub =
          getStub(
              temp, readerHandle, FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));

      MemorySegment pointer = temp.allocate(ADDRESS);
      handleException(wrapper.faiss_CustomIOReader_new(pointer, readerStub));
      MemorySegment customIOReaderPointer =
          pointer
              .get(ADDRESS, 0)
              // Ensure timely cleanup
              .reinterpret(temp, wrapper::faiss_CustomIOReader_free);

      // Read index
      handleException(
          wrapper.faiss_read_index_custom(
              customIOReaderPointer, FAISS_IO_FLAG_MMAP | FAISS_IO_FLAG_READ_ONLY, pointer));
      MemorySegment indexPointer = pointer.get(ADDRESS, 0);

      return new Index(indexPointer);
    }
  }

  private class Index implements FaissLibrary.Index {
    @FunctionalInterface
    private interface FloatToFloatFunction {
      float scale(float score);
    }

    private final Arena arena;
    private final MemorySegment indexPointer;
    private final FloatToFloatFunction scaler;
    private boolean closed;

    private Index(MemorySegment indexPointer) {
      this.arena = Arena.ofShared();
      this.indexPointer =
          indexPointer
              // Ensure timely cleanup
              .reinterpret(arena, wrapper::faiss_Index_free);

      // Get underlying function
      int metricType = wrapper.faiss_Index_metric_type(indexPointer);
      VectorSimilarityFunction function = metricToFunction(metricType);

      // Scale Faiss distances to Lucene scores, see VectorSimilarityFunction.java
      this.scaler =
          switch (function) {
            case DOT_PRODUCT ->
                // distance in Faiss === dotProduct in Lucene
                distance -> Math.max((1 + distance) / 2, 0);

            case EUCLIDEAN ->
                // distance in Faiss === squareDistance in Lucene
                distance -> 1 / (1 + distance);

            case COSINE, MAXIMUM_INNER_PRODUCT -> throw new AssertionError("Should not reach here");
          };

      this.closed = false;
    }

    @Override
    public void close() {
      if (closed == false) {
        arena.close();
        closed = true;
      }
    }

    @Override
    public void search(float[] query, KnnCollector knnCollector, AcceptDocs acceptDocs) {
      try (Arena temp = Arena.ofConfined()) {
        FixedBitSet fixedBitSet =
            switch (acceptDocs.bits()) {
              case null -> null;
              case FixedBitSet bitSet -> bitSet;
              // TODO: Add optimized case for SparseFixedBitSet
              case Bits bits -> FixedBitSet.copyOf(bits);
            };

        // Allocate queries in native memory
        MemorySegment queries = temp.allocateFrom(JAVA_FLOAT, query);

        // Faiss knn search
        int k = knnCollector.k();
        MemorySegment distancesPointer = temp.allocate(JAVA_FLOAT, k);
        MemorySegment idsPointer = temp.allocate(JAVA_LONG, k);

        MemorySegment localIndex = indexPointer.reinterpret(temp, null);
        if (fixedBitSet == null) {
          // Search without runtime filters
          handleException(
              wrapper.faiss_Index_search(localIndex, 1, queries, k, distancesPointer, idsPointer));
        } else {
          MemorySegment pointer = temp.allocate(ADDRESS);

          long[] bits = fixedBitSet.getBits();
          MemorySegment nativeBits =
              // Use LITTLE_ENDIAN to convert long[] -> uint8_t*
              temp.allocateFrom(JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN), bits);

          handleException(
              wrapper.faiss_IDSelectorBitmap_new(pointer, fixedBitSet.length(), nativeBits));
          MemorySegment idSelectorBitmapPointer =
              pointer
                  .get(ADDRESS, 0)
                  // Ensure timely cleanup
                  .reinterpret(temp, wrapper::faiss_IDSelectorBitmap_free);

          handleException(wrapper.faiss_SearchParameters_new(pointer, idSelectorBitmapPointer));
          MemorySegment searchParametersPointer =
              pointer
                  .get(ADDRESS, 0)
                  // Ensure timely cleanup
                  .reinterpret(temp, wrapper::faiss_SearchParameters_free);

          // Search with runtime filters
          handleException(
              wrapper.faiss_Index_search_with_params(
                  localIndex,
                  1,
                  queries,
                  k,
                  searchParametersPointer,
                  distancesPointer,
                  idsPointer));
        }

        // Record hits
        for (int i = 0; i < k; i++) {
          int id = (int) idsPointer.getAtIndex(JAVA_LONG, i);

          // Not enough results
          if (id == -1) {
            break;
          }

          // Collect result
          float distance = distancesPointer.getAtIndex(JAVA_FLOAT, i);
          knnCollector.collect(id, scaler.scale(distance));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(IndexOutput output) {
      try (Arena temp = Arena.ofConfined()) {
        MethodHandle writerHandle = WRITE_BYTES_HANDLE.bindTo(output);
        MemorySegment writerStub =
            getStub(
                temp,
                writerHandle,
                FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));

        MemorySegment pointer = temp.allocate(ADDRESS);
        handleException(wrapper.faiss_CustomIOWriter_new(pointer, writerStub));
        MemorySegment customIOWriterPointer =
            pointer
                .get(ADDRESS, 0)
                // Ensure timely cleanup
                .reinterpret(temp, wrapper::faiss_CustomIOWriter_free);

        // Write index
        handleException(
            wrapper.faiss_write_index_custom(
                indexPointer, customIOWriterPointer, FAISS_IO_FLAG_MMAP | FAISS_IO_FLAG_READ_ONLY));
      }
    }
  }
}
