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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.IndexInput;

/**
 * Reads Scalar Quantized vectors from the index segments along with index data structures.
 *
 * @lucene.experimental
 */
public final class Lucene99ScalarQuantizedVectorsReader {

  private final IndexInput quantizedVectorData;

  Lucene99ScalarQuantizedVectorsReader(IndexInput quantizedVectorData) {
    this.quantizedVectorData = quantizedVectorData;
  }

  static void validateFieldEntry(
      FieldInfo info, int fieldDimension, int size, long quantizedVectorDataLength) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldDimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldDimension);
    }

    // int8 quantized and calculated stored offset.
    long quantizedVectorBytes = dimension + Float.BYTES;
    long numQuantizedVectorBytes = Math.multiplyExact(quantizedVectorBytes, size);
    if (numQuantizedVectorBytes != quantizedVectorDataLength) {
      throw new IllegalStateException(
          "Quantized vector data length "
              + quantizedVectorDataLength
              + " not matching size="
              + size
              + " * (dim="
              + dimension
              + " + 4)"
              + " = "
              + numQuantizedVectorBytes);
    }
  }

  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(quantizedVectorData);
  }

  OffHeapQuantizedByteVectorValues getQuantizedVectorValues(
      Lucene99HnswVectorsReader.OrdToDocDISReaderConfiguration configuration,
      int dimension,
      int size,
      long quantizedVectorDataOffset,
      long quantizedVectorDataLength)
      throws IOException {
    return OffHeapQuantizedByteVectorValues.load(
        configuration,
        dimension,
        size,
        quantizedVectorDataOffset,
        quantizedVectorDataLength,
        quantizedVectorData);
  }
}
