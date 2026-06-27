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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.io.IOException;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.BaseQuantizedByteVectorValues;

/**
 * Off-heap random access to TurboQuant quantized vectors stored in a mmap'd {@code .vetq} file.
 * Each vector is stored as packed b-bit indices followed by a float32 norm.
 */
public class OffHeapTurboQuantVectorValues extends BaseQuantizedByteVectorValues {

  private final int dimension;
  private final int size;
  private final int bitsPerCoordinate;
  private final int packedBytesPerVector;
  private final int bytesPerVector; // packedBytes + 4 (float norm)
  private final long dataOffset;
  private final IndexInput data;
  private final float[] centroids;
  private final HadamardRotation rotation;
  private final byte[] packedBuffer;

  /** Creates off-heap quantized vector values. */
  public OffHeapTurboQuantVectorValues(
      int dimension,
      int size,
      TurboQuantEncoding encoding,
      long dataOffset,
      IndexInput data,
      float[] centroids,
      HadamardRotation rotation) {
    this.dimension = dimension;
    this.size = size;
    this.bitsPerCoordinate = encoding.bitsPerCoordinate;
    this.packedBytesPerVector = encoding.getPackedByteLength(dimension);
    this.bytesPerVector = packedBytesPerVector + Float.BYTES;
    this.dataOffset = dataOffset;
    this.data = data;
    this.centroids = centroids;
    this.rotation = rotation;
    this.packedBuffer = new byte[packedBytesPerVector];
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public byte[] vectorValue(int ord) throws IOException {
    long offset = dataOffset + (long) ord * bytesPerVector;
    data.seek(offset);
    byte[] buf = new byte[packedBytesPerVector];
    data.readBytes(buf, 0, packedBytesPerVector);
    return buf;
  }

  /** Returns the stored norm for the given ordinal. */
  public float getNorm(int ord) throws IOException {
    long offset = dataOffset + (long) ord * bytesPerVector + packedBytesPerVector;
    data.seek(offset);
    return Float.intBitsToFloat(data.readInt());
  }

  /** Returns the precomputed centroids scaled for this field's dimension. */
  public float[] getCentroids() {
    return centroids;
  }

  /** Returns the Hadamard rotation for this field. */
  public HadamardRotation getRotation() {
    return rotation;
  }

  /** Returns the bits per coordinate for this encoding. */
  public int getBitsPerCoordinate() {
    return bitsPerCoordinate;
  }

  @Override
  public OffHeapTurboQuantVectorValues copy() throws IOException {
    return new OffHeapTurboQuantVectorValues(
        dimension,
        size,
        TurboQuantEncoding.fromWireNumber(
                switch (bitsPerCoordinate) {
                  case 2 -> 0;
                  case 3 -> 1;
                  case 4 -> 2;
                  case 8 -> 3;
                  default -> throw new IllegalStateException();
                })
            .orElseThrow(),
        dataOffset,
        data.clone(),
        centroids,
        rotation);
  }

  @Override
  public VectorEncoding getEncoding() {
    return VectorEncoding.BYTE;
  }

  @Override
  public DocIndexIterator iterator() {
    return createDenseIterator();
  }

  @Override
  public IndexInput getSlice() {
    return data;
  }
}
