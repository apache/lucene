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

import java.util.Optional;

/**
 * Bit-width encoding for TurboQuant vector quantization. Each coordinate of a rotated vector is
 * quantized to this many bits using precomputed Beta-distribution-optimal Lloyd-Max centroids.
 */
public enum TurboQuantEncoding {
  /** 2 bits per coordinate, 16x compression, aggressive. */
  BITS_2(0, 2),
  /** 3 bits per coordinate, ~10.7x compression. */
  BITS_3(1, 3),
  /** 4 bits per coordinate, 8x compression, default, best recall/compression trade-off. */
  BITS_4(2, 4),
  /** 8 bits per coordinate, 4x compression, near-lossless. */
  BITS_8(3, 8);

  private final int wireNumber;

  /** Number of bits used per coordinate. */
  public final int bitsPerCoordinate;

  TurboQuantEncoding(int wireNumber, int bitsPerCoordinate) {
    this.wireNumber = wireNumber;
    this.bitsPerCoordinate = bitsPerCoordinate;
  }

  /** Returns the wire number used for serialization. */
  public int getWireNumber() {
    return wireNumber;
  }

  /**
   * Returns the number of bytes required to store a packed quantized vector of the given
   * dimensionality.
   */
  public int getPackedByteLength(int d) {
    return (d * bitsPerCoordinate + 7) / 8;
  }

  /**
   * Returns the number of dimensions rounded up so that the packed representation fills whole
   * bytes.
   */
  public int getDiscreteDimensions(int d) {
    int totalBits = d * bitsPerCoordinate;
    int roundedBits = (totalBits + 7) / 8 * 8;
    return roundedBits / bitsPerCoordinate;
  }

  /** Returns the encoding for the given wire number, or empty if unknown. */
  public static Optional<TurboQuantEncoding> fromWireNumber(int wireNumber) {
    for (TurboQuantEncoding encoding : values()) {
      if (encoding.wireNumber == wireNumber) {
        return Optional.of(encoding);
      }
    }
    return Optional.empty();
  }
}
