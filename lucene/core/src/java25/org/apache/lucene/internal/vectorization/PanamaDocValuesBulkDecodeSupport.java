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
package org.apache.lucene.internal.vectorization;

import java.nio.ByteOrder;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

/** Panama Vector API implementation of {@link DocValuesBulkDecodeSupport}. */
final class PanamaDocValuesBulkDecodeSupport implements DocValuesBulkDecodeSupport {

  static final PanamaDocValuesBulkDecodeSupport INSTANCE = new PanamaDocValuesBulkDecodeSupport();

  private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;

  private PanamaDocValuesBulkDecodeSupport() {}

  @Override
  public void decodeByteAligned(
      byte[] bytes, int bytesOffset, int bitsPerValue, long[] values, int valuesOffset, int count) {
    if (bitsPerValue != Long.SIZE
        || ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN
        || BYTE_SPECIES.vectorByteSize() < 32) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset, bitsPerValue, values, valuesOffset, count);
      return;
    }

    final int valuesPerVector = BYTE_SPECIES.vectorByteSize() / Long.BYTES;
    final int loopBound = count - count % valuesPerVector;
    int i = 0;
    for (; i < loopBound; i += valuesPerVector) {
      ByteVector.fromArray(BYTE_SPECIES, bytes, bytesOffset + i * Long.BYTES)
          .reinterpretAsLongs()
          .intoArray(values, valuesOffset + i);
    }
    if (i < count) {
      DefaultDocValuesBulkDecodeSupport.INSTANCE.decodeByteAligned(
          bytes, bytesOffset + i * Long.BYTES, bitsPerValue, values, valuesOffset + i, count - i);
    }
  }
}
