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
package org.apache.lucene.facet.facetset;

import java.io.IOException;
import org.apache.lucene.store.RandomAccessInput;

/**
 * A functional interface for decoding facet set values into comparable `long` ones. You can use it
 * by e.g. implementing a static method with the same signature and then pass it as {@code
 * YourClass::decode}.
 *
 * @lucene.experimental
 */
public interface FacetSetDecoder {

  /**
   * An implementation of {@link FacetSetDecoder#decode(RandomAccessInput, long, long[])} for
   * long/double dimension values which were encoded with {@link LongFacetSet} and {@link
   * DoubleFacetSet} respectively.
   */
  static int decodeLongs(RandomAccessInput input, long start, long[] dest) throws IOException {
    assert input != null;
    for (int i = 0; i < dest.length; i++) {
      dest[i] = sortableBytesToLong(input, start);
      start += Long.BYTES;
    }
    return dest.length * Long.BYTES;
  }

  /**
   * An implementation of {@link FacetSetDecoder#decode(RandomAccessInput, long, long[])} for
   * int/float dimension values which were encoded with {@link IntFacetSet} and {@link
   * FloatFacetSet} respectively.
   */
  static int decodeInts(RandomAccessInput input, long start, long[] dest) throws IOException {
    assert input != null;
    for (int i = 0; i < dest.length; i++) {
      dest[i] = sortableBytesToInt(input, start);
      start += Integer.BYTES;
    }
    return dest.length * Integer.BYTES;
  }

  static int sortableBytesToInt(RandomAccessInput encoded, long offset) throws IOException {
    int i = Integer.reverseBytes(encoded.readInt(offset));
    // Re-flip the sign bit to restore the original value:
    return i ^ 0x80000000;
  }

  static long sortableBytesToLong(RandomAccessInput encoded, long offset) throws IOException {
    long l = Long.reverseBytes(encoded.readLong(offset));
    // Flip the sign bit back
    return l ^ 0x8000000000000000L;
  }

  /**
   * Decodes the facet set dimension values into the given destination buffer and returns the number
   * of bytes read.
   */
  int decode(RandomAccessInput input, long start, long[] dest) throws IOException;
}
