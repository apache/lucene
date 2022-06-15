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

import java.util.stream.IntStream;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.util.BytesRef;

/** A {@link FacetSet} which encodes integer dimension values. */
public class IntFacetSet extends FacetSet {

  /** The raw dimension values of this facet set. */
  public final int[] intValues;

  /** Constructs a new instance of a facet set which stores {@code int} dimension values. */
  public IntFacetSet(int... values) {
    super(IntStream.of(values).mapToLong(Long::valueOf).toArray());

    this.intValues = values;
  }

  @Override
  public int packValues(byte[] buf, int start) {
    for (int i = 0, offset = start; i < intValues.length; i++, offset += Integer.BYTES) {
      IntPoint.encodeDimension(intValues[i], buf, offset);
    }
    return intValues.length * Integer.BYTES;
  }

  @Override
  public int sizePackedBytes() {
    return values.length * Integer.BYTES;
  }

  /**
   * An implementation of the {@link FacetSetDecoder} functional interface for integer dimension
   * values.
   */
  public static int decode(BytesRef bytesRef, int start, long[] dest) {
    for (int i = 0, offset = start; i < dest.length; i++, offset += Integer.BYTES) {
      dest[i] = IntPoint.decodeDimension(bytesRef.bytes, offset);
    }
    return dest.length * Integer.BYTES;
  }
}
