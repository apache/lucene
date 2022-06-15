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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.BytesRef;

/**
 * Holds a set of facet dimension values.
 *
 * @lucene.experimental
 */
public class FacetSet {

  /** The number of dimension values in this set. */
  public final int dims;

  /** The facet dimensions values as {@code long}. */
  public final long[] values;

  /**
   * Constructs a new instance of a facet set with the given dimension values encoded as {@code
   * long}. The actual encoding happens in {@link #packValues(byte[], int)}, so that the values are
   * encoded more efficiently (e.g. consuming less space) if needed.
   */
  public FacetSet(long... values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("values cannot be null or empty");
    }

    this.values = values;
    this.dims = values.length;
  }

  /**
   * Packs the dimension values in a {@code byte[]}. The default implementation packs the {@link
   * #values} using {@link LongPoint#encodeDimension(long, byte[], int)}, but you can override to
   * implement your own encoding scheme.
   */
  public int packValues(byte[] buf, int start) {
    for (int i = 0, offset = start; i < values.length; i++, offset += Long.BYTES) {
      LongPoint.encodeDimension(values[i], buf, offset);
    }
    return values.length * Long.BYTES;
  }

  /**
   * Returns the size of the packed values in this facet set. The default implementation returns
   * {@code values.length * Long.BYTES}, but you can override to return a value that puts a tighter
   * limit. If you are unsure of the final size (e.g. if you compress the values), you can always
   * return an upper limit.
   */
  public int sizePackedBytes() {
    return values.length * Long.BYTES;
  }

  /** An implementation of the {@link FacetSetDecoder} functional interface. */
  public static int decode(BytesRef bytesRef, int start, long[] dest) {
    LongPoint.unpack(bytesRef, start, dest);
    return dest.length * Long.BYTES;
  }
}
