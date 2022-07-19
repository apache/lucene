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

import java.util.Arrays;
import org.apache.lucene.document.IntPoint;

/**
 * A {@link FacetSet} which encodes integer dimension values.
 *
 * @lucene.experimental
 */
public class IntFacetSet extends FacetSet {

  /** The raw dimension values of this facet set. */
  public final int[] values;

  /** Constructs a new instance of a facet set which stores {@code int} dimension values. */
  public IntFacetSet(int... values) {
    super(validateValuesAndGetNumDims(values));

    this.values = values;
  }

  @Override
  public long[] getComparableValues() {
    return Arrays.stream(values).mapToLong(Long::valueOf).toArray();
  }

  @Override
  public int packValues(byte[] buf, int start) {
    for (int i = 0, offset = start; i < values.length; i++, offset += Integer.BYTES) {
      IntPoint.encodeDimension(values[i], buf, offset);
    }
    return values.length * Integer.BYTES;
  }

  @Override
  public int sizePackedBytes() {
    return dims * Integer.BYTES;
  }

  private static int validateValuesAndGetNumDims(int... values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("values cannot be null or empty");
    }
    return values.length;
  }
}
