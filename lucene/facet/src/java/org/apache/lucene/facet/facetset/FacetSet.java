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

/**
 * Holds a set of facet dimension values.
 *
 * @lucene.experimental
 */
public abstract class FacetSet {

  /** The number of dimension values in this set. */
  public final int dims;

  /** Constructs a new instance of a facet set with the given number of dimensions. */
  protected FacetSet(int dims) {
    this.dims = dims;
  }

  /** Returns the dimension values in this facet set as comparable longs. */
  public abstract long[] getComparableValues();

  /**
   * Packs the dimension values into the given {@code byte[]} and returns the number of
   * packed-values bytes. The default implementation packs the {@link #getComparableValues()
   * comparable values}, and you can override to implement your own scheme.
   */
  public int packValues(byte[] buf, int start) {
    long[] comparableValues = getComparableValues();
    for (int i = 0, offset = start; i < comparableValues.length; i++, offset += Long.BYTES) {
      LongPoint.encodeDimension(comparableValues[i], buf, offset);
    }
    return comparableValues.length * Long.BYTES;
  }

  /**
   * Returns the size of the packed values in this facet set. If the value is unknown in advance
   * (e.g. if the values are compressed), this method can return an upper limit. The default
   * implementations returns {@code dims * Long.BYTES} per the default implementation of {@link
   * #packValues(byte[], int)}. You should override if you implement {@link #packValues(byte[],
   * int)} differently.
   */
  public int sizePackedBytes() {
    return dims * Long.BYTES;
  }
}
