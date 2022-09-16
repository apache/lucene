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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link BinaryDocValuesField} which encodes a list of {@link FacetSet facet sets}. The encoding
 * scheme consists of a packed {@code byte[]} where the first value denotes the number of dimensions
 * in all the sets, followed by each set's values.
 *
 * @lucene.experimental
 */
public class FacetSetsField extends BinaryDocValuesField {

  /**
   * Create a new FacetSets field.
   *
   * @param name field name
   * @param facetSets the {@link FacetSet facet sets} to index in that field. All must have the same
   *     number of dimensions
   * @throws IllegalArgumentException if the field name is null or the given facet sets are invalid
   */
  public static FacetSetsField create(String name, FacetSet... facetSets) {
    if (facetSets == null || facetSets.length == 0) {
      throw new IllegalArgumentException("FacetSets cannot be null or empty!");
    }

    return new FacetSetsField(name, toPackedValues(facetSets));
  }

  private FacetSetsField(String name, BytesRef value) {
    super(name, value);
  }

  private static BytesRef toPackedValues(FacetSet... facetSets) {
    int numDims = facetSets[0].dims;
    Class<?> expectedClass = facetSets[0].getClass();
    byte[] buf = new byte[Integer.BYTES + facetSets[0].sizePackedBytes() * facetSets.length];
    IntPoint.encodeDimension(numDims, buf, 0);
    int offset = Integer.BYTES;
    for (FacetSet facetSet : facetSets) {
      if (facetSet.dims != numDims) {
        throw new IllegalArgumentException(
            "All FacetSets must have the same number of dimensions. Expected "
                + numDims
                + " found "
                + facetSet.dims);
      }
      // It doesn't make sense to index facet sets of different types in the same field
      if (facetSet.getClass() != expectedClass) {
        throw new IllegalArgumentException(
            "All FacetSets must be the same type. Expected "
                + expectedClass
                + " found "
                + facetSet.getClass());
      }
      offset += facetSet.packValues(buf, offset);
    }
    return new BytesRef(buf, 0, offset);
  }
}
