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
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link BinaryDocValuesField} which encodes a list of {@link FacetSet facet sets}. The encoding
 * scheme consists of a packed {@code long[]} where the first value denotes the number of dimensions
 * in all the sets, followed by each set's values.
 *
 * @lucene.experimental
 */
public class FacetSetsField extends BinaryDocValuesField {

  /**
   * Create a new FacetSets field.
   *
   * @param name field name
   * @param facetSets the {@link FacetSet} to index in that field. All must have the same number of
   *     dimensions
   * @throws IllegalArgumentException if the field name is null or the given facet sets are invalid
   */
  public static FacetSetsField create(String name, FacetSet... facetSets) {
    validateFacetSets(facetSets);

    return new FacetSetsField(name, toPackedLongs(facetSets));
  }

  private FacetSetsField(String name, BytesRef value) {
    super(name, value);
  }

  private static void validateFacetSets(FacetSet... facetSets) {
    if (facetSets == null || facetSets.length == 0) {
      throw new IllegalArgumentException("FacetSets cannot be null or empty!");
    }

    int dims = facetSets[0].values.length;
    if (!Arrays.stream(facetSets).allMatch(facetSet -> facetSet.values.length == dims)) {
      throw new IllegalArgumentException("All FacetSets must have the same number of dimensions!");
    }
  }

  // TODO: when there are many facet sets, it might be more efficient to pack each dimension
  // separately.
  // This however requires a matching "reader" in order to unpack them properly during aggregation
  // time, therefore
  // if the need arises we might need to factor this logic out to a FacetSetEncoder/Decoder or
  // PackedFacetSet
  private static BytesRef toPackedLongs(FacetSet... facetSets) {
    int numDims = facetSets[0].values.length;
    long[] dimsAndCount = new long[1 + numDims * facetSets.length];
    int idx = 0;
    dimsAndCount[idx++] = numDims;
    for (FacetSet facetSet : facetSets) {
      System.arraycopy(facetSet.values, 0, dimsAndCount, idx, numDims);
      idx += numDims;
    }
    return LongPoint.pack(dimsAndCount);
  }
}
