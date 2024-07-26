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
package org.apache.lucene.sandbox.facet.labels;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.facet.range.Range;
import org.apache.lucene.facet.taxonomy.FacetLabel;

/** {@link OrdLabelBiMap} for ranges. */
public class RangeOrdLabelBiMap implements OrdLabelBiMap {

  private final Range[] ranges;

  /** Constructor that takes array of Range objects as input * */
  public RangeOrdLabelBiMap(Range[] inputRanges) {
    ranges = inputRanges;
  }

  /** Constructor that takes List of Range objects as input * */
  public RangeOrdLabelBiMap(List<? extends Range> inputRanges) {
    ranges = inputRanges.toArray(new Range[0]);
  }

  @Override
  public FacetLabel getLabel(int ordinal) throws IOException {
    if (ordinal >= 0 && ordinal < ranges.length) {
      return new FacetLabel(ranges[ordinal].label);
    }
    return null;
  }

  @Override
  public FacetLabel[] getLabels(int[] ordinals) throws IOException {
    FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      facetLabels[i] = getLabel(ordinals[i]);
    }
    return facetLabels;
  }

  @Override
  public int getOrd(FacetLabel label) {
    throw new UnsupportedOperationException("Not yet supported for ranges");
  }

  @Override
  public int[] getOrds(FacetLabel[] labels) {
    throw new UnsupportedOperationException("Not yet supported for ranges");
  }
}
