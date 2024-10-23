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
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

/**
 * Map taxonomy labels to ordinals.
 *
 * @lucene.experimental
 */
public final class TaxonomyOrdLabelBiMap implements OrdToLabel, LabelToOrd {

  private final TaxonomyReader taxoReader;

  /** Construct */
  public TaxonomyOrdLabelBiMap(TaxonomyReader taxoReader) {
    this.taxoReader = taxoReader;
  }

  @Override
  public FacetLabel getLabel(int ordinal) throws IOException {
    return taxoReader.getPath(ordinal);
  }

  @Override
  public FacetLabel[] getLabels(int[] ordinals) throws IOException {
    return taxoReader.getBulkPath(
        ordinals.clone()); // Have to clone because getBulkPath shuffles its input array.
  }

  @Override
  public int getOrd(FacetLabel label) throws IOException {
    return taxoReader.getOrdinal(label);
  }

  @Override
  public int[] getOrds(FacetLabel[] labels) throws IOException {
    return taxoReader.getBulkOrdinals(labels);
  }
}
