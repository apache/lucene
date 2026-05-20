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
package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.sandbox.facet.cutters.LongValueFacetCutter;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;

/**
 * {@link FacetBuilder} for distinct numeric field values.
 *
 * @lucene.experimental
 */
public final class LongValueFacetBuilder extends BaseFacetBuilder<LongValueFacetBuilder> {

  private LongValueFacetCutter cutter;
  private final CollectionKey collectionKey;

  public LongValueFacetBuilder(String field) {
    super(field);
    // We can reuse collector manager if it is for the same field
    collectionKey = new CollectionKey(field);
    // Sort by field value by default
    sortOrderSupplier = () -> ComparableUtils.byLongValue(getFacetCutter());
  }

  @Override
  LongValueFacetCutter getFacetCutter() {
    return cutter;
  }

  @Override
  OrdToLabel getOrdToLabel() {
    return cutter;
  }

  @Override
  Number getOverallValue() {
    // TODO: we don't collect overall value anywhere, we should either change cutter to return ord
    // that corresponse to "and value",
    //       or change CountRecorder itself to collect overall value when needed.
    return -1;
  }

  @Override
  FacetBuilder initOrReuseCollector(FacetBuilder similar) {
    if (similar == null) {
      cutter = new LongValueFacetCutter(dimension);
    } else if (similar instanceof LongValueFacetBuilder castedSimilar) {
      cutter = castedSimilar.cutter;
    } else {
      assert false;
    }

    return super.initOrReuseCollector(similar);
  }

  @Override
  LongValueFacetBuilder self() {
    return this;
  }

  private record CollectionKey(String indexFieldName) {}

  @Override
  Object collectionKey() {
    return collectionKey;
  }
}
