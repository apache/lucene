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

import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;

/**
 * Common {@link FacetBuilder} that works with any {@link FacetCutter} and {@link OrdToLabel}
 * provided by user.
 *
 * @lucene.experimental
 */
public final class CommonFacetBuilder extends BaseFacetBuilder<CommonFacetBuilder> {

  private final FacetCutter cutter;
  private final OrdToLabel ordToLabel;

  public CommonFacetBuilder(String dimension, FacetCutter cutter, OrdToLabel ordToLabel) {
    super(dimension);
    this.cutter = cutter;
    this.ordToLabel = ordToLabel;
  }

  @Override
  FacetCutter getFacetCutter() {
    return cutter;
  }

  @Override
  Number getOverallValue() {
    // There is no common way to compute overall value for all facet types
    return -1;
  }

  @Override
  OrdToLabel getOrdToLabel() {
    return ordToLabel;
  }

  @Override
  CommonFacetBuilder self() {
    return this;
  }
}
