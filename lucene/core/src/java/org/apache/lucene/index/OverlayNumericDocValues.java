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
package org.apache.lucene.index;

import java.io.IOException;

/**
 * Merges several {@link NumericDocValues} layers (newest first, base last) into one view: a doc's
 * value comes from the newest layer that has it, else the base. The delta layers are set-only
 * (removals go through the dense rewrite), so a doc has a value iff some layer has it and the
 * newest wins, which makes iteration a plain union-merge.
 */
final class OverlayNumericDocValues extends NumericDocValues {

  private final NumericDocValues[] layers; // newest first, base last
  private final DocValuesOverlayMerger merger;

  OverlayNumericDocValues(NumericDocValues[] layers) {
    assert layers != null && layers.length > 0;
    this.layers = layers;
    this.merger = new DocValuesOverlayMerger(layers);
  }

  @Override
  public long longValue() throws IOException {
    return layers[merger.valueLayer()].longValue();
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    return merger.advanceExact(target);
  }

  @Override
  public int docID() {
    return merger.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return merger.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return merger.advance(target);
  }

  @Override
  public long cost() {
    return merger.cost();
  }
}
