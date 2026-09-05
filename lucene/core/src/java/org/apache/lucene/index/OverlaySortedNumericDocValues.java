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
import java.util.List;
import org.apache.lucene.codecs.DocValuesProducer;

/**
 * Merges several {@link SortedNumericDocValues} layers (newest generation first, base last) into
 * one view: for a given document the whole value set comes from the newest layer that has it, else
 * the base. Unlike {@link OverlayNumericDocValues} this preserves multi-valued documents, so it
 * works whether the base column is single- or multi-valued (a set-only sorted-numeric update writes
 * a single-valued delta on top of a possibly multi-valued base).
 */
final class OverlaySortedNumericDocValues extends SortedNumericDocValues {

  private final SortedNumericDocValues[] layers; // newest first, base last
  private final DocValuesOverlayMerger merger;

  OverlaySortedNumericDocValues(SortedNumericDocValues[] layers) {
    assert layers != null && layers.length > 0;
    this.layers = layers;
    this.merger = new DocValuesOverlayMerger(layers);
  }

  @Override
  public long nextValue() throws IOException {
    return layers[merger.valueLayer()].nextValue();
  }

  @Override
  public int docValueCount() {
    return layers[merger.valueLayer()].docValueCount();
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

  static SortedNumericDocValues from(FieldInfo fieldInfo, List<DocValuesProducer> producers)
      throws IOException {
    SortedNumericDocValues[] layers = new SortedNumericDocValues[producers.size()];
    boolean allSingleValued = true;
    for (int i = 0; i < layers.length; i++) {
      layers[i] = producers.get(i).getSortedNumeric(fieldInfo);
      allSingleValued &= DocValues.isSingleton(layers[i]);
    }
    if (allSingleValued) {
      NumericDocValues[] singletonLayers = new NumericDocValues[producers.size()];
      for (int i = 0; i < layers.length; i++) {
        singletonLayers[i] = DocValues.unwrapSingleton(layers[i]);
      }
      return DocValues.singleton(new OverlayNumericDocValues(singletonLayers));
    } else {
      return new OverlaySortedNumericDocValues(layers);
    }
  }
}
