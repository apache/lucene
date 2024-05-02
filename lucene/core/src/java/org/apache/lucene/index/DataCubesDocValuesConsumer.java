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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.DataCubesProducer;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;

/**
 * Abstract API that consumes numeric, binary and sorted docValues to write DataCubeIndices
 *
 * @lucene.experimental
 */
public abstract class DataCubesDocValuesConsumer extends DocValuesConsumer {

  Map<String, NumericDocValues> numericDocValuesMap = new ConcurrentHashMap<>();
  Map<String, BinaryDocValues> binaryDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedDocValues> sortedDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedNumericDocValues> sortedNumericDocValuesMap = new ConcurrentHashMap<>();
  Map<String, SortedSetDocValues> sortedSetDocValuesMap = new ConcurrentHashMap<>();

  /** Sole constructor */
  public DataCubesDocValuesConsumer() throws IOException {
    super();
  }

  /**
   * Temporarily hold numericDocValues in a map, so that it can be used during flush to create
   * DataCubeIndices
   */
  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    numericDocValuesMap.put(field.name, valuesProducer.getNumeric(field));
  }

  /**
   * Temporarily hold binaryDocValues in a map, so that it can be used during flush to create
   * DataCubeIndices
   */
  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    binaryDocValuesMap.put(field.name, valuesProducer.getBinary(field));
  }

  /**
   * Temporarily hold sortedDocValues in a map, so that it can be used during flush to create
   * DataCubeIndices
   */
  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    sortedDocValuesMap.put(field.name, valuesProducer.getSorted(field));
  }

  /**
   * Temporarily hold sortedNumericDocValues in a map, so that it can be used during flush to create
   * DataCubeIndices
   */
  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    sortedNumericDocValuesMap.put(field.name, valuesProducer.getSortedNumeric(field));
  }

  /**
   * Temporarily hold sortedSetDocValues in a map, so that it can be used during flush to create
   * DataCubeIndices
   */
  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    sortedSetDocValuesMap.put(field.name, valuesProducer.getSortedSet(field));
  }

  /** Get numeric docValues from the map */
  public Map<String, NumericDocValues> getNumericDocValuesMap() {
    return numericDocValuesMap;
  }

  /** Get binary docValues from the map */
  public Map<String, BinaryDocValues> getBinaryDocValuesMap() {
    return binaryDocValuesMap;
  }

  /** Get sorted docValues from the map */
  public Map<String, SortedDocValues> getSortedDocValuesMap() {
    return sortedDocValuesMap;
  }

  /** Get sorted numeric docValues from the map */
  public Map<String, SortedNumericDocValues> getSortedNumericDocValuesMap() {
    return sortedNumericDocValuesMap;
  }

  /** Get sorted set docValues from the map */
  public Map<String, SortedSetDocValues> getSortedSetDocValuesMap() {
    return sortedSetDocValuesMap;
  }

  /** Create the DataCubes index based on the DocValues and flushes the indices to disk */
  public abstract void flush(DataCubesConfig compositeConfig) throws IOException;

  /** Merges in the DataCubes fields from the readers in <code>mergeState</code>. */
  @Override
  public void merge(MergeState mergeState) throws IOException {
    for (DataCubesProducer<?> dataCubesProducer : mergeState.dataCubesProducers) {
      if (dataCubesProducer != null) {
        dataCubesProducer.checkIntegrity();
      }
    }
  }

  @Override
  public void close() throws IOException {}
}
