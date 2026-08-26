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
package org.apache.lucene.util.quantization;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

/**
 * Quantized vector reader
 *
 * @lucene.experimental
 */
public interface QuantizedVectorsReader extends Closeable, Accountable {

  BaseQuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException;

  ScalarQuantizer getQuantizationState(String fieldName);

  /**
   * Provides a scorer for merging this quantized vector reader. This way any additional merging
   * logic can be implemented by the user of this class.
   *
   * @param fieldInfo fieldInfo of the field to merge
   * @param segmentWriteState the SegmentWriteState to write temporary files if needed
   * @return a scorer over the newly merged flat vectors, which should be closed as it may hold
   *     temporary file handles to read over auxiliary data structures
   * @throws IOException if an I/O error occurs when merging
   */
  CloseableRandomVectorScorerSupplier getRandomVectorScorerSupplierForMerge(
      FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException;
}
