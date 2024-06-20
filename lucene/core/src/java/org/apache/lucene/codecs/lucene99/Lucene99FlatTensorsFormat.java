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

package org.apache.lucene.codecs.lucene99;

import org.apache.lucene.codecs.hnsw.FlatTensorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Lucene 9.9 flat vector format, which encodes numeric tensor values
 *
 * <h2>.tec (tensor data) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li>Tensor data ordered by field, and document ordinal. All vector values in a single tensor are
 *       concatenated and written as a single array. All vectors in a tensor have the same dimension.
 *       When vectorEncoding is BYTE, each sample is stored as a single byte. When it is FLOAT32, each
 *       sample is stored as an IEEE float in little-endian byte order.
 *   <li>DocIds encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)},
 *       note that only in sparse case
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 *   <li>TensorOffsets encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}. Since tensors
 *   can have variable number of vectors, tensor data has variable length slices. TensorOffsets holds the
 *   start and end offset for each tensor value. Splitting this slice by dimension should produce the individual
 *   vector values for a tensor.
 * </ul>
 *
 * <h2>.tem (tensor metadata) file</h2>
 *
 * <p>For each field:
 *
 * <ul>
 *   <li><b>[int32]</b> field number
 *   <li><b>[int32]</b> tensor encoding ordinal
 *   <li><b>[int32]</b> tensor similarity function ordinal
 *   <li><b>[vlong]</b> start offset to this field's tensor data in the .tec file
 *   <li><b>[vlong]</b> length of this field's tensor data, in bytes
 *   <li><b>[vint]</b> dimension of this field's vectors that compose the tensor
 *   <li><b>[int]</b> the number of documents having values for this field
 *   <li><b>[int8]</b> if equals to -1, dense – all documents have values for a field. If equals to
 *       0, sparse – some documents missing values.
 *   <li>DocIds were encoded by {@link IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput, byte)}
 *   <li>OrdToDoc was encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}, note
 *       that only in sparse case
 *   <li>TensorOffsets encoded by {@link org.apache.lucene.util.packed.DirectMonotonicWriter}. Since tensors
 *   can have variable number of vectors, tensor data has variable length slices. TensorOffsets holds the
 *   start and end offset for each tensor value. Splitting this slice by dimension should produce the individual
 *   vector values for a tensor.
 * </ul>
 *
 * @lucene.experimental
 */
public final class Lucene99FlatTensorsFormat extends FlatVectorsFormat {

  static final String META_CODEC_NAME = "Lucene99FlatTensorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene99FlatTensorsFormatData";
  static final String META_EXTENSION = "tem";
  static final String VECTOR_DATA_EXTENSION = "tec";

  public static final int VERSION_START = 0;
  public static final int VERSION_CURRENT = VERSION_START;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
  private final FlatTensorsScorer tensorsScorer;

  /** Constructs a format */
  public Lucene99FlatTensorsFormat(FlatTensorsScorer tensorsScorer) {
    this.tensorsScorer = tensorsScorer;
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return new Lucene99FlatTensorsWriter(state, tensorsScorer);
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return new Lucene99FlatVectorsReader(state, tensorsScorer);
  }

  @Override
  public String toString() {
    return "Lucene99FlatVectorsFormat(" + "vectorsScorer=" + tensorsScorer + ')';
  }
}
