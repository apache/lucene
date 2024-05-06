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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

final class CosineByteVectorScorerSupplier extends MemorySegmentByteVectorScorerSupplier {

  CosineByteVectorScorerSupplier(
      int dims,
      int maxOrd,
      int vectorByteSize,
      MemorySegmentAccessInput input,
      RandomAccessVectorValues values) {
    super(dims, maxOrd, vectorByteSize, input, values);
  }

  @Override
  public float score(int node) throws IOException {
    float raw = PanamaVectorUtilSupport.cosine(first, getSegment(node, scratch2));
    return (1 + raw) / 2;
  }

  @Override
  public CosineByteVectorScorerSupplier copy() throws IOException {
    return new CosineByteVectorScorerSupplier(dims, maxOrd, vectorByteSize, input.clone(), values);
  }
}
