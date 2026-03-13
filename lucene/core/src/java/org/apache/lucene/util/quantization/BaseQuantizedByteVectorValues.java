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

import java.io.IOException;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;

/**
 * A version of {@link ByteVectorValues} for Scalar quantization scores.
 *
 * @lucene.experimental
 */
public abstract class BaseQuantizedByteVectorValues extends ByteVectorValues
    implements HasIndexSlice {

  /**
   * Return a {@link VectorScorer} for the given query vector.
   *
   * @param query the query vector
   * @return a {@link VectorScorer} instance or null
   */
  public VectorScorer scorer(float[] query) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput getSlice() {
    return null;
  }
}
