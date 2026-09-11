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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.VectorUtil;

/** A view of {@link FloatVectorValues} that returns every vector normalized to unit length. */
final class NormalizedFloatVectorValues extends FloatVectorValues {
  private final FloatVectorValues values;
  private final float[] normalizedVector;

  NormalizedFloatVectorValues(FloatVectorValues values) {
    this.values = values;
    this.normalizedVector = new float[values.dimension()];
  }

  @Override
  public int dimension() {
    return values.dimension();
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public int ordToDoc(int ord) {
    return values.ordToDoc(ord);
  }

  @Override
  public float[] vectorValue(int ord) throws IOException {
    System.arraycopy(values.vectorValue(ord), 0, normalizedVector, 0, normalizedVector.length);
    VectorUtil.l2normalize(normalizedVector);
    return normalizedVector;
  }

  @Override
  public DocIndexIterator iterator() {
    return values.iterator();
  }

  @Override
  public NormalizedFloatVectorValues copy() throws IOException {
    return new NormalizedFloatVectorValues(values.copy());
  }
}
