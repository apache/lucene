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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/** Function that returns a constant byte vector value for every document. */
public class ConstKnnByteVectorValueSource extends ValueSource {
  private final byte[] vector;

  public ConstKnnByteVectorValueSource(byte[] constVector) {
    this.vector = Objects.requireNonNull(constVector, "constVector");
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    return new FunctionValues() {
      @Override
      public byte[] byteVectorVal(int doc) {
        return vector;
      }

      @Override
      public String strVal(int doc) {
        return Arrays.toString(vector);
      }

      @Override
      public String toString(int doc) throws IOException {
        return description() + '=' + strVal(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConstKnnByteVectorValueSource other = (ConstKnnByteVectorValueSource) o;
    return Arrays.equals(vector, other.vector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().hashCode(), Arrays.hashCode(vector));
  }

  @Override
  public String description() {
    return "ConstKnnByteVectorValueSource(" + Arrays.toString(vector) + ')';
  }
}
