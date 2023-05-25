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
import java.util.Map;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * An implementation for retrieving {@link FunctionValues} instances for float knn vectors fields.
 */
public class FloatVectorFieldSource extends ValueSource {
  private final String fieldName;

  public FloatVectorFieldSource(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {

    final FloatVectorValues vectorValues = readerContext.reader().getFloatVectorValues(fieldName);

    if (vectorValues == null) {
      throw new IllegalArgumentException(
          "no float vector value is indexed for field '" + fieldName + "'");
    }
    return new VectorFieldFunction(this) {

      @Override
      public float[] floatVectorVal(int doc) throws IOException {
        if (exists(doc)) {
          return vectorValues.vectorValue();
        } else {
          return null;
        }
      }

      @Override
      protected DocIdSetIterator getVectorIterator() {
        return vectorValues;
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != FloatVectorFieldSource.class) return false;
    FloatVectorFieldSource other = (FloatVectorFieldSource) o;
    return fieldName.equals(other.fieldName);
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() * 31 + fieldName.getClass().hashCode();
  }

  @Override
  public String description() {
    return "denseFloatVectorField(" + fieldName + ")";
  }
}
