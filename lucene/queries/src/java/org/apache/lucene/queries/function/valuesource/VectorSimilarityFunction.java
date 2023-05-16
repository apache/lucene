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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

public abstract class VectorSimilarityFunction extends ValueSource {

  protected final org.apache.lucene.index.VectorSimilarityFunction similarityFunction;
  protected final ValueSource vector1;
  protected final ValueSource vector2;

  public VectorSimilarityFunction(
          org.apache.lucene.index.VectorSimilarityFunction similarityFunction, ValueSource vector1, ValueSource vector2) {

    this.similarityFunction = similarityFunction;
    this.vector1 = vector1;
    this.vector2 = vector2;
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {

    final FunctionValues vector1Vals = vector1.getValues(context, readerContext);
    final FunctionValues vector2Vals = vector2.getValues(context, readerContext);
    return new FunctionValues() {
      @Override
      public float floatVal(int doc) throws IOException {
        return func(doc, vector1Vals, vector2Vals);
      }

      @Override
      public String strVal(int doc) throws IOException {
        return Float.toString(floatVal(doc));
      }

      @Override
      public boolean exists(int doc) throws IOException {
        return MultiFunction.allExists(doc, vector1Vals, vector2Vals);
      }

      @Override
      public String toString(int doc) throws IOException {
        return description() + " = " + strVal(doc);
      }
    };
  }

  protected abstract float func(int doc, FunctionValues f1, FunctionValues f2) throws IOException;

  @Override
  public boolean equals(Object o) {
    return o instanceof VectorSimilarityFunction
        && similarityFunction.equals(((VectorSimilarityFunction) o).similarityFunction)
        && vector1.equals(((VectorSimilarityFunction) o).vector1)
        && vector2.equals(((VectorSimilarityFunction) o).vector2);
  }

  @Override
  public int hashCode() {
    int h = similarityFunction.hashCode();
    h = 31 * h + vector1.hashCode();
    h = 31 * h + vector2.hashCode();
    return h;
  }

  @Override
  public String description() {
    return similarityFunction.name()
        + "("
        + vector1.description()
        + ", "
        + vector2.description()
        + ")";
  }
}
