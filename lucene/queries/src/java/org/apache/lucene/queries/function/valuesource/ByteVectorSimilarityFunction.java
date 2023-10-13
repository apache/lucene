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
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * <code>ByteVectorSimilarityFunction</code> returns a similarity function between two knn vectors
 * with byte elements.
 */
public class ByteVectorSimilarityFunction extends VectorSimilarityFunction {
  public ByteVectorSimilarityFunction(
      org.apache.lucene.index.VectorSimilarityFunction similarityFunction,
      ValueSource vector1,
      ValueSource vector2) {
    super(similarityFunction, vector1, vector2);
  }

  @Override
  protected float func(int doc, FunctionValues f1, FunctionValues f2) throws IOException {

    var v1 = f1.byteVectorVal(doc);
    var v2 = f2.byteVectorVal(doc);

    if (v1 == null || v2 == null) {
      return 0.f;
    }

    assert v1.length == v2.length : "Vectors must have the same length";

    return similarityFunction.compare(v1, v2);
  }
}
