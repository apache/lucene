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
package org.apache.lucene.document;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * A field that contains a single floating-point numeric vector (or none) for each document. Vectors
 * are dense - that is, every dimension of a vector contains an explicit value, stored packed into
 * an array (of type float[]) whose length is the vector dimension. Values can be retrieved using
 * {@link FloatVectorValues}, which is a forward-only docID-based iterator and also offers
 * random-access by dense ordinal (not docId). {@link VectorSimilarityFunction} may be used to
 * compare vectors at query time (for example as part of result ranking). A KnnVectorField may be
 * associated with a search similarity function defining the metric used for nearest-neighbor search
 * among vectors of that field.
 *
 * @deprecated use {@link KnnFloatVectorField} instead
 */
@Deprecated
public class KnnVectorField extends KnnFloatVectorField {
  public KnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    super(name, vector, similarityFunction);
  }

  public KnnVectorField(String name, float[] vector) {
    super(name, vector);
  }

  public KnnVectorField(String name, float[] vector, FieldType fieldType) {
    super(name, vector, fieldType);
  }
}
