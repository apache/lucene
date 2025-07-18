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

package org.apache.lucene.sandbox.codecs.jvector;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;

/**
 * A VectorScorer that computes similarity scores between a target query vector and document vectors
 * using JVector's float vector representation. Uses a VectorSimilarityFunction to compare the
 * target vector with each document vector, and iterates over candidates via the associated
 * DocIndexIterator.
 */
public class JVectorVectorScorer implements VectorScorer {
  private final JVectorFloatVectorValues floatVectorValues;
  private final KnnVectorValues.DocIndexIterator docIndexIterator;
  private final VectorFloat<?> target;
  private final VectorSimilarityFunction similarityFunction;

  public JVectorVectorScorer(
      JVectorFloatVectorValues vectorValues,
      VectorFloat<?> target,
      VectorSimilarityFunction similarityFunction) {
    this.floatVectorValues = vectorValues;
    this.docIndexIterator = floatVectorValues.iterator();
    this.target = target;
    this.similarityFunction = similarityFunction;
  }

  @Override
  public float score() throws IOException {
    return similarityFunction.compare(
        target, floatVectorValues.vectorFloatValue(docIndexIterator.index()));
  }

  @Override
  public DocIdSetIterator iterator() {
    return docIndexIterator;
  }
}
