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

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.util.Bits.MatchAllBits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;

/// Implements Lucene vector access over a JVector on-disk index
public class JVectorFloatVectorValues extends FloatVectorValues {
  private static final VectorTypeSupport VECTOR_TYPE_SUPPORT =
      VectorizationProvider.getInstance().getVectorTypeSupport();

  private final OnDiskGraphIndex.View view;
  private final PQVectors pq;
  private final VectorSimilarityFunction similarityFunction;
  private final GraphNodeIdToDocMap graphNodeIdToDocMap;

  public JVectorFloatVectorValues(
      OnDiskGraphIndex onDiskGraphIndex,
      PQVectors pq,
      VectorSimilarityFunction similarityFunction,
      GraphNodeIdToDocMap graphNodeIdToDocMap)
      throws IOException {
    this.view = onDiskGraphIndex.getView();
    this.pq = pq;
    this.similarityFunction = similarityFunction;
    this.graphNodeIdToDocMap = graphNodeIdToDocMap;
  }

  @Override
  public int dimension() {
    return view.dimension();
  }

  @Override
  public int size() {
    return view.size();
  }

  @Override
  public int ordToDoc(int ord) {
    return graphNodeIdToDocMap.getLuceneDocId(ord);
  }

  // This allows us to access the vector without copying it to float[]
  public VectorFloat<?> vectorFloatValue(int ord) {
    return view.getVector(ord);
  }

  public void getVectorInto(int node, VectorFloat<?> vector, int offset) {
    view.getVectorInto(node, vector, offset);
  }

  @Override
  public DocIndexIterator iterator() {
    assert view.liveNodes() instanceof MatchAllBits : "All OnDiskGraphIndex nodes must be live";
    return graphNodeIdToDocMap.iterator();
  }

  @Override
  public float[] vectorValue(int i) throws IOException {
    try {
      final VectorFloat<?> vector = vectorFloatValue(i);
      return (float[]) vector.get();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FloatVectorValues copy() throws IOException {
    return this;
  }

  @Override
  public VectorScorer scorer(float[] query) throws IOException {
    if (pq != null) {
      final var vector = VECTOR_TYPE_SUPPORT.createFloatVector(query);
      final var quantizedScoreFunction = pq.precomputedScoreFunctionFor(vector, similarityFunction);
      return new JVectorScorer(quantizedScoreFunction, iterator());
    } else {
      return rescorer(query);
    }
  }

  @Override
  public VectorScorer rescorer(float[] target) throws IOException {
    final var vector = VECTOR_TYPE_SUPPORT.createFloatVector(target);
    final var scoreFunction = view.rerankerFor(vector, similarityFunction);
    return new JVectorScorer(scoreFunction, iterator());
  }

  private static class JVectorScorer implements VectorScorer {
    private final ScoreFunction scoreFunction;
    private final DocIndexIterator iterator;

    JVectorScorer(ScoreFunction scoreFunction, DocIndexIterator iterator) {
      this.scoreFunction = scoreFunction;
      this.iterator = iterator;
    }

    @Override
    public float score() throws IOException {
      return scoreFunction.similarityTo(iterator.index());
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }
  }
}
