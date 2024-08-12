package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

import java.io.IOException;

public interface BinaryFlatVectorsScorer extends FlatVectorsScorer {

  /**
   *
   * @param similarityFunction vector similarity function
   * @param scoringVectors the vectors over which to score
   * @param targetVectors the target vectors
   * @return a {@link RandomVectorScorerSupplier} that can be used to score vectors
   * @throws IOException if an I/O error occurs
   */
  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
    VectorSimilarityFunction similarityFunction,
    RandomAccessVectorValues scoringVectors,
    RandomAccessVectorValues targetVectors
  ) throws IOException;

}
