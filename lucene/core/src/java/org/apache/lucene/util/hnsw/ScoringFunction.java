package org.apache.lucene.util.hnsw;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Stores a VectorSimilarityFunction that evaluates the score between a vector and a neighborVector
 * at a later time.
 */
public abstract class ScoringFunction {
    protected final VectorSimilarityFunction _similarityFunction;

    public ScoringFunction(VectorSimilarityFunction similarityFunction) {
        _similarityFunction = similarityFunction;
    }

    public abstract float calculateScore();
}
