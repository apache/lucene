package org.apache.lucene.util.hnsw;

import org.apache.lucene.index.VectorSimilarityFunction;


public class FloatVectorScoringFunction extends ScoringFunction{
    private final float[] _vector;
    private final float[] _neighborVector;
    public FloatVectorScoringFunction(float[] vector, float[] neighborVector, VectorSimilarityFunction similarityFunction) {
        super(similarityFunction);
        _vector = vector;
        _neighborVector = neighborVector;
    }
    public float calculateScore() {
        return _similarityFunction.compare(_vector, _neighborVector);
    }
}
