package org.apache.lucene.util.hnsw;

import org.apache.lucene.index.VectorSimilarityFunction;


public class ByteVectorScoringFunction extends ScoringFunction{
    private final byte[] _vector;
    private final byte[] _neighborVector;
    public ByteVectorScoringFunction(byte[] vector, byte[] neighborVector, VectorSimilarityFunction similarityFunction) {
        super(similarityFunction);
        _vector = vector;
        _neighborVector = neighborVector;
    }

    public float calculateScore() {
        return _similarityFunction.compare(_vector, _neighborVector);
    }
}
