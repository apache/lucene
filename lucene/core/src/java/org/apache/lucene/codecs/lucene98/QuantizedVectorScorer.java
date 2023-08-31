package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;


public interface QuantizedVectorScorer extends VectorScorer {

    static QuantizedVectorScorer fromFieldEntry(
            Lucene98ScalarQuantizedVectorsReader.FieldEntry fieldEntry,
            OffHeapQuantizedByteVectorValues values,
            float[] query
    ) {
        float[] processedQuery = switch (fieldEntry.similarityFunction) {
            case EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> query;
            case COSINE -> {
                float[] queryCopy =  ArrayUtil.copyOfSubArray(query, 0, query.length);
                VectorUtil.l2normalize(queryCopy);
                yield queryCopy;
            }
        };
        final ScalarQuantizer scalarQuantizer = fieldEntry.scalarQuantizer;
        final byte[] quantizedQuery = scalarQuantizer.quantize(processedQuery);
        final float queryScoreCorrection = ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
                fieldEntry.similarityFunction,
                quantizedQuery,
                scalarQuantizer.getAlpha(),
                scalarQuantizer.getOffset()
        );
        final float globalOffsetCorrection = switch (fieldEntry.similarityFunction) {
            case EUCLIDEAN -> 0f;
            case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> fieldEntry.dimension * scalarQuantizer.getOffset() * scalarQuantizer.getOffset();
        };
        final float correctiveMultiplier = scalarQuantizer.getAlpha() * scalarQuantizer.getAlpha();
        ScalarQuantizedVectorSimilarity similarity = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
                fieldEntry.similarityFunction,
                correctiveMultiplier
        );
        return vectorOrdinal -> {
            byte[] storedVectorValue = values.vectorValue(vectorOrdinal);
            float storedVectorCorrection = values.scoreCorrectionConstant + globalOffsetCorrection;
            return similarity.score(quantizedQuery, queryScoreCorrection, storedVectorValue, storedVectorCorrection);
        };
    }


}
