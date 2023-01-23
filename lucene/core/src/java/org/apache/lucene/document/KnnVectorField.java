package org.apache.lucene.document;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.FloatVectorValues;

/**
 * A field that contains a single floating-point numeric vector (or none) for each document. Vectors
 * are dense - that is, every dimension of a vector contains an explicit value, stored packed into
 * an array (of type float[]) whose length is the vector dimension. Values can be retrieved using
 * {@link FloatVectorValues}, which is a forward-only docID-based iterator and also offers random-access
 * by dense ordinal (not docId). {@link VectorSimilarityFunction} may be used to compare vectors at
 * query time (for example as part of result ranking). A KnnVectorField may be associated with a
 * search similarity function defining the metric used for nearest-neighbor search among vectors of
 * that field.
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
