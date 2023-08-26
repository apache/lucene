package org.apache.lucene.codecs.lucene98;

import java.io.IOException;

public interface VectorScorerSupplier {
    VectorScorer vectorScorer(int vectorOrdinal) throws IOException;

    int numVectors();
}
