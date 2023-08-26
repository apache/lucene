package org.apache.lucene.codecs.lucene98;

import java.io.IOException;

public interface VectorScorer {
    float score(int vectorOrdinal) throws IOException;
}
