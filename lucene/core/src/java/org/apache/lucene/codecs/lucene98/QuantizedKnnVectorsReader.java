package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.codecs.KnnVectorsReader;

abstract class QuantizedKnnVectorsReader extends KnnVectorsReader {

  abstract QuantizedByteVectorValues getQuantizedVectorValues(String fieldName);

  abstract QuantizationState getQuantizationState(String fieldName);
}
