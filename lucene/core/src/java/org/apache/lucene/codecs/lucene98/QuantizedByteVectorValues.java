package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.index.ByteVectorValues;

abstract class QuantizedByteVectorValues extends ByteVectorValues {
    abstract float getScoreCorrectionConstant();
}
