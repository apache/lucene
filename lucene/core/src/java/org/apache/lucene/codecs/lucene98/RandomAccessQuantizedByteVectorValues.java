package org.apache.lucene.codecs.lucene98;

import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * Random access values for <code>byte[]</code>, but also includes accessing the score correction
 * constant for the current vector in the buffer.
 */
interface RandomAccessQuantizedByteVectorValues extends RandomAccessVectorValues<byte[]> {
  float getScoreCorrectionConstant();
}
