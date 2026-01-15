package org.apache.lucene.codecs.spann;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.RamUsageEstimator;

/**
* Buffers vectors in memory until flush.
*
* <p>
* Future improvements could include off-heap or disk-backed buffering (e.g.
* ByteBlockPool) to
* support larger segments without significant heap pressure.
*/
public class SpannFieldVectorsWriter extends KnnFieldVectorsWriter<float[]> {
  private final FieldInfo fieldInfo;
  private final List<float[]> vectors = new ArrayList<>();
  private final List<Integer> docIds = new ArrayList<>();

  private long ramBytesUsed = 0;

  public SpannFieldVectorsWriter(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
  }

  @Override
  public void addValue(int docID, float[] vectorValue) throws IOException {
    vectors.add(vectorValue);
    docIds.add(docID);
    ramBytesUsed += RamUsageEstimator.sizeOf(vectorValue) + Integer.BYTES;
  }

  @Override
  public float[] copyValue(float[] vectorValue) {
    return vectorValue.clone();
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed
        + RamUsageEstimator.shallowSizeOf(vectors)
        + RamUsageEstimator.shallowSizeOf(docIds);
  }

  public List<float[]> getVectors() {
    return vectors;
  }

  public List<Integer> getDocIds() {
    return docIds;
  }

  public FieldInfo getFieldInfo() {
    return fieldInfo;
  }
}
