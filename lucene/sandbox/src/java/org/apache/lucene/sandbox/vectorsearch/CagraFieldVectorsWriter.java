package org.apache.lucene.sandbox.vectorsearch;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.index.FieldInfo;

public class CagraFieldVectorsWriter extends KnnFieldVectorsWriter<float[]> {

  public final String fieldName;
  public final ConcurrentHashMap<Integer, float[]> vectors = new ConcurrentHashMap<Integer, float[]>();
  public int fieldVectorDimension = -1;

  public CagraFieldVectorsWriter(FieldInfo fieldInfo) {
    this.fieldName = fieldInfo.getName();
    this.fieldVectorDimension = fieldInfo.getVectorDimension();
  }

  @Override
  public long ramBytesUsed() {
    return fieldName.getBytes().length + Integer.BYTES + (vectors.size() * fieldVectorDimension * Float.BYTES);
  }

  @Override
  public void addValue(int docID, float[] vectorValue) throws IOException {
    vectors.put(docID, vectorValue);
  }

  @Override
  public float[] copyValue(float[] vectorValue) {
    throw new UnsupportedOperationException();
  }

}
