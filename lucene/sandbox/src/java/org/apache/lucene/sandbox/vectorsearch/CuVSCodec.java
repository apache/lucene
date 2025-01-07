package org.apache.lucene.sandbox.vectorsearch;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.sandbox.vectorsearch.CuVSVectorsWriter.MergeStrategy;


public class CuVSCodec extends FilterCodec {

  public CuVSCodec() {
    this("CuVSCodec", new Lucene101Codec());
  }

  public CuVSCodec(String name, Codec delegate) {
    super(name, delegate);
    setKnnFormat(new CuVSVectorsFormat(1, 128, 64, MergeStrategy.NON_TRIVIAL_MERGE));
  }
  
  KnnVectorsFormat knnFormat = null;

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return knnFormat;
  }
  
  public void setKnnFormat(KnnVectorsFormat format) {
    this.knnFormat = format;
  }
}