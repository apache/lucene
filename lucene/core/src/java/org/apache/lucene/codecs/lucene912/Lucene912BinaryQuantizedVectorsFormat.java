package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class Lucene912BinaryQuantizedVectorsFormat  extends FlatVectorsFormat {

  public static final String BINARIZED_VECTOR_COMPONENT = "BVEC";
  public static final String NAME = "Lucene912BinaryQuantizedVectorsFormat";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final String META_CODEC_NAME = "Lucene912BinaryQuantizedVectorsFormatMeta";
  static final String VECTOR_DATA_CODEC_NAME = "Lucene912BinaryQuantizedVectorsFormatData";
  static final String META_EXTENSION = "vemb";
  static final String VECTOR_DATA_EXTENSION = "veb";

  private static final FlatVectorsFormat rawVectorFormat =
    new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  /**
   * Sole constructor
   */
  protected Lucene912BinaryQuantizedVectorsFormat() {
    super(NAME);
  }

  @Override
  public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
    return null;
  }

  @Override
  public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
    return null;
  }
}
