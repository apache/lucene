package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BasePointsFormatTestCase;

public class TestLucene90PointsFormatV0 extends BasePointsFormatTestCase {

  @Override
  protected Codec getCodec() {
    return new AssertingCodec() {
      @Override
      public PointsFormat pointsFormat() {
        return new Lucene90PointsFormat(Lucene90PointsFormat.VERSION_START);
      }
    };
  }
}
