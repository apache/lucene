package org.apache.lucene.codecs.compressing;

import org.apache.lucene.codecs.lucene90.DeflateWithPresetDictCompressionMode;

public class TestDeflateWithPresetDictCompressionMode extends AbstractTestCompressionMode {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mode = new DeflateWithPresetDictCompressionMode();
  }
}
