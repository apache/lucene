package org.apache.lucene.codecs.compressing;

import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;

public class TestLZ4WithPresetDictCompressionMode extends AbstractTestCompressionMode{

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mode = new LZ4WithPresetDictCompressionMode();
    }
}
