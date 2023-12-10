package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

import java.io.IOException;


/**
 * Tests for {@link JapaneseKanaUppercaseFilter}
 */
public class TestJapaneseKanaUppercaseFilter extends BaseTokenStreamTestCase {
    private Analyzer analyzer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                // Use a MockTokenizer here since this filter doesn't really depend on Kuromoji
                Tokenizer source = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(source, new JapaneseKanaUppercaseFilter(source));
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        analyzer.close();
        super.tearDown();
    }

    public void testKanaUppercase() throws IOException {
        assertAnalyzesTo(analyzer,
                "ストップウォッチ しょうじょ",
                new String[]{"ストツプウオツチ", "しようじよ"});
    }

}
