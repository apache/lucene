package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

import java.io.IOException;


/**
 * Tests for {@link JapaneseKanaUppercaseFilter}
 */
public class TestJapaneseKanaUppercaseFilter extends BaseTokenStreamTestCase {
    private Analyzer keywordAnalyzer, japaneseAnalyzer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keywordAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
                return new TokenStreamComponents(tokenizer, new JapaneseKanaUppercaseFilter(tokenizer));
            }
        };
        japaneseAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new JapaneseTokenizer(newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
                return new TokenStreamComponents(tokenizer, new JapaneseKanaUppercaseFilter(tokenizer));
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        keywordAnalyzer.close();
        keywordAnalyzer.close();
        super.tearDown();
    }

    public void testKanaUppercase() throws IOException {
        assertAnalyzesTo(
                keywordAnalyzer,
                "ぁぃぅぇぉっゃゅょゎゕゖァィゥェォヵㇰヶㇱㇲッㇳㇴㇵㇶㇷㇷ゚ㇸㇹㇺャュョㇻㇼㇽㇾㇿヮ",
                new String[]{"あいうえおつやゆよわかけアイウエオカクケシスツトヌハヒフプヘホムヤユヨラリルレロワ"}
        );
        assertAnalyzesTo(
                keywordAnalyzer,
                "ストップウォッチ しょうじょ",
                new String[]{"ストツプウオツチ", "しようじよ"}
        );
        assertAnalyzesTo(
                keywordAnalyzer,
                "サラニㇷ゚ カムイチェㇷ゚ ㇷ゚ㇷ゚",
                new String[]{"サラニプ", "カムイチエプ", "ププ"}
        );
    }

    public void testKanaUppercaseWithJapaneseTokenizer() throws IOException {
        assertAnalyzesTo(japaneseAnalyzer, "時間をストップウォッチで測る", new String[]{"時間", "を", "ストツプウオツチ", "で", "測る"});
    }

    public void testUnsupportedHalfWidthVariants() throws IOException {
        // The below result is expected since only full-width katakana is supported
        assertAnalyzesTo(keywordAnalyzer, "ｽﾄｯﾌﾟｳｫｯﾁ", new String[]{"ｽﾄｯﾌﾟｳｫｯﾁ"});
    }

    public void testRandomData() throws IOException {
        checkRandomData(random(), keywordAnalyzer, 200 * RANDOM_MULTIPLIER);
    }

    public void testEmptyTerm() throws IOException {
        assertAnalyzesTo(keywordAnalyzer, "", new String[]{});
    }

}
