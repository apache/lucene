package org.apache.lucene.analysis.ja;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.analysis.MockTokenizer;

/** Tests for {@link JapaneseHiraganaUppercaseFilter} */
public class TestJapaneseHiraganaUppercaseFilter extends BaseTokenStreamTestCase {
  private Analyzer keywordAnalyzer, japaneseAnalyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    keywordAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
            return new TokenStreamComponents(
                tokenizer, new JapaneseHiraganaUppercaseFilter(tokenizer));
          }
        };
    japaneseAnalyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer =
                new JapaneseTokenizer(
                    newAttributeFactory(), null, false, JapaneseTokenizer.Mode.SEARCH);
            return new TokenStreamComponents(
                tokenizer, new JapaneseHiraganaUppercaseFilter(tokenizer));
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    keywordAnalyzer.close();
    japaneseAnalyzer.close();
    super.tearDown();
  }

  public void testKanaUppercase() throws IOException {
    assertAnalyzesTo(keywordAnalyzer, "ぁぃぅぇぉっゃゅょゎゕゖ", new String[] {"あいうえおつやゆよわかけ"});
    assertAnalyzesTo(keywordAnalyzer, "ちょっとまって", new String[] {"ちよつとまつて"});
  }

  public void testKanaUppercaseWithSurrogatePair() throws IOException {
    // 𠀋 : \uD840\uDC0B
    assertAnalyzesTo(
        keywordAnalyzer,
        "\uD840\uDC0Bちょっとまって ちょっと\uD840\uDC0Bまって ちょっとまって\uD840\uDC0B",
        new String[] {"\uD840\uDC0Bちよつとまつて", "ちよつと\uD840\uDC0Bまつて", "ちよつとまつて\uD840\uDC0B"});
  }

  public void testKanaUppercaseWithJapaneseTokenizer() throws IOException {
    assertAnalyzesTo(japaneseAnalyzer, "ちょっとまって", new String[] {"ちよつと", "まつ", "て"});
  }

  public void testRandomData() throws IOException {
    checkRandomData(random(), keywordAnalyzer, 200 * RANDOM_MULTIPLIER);
  }

  public void testEmptyTerm() throws IOException {
    assertAnalyzesTo(keywordAnalyzer, "", new String[] {});
  }
}
