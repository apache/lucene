package org.apache.lucene.analysis.ja;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.StringMockResourceLoader;

/** Tests for {@link JapaneseKatakanaUppercaseFilterFactory} */
public class TestJapaneseKatakanaUppercaseFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {

    Map<String, String> args = new HashMap<>();
    args.put("discardPunctuation", "false");

    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(args);

    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream tokenStream = tokenizerFactory.create(newAttributeFactory());
    ((Tokenizer) tokenStream).setReader(new StringReader("ストップウォッチ"));

    JapaneseKatakanaUppercaseFilterFactory factory =
        new JapaneseKatakanaUppercaseFilterFactory(new HashMap<>());
    tokenStream = factory.create(tokenStream);
    assertTokenStreamContents(tokenStream, new String[] {"ストツプウオツチ"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                new JapaneseKatakanaUppercaseFilterFactory(
                    new HashMap<>() {
                      {
                        put("bogusArg", "bogusValue");
                      }
                    }));
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
