package org.apache.lucene.analysis.ja;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.tests.util.StringMockResourceLoader;

/** Tests for {@link JapaneseHiraganaUppercaseFilterFactory} */
public class TestJapaneseHiraganaUppercaseFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {

    Map<String, String> args = new HashMap<>();
    args.put("discardPunctuation", "false");

    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(args);

    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream tokenStream = tokenizerFactory.create(newAttributeFactory());
    ((Tokenizer) tokenStream).setReader(new StringReader("ちょっとまって"));

    JapaneseHiraganaUppercaseFilterFactory factory =
        new JapaneseHiraganaUppercaseFilterFactory(new HashMap<>());
    tokenStream = factory.create(tokenStream);
    assertTokenStreamContents(tokenStream, new String[] {"ちよつと", "まつ", "て"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () ->
                new JapaneseHiraganaUppercaseFilterFactory(
                    new HashMap<>() {
                      {
                        put("bogusArg", "bogusValue");
                      }
                    }));
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
