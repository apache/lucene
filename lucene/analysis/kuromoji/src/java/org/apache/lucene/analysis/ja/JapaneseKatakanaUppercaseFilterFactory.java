package org.apache.lucene.analysis.ja;

import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link JapaneseKatakanaUppercaseFilter}.
 *
 * @lucene.spi {@value #NAME}
 */
public class JapaneseKatakanaUppercaseFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "japaneseKatakanaUppercase";

  public JapaneseKatakanaUppercaseFilterFactory(Map<String, String> args) {
    super(args);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public JapaneseKatakanaUppercaseFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new JapaneseKatakanaUppercaseFilter(input);
  }
}
