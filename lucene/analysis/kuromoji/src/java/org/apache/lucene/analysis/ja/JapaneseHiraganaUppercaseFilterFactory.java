package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

import java.util.Map;

/**
 * Factory for {@link JapaneseHiraganaUppercaseFilter}.
 *
 * @lucene.spi {@value #NAME}
 */
public class JapaneseHiraganaUppercaseFilterFactory extends TokenFilterFactory {

    /** SPI name */
    public static final String NAME = "japaneseHiraganaUppercase";

    public JapaneseHiraganaUppercaseFilterFactory(Map<String, String> args) {
        super(args);
        if (!args.isEmpty()) {
            throw new IllegalArgumentException("Unknown parameters: " + args);
        }
    }

    /** Default ctor for compatibility with SPI */
    public JapaneseHiraganaUppercaseFilterFactory() {
        throw defaultCtorException();
    }

    @Override
    public TokenStream create(TokenStream input) {
        return new JapaneseHiraganaUppercaseFilter(input);
    }
}
