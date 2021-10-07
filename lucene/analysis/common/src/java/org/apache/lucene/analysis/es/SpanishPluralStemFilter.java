package org.apache.lucene.analysis.es;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

import java.io.IOException;
/**
 * A {@link TokenFilter} that applies {@link SpanishPluralStemmer} to stem Spanish words.
 *
 * <p>To prevent terms from being stemmed use an instance of {@link SetKeywordMarkerFilter} or a
 * custom {@link TokenFilter} that sets the {@link KeywordAttribute} before this {@link
 * TokenStream}.
 */
public final class SpanishPluralStemFilter extends TokenFilter {
  private final SpanishPluralStemmer stemmer = new SpanishPluralStemmer();
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  public SpanishPluralStemFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAttr.isKeyword()) {
        final int newlen = stemmer.stem(termAtt.buffer(), termAtt.length());
        termAtt.setLength(newlen);
      }
      return true;
    } else {
      return false;
    }
  }
}
