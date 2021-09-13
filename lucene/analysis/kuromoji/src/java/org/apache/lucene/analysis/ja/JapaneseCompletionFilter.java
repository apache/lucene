package org.apache.lucene.analysis.ja;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.completion.StringUtils;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

import java.io.IOException;
import java.util.Iterator;

public class JapaneseCompletionFilter extends TokenFilter {
  public static final Mode DEFAULT_MODE = Mode.INDEX;

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final ReadingAttribute readingAttr = addAttribute(ReadingAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

  private final Mode mode;
  private final CompletionTokenGenerator tokenGenerator;

  /**
   * Completion mode
   */
  public enum Mode {
    /**
     * Simple romanization. Expected to be used when indexing.
     */
    INDEX,
    /**
     * Input Method aware romanization. Expected to be used when querying.
     */
    QUERY
  }

  public JapaneseCompletionFilter(TokenStream input) {
    this(input, DEFAULT_MODE);
  }

  public JapaneseCompletionFilter(TokenStream input, Mode mode) {
    super(input);
    this.mode = mode;
    this.tokenGenerator = new CompletionTokenGenerator();
  }

  @Override
  public boolean incrementToken() throws IOException {
    mayIncrementToken();
    if (tokenGenerator.hasNext()) {
      clearAttributes();
      if (tokenGenerator.isFirstToken()) {
        posIncAtt.setPositionIncrement(1);
      } else {
        posIncAtt.setPositionIncrement(0);
      }
      termAttr.setEmpty().append(tokenGenerator.next());
      return true;
    } else {
      return false;
    }
  }

  private void mayIncrementToken() throws IOException {
    while (!tokenGenerator.hasNext()) {
      if (input.incrementToken()) {
        String surface = termAttr.toString();
        String reading = readingAttr.getReading();
        if (reading == null && StringUtils.isKana(surface)) {
          reading = StringUtils.toKatakana(surface);
        }

        if (tokenGenerator.hasPendingTokens()) {

        } else {
          tokenGenerator.resetPendingTokens(surface, reading);
        }
      } else {
        if (tokenGenerator.hasPendingTokens()) {
          // there are pending tokens to be consumed.
          String pdgSurface = tokenGenerator.getPendingSurface();
          String pdgReading = tokenGenerator.getPendingReading();
          // reset token generator's state with the pending token.
          tokenGenerator.addToken(pdgSurface, pdgReading);
          tokenGenerator.resetPendingTokens(null, null);
        } else {
          // already consumed all tokens. there's no next token to output.
          break;
        }
      }
    }
  }

  private static class CompletionTokenGenerator implements Iterator<String> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public String next() {
      return null;
    }

    public boolean isFirstToken() {
      return false;
    }

    public boolean hasPendingTokens() {
      return false;
    }

    public String getPendingSurface() {
      return null;
    }

    public String getPendingReading() {
      return null;
    }

    public void addToken(String surface, String reading) {
    }

    public void resetPendingTokens(String surface, String reading) {
    }

  }
}
