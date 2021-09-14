/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.completion.KatakanaRomanizer;
import org.apache.lucene.analysis.ja.completion.StringUtils;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that adds Japanese romanized tokens to the term
 * attribute. Also, this keeps original tokens (surace forms).
 *
 * <p>Supported romanization form: (modified) Hepburn-shiki, Kunrei-shiki (Nihon-shiki) and Wāpuro
 * shiki.
 *
 * <p>This does NOT support romanji forms which are official but not used with commonly used
 * Japanese <a href="https://en.wikipedia.org/wiki/Input_method">Input Methods</a>. e.g.: circumflex
 * or macron representing <a href="https://en.wikipedia.org/wiki/Ch%C5%8Donpu">Chōonpu (長音符)</a> are
 * not supported.
 *
 * <p>The romanization behaviour changes according to its {@link Mode}. The default mode is {@link
 * Mode#INDEX}.
 */
public final class JapaneseCompletionFilter extends TokenFilter {
  public static final Mode DEFAULT_MODE = Mode.INDEX;

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final ReadingAttribute readingAttr = addAttribute(ReadingAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);

  private final CompletionTokenGenerator tokenGenerator;

  /** Completion mode */
  public enum Mode {
    /** Simple romanization. Expected to be used when indexing. */
    INDEX,
    /** Input Method aware romanization. Expected to be used when querying. */
    QUERY
  }

  /** Creates a new {@code JapaneseCompletionFilter} with default configurations */
  public JapaneseCompletionFilter(TokenStream input) {
    this(input, DEFAULT_MODE);
  }

  /** Creates a new {@code JapaneseCompletionFilter} */
  public JapaneseCompletionFilter(TokenStream input, Mode mode) {
    super(input);
    this.tokenGenerator = new CompletionTokenGenerator(mode);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    tokenGenerator.reset();
  }

  @Override
  public boolean incrementToken() throws IOException {
    mayIncrementToken();
    if (tokenGenerator.hasNext()) {
      clearAttributes();
      CompletionToken token = tokenGenerator.next();
      termAttr.setEmpty().append(token.term);
      if (token.isFirst) {
        posIncAtt.setPositionIncrement(1);
      } else {
        posIncAtt.setPositionIncrement(0);
      }
      // TODO: correct offsets.

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
          // use the surface form as reading when possible.
          reading = StringUtils.toKatakana(surface);
        }
        tokenGenerator.addToken(surface, reading);
      } else {
        if (tokenGenerator.hasPendingToken()) {
          // a pending token remains.
          tokenGenerator.finish();
        } else {
          // already consumed all tokens. there's no next token to output.
          break;
        }
      }
    }
  }

  private static class CompletionToken {
    final String term;
    final boolean isFirst;

    CompletionToken(String term, boolean isFirst) {
      this.term = term;
      this.isFirst = isFirst;
    }
  }

  private static class CompletionTokenGenerator implements Iterator<CompletionToken> {

    private final Mode mode;

    private List<CompletionToken> outputs;

    private String pdgSurface;
    private String pdgReading;

    CompletionTokenGenerator(Mode mode) {
      this.mode = mode;
      outputs = new ArrayList<>();
    }

    public void reset() {
      clearPendingToken();
      outputs.clear();
    }

    @Override
    public boolean hasNext() {
      return outputs.size() > 0;
    }

    @Override
    public CompletionToken next() {
      return outputs.remove(0);
    }

    void addToken(String surface, String reading) {
      assert surface != null : "surface must not be null.";

      if (hasPendingToken()) {
        if (mode == Mode.QUERY && pdgReading != null && StringUtils.isLowercaseAlphabets(surface)) {
          // words that are in mid-IME composition are split into two tokens by JapaneseTokenizer;
          // should be recovered when querying.
          // Note: in this case, the reading attribute is null; use the surface form in place of the
          // reading.
          // e.g.: "サッ" + "k" => "サッk", "反" + "sy" => "反sy"
          generateOutputs(pdgSurface + surface, pdgReading + surface);
          clearPendingToken();
        } else if (mode == Mode.QUERY
            && StringUtils.isKana(pdgSurface)
            && StringUtils.isKana(surface)) {
          // words that are all composed only of Katakana or Hiragana should be concatenated when
          // querying.
          // e.g.: "こい" + "ぬ" => "こいぬ"
          pdgSurface = StringUtils.toKatakana(pdgSurface + surface);
          pdgReading = pdgReading + reading;
        } else {
          generateOutputs(pdgSurface, pdgReading);
          pdgSurface = surface;
          pdgReading = reading;
        }
      } else {
        pdgSurface = surface;
        pdgReading = reading;
      }
    }

    void finish() {
      generateOutputs(pdgSurface, pdgReading);
      clearPendingToken();
    }

    private void generateOutputs(String surface, String reading) {
      // preserve original surface form as an output.
      outputs.add(new CompletionToken(surface, true));
      // skip readings that cannot be translated to romaji.
      if (reading == null || reading.isEmpty() || !StringUtils.isKatakanaOrHWAlphabets(reading)) {
        return;
      }
      // translate the reading to romaji.
      List<CharsRef> romaji = KatakanaRomanizer.getInstance().romanize(new CharsRef(reading));
      for (CharsRef ref : romaji) {
        outputs.add(new CompletionToken(ref.toString(), false));
      }
    }

    boolean hasPendingToken() {
      return pdgSurface != null && !pdgSurface.isEmpty();
    }

    void clearPendingToken() {
      this.pdgSurface = null;
      this.pdgReading = null;
    }
  }
}
