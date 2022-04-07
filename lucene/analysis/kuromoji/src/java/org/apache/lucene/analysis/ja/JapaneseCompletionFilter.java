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
import org.apache.lucene.analysis.ja.completion.CharSequenceUtils;
import org.apache.lucene.analysis.ja.completion.KatakanaRomanizer;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IgnoreRandomChains;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that adds Japanese romanized tokens to the term
 * attribute. Also, this keeps original tokens (surface forms). Main usage of this filter is Query
 * Auto-Completion.
 *
 * <p>Supported romanization systems: (modified) Hepburn-shiki, Kunrei-shiki (Nihon-shiki) and
 * Wāpuro shiki.
 *
 * <p>This does not strictly comply with the romanization systems listed above, but tries to cover
 * possible keystroke supported by various <a
 * href="https://en.wikipedia.org/wiki/Input_method">Input Methods</a>. e.g.: Circumflex / Macron
 * representing <a href="https://en.wikipedia.org/wiki/Ch%C5%8Donpu">Chōonpu (長音符)</a> are not
 * supported.
 *
 * <p>The romanization behaviour changes according to its {@link Mode}. The default mode is {@link
 * Mode#INDEX}.
 *
 * <p>Note: This filter must be applied AFTER half-width and full-width normalization. Please ensure
 * that a width normalizer such as {@link org.apache.lucene.analysis.cjk.CJKWidthCharFilter} or
 * {@link org.apache.lucene.analysis.cjk.CJKWidthFilter} is included in your analysis chain. IF THE
 * WIDTH NORMALIZATION IS NOT PERFORMED, THIS DOES NOT WORK AS EXPECTED. See also: {@link
 * JapaneseCompletionAnalyzer}.
 */
@IgnoreRandomChains(reason = "LUCENE-10363: fails with incorrect offsets")
public final class JapaneseCompletionFilter extends TokenFilter {
  public static final Mode DEFAULT_MODE = Mode.INDEX;

  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final ReadingAttribute readingAttr = addAttribute(ReadingAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final CompletionTokenGenerator tokenGenerator;

  private boolean inputStreamConsumed = false;

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
    inputStreamConsumed = false;
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
      offsetAtt.setOffset(token.startOffset, token.endOffset);
      return true;
    } else {
      return false;
    }
  }

  private void mayIncrementToken() throws IOException {
    while (!tokenGenerator.hasNext()) {
      if (!inputStreamConsumed && input.incrementToken()) {
        String surface = termAttr.toString();
        String reading = readingAttr.getReading();
        int startOffset = offsetAtt.startOffset();
        int endOffset = offsetAtt.endOffset();
        if (reading == null && CharSequenceUtils.isKana(surface)) {
          // use the surface form as reading when possible.
          reading = CharSequenceUtils.toKatakana(surface);
        }
        tokenGenerator.addToken(surface, reading, startOffset, endOffset);
      } else {
        inputStreamConsumed = true;
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
    final int startOffset;
    final int endOffset;

    CompletionToken(String term, boolean isFirst, int startOffset, int endOffset) {
      this.term = term;
      this.isFirst = isFirst;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }

  private static class CompletionTokenGenerator implements Iterator<CompletionToken> {

    private final Mode mode;

    private List<CompletionToken> outputs;

    private CharsRefBuilder pdgSurface;
    private CharsRefBuilder pdgReading;
    private int pdgStartOffset;
    private int pdgEndOffset;

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

    void addToken(String surface, String reading, int startOffset, int endOffset) {
      assert surface != null : "surface must not be null.";

      if (hasPendingToken()) {
        if (mode == Mode.QUERY
            && pdgReading != null
            && !CharSequenceUtils.isLowercaseAlphabets(pdgSurface.get())
            && CharSequenceUtils.isLowercaseAlphabets(surface)) {
          // words that are in mid-IME composition are split into two tokens by JapaneseTokenizer;
          // should be recovered when querying.
          // Note: in this case, the reading attribute is null; use the surface form in place of the
          // reading.
          // e.g.: "サッ" + "k" => "サッk", "反" + "sy" => "反sy"
          pdgSurface.append(surface);
          pdgReading.append(surface);
          pdgEndOffset = endOffset;
          generateOutputs();
          clearPendingToken();
        } else if (mode == Mode.QUERY
            && CharSequenceUtils.isKana(pdgSurface.get())
            && CharSequenceUtils.isKana(surface)) {
          // words that are all composed only of Katakana or Hiragana should be concatenated when
          // querying.
          // e.g.: "こい" + "ぬ" => "こいぬ"
          pdgSurface.append(surface);
          pdgReading.append(reading);
          pdgEndOffset = endOffset;
        } else {
          generateOutputs();
          resetPendingToken(surface, reading, startOffset, endOffset);
        }
      } else {
        resetPendingToken(surface, reading, startOffset, endOffset);
      }
    }

    void finish() {
      generateOutputs();
      clearPendingToken();
    }

    private void generateOutputs() {
      // preserve original surface form as an output.
      outputs.add(new CompletionToken(pdgSurface.toString(), true, pdgStartOffset, pdgEndOffset));
      // skip readings that cannot be translated to romaji.
      if (pdgReading == null
          || pdgReading.length() == 0
          || !CharSequenceUtils.isKatakanaOrHWAlphabets(pdgReading.get())) {
        return;
      }
      // translate the reading to romaji.
      List<CharsRef> romaji = KatakanaRomanizer.getInstance().romanize(pdgReading.get());
      for (CharsRef ref : romaji) {
        // set the same start/end offset as the original surface form for romanized tokens.
        outputs.add(new CompletionToken(ref.toString(), false, pdgStartOffset, pdgEndOffset));
      }
    }

    boolean hasPendingToken() {
      return pdgSurface != null;
    }

    void resetPendingToken(
        CharSequence surface, CharSequence reading, int startOffset, int endOffset) {
      if (this.pdgSurface == null) {
        this.pdgSurface = new CharsRefBuilder();
      } else {
        this.pdgSurface.clear();
      }
      this.pdgSurface.append(surface);
      if (this.pdgReading == null) {
        this.pdgReading = new CharsRefBuilder();
      } else {
        this.pdgReading.clear();
      }
      this.pdgReading.append(reading);
      this.pdgStartOffset = startOffset;
      this.pdgEndOffset = endOffset;
    }

    void clearPendingToken() {
      this.pdgSurface = null;
      this.pdgReading = null;
      this.pdgStartOffset = 0;
      this.pdgEndOffset = 0;
    }
  }
}
