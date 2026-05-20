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
import java.io.StringReader;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.dict.CharacterDefinition;
import org.apache.lucene.analysis.ja.dict.ConnectionCosts;
import org.apache.lucene.analysis.ja.dict.JaMorphData;
import org.apache.lucene.analysis.ja.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ja.dict.TokenInfoFST;
import org.apache.lucene.analysis.ja.dict.UnknownDictionary;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.InflectionAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.morph.GraphvizFormatter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IgnoreRandomChains;
import org.apache.lucene.util.fst.FST;

/**
 * Tokenizer for Japanese that uses morphological analysis.
 *
 * <p>This tokenizer sets a number of additional attributes:
 *
 * <ul>
 *   <li>{@link BaseFormAttribute} containing base form for inflected adjectives and verbs.
 *   <li>{@link PartOfSpeechAttribute} containing part-of-speech.
 *   <li>{@link ReadingAttribute} containing reading and pronunciation.
 *   <li>{@link InflectionAttribute} containing additional part-of-speech information for inflected
 *       forms.
 * </ul>
 *
 * <p>This tokenizer uses a rolling Viterbi search to find the least cost segmentation (path) of the
 * incoming characters. For tokens that appear to be compound (&gt; length 2 for all Kanji, or &gt;
 * length 7 for non-Kanji), we see if there is a 2nd best segmentation of that token after applying
 * penalties to the long tokens. If so, and the Mode is {@link Mode#SEARCH}, we output the alternate
 * segmentation as well.
 */
public final class JapaneseTokenizer extends Tokenizer {

  /** Tokenization mode: this determines how the tokenizer handles compound and unknown words. */
  public enum Mode {
    /** Ordinary segmentation: no decomposition for compounds, */
    NORMAL,

    /**
     * Segmentation geared towards search: this includes a decompounding process for long nouns,
     * also including the full compound token as a synonym.
     */
    SEARCH,

    /**
     * Extended mode outputs unigrams for unknown words.
     *
     * @lucene.experimental
     */
    EXTENDED
  }

  /** Default tokenization mode. Currently this is {@link Mode#SEARCH}. */
  public static final Mode DEFAULT_MODE = Mode.SEARCH;

  private static final boolean VERBOSE = false;

  // Position of last token we returned; we use this to
  // figure out whether to set posIncr to 0 or 1:
  private int lastTokenPos;

  private final ViterbiNBest viterbi;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final BaseFormAttribute basicFormAtt = addAttribute(BaseFormAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);
  private final InflectionAttribute inflectionAtt = addAttribute(InflectionAttribute.class);

  /**
   * Create a new JapaneseTokenizer.
   *
   * <p>Uses the default AttributeFactory.
   *
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer(UserDictionary userDictionary, boolean discardPunctuation, Mode mode) {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, userDictionary, discardPunctuation, true, mode);
  }

  /**
   * Create a new JapaneseTokenizer.
   *
   * <p>Uses the default AttributeFactory.
   *
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param discardCompoundToken true if compound tokens should be dropped from the output when
   *     tokenization mode is not NORMAL.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer(
      UserDictionary userDictionary,
      boolean discardPunctuation,
      boolean discardCompoundToken,
      Mode mode) {
    this(
        DEFAULT_TOKEN_ATTRIBUTE_FACTORY,
        userDictionary,
        discardPunctuation,
        discardCompoundToken,
        mode);
  }

  /**
   * Create a new JapaneseTokenizer using the system and unknown dictionaries shipped with Lucene.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer(
      AttributeFactory factory,
      UserDictionary userDictionary,
      boolean discardPunctuation,
      Mode mode) {
    this(
        factory,
        TokenInfoDictionary.getInstance(),
        UnknownDictionary.getInstance(),
        ConnectionCosts.getInstance(),
        userDictionary,
        discardPunctuation,
        true,
        mode);
  }

  /**
   * Create a new JapaneseTokenizer using the system and unknown dictionaries shipped with Lucene.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param discardCompoundToken true if compound tokens should be dropped from the output when
   *     tokenization mode is not NORMAL.
   * @param mode tokenization mode.
   */
  public JapaneseTokenizer(
      AttributeFactory factory,
      UserDictionary userDictionary,
      boolean discardPunctuation,
      boolean discardCompoundToken,
      Mode mode) {
    this(
        factory,
        TokenInfoDictionary.getInstance(),
        UnknownDictionary.getInstance(),
        ConnectionCosts.getInstance(),
        userDictionary,
        discardPunctuation,
        discardCompoundToken,
        mode);
  }

  /**
   * Create a new JapaneseTokenizer, supplying a custom system dictionary and unknown dictionary.
   * This constructor provides an entry point for users that want to construct custom language
   * models that can be used as input to {@link
   * org.apache.lucene.analysis.ja.dict.DictionaryBuilder}.
   *
   * @param factory the AttributeFactory to use
   * @param systemDictionary a custom known token dictionary
   * @param unkDictionary a custom unknown token dictionary
   * @param connectionCosts custom token transition costs
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @param discardCompoundToken true if compound tokens should be dropped from the output when
   *     tokenization mode is not NORMAL.
   * @param mode tokenization mode.
   * @lucene.experimental
   */
  @IgnoreRandomChains(reason = "Parameters are too complex to be tested")
  public JapaneseTokenizer(
      AttributeFactory factory,
      TokenInfoDictionary systemDictionary,
      UnknownDictionary unkDictionary,
      ConnectionCosts connectionCosts,
      UserDictionary userDictionary,
      boolean discardPunctuation,
      boolean discardCompoundToken,
      Mode mode) {
    super(factory);
    TokenInfoFST fst = systemDictionary.getFST();
    FST.BytesReader fstReader = fst.getBytesReader();
    TokenInfoFST userFST = null;
    FST.BytesReader userFSTReader = null;
    if (userDictionary != null) {
      userFST = userDictionary.getFST();
      userFSTReader = userFST.getBytesReader();
    }
    boolean searchMode;
    boolean extendedMode;
    boolean outputCompounds;
    switch (mode) {
      case SEARCH:
        searchMode = true;
        extendedMode = false;
        outputCompounds = !discardCompoundToken;
        break;
      case EXTENDED:
        searchMode = true;
        extendedMode = true;
        outputCompounds = !discardCompoundToken;
        break;
      case NORMAL:
      default:
        searchMode = false;
        extendedMode = false;
        outputCompounds = false;
        break;
    }
    CharacterDefinition characterDefinition = unkDictionary.getCharacterDefinition();
    this.viterbi =
        new ViterbiNBest(
            fst,
            fstReader,
            systemDictionary,
            userFST,
            userFSTReader,
            userDictionary,
            connectionCosts,
            unkDictionary,
            characterDefinition,
            discardPunctuation,
            searchMode,
            extendedMode,
            outputCompounds);
    viterbi.resetBuffer(this.input);
    viterbi.resetState();
  }

  /** Expert: set this to produce graphviz (dot) output of the Viterbi lattice */
  public void setGraphvizFormatter(GraphvizFormatter<JaMorphData> dotOut) {
    viterbi.setGraphvizFormatter(dotOut);
  }

  @Override
  public void close() throws IOException {
    super.close();
    viterbi.resetBuffer(input);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    viterbi.resetBuffer(input);
    viterbi.resetState();
    lastTokenPos = -1;
  }

  @Override
  public void end() throws IOException {
    super.end();
    // Set final offset
    int finalOffset = correctOffset(viterbi.getPos());
    offsetAtt.setOffset(finalOffset, finalOffset);
  }

  @Override
  public boolean incrementToken() throws IOException {

    // forward() can return w/o producing any new
    // tokens, when the tokens it had produced were entirely
    // punctuation.  So we loop here until we get a real
    // token or we end:
    while (viterbi.getPending().size() == 0) {
      if (viterbi.isEnd()) {
        return false;
      }
      // Push Viterbi forward some more:
      viterbi.forward();
    }

    final Token token = viterbi.getPending().remove(viterbi.getPending().size() - 1);

    int length = token.getLength();
    clearAttributes();
    assert length > 0;
    // System.out.println("off=" + token.getOffset() + " len=" + length + " vs " +
    // token.getSurfaceForm().length);
    termAtt.copyBuffer(token.getSurfaceForm(), token.getOffset(), length);
    offsetAtt.setOffset(correctOffset(token.getStartOffset()), correctOffset(token.getEndOffset()));
    basicFormAtt.setToken(token);
    posAtt.setToken(token);
    readingAtt.setToken(token);
    inflectionAtt.setToken(token);
    if (token.getStartOffset() == lastTokenPos) {
      posIncAtt.setPositionIncrement(0);
      posLengthAtt.setPositionLength(token.getPositionLength());
    } else if (viterbi.isOutputNBest()) {
      // The position length is always calculated if outputNBest is true.
      assert token.getStartOffset() > lastTokenPos;
      posIncAtt.setPositionIncrement(1);
      posLengthAtt.setPositionLength(token.getPositionLength());
    } else {
      assert token.getStartOffset() > lastTokenPos;
      posIncAtt.setPositionIncrement(1);
      posLengthAtt.setPositionLength(1);
    }
    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ":    incToken: return token=" + token);
    }
    lastTokenPos = token.getStartOffset();

    return true;
  }

  private int probeDelta(String inText, String requiredToken) throws IOException {
    int start = inText.indexOf(requiredToken);
    if (start < 0) {
      // -1 when no requiredToken.
      return -1;
    }

    int delta = Integer.MAX_VALUE;
    int saveNBestCost = viterbi.getNBestCost();
    setReader(new StringReader(inText));
    reset();
    try {
      setNBestCost(1);
      int prevRootBase = -1;
      while (incrementToken()) {
        if (viterbi.getLatticeRootBase() != prevRootBase) {
          prevRootBase = viterbi.getLatticeRootBase();
          delta = Math.min(delta, viterbi.probeDelta(start, start + requiredToken.length()));
        }
      }
    } finally {
      // reset & end
      end();
      // setReader & close
      close();
      setNBestCost(saveNBestCost);
    }

    if (VERBOSE) {
      System.out.printf("JapaneseTokenizer: delta = %d: %s-%s\n", delta, inText, requiredToken);
    }
    return delta == Integer.MAX_VALUE ? -1 : delta;
  }

  public int calcNBestCost(String examples) {
    int maxDelta = 0;
    for (String example : examples.split("/")) {
      if (!example.isEmpty()) {
        String[] pair = example.split("-");
        if (pair.length != 2) {
          throw new RuntimeException("Unexpected example form: " + example + " (expected two '-')");
        } else {
          try {
            maxDelta = Math.max(maxDelta, probeDelta(pair[0], pair[1]));
          } catch (IOException e) {
            throw new RuntimeException(
                "Internal error calculating best costs from examples. Got ", e);
          }
        }
      }
    }
    return maxDelta;
  }

  public void setNBestCost(int value) {
    viterbi.setNBestCost(value);
  }
}
