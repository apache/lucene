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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.dict.ConnectionCosts;
import org.apache.lucene.analysis.ko.dict.DictionaryBuilder;
import org.apache.lucene.analysis.ko.dict.KoMorphData;
import org.apache.lucene.analysis.ko.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ko.dict.TokenInfoFST;
import org.apache.lucene.analysis.ko.dict.UnknownDictionary;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.apache.lucene.analysis.ko.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.ko.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.morph.GraphvizFormatter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IgnoreRandomChains;
import org.apache.lucene.util.fst.FST;

/**
 * Tokenizer for Korean that uses morphological analysis.
 *
 * <p>This tokenizer sets a number of additional attributes:
 *
 * <ul>
 *   <li>{@link PartOfSpeechAttribute} containing part-of-speech.
 *   <li>{@link ReadingAttribute} containing reading.
 * </ul>
 *
 * <p>This tokenizer uses a rolling Viterbi search to find the least cost segmentation (path) of the
 * incoming characters.
 *
 * @lucene.experimental
 */
@IgnoreRandomChains(reason = "LUCENE-10359: fails with incorrect offsets")
public final class KoreanTokenizer extends Tokenizer {

  /**
   * Decompound mode: this determines how the tokenizer handles {@link POS.Type#COMPOUND}, {@link
   * POS.Type#INFLECT} and {@link POS.Type#PREANALYSIS} tokens.
   */
  public enum DecompoundMode {
    /** No decomposition for compound. */
    NONE,

    /** Decompose compounds and discards the original form (default). */
    DISCARD,

    /** Decompose compounds and keeps the original form. */
    MIXED
  }

  /** Default mode for the decompound of tokens ({@link DecompoundMode#DISCARD}. */
  public static final DecompoundMode DEFAULT_DECOMPOUND = DecompoundMode.DISCARD;

  private static final boolean VERBOSE = false;

  private final Viterbi viterbi;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);

  /**
   * Creates a new KoreanTokenizer with default parameters.
   *
   * <p>Uses the default AttributeFactory.
   */
  public KoreanTokenizer() {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, null, DEFAULT_DECOMPOUND, false, true);
  }

  /**
   * Create a new KoreanTokenizer using the system and unknown dictionaries shipped with Lucene.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param mode Decompound mode.
   * @param outputUnknownUnigrams if true outputs unigrams for unknown words.
   */
  public KoreanTokenizer(
      AttributeFactory factory,
      UserDictionary userDictionary,
      DecompoundMode mode,
      boolean outputUnknownUnigrams) {
    this(factory, userDictionary, mode, outputUnknownUnigrams, true);
  }

  /**
   * Create a new KoreanTokenizer using the system and unknown dictionaries shipped with Lucene.
   *
   * @param factory the AttributeFactory to use
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param mode Decompound mode.
   * @param outputUnknownUnigrams if true outputs unigrams for unknown words.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   */
  public KoreanTokenizer(
      AttributeFactory factory,
      UserDictionary userDictionary,
      DecompoundMode mode,
      boolean outputUnknownUnigrams,
      boolean discardPunctuation) {
    this(
        factory,
        TokenInfoDictionary.getInstance(),
        UnknownDictionary.getInstance(),
        ConnectionCosts.getInstance(),
        userDictionary,
        mode,
        outputUnknownUnigrams,
        discardPunctuation);
  }

  /**
   * Create a new KoreanTokenizer supplying a custom system dictionary and unknown dictionary. This
   * constructor provides an entry point for users that want to construct custom language models
   * that can be used as input to {@link DictionaryBuilder}.
   *
   * @param factory the AttributeFactory to use
   * @param systemDictionary a custom known token dictionary
   * @param unkDictionary a custom unknown token dictionary
   * @param connectionCosts custom token transition costs
   * @param userDictionary Optional: if non-null, user dictionary.
   * @param mode Decompound mode.
   * @param outputUnknownUnigrams if true outputs unigrams for unknown words.
   * @param discardPunctuation true if punctuation tokens should be dropped from the output.
   * @lucene.experimental
   */
  @IgnoreRandomChains(reason = "Parameters are too complex to be tested")
  public KoreanTokenizer(
      AttributeFactory factory,
      TokenInfoDictionary systemDictionary,
      UnknownDictionary unkDictionary,
      ConnectionCosts connectionCosts,
      UserDictionary userDictionary,
      DecompoundMode mode,
      boolean outputUnknownUnigrams,
      boolean discardPunctuation) {
    super(factory);
    TokenInfoFST fst = systemDictionary.getFST();
    FST.BytesReader fstReader = fst.getBytesReader();
    TokenInfoFST userFST = null;
    FST.BytesReader userFSTReader = null;
    if (userDictionary != null) {
      userFST = userDictionary.getFST();
      userFSTReader = userFST.getBytesReader();
    }

    viterbi =
        new Viterbi(
            fst,
            fstReader,
            systemDictionary,
            userFST,
            userFSTReader,
            userDictionary,
            connectionCosts,
            unkDictionary,
            unkDictionary.getCharacterDefinition(),
            discardPunctuation,
            mode,
            outputUnknownUnigrams);
    viterbi.resetBuffer(input);
    viterbi.resetState();
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

    // parse() is able to return w/o producing any new
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
    posAtt.setToken(token);
    readingAtt.setToken(token);
    posIncAtt.setPositionIncrement(token.getPositionIncrement());
    posLengthAtt.setPositionLength(token.getPositionLength());
    if (VERBOSE) {
      System.out.println(Thread.currentThread().getName() + ":    incToken: return token=" + token);
    }
    return true;
  }

  /** Expert: set this to produce graphviz (dot) output of the Viterbi lattice */
  public void setGraphvizFormatter(GraphvizFormatter<KoMorphData> dotOut) {
    viterbi.setGraphvizFormatter(dotOut);
  }
}
