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
import java.util.EnumMap;
import org.apache.lucene.analysis.ko.dict.CharacterDefinition;
import org.apache.lucene.analysis.ko.dict.KoMorphData;
import org.apache.lucene.analysis.ko.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ko.dict.UnknownDictionary;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.apache.lucene.analysis.morph.ConnectionCosts;
import org.apache.lucene.analysis.morph.Dictionary;
import org.apache.lucene.analysis.morph.GraphvizFormatter;
import org.apache.lucene.analysis.morph.MorphData;
import org.apache.lucene.analysis.morph.TokenInfoFST;
import org.apache.lucene.analysis.morph.TokenType;
import org.apache.lucene.util.fst.FST;

/** {@link org.apache.lucene.analysis.morph.Viterbi} subclass for Korean morphological analysis. */
final class Viterbi
    extends org.apache.lucene.analysis.morph.Viterbi<
        Token, org.apache.lucene.analysis.morph.Viterbi.Position> {

  private final EnumMap<TokenType, Dictionary<? extends KoMorphData>> dictionaryMap =
      new EnumMap<>(TokenType.class);

  private final UnknownDictionary unkDictionary;
  private final CharacterDefinition characterDefinition;

  private final boolean discardPunctuation;
  private final KoreanTokenizer.DecompoundMode mode;
  private final boolean outputUnknownUnigrams;

  private GraphvizFormatter<KoMorphData> dotOut;

  Viterbi(
      TokenInfoFST fst,
      FST.BytesReader fstReader,
      TokenInfoDictionary dictionary,
      TokenInfoFST userFST,
      FST.BytesReader userFSTReader,
      UserDictionary userDictionary,
      ConnectionCosts costs,
      UnknownDictionary unkDictionary,
      CharacterDefinition characterDefinition,
      boolean discardPunctuation,
      KoreanTokenizer.DecompoundMode mode,
      boolean outputUnknownUnigrams) {
    super(
        fst, fstReader, dictionary, userFST, userFSTReader, userDictionary, costs, Position.class);
    this.unkDictionary = unkDictionary;
    this.characterDefinition = characterDefinition;
    this.discardPunctuation = discardPunctuation;
    this.mode = mode;
    this.outputUnknownUnigrams = outputUnknownUnigrams;
    this.enableSpacePenaltyFactor = true;
    this.outputLongestUserEntryOnly = true;
    dictionaryMap.put(TokenType.KNOWN, dictionary);
    dictionaryMap.put(TokenType.UNKNOWN, unkDictionary);
    dictionaryMap.put(TokenType.USER, userDictionary);
  }

  @Override
  protected int processUnknownWord(boolean anyMatches, Position posData) throws IOException {
    final char firstCharacter = (char) buffer.get(pos);
    if (!anyMatches || characterDefinition.isInvoke(firstCharacter)) {

      // Find unknown match:
      int characterId = characterDefinition.getCharacterClass(firstCharacter);
      // NOTE: copied from UnknownDictionary.lookup:
      int unknownWordLength;
      if (!characterDefinition.isGroup(firstCharacter)) {
        unknownWordLength = 1;
      } else {
        // Extract unknown word. Characters with the same script are considered to be part of
        // unknown word
        unknownWordLength = 1;
        Character.UnicodeScript scriptCode = Character.UnicodeScript.of(firstCharacter);
        final boolean isPunct = isPunctuation(firstCharacter);
        final boolean isDigit = Character.isDigit(firstCharacter);
        for (int posAhead = pos + 1; unknownWordLength < MAX_UNKNOWN_WORD_LENGTH; posAhead++) {
          int next = buffer.get(posAhead);
          if (next == -1) {
            break;
          }
          char ch = (char) next;
          int chType = Character.getType(ch);
          Character.UnicodeScript sc = Character.UnicodeScript.of(next);
          boolean sameScript =
              isSameScript(scriptCode, sc)
                  // Non-spacing marks inherit the script of their base character,
                  // following recommendations from UTR #24.
                  || chType == Character.NON_SPACING_MARK;

          if (sameScript
              // split on punctuation
              && isPunctuation(ch, chType) == isPunct
              // split on digit
              && Character.isDigit(ch) == isDigit
              && characterDefinition.isGroup(ch)) {
            unknownWordLength++;
          } else {
            break;
          }
          // Update the script code and character class if the original script
          // is Inherited or Common.
          if (isCommonOrInherited(scriptCode) && isCommonOrInherited(sc) == false) {
            scriptCode = sc;
            characterId = characterDefinition.getCharacterClass(ch);
          }
        }
      }

      unkDictionary.lookupWordIds(
          characterId, wordIdRef); // characters in input text are supposed to be the same
      if (VERBOSE) {
        System.out.println(
            "    UNKNOWN word len=" + unknownWordLength + " " + wordIdRef.length + " wordIDs");
      }
      for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
        add(
            unkDictionary.getMorphAttributes(),
            posData,
            pos,
            pos + unknownWordLength,
            wordIdRef.ints[wordIdRef.offset + ofs],
            TokenType.UNKNOWN,
            false);
      }
    }
    // TODO: should return meaningful value?
    return 0;
  }

  public void setGraphvizFormatter(GraphvizFormatter<KoMorphData> dotOut) {
    this.dotOut = dotOut;
  }

  @Override
  protected void backtrace(Position endPosData, int fromIDX) {
    final int endPos = endPosData.getPos();

    if (VERBOSE) {
      System.out.println(
          "\n  backtrace: endPos="
              + endPos
              + " pos="
              + pos
              + "; "
              + (pos - lastBackTracePos)
              + " characters; last="
              + lastBackTracePos
              + " cost="
              + endPosData.getCost(fromIDX));
    }

    final char[] fragment = buffer.get(lastBackTracePos, endPos - lastBackTracePos);

    if (dotOut != null) {
      dotOut.onBacktrace(
          this::getDict, positions, lastBackTracePos, endPosData, fromIDX, fragment, end);
    }

    int pos = endPos;
    int bestIDX = fromIDX;

    // TODO: sort of silly to make Token instances here; the
    // back trace has all info needed to generate the
    // token.  So, we could just directly set the attrs,
    // from the backtrace, in incrementToken w/o ever
    // creating Token; we'd have to defer calling freeBefore
    // until after the backtrace was fully "consumed" by
    // incrementToken.

    while (pos > lastBackTracePos) {
      // System.out.println("BT: back pos=" + pos + " bestIDX=" + bestIDX);
      final Position posData = positions.get(pos);
      assert bestIDX < posData.getCount();

      int backPos = posData.getBackPos(bestIDX);
      int backWordPos = posData.getBackWordPos(bestIDX);
      assert backPos >= lastBackTracePos
          : "backPos=" + backPos + " vs lastBackTracePos=" + lastBackTracePos;
      // the length of the word without the whitespaces at the beginning.
      int length = pos - backWordPos;
      TokenType backType = posData.getBackType(bestIDX);
      int backID = posData.getBackID(bestIDX);
      int nextBestIDX = posData.getBackIndex(bestIDX);
      // the start of the word after the whitespace at the beginning.
      final int fragmentOffset = backWordPos - lastBackTracePos;
      assert fragmentOffset >= 0;

      final Dictionary<? extends KoMorphData> dict = getDict(backType);

      if (outputUnknownUnigrams && backType == TokenType.UNKNOWN) {
        // outputUnknownUnigrams converts unknown word into unigrams:
        for (int i = length - 1; i >= 0; i--) {
          int charLen = 1;
          if (i > 0 && Character.isLowSurrogate(fragment[fragmentOffset + i])) {
            i--;
            charLen = 2;
          }
          final DictionaryToken token =
              new DictionaryToken(
                  TokenType.UNKNOWN,
                  unkDictionary.getMorphAttributes(),
                  CharacterDefinition.NGRAM,
                  fragment,
                  fragmentOffset + i,
                  charLen,
                  backWordPos + i,
                  backWordPos + i + charLen);
          pending.add(token);
          if (VERBOSE) {
            System.out.println("    add token=" + pending.get(pending.size() - 1));
          }
        }
      } else {
        final DictionaryToken token =
            new DictionaryToken(
                backType,
                dict.getMorphAttributes(),
                backID,
                fragment,
                fragmentOffset,
                length,
                backWordPos,
                backWordPos + length);
        if (token.getPOSType() == POS.Type.MORPHEME
            || mode == KoreanTokenizer.DecompoundMode.NONE) {
          if (shouldFilterToken(token) == false) {
            pending.add(token);
            if (VERBOSE) {
              System.out.println("    add token=" + pending.get(pending.size() - 1));
            }
          }
        } else {
          KoMorphData.Morpheme[] morphemes = token.getMorphemes();
          if (morphemes == null) {
            pending.add(token);
            if (VERBOSE) {
              System.out.println("    add token=" + pending.get(pending.size() - 1));
            }
          } else {
            int endOffset = backWordPos + length;
            int posLen = 0;
            // decompose the compound
            for (int i = morphemes.length - 1; i >= 0; i--) {
              final KoMorphData.Morpheme morpheme = morphemes[i];
              final Token compoundToken;
              if (token.getPOSType() == POS.Type.COMPOUND) {
                assert endOffset - morpheme.surfaceForm.length() >= 0;
                compoundToken =
                    new DecompoundToken(
                        morpheme.posTag,
                        morpheme.surfaceForm,
                        endOffset - morpheme.surfaceForm.length(),
                        endOffset,
                        backType);
              } else {
                compoundToken =
                    new DecompoundToken(
                        morpheme.posTag,
                        morpheme.surfaceForm,
                        token.getStartOffset(),
                        token.getEndOffset(),
                        backType);
              }
              if (i == 0 && mode == KoreanTokenizer.DecompoundMode.MIXED) {
                compoundToken.setPositionIncrement(0);
              }
              ++posLen;
              endOffset -= morpheme.surfaceForm.length();
              pending.add(compoundToken);
              if (VERBOSE) {
                System.out.println("    add token=" + pending.get(pending.size() - 1));
              }
            }
            if (mode == KoreanTokenizer.DecompoundMode.MIXED) {
              token.setPositionLength(Math.max(1, posLen));
              pending.add(token);
              if (VERBOSE) {
                System.out.println("    add token=" + pending.get(pending.size() - 1));
              }
            }
          }
        }
      }
      if (discardPunctuation == false && backWordPos != backPos) {
        // Add a token for whitespaces between terms
        int offset = backPos - lastBackTracePos;
        int len = backWordPos - backPos;
        // System.out.println(offset + " " + fragmentOffset + " " + len + " " + backWordPos + " " +
        // backPos);
        unkDictionary.lookupWordIds(characterDefinition.getCharacterClass(' '), wordIdRef);
        DictionaryToken spaceToken =
            new DictionaryToken(
                TokenType.UNKNOWN,
                unkDictionary.getMorphAttributes(),
                wordIdRef.ints[wordIdRef.offset],
                fragment,
                offset,
                len,
                backPos,
                backPos + len);
        pending.add(spaceToken);
      }

      pos = backPos;
      bestIDX = nextBestIDX;
    }

    lastBackTracePos = endPos;

    if (VERBOSE) {
      System.out.println("  freeBefore pos=" + endPos);
    }
    // Notify the circular buffers that we are done with
    // these positions:
    buffer.freeBefore(endPos);
    positions.freeBefore(endPos);
  }

  /** Returns the space penalty associated with the provided {@link POS.Tag}. */
  @Override
  protected int computeSpacePenalty(MorphData morphData, int wordID, int numSpaces) {
    final POS.Tag leftPOS = ((KoMorphData) morphData).getLeftPOS(wordID);
    int spacePenalty = 0;
    if (numSpaces > 0) {
      // TODO we should extract the penalty (left-space-penalty-factor) from the dicrc file.
      switch (leftPOS) {
        case E:
        case J:
        case VCP:
        case XSA:
        case XSN:
        case XSV:
          spacePenalty = 3000;
          break;
        case IC:
        case MAG:
        case MAJ:
        case MM:
        case NA:
        case NNB:
        case NNBC:
        case NNG:
        case NNP:
        case NP:
        case NR:
        case SC:
        case SE:
        case SF:
        case SH:
        case SL:
        case SN:
        case SP:
        case SSC:
        case SSO:
        case SY:
        case UNA:
        case UNKNOWN:
        case VA:
        case VCN:
        case VSV:
        case VV:
        case VX:
        case XPN:
        case XR:
        default:
          break;
      }
    }
    return spacePenalty;
  }

  Dictionary<? extends KoMorphData> getDict(TokenType type) {
    return dictionaryMap.get(type);
  }

  private boolean shouldFilterToken(Token token) {
    return discardPunctuation && isPunctuation(token.getSurfaceForm()[token.getOffset()]);
  }

  private static boolean isPunctuation(char ch) {
    return isPunctuation(ch, Character.getType(ch));
  }

  private static boolean isPunctuation(char ch, int cid) {
    // special case for Hangul Letter Araea (interpunct)
    if (ch == 0x318D) {
      return true;
    }
    switch (cid) {
      case Character.SPACE_SEPARATOR:
      case Character.LINE_SEPARATOR:
      case Character.PARAGRAPH_SEPARATOR:
      case Character.CONTROL:
      case Character.FORMAT:
      case Character.DASH_PUNCTUATION:
      case Character.START_PUNCTUATION:
      case Character.END_PUNCTUATION:
      case Character.CONNECTOR_PUNCTUATION:
      case Character.OTHER_PUNCTUATION:
      case Character.MATH_SYMBOL:
      case Character.CURRENCY_SYMBOL:
      case Character.MODIFIER_SYMBOL:
      case Character.OTHER_SYMBOL:
      case Character.INITIAL_QUOTE_PUNCTUATION:
      case Character.FINAL_QUOTE_PUNCTUATION:
        return true;
      default:
        return false;
    }
  }

  private static boolean isCommonOrInherited(Character.UnicodeScript script) {
    return script == Character.UnicodeScript.INHERITED || script == Character.UnicodeScript.COMMON;
  }

  /** Determine if two scripts are compatible. */
  private static boolean isSameScript(
      Character.UnicodeScript scriptOne, Character.UnicodeScript scriptTwo) {
    return scriptOne == scriptTwo
        || isCommonOrInherited(scriptOne)
        || isCommonOrInherited(scriptTwo);
  }
}
