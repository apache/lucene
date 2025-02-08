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
import java.util.Collections;
import org.apache.lucene.analysis.ja.dict.CharacterDefinition;
import org.apache.lucene.analysis.ja.dict.JaMorphData;
import org.apache.lucene.analysis.ja.dict.TokenInfoDictionary;
import org.apache.lucene.analysis.ja.dict.UnknownDictionary;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.morph.ConnectionCosts;
import org.apache.lucene.analysis.morph.Dictionary;
import org.apache.lucene.analysis.morph.GraphvizFormatter;
import org.apache.lucene.analysis.morph.TokenInfoFST;
import org.apache.lucene.analysis.morph.TokenType;
import org.apache.lucene.util.fst.FST;

/**
 * {@link org.apache.lucene.analysis.morph.Viterbi} subclass for Japanese morphological analysis.
 * This also performs n-best path calculation
 */
final class ViterbiNBest extends org.apache.lucene.analysis.morph.ViterbiNBest<Token, JaMorphData> {

  private final UnknownDictionary unkDictionary;
  private final CharacterDefinition characterDefinition;
  private final UserDictionary userDictionary;

  private final boolean discardPunctuation;
  private final boolean searchMode;
  private final boolean extendedMode;
  private final boolean outputCompounds;

  private GraphvizFormatter<JaMorphData> dotOut;

  ViterbiNBest(
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
      boolean searchMode,
      boolean extendedMode,
      boolean outputCompounds) {
    super(fst, fstReader, dictionary, userFST, userFSTReader, userDictionary, costs);
    this.unkDictionary = unkDictionary;
    this.characterDefinition = characterDefinition;
    this.userDictionary = userDictionary;
    this.discardPunctuation = discardPunctuation;
    this.searchMode = searchMode;
    this.extendedMode = extendedMode;
    this.outputCompounds = outputCompounds;
    dictionaryMap.put(TokenType.KNOWN, dictionary);
    dictionaryMap.put(TokenType.UNKNOWN, unkDictionary);
    dictionaryMap.put(TokenType.USER, userDictionary);
  }

  @Override
  protected boolean shouldSkipProcessUnknownWord(int unknownWordEndIndex, Position posData) {
    return !searchMode && super.shouldSkipProcessUnknownWord(unknownWordEndIndex, posData);
  }

  private static final int SEARCH_MODE_KANJI_LENGTH = 2;
  private static final int SEARCH_MODE_OTHER_LENGTH = 7; // Must be >= SEARCH_MODE_KANJI_LENGTH
  private static final int SEARCH_MODE_KANJI_PENALTY = 3000;
  private static final int SEARCH_MODE_OTHER_PENALTY = 1700;

  @Override
  protected int computePenalty(int pos, int length) throws IOException {
    if (length > SEARCH_MODE_KANJI_LENGTH) {
      boolean allKanji = true;
      // check if node consists of only kanji
      final int endPos = pos + length;
      for (int pos2 = pos; pos2 < endPos; pos2++) {
        if (!characterDefinition.isKanji((char) buffer.get(pos2))) {
          allKanji = false;
          break;
        }
      }
      if (allKanji) { // Process only Kanji keywords
        return (length - SEARCH_MODE_KANJI_LENGTH) * SEARCH_MODE_KANJI_PENALTY;
      } else if (length > SEARCH_MODE_OTHER_LENGTH) {
        return (length - SEARCH_MODE_OTHER_LENGTH) * SEARCH_MODE_OTHER_PENALTY;
      }
    }
    return 0;
  }

  // Returns the added cost that a 2nd best segmentation is
  // allowed to have.  Ie, if we see path with cost X,
  // ending in a compound word, and this method returns
  // threshold > 0, then we will also find the 2nd best
  // segmentation and if its path score is within this
  // threshold of X, we'll include it in the output:
  private int computeSecondBestThreshold(int pos, int length) throws IOException {
    // TODO: maybe we do something else here, instead of just
    // using the penalty...?  EG we can be more aggressive on
    // when to also test for 2nd best path
    return computePenalty(pos, length);
  }

  @Override
  protected int processUnknownWord(boolean anyMatches, Position posData) throws IOException {
    final char firstCharacter = (char) buffer.get(pos);
    if (!anyMatches || characterDefinition.isInvoke(firstCharacter)) {

      // Find unknown match:
      final int characterId = characterDefinition.getCharacterClass(firstCharacter);
      final boolean isPunct = isPunctuation(firstCharacter);

      // NOTE: copied from UnknownDictionary.lookup:
      int unknownWordLength;
      if (!characterDefinition.isGroup(firstCharacter)) {
        unknownWordLength = 1;
      } else {
        // Extract unknown word. Characters with the same character class are considered to be
        // part of unknown word
        unknownWordLength = 1;
        for (int posAhead = pos + 1; unknownWordLength < MAX_UNKNOWN_WORD_LENGTH; posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          if (characterId == characterDefinition.getCharacterClass((char) ch)
              && isPunctuation((char) ch) == isPunct) {
            unknownWordLength++;
          } else {
            break;
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
            posData.getPos() + unknownWordLength,
            wordIdRef.ints[wordIdRef.offset + ofs],
            TokenType.UNKNOWN,
            false);
      }

      return unknownWordLength;
    }
    return 0;
  }

  void setGraphvizFormatter(GraphvizFormatter<JaMorphData> dotOut) {
    this.dotOut = dotOut;
  }

  @Override
  protected void backtrace(Position endPosData, int fromIDX) throws IOException {
    final int endPos = endPosData.getPos();

    /*
     * LUCENE-10059: If the endPos is the same as lastBackTracePos, we don't want to backtrace to
     * avoid an assertion error {@link RollingCharBuffer#get(int)} when it tries to generate an
     * empty buffer
     */
    if (endPos == lastBackTracePos) {
      return;
    }

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
    Token altToken = null;

    // We trace backwards, so this will be the leftWordID of
    // the token after the one we are now on:
    int lastLeftWordID = -1;

    int backCount = 0;

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
      assert backPos >= lastBackTracePos
          : "backPos=" + backPos + " vs lastBackTracePos=" + lastBackTracePos;
      int length = pos - backPos;
      TokenType backType = posData.getBackType(bestIDX);
      int backID = posData.getBackID(bestIDX);
      int nextBestIDX = posData.getBackIndex(bestIDX);

      if (searchMode && altToken == null && backType != TokenType.USER) {

        // In searchMode, if best path had picked a too-long
        // token, we use the "penalty" to compute the allowed
        // max cost of an alternate back-trace.  If we find an
        // alternate back trace with cost below that
        // threshold, we pursue it instead (but also output
        // the long token).
        // System.out.println("    2nd best backPos=" + backPos + " pos=" + pos);

        final int penalty = computeSecondBestThreshold(backPos, pos - backPos);

        if (penalty > 0) {
          if (VERBOSE) {
            System.out.println(
                "  compound="
                    + new String(buffer.get(backPos, pos - backPos))
                    + " backPos="
                    + backPos
                    + " pos="
                    + pos
                    + " penalty="
                    + penalty
                    + " cost="
                    + posData.getCost(bestIDX)
                    + " bestIDX="
                    + bestIDX
                    + " lastLeftID="
                    + lastLeftWordID);
          }

          // Use the penalty to set maxCost on the 2nd best
          // segmentation:
          int maxCost = posData.getCost(bestIDX) + penalty;
          if (lastLeftWordID != -1) {
            maxCost += costs.get(getDict(backType).getRightId(backID), lastLeftWordID);
          }

          // Now, prune all too-long tokens from the graph:
          pruneAndRescore(backPos, pos, posData.getBackIndex(bestIDX));

          // Finally, find 2nd best back-trace and resume
          // backtrace there:
          int leastCost = Integer.MAX_VALUE;
          int leastIDX = -1;
          for (int idx = 0; idx < posData.getCount(); idx++) {
            int cost = posData.getCost(idx);
            // System.out.println("    idx=" + idx + " prevCost=" + cost);

            if (lastLeftWordID != -1) {
              cost +=
                  costs.get(
                      getDict(posData.getBackType(idx)).getRightId(posData.getBackID(idx)),
                      lastLeftWordID);
              // System.out.println("      += bgCost=" +
              // costs.get(getDict(posData.backType[idx]).getRightId(posData.backID[idx]),
              // lastLeftWordID) + " -> " + cost);
            }
            // System.out.println("penalty " + posData.backPos[idx] + " to " + pos);
            // cost += computePenalty(posData.backPos[idx], pos - posData.backPos[idx]);
            if (cost < leastCost) {
              // System.out.println("      ** ");
              leastCost = cost;
              leastIDX = idx;
            }
          }
          // System.out.println("  leastIDX=" + leastIDX);

          if (VERBOSE) {
            System.out.println(
                "  afterPrune: "
                    + posData.getCount()
                    + " arcs arriving; leastCost="
                    + leastCost
                    + " vs threshold="
                    + maxCost
                    + " lastLeftWordID="
                    + lastLeftWordID);
          }

          if (leastIDX != -1 && leastCost <= maxCost && posData.getBackPos(leastIDX) != backPos) {
            // We should have pruned the altToken from the graph:
            assert posData.getBackPos(leastIDX) != backPos;

            // Save the current compound token, to output when
            // this alternate path joins back:
            altToken =
                new Token(
                    fragment,
                    backPos - lastBackTracePos,
                    length,
                    backPos,
                    backPos + length,
                    backID,
                    backType,
                    getDict(backType).getMorphAttributes());

            // Redirect our backtrace to 2nd best:
            bestIDX = leastIDX;
            nextBestIDX = posData.getBackIndex(bestIDX);

            backPos = posData.getBackPos(bestIDX);
            length = pos - backPos;
            backType = posData.getBackType(bestIDX);
            backID = posData.getBackID(bestIDX);
            backCount = 0;
            // System.out.println("  do alt token!");

          } else {
            // I think in theory it's possible there is no
            // 2nd best path, which is fine; in this case we
            // only output the compound token:
            // System.out.println("  no alt token! bestIDX=" + bestIDX);
          }
        }
      }

      final int offset = backPos - lastBackTracePos;
      assert offset >= 0;

      if (altToken != null && altToken.getStartOffset() >= backPos) {
        if (outputCompounds) {
          // We've backtraced to the position where the
          // compound token starts; add it now:

          // The pruning we did when we created the altToken
          // ensures that the back trace will align back with
          // the start of the altToken:
          assert altToken.getStartOffset() == backPos
              : altToken.getStartOffset() + " vs " + backPos;

          // NOTE: not quite right: the compound token may
          // have had all punctuation back traced so far, but
          // then the decompounded token at this position is
          // not punctuation.  In this case backCount is 0,
          // but we should maybe add the altToken anyway...?

          if (backCount > 0) {
            backCount++;
            altToken.setPositionLength(backCount);
            if (VERBOSE) {
              System.out.println("    add altToken=" + altToken);
            }
            pending.add(altToken);
          } else {
            // This means alt token was all punct tokens:
            if (VERBOSE) {
              System.out.println("    discard all-punctuation altToken=" + altToken);
            }
            assert discardPunctuation;
          }
        }
        altToken = null;
      }

      final Dictionary<? extends JaMorphData> dict = getDict(backType);

      if (backType == TokenType.USER) {

        // Expand the phraseID we recorded into the actual
        // segmentation:
        final int[] wordIDAndLength = userDictionary.lookupSegmentation(backID);
        int wordID = wordIDAndLength[0];
        int current = 0;
        for (int j = 1; j < wordIDAndLength.length; j++) {
          final int len = wordIDAndLength[j];
          // System.out.println("    add user: len=" + len);
          int startOffset = current + backPos;
          pending.add(
              new Token(
                  fragment,
                  current + offset,
                  len,
                  startOffset,
                  startOffset + len,
                  wordID + j - 1,
                  TokenType.USER,
                  dict.getMorphAttributes()));
          if (VERBOSE) {
            System.out.println("    add USER token=" + pending.get(pending.size() - 1));
          }
          current += len;
        }

        // Reverse the tokens we just added, because when we
        // serve them up from incrementToken we serve in
        // reverse:
        Collections.reverse(
            pending.subList(pending.size() - (wordIDAndLength.length - 1), pending.size()));

        backCount += wordIDAndLength.length - 1;
      } else {

        if (extendedMode && backType == TokenType.UNKNOWN) {
          // In EXTENDED mode we convert unknown word into
          // unigrams:
          int unigramTokenCount = 0;
          for (int i = length - 1; i >= 0; i--) {
            int charLen = 1;
            if (i > 0 && Character.isLowSurrogate(fragment[offset + i])) {
              i--;
              charLen = 2;
            }
            // System.out.println("    extended tok offset="
            // + (offset + i));
            if (!discardPunctuation || !isPunctuation(fragment[offset + i])) {
              int startOffset = backPos + i;
              pending.add(
                  new Token(
                      fragment,
                      offset + i,
                      charLen,
                      startOffset,
                      startOffset + charLen,
                      CharacterDefinition.NGRAM,
                      TokenType.UNKNOWN,
                      unkDictionary.getMorphAttributes()));
              unigramTokenCount++;
            }
          }
          backCount += unigramTokenCount;

        } else if (!discardPunctuation || length == 0 || !isPunctuation(fragment[offset])) {
          pending.add(
              new Token(
                  fragment,
                  offset,
                  length,
                  backPos,
                  backPos + length,
                  backID,
                  backType,
                  dict.getMorphAttributes()));
          if (VERBOSE) {
            System.out.println("    add token=" + pending.get(pending.size() - 1));
          }
          backCount++;
        } else {
          if (VERBOSE) {
            System.out.println(
                "    skip punctuation token=" + new String(fragment, offset, length));
          }
        }
      }

      lastLeftWordID = dict.getLeftId(backID);
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

  // Eliminates arcs from the lattice that are compound
  // tokens (have a penalty) or are not congruent with the
  // compound token we've matched (ie, span across the
  // startPos).  This should be fairly efficient, because we
  // just keep the already intersected structure of the
  // graph, eg we don't have to consult the FSTs again:
  private void pruneAndRescore(int startPos, int endPos, int bestStartIDX) throws IOException {
    if (VERBOSE) {
      System.out.println(
          "  pruneAndRescore startPos="
              + startPos
              + " endPos="
              + endPos
              + " bestStartIDX="
              + bestStartIDX);
    }

    // First pass: walk backwards, building up the forward
    // arcs and pruning inadmissible arcs:
    for (int pos = endPos; pos > startPos; pos--) {
      final Position posData = positions.get(pos);
      if (VERBOSE) {
        System.out.println("    back pos=" + pos);
      }
      for (int arcIDX = 0; arcIDX < posData.getCount(); arcIDX++) {
        final int backPos = posData.getBackPos(arcIDX);
        if (backPos >= startPos) {
          // Keep this arc:
          // System.out.println("      keep backPos=" + backPos);
          positions
              .get(backPos)
              .addForward(pos, arcIDX, posData.getBackID(arcIDX), posData.getBackType(arcIDX));
        } else {
          if (VERBOSE) {
            System.out.println("      prune");
          }
        }
      }
      if (pos != startPos) {
        posData.setCount(0);
      }
    }

    // Second pass: walk forward, re-scoring:
    for (int pos = startPos; pos < endPos; pos++) {
      final PositionNBest posData = positions.get(pos);
      if (VERBOSE) {
        System.out.println("    forward pos=" + pos + " count=" + posData.getForwardCount());
      }
      if (posData.getCount() == 0) {
        // No arcs arrive here...
        if (VERBOSE) {
          System.out.println("      skip");
        }
        posData.setForwardCount(0);
        continue;
      }

      if (pos == startPos) {
        // On the initial position, only consider the best
        // path so we "force congruence":  the
        // sub-segmentation is "in context" of what the best
        // path (compound token) had matched:
        final int rightID;
        if (startPos == 0) {
          rightID = 0;
        } else {
          rightID =
              getDict(posData.getBackType(bestStartIDX))
                  .getRightId(posData.getBackID(bestStartIDX));
        }
        final int pathCost = posData.getCost(bestStartIDX);
        for (int forwardArcIDX = 0; forwardArcIDX < posData.getForwardCount(); forwardArcIDX++) {
          final TokenType forwardType = posData.getForwardType(forwardArcIDX);
          final Dictionary<? extends JaMorphData> dict2 = getDict(forwardType);
          final int wordID = posData.getForwardID(forwardArcIDX);
          final int toPos = posData.getForwardPos(forwardArcIDX);
          final int newCost =
              pathCost
                  + dict2.getWordCost(wordID)
                  + costs.get(rightID, dict2.getLeftId(wordID))
                  + computePenalty(pos, toPos - pos);
          if (VERBOSE) {
            System.out.println(
                "      + "
                    + forwardType
                    + " word "
                    + new String(buffer.get(pos, toPos - pos))
                    + " toPos="
                    + toPos
                    + " cost="
                    + newCost
                    + " penalty="
                    + computePenalty(pos, toPos - pos)
                    + " toPos.idx="
                    + positions.get(toPos).getCount());
          }
          positions
              .get(toPos)
              .add(newCost, dict2.getRightId(wordID), pos, -1, bestStartIDX, wordID, forwardType);
        }
      } else {
        // On non-initial positions, we maximize score
        // across all arriving lastRightIDs:
        for (int forwardArcIDX = 0; forwardArcIDX < posData.getForwardCount(); forwardArcIDX++) {
          final TokenType forwardType = posData.getForwardType(forwardArcIDX);
          final int toPos = posData.getForwardPos(forwardArcIDX);
          if (VERBOSE) {
            System.out.println(
                "      + "
                    + forwardType
                    + " word "
                    + new String(buffer.get(pos, toPos - pos))
                    + " toPos="
                    + toPos);
          }
          add(
              getDict(forwardType).getMorphAttributes(),
              posData,
              pos,
              toPos,
              posData.getForwardID(forwardArcIDX),
              forwardType,
              true);
        }
      }
      posData.setForwardCount(0);
    }
  }

  @Override
  protected void registerNode(int node, char[] fragment) {
    int left = lattice.getNodeLeft(node);
    int right = lattice.getNodeRight(node);
    TokenType type = lattice.getNodeDicType(node);
    if (!discardPunctuation || !isPunctuation(fragment[left])) {
      if (type == TokenType.USER) {
        // The code below are based on backtrace().
        //
        // Expand the phraseID we recorded into the actual segmentation:
        final int[] wordIDAndLength =
            userDictionary.lookupSegmentation(lattice.getNodeWordID(node));
        int wordID = wordIDAndLength[0];
        pending.add(
            new Token(
                fragment,
                left,
                right - left,
                lattice.getRootBase() + left,
                lattice.getRootBase() + right,
                wordID,
                TokenType.USER,
                userDictionary.getMorphAttributes()));
        // Output compound
        int current = 0;
        for (int j = 1; j < wordIDAndLength.length; j++) {
          final int len = wordIDAndLength[j];
          if (len < right - left) {
            int startOffset = lattice.getRootBase() + current + left;
            pending.add(
                new Token(
                    fragment,
                    current + left,
                    len,
                    startOffset,
                    startOffset + len,
                    wordID + j - 1,
                    TokenType.USER,
                    userDictionary.getMorphAttributes()));
          }
          current += len;
        }
      } else {
        pending.add(
            new Token(
                fragment,
                left,
                right - left,
                lattice.getRootBase() + left,
                lattice.getRootBase() + right,
                lattice.getNodeWordID(node),
                type,
                getDict(type).getMorphAttributes()));
      }
    }
  }

  Dictionary<? extends JaMorphData> getDict(TokenType type) {
    return dictionaryMap.get(type);
  }

  @Override
  protected void setNBestCost(int value) {
    super.setNBestCost(value);
  }

  @Override
  protected int getNBestCost() {
    return super.getNBestCost();
  }

  private static boolean isPunctuation(char ch) {
    switch (Character.getType(ch)) {
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
}
