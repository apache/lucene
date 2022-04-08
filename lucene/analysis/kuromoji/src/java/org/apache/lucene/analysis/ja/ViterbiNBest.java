package org.apache.lucene.analysis.ja;

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
import org.apache.lucene.analysis.morph.Viterbi;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;

public class ViterbiNBest extends org.apache.lucene.analysis.morph.Viterbi<Token, ViterbiNBest.PositionNBest>{

  private final EnumMap<TokenType, Dictionary<? extends JaMorphData>> dictionaryMap =
    new EnumMap<>(TokenType.class);

  private final UnknownDictionary unkDictionary;
  private final CharacterDefinition characterDefinition;
  private final UserDictionary userDictionary;

  private final boolean discardPunctuation;
  private final boolean searchMode;
  private final boolean extendedMode;
  private final boolean outputCompounds;

  // Allowable cost difference for N-best output:
  private int nBestCost = 0;

  Lattice lattice = null;

  private GraphvizFormatter<JaMorphData> dotOut;

  protected ViterbiNBest(TokenInfoFST fst, FST.BytesReader fstReader, TokenInfoDictionary dictionary,
                         TokenInfoFST userFST, FST.BytesReader userFSTReader, UserDictionary userDictionary,
                         ConnectionCosts costs, Class<PositionNBest> positionImpl,
                         UnknownDictionary unkDictionary, CharacterDefinition characterDefinition,
                         boolean discardPunctuation, boolean searchMode, boolean extendedMode, boolean outputCompounds) {
    super(fst, fstReader, dictionary, userFST, userFSTReader, userDictionary, costs, positionImpl);
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

  public void setGraphvizFormatter(GraphvizFormatter<JaMorphData> dotOut) {
    this.dotOut = dotOut;
  }

  @Override
  protected void backtrace(Position endPosData, int fromIDX) throws IOException {
    final int endPos = endPosData.getPos();

    /**
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
      dotOut.onBacktrace(this::getDict, positions, lastBackTracePos, endPosData, fromIDX, fragment, end);
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
        System.out.println("    forward pos=" + pos + " count=" + posData.forwardCount);
      }
      if (posData.getCount() == 0) {
        // No arcs arrive here...
        if (VERBOSE) {
          System.out.println("      skip");
        }
        posData.forwardCount = 0;
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
            getDict(posData.getBackType(bestStartIDX)).getRightId(posData.getBackID(bestStartIDX));
        }
        final int pathCost = posData.getCost(bestStartIDX);
        for (int forwardArcIDX = 0; forwardArcIDX < posData.forwardCount; forwardArcIDX++) {
          final TokenType forwardType = posData.forwardType[forwardArcIDX];
          final Dictionary<? extends JaMorphData> dict2 = getDict(forwardType);
          final int wordID = posData.forwardID[forwardArcIDX];
          final int toPos = posData.forwardPos[forwardArcIDX];
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
        for (int forwardArcIDX = 0; forwardArcIDX < posData.forwardCount; forwardArcIDX++) {
          final TokenType forwardType = posData.forwardType[forwardArcIDX];
          final int toPos = posData.forwardPos[forwardArcIDX];
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
            posData.forwardID[forwardArcIDX],
            forwardType,
            true);
        }
      }
      posData.forwardCount = 0;
    }
  }


  @Override
  protected void backtraceNBest(final Position endPosData, final boolean useEOS) throws IOException {
    if (lattice == null) {
      lattice = new Lattice();
    }

    final int endPos = endPosData.getPos();
    char[] fragment = buffer.get(lastBackTracePos, endPos - lastBackTracePos);
    lattice.setup(fragment, dictionaryMap, positions, lastBackTracePos, endPos, useEOS);
    lattice.markUnreachable();
    lattice.calcLeftCost(costs);
    lattice.calcRightCost(costs);

    int bestCost = lattice.bestCost();
    if (VERBOSE) {
      System.out.printf("DEBUG: 1-BEST COST: %d\n", bestCost);
    }
    for (int node : lattice.bestPathNodeList()) {
      registerNode(node, fragment);
    }

    for (int n = 2; ; ++n) {
      List<Integer> nbest = lattice.nBestNodeList(n);
      if (nbest.isEmpty()) {
        break;
      }
      int cost = lattice.cost(nbest.get(0));
      if (VERBOSE) {
        System.out.printf("DEBUG: %d-BEST COST: %d\n", n, cost);
      }
      if (bestCost + nBestCost < cost) {
        break;
      }
      for (int node : nbest) {
        registerNode(node, fragment);
      }
    }
    if (VERBOSE) {
      lattice.debugPrint();
    }
  }

  private void registerNode(int node, char[] fragment) {
    int left = lattice.nodeLeft[node];
    int right = lattice.nodeRight[node];
    TokenType type = lattice.nodeDicType[node];
    if (!discardPunctuation || !isPunctuation(fragment[left])) {
      if (type == TokenType.USER) {
        // The code below are based on backtrace().
        //
        // Expand the phraseID we recorded into the actual segmentation:
        final int[] wordIDAndLength = userDictionary.lookupSegmentation(lattice.nodeWordID[node]);
        int wordID = wordIDAndLength[0];
        pending.add(
          new Token(
            fragment,
            left,
            right - left,
            lattice.rootBase + left,
            lattice.rootBase + right,
            wordID,
            TokenType.USER,
            userDictionary.getMorphAttributes()));
        // Output compound
        int current = 0;
        for (int j = 1; j < wordIDAndLength.length; j++) {
          final int len = wordIDAndLength[j];
          if (len < right - left) {
            int startOffset = lattice.rootBase + current + left;
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
            lattice.rootBase + left,
            lattice.rootBase + right,
            lattice.nodeWordID[node],
            type,
            getDict(type).getMorphAttributes()));
      }
    }
  }

  @Override
  protected void fixupPendingList() {
    // Sort for removing same tokens.
    // USER token should be ahead from normal one.
    Collections.sort(
      pending,
      (a, b) -> {
        int aOff = a.getOffset();
        int bOff = b.getOffset();
        if (aOff != bOff) {
          return aOff - bOff;
        }
        int aLen = a.getLength();
        int bLen = b.getLength();
        if (aLen != bLen) {
          return aLen - bLen;
        }
        // order of Type is KNOWN, UNKNOWN, USER,
        // so we use reversed comparison here.
        return b.getType().ordinal() - a.getType().ordinal();
      });

    // Remove same token.
    for (int i = 1; i < pending.size(); ++i) {
      Token a = pending.get(i - 1);
      Token b = pending.get(i);
      if (a.getOffset() == b.getOffset() && a.getLength() == b.getLength()) {
        pending.remove(i);
        // It is important to decrement "i" here, because a next may be removed.
        --i;
      }
    }

    // offset=>position map
    HashMap<Integer, Integer> map = new HashMap<>();
    for (Token t : pending) {
      map.put(t.getOffset(), 0);
      map.put(t.getOffset() + t.getLength(), 0);
    }

    // Get uniqe and sorted list of all edge position of tokens.
    Integer[] offsets = map.keySet().toArray(new Integer[0]);
    Arrays.sort(offsets);

    // setup all value of map.  It specify N-th position from begin.
    for (int i = 0; i < offsets.length; ++i) {
      map.put(offsets[i], i);
    }

    // We got all position length now.
    for (Token t : pending) {
      t.setPositionLength(map.get(t.getOffset() + t.getLength()) - map.get(t.getOffset()));
    }

    // Make PENDING to be reversed order to fit its usage.
    // If you would like to speedup, you can try reversed order sort
    // at first of this function.
    Collections.reverse(pending);
  }

  Dictionary<? extends JaMorphData> getDict(TokenType type) {
    return dictionaryMap.get(type);
  }

  public void setNBestCost(int value) {
    nBestCost = value;
    outputNBest = 0 < nBestCost;
  }

  public int getNBestCost() {
    return nBestCost;
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

  public static class PositionNBest extends Viterbi.Position {
    // Only used when finding 2nd best segmentation under a
    // too-long token:
    int forwardCount;
    int[] forwardPos = new int[8];
    int[] forwardID = new int[8];
    int[] forwardIndex = new int[8];
    TokenType[] forwardType = new TokenType[8];

    public void growForward() {
      forwardPos = ArrayUtil.grow(forwardPos, 1 + forwardCount);
      forwardID = ArrayUtil.grow(forwardID, 1 + forwardCount);
      forwardIndex = ArrayUtil.grow(forwardIndex, 1 + forwardCount);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final TokenType[] newForwardType = new TokenType[forwardPos.length];
      System.arraycopy(forwardType, 0, newForwardType, 0, forwardType.length);
      forwardType = newForwardType;
    }

    public void addForward(int forwardPos, int forwardIndex, int forwardID, TokenType forwardType) {
      if (forwardCount == this.forwardID.length) {
        growForward();
      }
      this.forwardPos[forwardCount] = forwardPos;
      this.forwardIndex[forwardCount] = forwardIndex;
      this.forwardID[forwardCount] = forwardID;
      this.forwardType[forwardCount] = forwardType;
      forwardCount++;
    }

    public void reset() {
      super.reset();
      // forwardCount naturally resets after it runs:
      assert forwardCount == 0 : "pos=" + getPos() + " forwardCount=" + forwardCount;
    }

  }

  // yet another lattice data structure
  static final class Lattice {
    char[] fragment;
    EnumMap<TokenType, Dictionary<? extends JaMorphData>> dictionaryMap;
    boolean useEOS;

    int rootCapacity = 0;
    int rootSize = 0;
    int rootBase = 0;

    // root pointers of node chain by leftChain_ that have same start offset.
    int[] lRoot;
    // root pointers of node chain by rightChain_ that have same end offset.
    int[] rRoot;

    int capacity = 0;
    int nodeCount = 0;

    // The variables below are elements of lattice node that indexed by node number.
    TokenType[] nodeDicType;
    int[] nodeWordID;
    // nodeMark - -1:excluded, 0:unused, 1:bestpath, 2:2-best-path, ... N:N-best-path
    int[] nodeMark;
    int[] nodeLeftID;
    int[] nodeRightID;
    int[] nodeWordCost;
    int[] nodeLeftCost;
    int[] nodeRightCost;
    // nodeLeftNode, nodeRightNode - are left/right node number with minimum cost path.
    int[] nodeLeftNode;
    int[] nodeRightNode;
    // nodeLeft, nodeRight - start/end offset
    int[] nodeLeft;
    int[] nodeRight;
    int[] nodeLeftChain;
    int[] nodeRightChain;

    private void setupRoot(int baseOffset, int lastOffset) {
      assert baseOffset <= lastOffset;
      int size = lastOffset - baseOffset + 1;
      if (rootCapacity < size) {
        int oversize = ArrayUtil.oversize(size, Integer.BYTES);
        lRoot = new int[oversize];
        rRoot = new int[oversize];
        rootCapacity = oversize;
      }
      Arrays.fill(lRoot, 0, size, -1);
      Arrays.fill(rRoot, 0, size, -1);
      rootSize = size;
      rootBase = baseOffset;
    }

    // Reserve at least N nodes.
    private void reserve(int n) {
      if (capacity < n) {
        int oversize = ArrayUtil.oversize(n, Integer.BYTES);
        nodeDicType = new TokenType[oversize];
        nodeWordID = new int[oversize];
        nodeMark = new int[oversize];
        nodeLeftID = new int[oversize];
        nodeRightID = new int[oversize];
        nodeWordCost = new int[oversize];
        nodeLeftCost = new int[oversize];
        nodeRightCost = new int[oversize];
        nodeLeftNode = new int[oversize];
        nodeRightNode = new int[oversize];
        nodeLeft = new int[oversize];
        nodeRight = new int[oversize];
        nodeLeftChain = new int[oversize];
        nodeRightChain = new int[oversize];
        capacity = oversize;
      }
    }

    private void setupNodePool(int n) {
      reserve(n);
      nodeCount = 0;
      if (VERBOSE) {
        System.out.printf("DEBUG: setupNodePool: n = %d\n", n);
        System.out.printf("DEBUG: setupNodePool: lattice.capacity = %d\n", capacity);
      }
    }

    private int addNode(TokenType dicType, int wordID, int left, int right) {
      if (VERBOSE) {
        System.out.printf(
          "DEBUG: addNode: dicType=%s, wordID=%d, left=%d, right=%d, str=%s\n",
          dicType.toString(),
          wordID,
          left,
          right,
          left == -1 ? "BOS" : right == -1 ? "EOS" : new String(fragment, left, right - left));
      }
      assert nodeCount < capacity;
      assert left == -1 || right == -1 || left < right;
      assert left == -1 || (0 <= left && left < rootSize);
      assert right == -1 || (0 <= right && right < rootSize);

      int node = nodeCount++;

      if (VERBOSE) {
        System.out.printf("DEBUG: addNode: node=%d\n", node);
      }

      nodeDicType[node] = dicType;
      nodeWordID[node] = wordID;
      nodeMark[node] = 0;

      if (wordID < 0) {
        nodeWordCost[node] = 0;
        nodeLeftCost[node] = 0;
        nodeRightCost[node] = 0;
        nodeLeftID[node] = 0;
        nodeRightID[node] = 0;
      } else {
        Dictionary<? extends JaMorphData> dic = dictionaryMap.get(dicType);
        nodeWordCost[node] = dic.getWordCost(wordID);
        nodeLeftID[node] = dic.getLeftId(wordID);
        nodeRightID[node] = dic.getRightId(wordID);
      }

      if (VERBOSE) {
        System.out.printf(
          "DEBUG: addNode: wordCost=%d, leftID=%d, rightID=%d\n",
          nodeWordCost[node], nodeLeftID[node], nodeRightID[node]);
      }

      nodeLeft[node] = left;
      nodeRight[node] = right;
      if (0 <= left) {
        nodeLeftChain[node] = lRoot[left];
        lRoot[left] = node;
      } else {
        nodeLeftChain[node] = -1;
      }
      if (0 <= right) {
        nodeRightChain[node] = rRoot[right];
        rRoot[right] = node;
      } else {
        nodeRightChain[node] = -1;
      }
      return node;
    }

    // Sum of positions.get(i).count in [beg, end) range.
    // using stream:
    //   return IntStream.range(beg, end).map(i -> positions.get(i).count).sum();
    private int positionCount(WrappedPositionArray<PositionNBest> positions, int beg, int end) {
      int count = 0;
      for (int i = beg; i < end; ++i) {
        count += positions.get(i).getCount();
      }
      return count;
    }

    void setup(
      char[] fragment,
      EnumMap<TokenType, Dictionary<? extends JaMorphData>> dictionaryMap,
      WrappedPositionArray<PositionNBest> positions,
      int prevOffset,
      int endOffset,
      boolean useEOS) {
      assert positions.get(prevOffset).getCount() == 1;
      if (VERBOSE) {
        System.out.printf("DEBUG: setup: prevOffset=%d, endOffset=%d\n", prevOffset, endOffset);
      }

      this.fragment = fragment;
      this.dictionaryMap = dictionaryMap;
      this.useEOS = useEOS;

      // Initialize lRoot and rRoot.
      setupRoot(prevOffset, endOffset);

      // "+ 2" for first/last record.
      setupNodePool(positionCount(positions, prevOffset + 1, endOffset + 1) + 2);

      // substitute for BOS = 0
      Position first = positions.get(prevOffset);
      if (addNode(first.getBackType(0), first.getBackID(0), -1, 0) != 0) {
        assert false;
      }

      // EOS = 1
      if (addNode(TokenType.KNOWN, -1, endOffset - rootBase, -1) != 1) {
        assert false;
      }

      for (int offset = endOffset; prevOffset < offset; --offset) {
        int right = offset - rootBase;
        // optimize: exclude disconnected nodes.
        if (0 <= lRoot[right]) {
          Position pos = positions.get(offset);
          for (int i = 0; i < pos.getCount(); ++i) {
            addNode(pos.getBackType(i), pos.getBackID(i), pos.getBackPos(i) - rootBase, right);
          }
        }
      }
    }

    // set mark = -1 for unreachable nodes.
    void markUnreachable() {
      for (int index = 1; index < rootSize - 1; ++index) {
        if (rRoot[index] < 0) {
          for (int node = lRoot[index]; 0 <= node; node = nodeLeftChain[node]) {
            if (VERBOSE) {
              System.out.printf("DEBUG: markUnreachable: node=%d\n", node);
            }
            nodeMark[node] = -1;
          }
        }
      }
    }

    int connectionCost(ConnectionCosts costs, int left, int right) {
      int leftID = nodeLeftID[right];
      return ((leftID == 0 && !useEOS) ? 0 : costs.get(nodeRightID[left], leftID));
    }

    void calcLeftCost(ConnectionCosts costs) {
      for (int index = 0; index < rootSize; ++index) {
        for (int node = lRoot[index]; 0 <= node; node = nodeLeftChain[node]) {
          if (0 <= nodeMark[node]) {
            int leastNode = -1;
            int leastCost = Integer.MAX_VALUE;
            for (int leftNode = rRoot[index]; 0 <= leftNode; leftNode = nodeRightChain[leftNode]) {
              if (0 <= nodeMark[leftNode]) {
                int cost =
                  nodeLeftCost[leftNode]
                    + nodeWordCost[leftNode]
                    + connectionCost(costs, leftNode, node);
                if (cost < leastCost) {
                  leastCost = cost;
                  leastNode = leftNode;
                }
              }
            }
            assert 0 <= leastNode;
            nodeLeftNode[node] = leastNode;
            nodeLeftCost[node] = leastCost;
            if (VERBOSE) {
              System.out.printf(
                "DEBUG: calcLeftCost: node=%d, leftNode=%d, leftCost=%d\n",
                node, nodeLeftNode[node], nodeLeftCost[node]);
            }
          }
        }
      }
    }

    void calcRightCost(ConnectionCosts costs) {
      for (int index = rootSize - 1; 0 <= index; --index) {
        for (int node = rRoot[index]; 0 <= node; node = nodeRightChain[node]) {
          if (0 <= nodeMark[node]) {
            int leastNode = -1;
            int leastCost = Integer.MAX_VALUE;
            for (int rightNode = lRoot[index];
                 0 <= rightNode;
                 rightNode = nodeLeftChain[rightNode]) {
              if (0 <= nodeMark[rightNode]) {
                int cost =
                  nodeRightCost[rightNode]
                    + nodeWordCost[rightNode]
                    + connectionCost(costs, node, rightNode);
                if (cost < leastCost) {
                  leastCost = cost;
                  leastNode = rightNode;
                }
              }
            }
            assert 0 <= leastNode;
            nodeRightNode[node] = leastNode;
            nodeRightCost[node] = leastCost;
            if (VERBOSE) {
              System.out.printf(
                "DEBUG: calcRightCost: node=%d, rightNode=%d, rightCost=%d\n",
                node, nodeRightNode[node], nodeRightCost[node]);
            }
          }
        }
      }
    }

    // Mark all nodes that have same text and different par-of-speech or reading.
    void markSameSpanNode(int refNode, int value) {
      int left = nodeLeft[refNode];
      int right = nodeRight[refNode];
      for (int node = lRoot[left]; 0 <= node; node = nodeLeftChain[node]) {
        if (nodeRight[node] == right) {
          nodeMark[node] = value;
        }
      }
    }

    List<Integer> bestPathNodeList() {
      List<Integer> list = new ArrayList<>();
      for (int node = nodeRightNode[0]; node != 1; node = nodeRightNode[node]) {
        list.add(node);
        markSameSpanNode(node, 1);
      }
      return list;
    }

    private int cost(int node) {
      return nodeLeftCost[node] + nodeWordCost[node] + nodeRightCost[node];
    }

    List<Integer> nBestNodeList(int N) {
      List<Integer> list = new ArrayList<>();
      int leastCost = Integer.MAX_VALUE;
      int leastLeft = -1;
      int leastRight = -1;
      for (int node = 2; node < nodeCount; ++node) {
        if (nodeMark[node] == 0) {
          int cost = cost(node);
          if (cost < leastCost) {
            leastCost = cost;
            leastLeft = nodeLeft[node];
            leastRight = nodeRight[node];
            list.clear();
            list.add(node);
          } else if (cost == leastCost
            && (nodeLeft[node] != leastLeft || nodeRight[node] != leastRight)) {
            list.add(node);
          }
        }
      }
      for (int node : list) {
        markSameSpanNode(node, N);
      }
      return list;
    }

    int bestCost() {
      return nodeLeftCost[1];
    }

    int probeDelta(int start, int end) {
      int left = start - rootBase;
      int right = end - rootBase;
      if (left < 0 || rootSize < right) {
        return Integer.MAX_VALUE;
      }
      int probedCost = Integer.MAX_VALUE;
      for (int node = lRoot[left]; 0 <= node; node = nodeLeftChain[node]) {
        if (nodeRight[node] == right) {
          probedCost = Math.min(probedCost, cost(node));
        }
      }
      return probedCost - bestCost();
    }

    void debugPrint() {
      if (VERBOSE) {
        for (int node = 0; node < nodeCount; ++node) {
          System.out.printf(
            "DEBUG NODE: node=%d, mark=%d, cost=%d, left=%d, right=%d\n",
            node, nodeMark[node], cost(node), nodeLeft[node], nodeRight[node]);
        }
      }
    }
  }

}
