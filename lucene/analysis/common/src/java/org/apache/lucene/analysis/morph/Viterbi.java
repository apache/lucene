package org.apache.lucene.analysis.morph;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import org.apache.lucene.analysis.util.RollingCharBuffer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST;

public abstract class Viterbi<T extends Token> {
  protected static final boolean VERBOSE = false;

  // For safety:
  protected static final int MAX_UNKNOWN_WORD_LENGTH = 1024;
  protected static final int MAX_BACKTRACE_GAP = 1024;

  protected final TokenInfoFST fst;
  protected final BinaryDictionary<? extends MorphData> dictionary;
  protected final ConnectionCosts costs;
  protected final Dictionary<? extends MorphData> userDictionary;

  protected final FST.Arc<Long> arc = new FST.Arc<>();
  protected final FST.BytesReader fstReader;
  protected final IntsRef wordIdRef = new IntsRef();

  protected final FST.BytesReader userFSTReader;
  protected final TokenInfoFST userFST;

  protected final RollingCharBuffer buffer = new RollingCharBuffer();

  protected final WrappedPositionArray positions = new WrappedPositionArray();

  // True once we've hit the EOF from the input reader:
  protected boolean end;

  // Last absolute position we backtraced from:
  protected int lastBackTracePos;

  // Next absolute position to process:
  protected int pos;

  // Already parsed, but not yet passed to caller, tokens:
  protected final List<T> pending = new ArrayList<>();

  protected Viterbi(
      TokenInfoFST fst,
      FST.BytesReader fstReader,
      BinaryDictionary<? extends MorphData> dictionary,
      TokenInfoFST userFST,
      FST.BytesReader userFSTReader,
      Dictionary<? extends MorphData> userDictionary,
      ConnectionCosts costs
      ) {
    this.fst = fst;
    this.fstReader = fstReader;
    this.dictionary = dictionary;
    this.userFST = userFST;
    this.userFSTReader = userFSTReader;
    this.userDictionary = userDictionary;
    this.costs = costs;
  }


  /* Incrementally parse some more characters.  This runs
   * the viterbi search forwards "enough" so that we
   * generate some more tokens.  How much forward depends on
   * the chars coming in, since some chars could cause
   * longer-lasting ambiguity in the parsing.  Once the
   * ambiguity is resolved, then we back trace, produce
   * the pending tokens, and return. */
  public void forward() throws IOException {
    if (VERBOSE) {
      System.out.println("\nPARSE");
    }

    // Index of the last character of unknown word:
    int unknownWordEndIndex = -1;

    // Maximum posAhead of user word in the entire input
    int userWordMaxPosAhead = -1;

    // Advances over each position (character):
    while (buffer.get(pos) != -1) {
      final Position posData = positions.get(pos);
      final boolean isFrontier = positions.getNextPos() == pos + 1;

      if (posData.count == 0) {
        // No arcs arrive here; move to next position:
        if (VERBOSE) {
          System.out.println("    no arcs in; skip pos=" + pos);
        }
        pos++;
        continue;
      }

      if (pos > lastBackTracePos && posData.count == 1 && isFrontier) {
        // We are at a "frontier", and only one node is
        // alive, so whatever the eventual best path is must
        // come through this node.  So we can safely commit
        // to the prefix of the best path at this point:
        backtrace(posData, 0);

        // Re-base cost so we don't risk int overflow:
        posData.costs[0] = 0;
        if (pending.size() > 0) {
          return;
        } else {
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
        }
      }

      if (pos - lastBackTracePos >= MAX_BACKTRACE_GAP) {
        // Safety: if we've buffered too much, force a
        // backtrace now.  We find the least-cost partial
        // path, across all paths, backtrace from it, and
        // then prune all others.  Note that this, in
        // general, can produce the wrong result, if the
        // total best path did not in fact back trace
        // through this partial best path.  But it's the
        // best we can do... (short of not having a
        // safety!).

        // First pass: find least cost partial path so far,
        // including ending at future positions:
        int leastIDX = -1;
        int leastCost = Integer.MAX_VALUE;
        Position leastPosData = null;
        for (int pos2 = pos; pos2 < positions.getNextPos(); pos2++) {
          final Position posData2 = positions.get(pos2);
          for (int idx = 0; idx < posData2.count; idx++) {
            // System.out.println("    idx=" + idx + " cost=" + cost);
            final int cost = posData2.costs[idx];
            if (cost < leastCost) {
              leastCost = cost;
              leastIDX = idx;
              leastPosData = posData2;
            }
          }
        }

        // We will always have at least one live path:
        assert leastIDX != -1;

        // Second pass: prune all but the best path:
        for (int pos2 = pos; pos2 < positions.getNextPos(); pos2++) {
          final Position posData2 = positions.get(pos2);
          if (posData2 != leastPosData) {
            posData2.reset();
          } else {
            if (leastIDX != 0) {
              posData2.costs[0] = posData2.costs[leastIDX];
              posData2.lastRightID[0] = posData2.lastRightID[leastIDX];
              posData2.backPos[0] = posData2.backPos[leastIDX];
              posData2.backWordPos[0] = posData2.backWordPos[leastIDX];
              posData2.backIndex[0] = posData2.backIndex[leastIDX];
              posData2.backID[0] = posData2.backID[leastIDX];
              posData2.backType[0] = posData2.backType[leastIDX];
            }
            posData2.count = 1;
          }
        }

        backtrace(leastPosData, 0);

        // Re-base cost so we don't risk int overflow:
        Arrays.fill(leastPosData.costs, 0, leastPosData.count, 0);

        if (pos != leastPosData.pos) {
          // We jumped into a future position:
          assert pos < leastPosData.pos;
          pos = leastPosData.pos;
        }
        if (pending.size() > 0) {
          return;
        } else {
          // This means the backtrace only produced
          // punctuation tokens, so we must keep parsing.
          continue;
        }
      }

      if (VERBOSE) {
        System.out.println(
            "\n  extend @ pos="
                + pos
                + " char="
                + (char) buffer.get(pos)
                + " hex="
                + Integer.toHexString(buffer.get(pos)));
      }

      if (VERBOSE) {
        System.out.println("    " + posData.count + " arcs in");
      }

      // We add single space separator as prefixes of the terms that we extract.
      // This information is needed to compute the space penalty factor of each term.
      // These whitespace prefixes are removed when the final tokens are generated, or
      // added as separated tokens when discardPunctuation is unset.
      if (Character.getType(buffer.get(pos)) == Character.SPACE_SEPARATOR) {
        if (buffer.get(++pos) == -1) {
          pos = posData.pos;
        }
      }

      boolean anyMatches = false;

      // First try user dict:
      if (userFST != null) {
        userFST.getFirstArc(arc);
        int output = 0;
        int maxPosAhead = 0;
        int outputMaxPosAhead = 0;
        int arcFinalOutMaxPosAhead = 0;

        for (int posAhead = pos; ; posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          if (userFST.findTargetArc(ch, arc, arc, posAhead == pos, userFSTReader) == null) {
            break;
          }
          output += arc.output().intValue();
          if (arc.isFinal()) {
            maxPosAhead = posAhead;
            outputMaxPosAhead = output;
            arcFinalOutMaxPosAhead = arc.nextFinalOutput().intValue();
            anyMatches = true;
          }
        }

        // Longest matching for user word
        if (anyMatches && maxPosAhead > userWordMaxPosAhead) {
          if (VERBOSE) {
            System.out.println(
                "    USER word "
                    + new String(buffer.get(pos, maxPosAhead + 1))
                    + " toPos="
                    + (maxPosAhead + 1));
          }
          add(
              userDictionary.getMorphAttributes(),
              posData,
              pos,
              maxPosAhead + 1,
              outputMaxPosAhead + arcFinalOutMaxPosAhead,
              TokenType.USER);
          userWordMaxPosAhead = Math.max(userWordMaxPosAhead, maxPosAhead);
        }
      }

      // TODO: we can be more aggressive about user
      // matches?  if we are "under" a user match then don't
      // extend KNOWN/UNKNOWN paths?

      if (!anyMatches) {
        // Next, try known dictionary matches
        fst.getFirstArc(arc);
        int output = 0;

        for (int posAhead = pos; ; posAhead++) {
          final int ch = buffer.get(posAhead);
          if (ch == -1) {
            break;
          }
          // System.out.println("    match " + (char) ch + " posAhead=" + posAhead);

          if (fst.findTargetArc(ch, arc, arc, posAhead == pos, fstReader) == null) {
            break;
          }

          output += arc.output().intValue();

          // Optimization: for known words that are too-long
          // (compound), we should pre-compute the 2nd
          // best segmentation and store it in the
          // dictionary instead of recomputing it each time a
          // match is found.

          if (arc.isFinal()) {
            dictionary.lookupWordIds(output + arc.nextFinalOutput().intValue(), wordIdRef);
            if (VERBOSE) {
              System.out.println(
                  "    KNOWN word "
                      + new String(buffer.get(pos, posAhead - pos + 1))
                      + " toPos="
                      + (posAhead + 1)
                      + " "
                      + wordIdRef.length
                      + " wordIDs");
            }
            for (int ofs = 0; ofs < wordIdRef.length; ofs++) {
              add(
                  dictionary.getMorphAttributes(),
                  posData,
                  pos,
                  posAhead + 1,
                  wordIdRef.ints[wordIdRef.offset + ofs],
                  TokenType.KNOWN);
              anyMatches = true;
            }
          }
        }
      }

      if (!shouldSkipProcessUnknownWord(unknownWordEndIndex, posData)) {
        processUnknownWord(anyMatches, posData);
      }
      pos++;
    }

    end = true;

    if (pos > 0) {

      final Position endPosData = positions.get(pos);
      int leastCost = Integer.MAX_VALUE;
      int leastIDX = -1;
      if (VERBOSE) {
        System.out.println("  end: " + endPosData.count + " nodes");
      }
      for (int idx = 0; idx < endPosData.count; idx++) {
        // Add EOS cost:
        final int cost = endPosData.costs[idx] + costs.get(endPosData.lastRightID[idx], 0);
        // System.out.println("    idx=" + idx + " cost=" + cost + " (pathCost=" +
        // endPosData.costs[idx] + " bgCost=" + costs.get(endPosData.lastRightID[idx], 0) + ")
        // backPos=" + endPosData.backPos[idx]);
        if (cost < leastCost) {
          leastCost = cost;
          leastIDX = idx;
        }
      }

      backtrace(endPosData, leastIDX);
    } else {
      // No characters in the input string; return no tokens!
    }
  }

  protected boolean shouldSkipProcessUnknownWord(int unknownWordEndIndex, Position posData) {
    return unknownWordEndIndex > posData.pos;
  }

  protected abstract void processUnknownWord(boolean anyMatches, Position posData)
      throws IOException;

  // Backtrace from the provided position, back to the last
  // time we back-traced, accumulating the resulting tokens to
  // the pending list.  The pending list is then in-reverse
  // (last token should be returned first).
  protected abstract void backtrace(final Position endPosData, final int fromIDX);

  protected void add(
      MorphData morphData,
      Position fromPosData,
      int wordPos,
      int endPos,
      int wordID,
      TokenType type) {
    final int wordCost = morphData.getWordCost(wordID);
    final int leftID = morphData.getLeftId(wordID);
    int leastCost = Integer.MAX_VALUE;
    int leastIDX = -1;
    assert fromPosData.count > 0;
    for (int idx = 0; idx < fromPosData.count; idx++) {
      // The number of spaces before the term
      int numSpaces = wordPos - fromPosData.pos;

      // Cost is path cost so far, plus word cost (added at
      // end of loop), plus bigram cost and space penalty cost.
      final int cost =
          fromPosData.costs[idx]
              + costs.get(fromPosData.lastRightID[idx], leftID)
              + computeSpacePenalty(morphData, wordID, numSpaces);
      if (VERBOSE) {
        System.out.println(
            "      fromIDX="
                + idx
                + ": cost="
                + cost
                + " (prevCost="
                + fromPosData.costs[idx]
                + " wordCost="
                + wordCost
                + " bgCost="
                + costs.get(fromPosData.lastRightID[idx], leftID)
                + " spacePenalty="
                + computeSpacePenalty(morphData, wordID, numSpaces)
                + ") leftID="
                + leftID
                // + " leftPOS="
                // + leftPOS.name()
                + ")");
      }
      if (cost < leastCost) {
        leastCost = cost;
        leastIDX = idx;
        if (VERBOSE) {
          System.out.println("        **");
        }
      }
    }

    leastCost += wordCost;

    if (VERBOSE) {
      System.out.println(
          "      + cost="
              + leastCost
              + " wordID="
              + wordID
              + " leftID="
              + leftID
              + " leastIDX="
              + leastIDX
              + " toPos="
              + endPos
              + " toPos.idx="
              + positions.get(endPos).count);
    }

    positions
        .get(endPos)
        .add(
            leastCost,
            morphData.getRightId(wordID),
            fromPosData.pos,
            wordPos,
            leastIDX,
            wordID,
            type);
  }

  /** Returns the space penalty. */
  protected int computeSpacePenalty(MorphData morphData, int wordID, int numSpaces) {
    return 0;
  }

  public int getPos() {
    return pos;
  }

  public boolean isEnd() {
    return end;
  }

  public List<T> getPending() {
    return pending;
  }

  public void resetBuffer(Reader reader) {
    buffer.reset(reader);
  }

  public void resetState() {
    positions.reset();
    pos = 0;
    end = false;
    lastBackTracePos = 0;
    pending.clear();

    // Add BOS:
    positions.get(0).add(0, 0, -1, -1, -1, -1, TokenType.KNOWN);
  }

  // Holds all back pointers arriving to this position:
  public static class Position {

    int pos;

    int count;

    // maybe single int array * 5?
    int[] costs = new int[8];
    int[] lastRightID = new int[8];
    int[] backPos = new int[8];
    int[] backWordPos = new int[8];
    int[] backIndex = new int[8];
    int[] backID = new int[8];
    TokenType[] backType = new TokenType[8];

    public void grow() {
      costs = ArrayUtil.grow(costs, 1 + count);
      lastRightID = ArrayUtil.grow(lastRightID, 1 + count);
      backPos = ArrayUtil.grow(backPos, 1 + count);
      backWordPos = ArrayUtil.grow(backWordPos, 1 + count);
      backIndex = ArrayUtil.grow(backIndex, 1 + count);
      backID = ArrayUtil.grow(backID, 1 + count);

      // NOTE: sneaky: grow separately because
      // ArrayUtil.grow will otherwise pick a different
      // length than the int[]s we just grew:
      final TokenType[] newBackType = new TokenType[backID.length];
      System.arraycopy(backType, 0, newBackType, 0, backType.length);
      backType = newBackType;
    }

    public void add(
        int cost,
        int lastRightID,
        int backPos,
        int backRPos,
        int backIndex,
        int backID,
        TokenType backType) {
      // NOTE: this isn't quite a true Viterbi search,
      // because we should check if lastRightID is
      // already present here, and only update if the new
      // cost is less than the current cost, instead of
      // simply appending.  However, that will likely hurt
      // performance (usually we add a lastRightID only once),
      // and it means we actually create the full graph
      // intersection instead of a "normal" Viterbi lattice:
      if (count == costs.length) {
        grow();
      }
      this.costs[count] = cost;
      this.lastRightID[count] = lastRightID;
      this.backPos[count] = backPos;
      this.backWordPos[count] = backRPos;
      this.backIndex[count] = backIndex;
      this.backID[count] = backID;
      this.backType[count] = backType;
      count++;
    }

    public void reset() {
      count = 0;
    }

    public int getPos() {
      return pos;
    }

    public int getCount() {
      return count;
    }

    public int getCost(int index) {
      return costs[index];
    }

    public int getBackPos(int index) {
      return backPos[index];
    }

    public int getBackWordPos(int index) {
      return backWordPos[index];
    }

    public int getBackID(int index) {
      return backID[index];
    }

    public int getBackIndex(int index) {
      return backIndex[index];
    }

    public TokenType getBackType(int index) {
      return backType[index];
    }

    public int getLastRightID(int index) {
      return lastRightID[index];
    }
  }

  // TODO: make generic'd version of this "circular array"?
  // It's a bit tricky because we do things to the Position
  // (eg, set .pos = N on reuse)...
  public static final class WrappedPositionArray {
    private Position[] positions = new Position[8];

    public WrappedPositionArray() {
      for (int i = 0; i < positions.length; i++) {
        positions[i] = new Position();
      }
    }

    // Next array index to write to in positions:
    private int nextWrite;

    // Next position to write:
    private int nextPos;

    // How many valid Position instances are held in the
    // positions array:
    private int count;

    public void reset() {
      nextWrite--;
      while (count > 0) {
        if (nextWrite == -1) {
          nextWrite = positions.length - 1;
        }
        positions[nextWrite--].reset();
        count--;
      }
      nextWrite = 0;
      nextPos = 0;
      count = 0;
    }

    /**
     * Get Position instance for this absolute position; this is allowed to be arbitrarily far "in
     * the future" but cannot be before the last freeBefore.
     */
    public Position get(int pos) {
      while (pos >= nextPos) {
        // System.out.println("count=" + count + " vs len=" + positions.length);
        if (count == positions.length) {
          Position[] newPositions =
              new Position[ArrayUtil.oversize(1 + count, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
          // System.out.println("grow positions " + newPositions.length);
          System.arraycopy(positions, nextWrite, newPositions, 0, positions.length - nextWrite);
          System.arraycopy(positions, 0, newPositions, positions.length - nextWrite, nextWrite);
          for (int i = positions.length; i < newPositions.length; i++) {
            newPositions[i] = new Position();
          }
          nextWrite = positions.length;
          positions = newPositions;
        }
        if (nextWrite == positions.length) {
          nextWrite = 0;
        }
        // Should have already been reset:
        assert positions[nextWrite].count == 0;
        positions[nextWrite++].pos = nextPos++;
        count++;
      }
      assert inBounds(pos);
      final int index = getIndex(pos);
      assert positions[index].pos == pos;
      return positions[index];
    }

    public int getNextPos() {
      return nextPos;
    }

    // For assert:
    private boolean inBounds(int pos) {
      return pos < nextPos && pos >= nextPos - count;
    }

    private int getIndex(int pos) {
      int index = nextWrite - (nextPos - pos);
      if (index < 0) {
        index += positions.length;
      }
      return index;
    }

    public void freeBefore(int pos) {
      final int toFree = count - (nextPos - pos);
      assert toFree >= 0;
      assert toFree <= count;
      int index = nextWrite - count;
      if (index < 0) {
        index += positions.length;
      }
      for (int i = 0; i < toFree; i++) {
        if (index == positions.length) {
          index = 0;
        }
        // System.out.println("  fb idx=" + index);
        positions[index].reset();
        index++;
      }
      count -= toFree;
    }
  }
}
