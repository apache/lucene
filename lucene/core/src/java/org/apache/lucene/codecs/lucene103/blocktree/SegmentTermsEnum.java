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
package org.apache.lucene.codecs.lucene103.blocktree;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.RamUsageEstimator;

/** Iterates through terms in this field. */
final class SegmentTermsEnum extends BaseTermsEnum {

  // Lazy init:
  IndexInput in;
  private SegmentTermsEnumFrame[] stack = new SegmentTermsEnumFrame[0];
  private final SegmentTermsEnumFrame staticFrame;
  SegmentTermsEnumFrame currentFrame;
  boolean termExists;
  final FieldReader fr;
  private int targetBeforeCurrentLength;

  // static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // What prefix of the current term was present in the index; when we only next() through the
  // index, this stays at 0.  It's only set when
  // we seekCeil/Exact:
  private int validIndexPrefix;

  // assert only:
  private boolean eof;

  final BytesRefBuilder term = new BytesRefBuilder();
  private final TrieReader trieReader;
  private TrieReader.Node[] nodes = new TrieReader.Node[1];

  public SegmentTermsEnum(FieldReader fr, TrieReader reader) throws IOException {
    this.fr = fr;
    // Used to hold seek by TermState, or cached seek
    staticFrame = new SegmentTermsEnumFrame(this, -1);
    trieReader = reader;
    currentFrame = staticFrame;
    nodes[0] = trieReader.root;

    // currentFrame = pushFrame(arc, rootCode, 0);
    // currentFrame.loadBlock();
    validIndexPrefix = 0;
    // if (DEBUG) {
    //   System.out.println("init frame state " + currentFrame.ord);
    //   printSeekState();
    // }

    // System.out.println();
    // computeBlockStats().print(System.out);
  }

  // Not private to avoid synthetic access$NNN methods
  void initIndexInput() {
    if (this.in == null) {
      this.in = fr.parent.termsIn.clone();
    }
  }

  /** Runs next() through the entire terms dict, computing aggregate statistics. */
  public Stats computeBlockStats() throws IOException {
    Stats stats = new Stats(fr.parent.segment, fr.fieldInfo.name);

    currentFrame = staticFrame;
    TrieReader.Node node = nodes[0] = trieReader.root;

    // Empty string prefix must have an output in the
    // index!
    currentFrame = pushFrame(node, 0);
    currentFrame.fpOrig = currentFrame.fp;
    currentFrame.loadBlock();
    validIndexPrefix = 0;

    stats.startBlock(currentFrame, !currentFrame.isLastInFloor);

    allTerms:
    while (true) {

      // Pop finished blocks
      while (currentFrame.nextEnt == currentFrame.entCount) {
        stats.endBlock(currentFrame);
        if (!currentFrame.isLastInFloor) {
          // Advance to next floor block
          currentFrame.loadNextFloorBlock();
          stats.startBlock(currentFrame, true);
          break;
        } else {
          if (currentFrame.ord == 0) {
            break allTerms;
          }
          final long lastFP = currentFrame.fpOrig;
          currentFrame = stack[currentFrame.ord - 1];
          assert lastFP == currentFrame.lastSubFP;
          // if (DEBUG) {
          //   System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
          // }
        }
      }

      while (true) {
        if (currentFrame.next()) {
          // Push to new block:
          currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length());
          currentFrame.fpOrig = currentFrame.fp;
          // This is a "next" frame -- even if it's
          // floor'd we must pretend it isn't so we don't
          // try to scan to the right floor frame:
          currentFrame.loadBlock();
          stats.startBlock(currentFrame, !currentFrame.isLastInFloor);
        } else {
          stats.term(term.get());
          break;
        }
      }
    }

    stats.finish();

    // Put root frame back:
    currentFrame = staticFrame;

    node = nodes[0] = trieReader.root;
    // Empty string prefix must have an output in the index!
    assert node.hasOutput();

    currentFrame = pushFrame(node, 0);
    currentFrame.rewind();
    currentFrame.loadBlock();
    validIndexPrefix = 0;
    term.clear();

    return stats;
  }

  private SegmentTermsEnumFrame getFrame(int ord) throws IOException {
    if (ord >= stack.length) {
      final SegmentTermsEnumFrame[] next =
          new SegmentTermsEnumFrame
              [ArrayUtil.oversize(1 + ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(stack, 0, next, 0, stack.length);
      for (int stackOrd = stack.length; stackOrd < next.length; stackOrd++) {
        next[stackOrd] = new SegmentTermsEnumFrame(this, stackOrd);
      }
      stack = next;
    }
    assert stack[ord].ord == ord;
    return stack[ord];
  }

  private TrieReader.Node getNode(int ord) {
    if (ord >= nodes.length) {
      final TrieReader.Node[] next =
          new TrieReader.Node[ArrayUtil.oversize(1 + ord, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(nodes, 0, next, 0, nodes.length);
      for (int nodeOrd = nodes.length; nodeOrd < next.length; nodeOrd++) {
        next[nodeOrd] = new TrieReader.Node();
      }
      nodes = next;
    }
    return nodes[ord];
  }

  // Pushes a frame we seek'd to
  SegmentTermsEnumFrame pushFrame(TrieReader.Node node, int length) throws IOException {
    final SegmentTermsEnumFrame f = getFrame(1 + currentFrame.ord);
    f.hasTerms = node.hasTerms;
    f.hasTermsOrig = f.hasTerms;
    f.isFloor = node.isFloor();
    if (f.isFloor) {
      f.setFloorData(node.floorData(trieReader));
    }
    pushFrame(node, node.outputFp, length);

    return f;
  }

  // Pushes next'd frame or seek'd frame; we later
  // lazy-load the frame only when needed
  SegmentTermsEnumFrame pushFrame(TrieReader.Node node, long fp, int length) throws IOException {
    final SegmentTermsEnumFrame f = getFrame(1 + currentFrame.ord);
    f.node = node;
    if (f.fpOrig == fp && f.nextEnt != -1) {
      // if (DEBUG) System.out.println("      push reused frame ord=" + f.ord + " fp=" + f.fp +
      // " isFloor?=" + f.isFloor + " hasTerms=" + f.hasTerms + " pref=" + term + " nextEnt=" +
      // f.nextEnt + " targetBeforeCurrentLength=" + targetBeforeCurrentLength + " term.length=" +
      // term.length + " vs prefix=" + f.prefix);
      // if (f.prefix > targetBeforeCurrentLength) {
      if (f.ord > targetBeforeCurrentLength) {
        f.rewind();
      } else {
        // if (DEBUG) {
        //   System.out.println("        skip rewind!");
        // }
      }
      assert length == f.prefixLength;
    } else {
      f.nextEnt = -1;
      f.prefixLength = length;
      f.state.termBlockOrd = 0;
      f.fpOrig = f.fp = fp;
      f.lastSubFP = -1;
      // if (DEBUG) {
      //   final int sav = term.length;
      //   term.length = length;
      //   System.out.println("      push new frame ord=" + f.ord + " fp=" + f.fp + " hasTerms=" +
      // f.hasTerms + " isFloor=" + f.isFloor + " pref=" + ToStringUtils.bytesRefToString(term));
      //   term.length = sav;
      // }
    }

    return f;
  }

  // asserts only
  private boolean clearEOF() {
    eof = false;
    return true;
  }

  // asserts only
  private boolean setEOF() {
    eof = true;
    return true;
  }

  private IOBooleanSupplier prepareSeekExact(BytesRef target, boolean prefetch) throws IOException {
    if (fr.size() > 0 && (target.compareTo(fr.getMin()) < 0 || target.compareTo(fr.getMax()) > 0)) {
      return null;
    }

    term.grow(1 + target.length);

    assert clearEOF();

    // if (DEBUG) {
    //   System.out.println("\nBTTR.seekExact seg=" + fr.parent.segment + " target=" +
    // fr.fieldInfo.name + ":" + ToStringUtils.bytesRefToString(target) + " current=" +
    // ToStringUtils.bytesRefToString(term) +
    // " (exists?=" + termExists + ") validIndexPrefix=" + validIndexPrefix);
    //   printSeekState(System.out);
    // }

    TrieReader.Node node;
    int targetUpto;

    targetBeforeCurrentLength = currentFrame.ord;

    if (currentFrame != staticFrame) {

      // We are already seek'd; find the common
      // prefix of new seek term vs current term and
      // re-use the corresponding seek state.  For
      // example, if app first seeks to foobar, then
      // seeks to foobaz, we can re-use the seek state
      // for the first 5 bytes.

      // if (DEBUG) {
      //   System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
      // }

      node = nodes[0];
      assert node.hasOutput();
      targetUpto = 0;

      SegmentTermsEnumFrame lastFrame = stack[0];
      assert validIndexPrefix <= term.length();

      final int targetLimit = Math.min(target.length, validIndexPrefix);

      int cmp = 0;

      // First compare up to valid seek frames:
      while (targetUpto < targetLimit) {
        cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
        // if (DEBUG) {
        //    System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit
        // + ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) +
        // " vs termLabel=" + (char) (term.bytes[targetUpto]) + ")"   + " node.output=" +
        // node.output
        // + " output=" + output);
        // }
        if (cmp != 0) {
          break;
        }
        node = nodes[1 + targetUpto];
        assert node.label == (target.bytes[target.offset + targetUpto] & 0xFF)
            : "node.label="
                + (char) node.label
                + " targetLabel="
                + (char) (target.bytes[target.offset + targetUpto] & 0xFF);

        if (node.hasOutput()) {
          lastFrame = stack[1 + lastFrame.ord];
        }
        targetUpto++;
      }

      if (cmp == 0) {
        // Second compare the rest of the term, but
        // don't save node/output/frame; we only do this
        // to find out if the target term is before,
        // equal or after the current term
        cmp =
            Arrays.compareUnsigned(
                term.bytes(),
                targetUpto,
                term.length(),
                target.bytes,
                target.offset + targetUpto,
                target.offset + target.length);
      }

      if (cmp < 0) {
        // Common case: target term is after current
        // term, ie, app is seeking multiple terms
        // in sorted order
        // if (DEBUG) {
        //   System.out.println("  target is after current (shares prefixLen=" + targetUpto + ");
        // frame.ord=" + lastFrame.ord);
        // }
        currentFrame = lastFrame;

      } else if (cmp > 0) {
        // Uncommon case: target term
        // is before current term; this means we can
        // keep the currentFrame but we must rewind it
        // (so we scan from the start)
        targetBeforeCurrentLength = lastFrame.ord;
        // if (DEBUG) {
        //   System.out.println("  target is before current (shares prefixLen=" + targetUpto + ");
        // rewind frame ord=" + lastFrame.ord);
        // }
        currentFrame = lastFrame;
        currentFrame.rewind();
      } else {
        // Target is exactly the same as current term
        assert term.length() == target.length;
        if (termExists) {
          // if (DEBUG) {
          //   System.out.println("  target is same as current; return true");
          // }
          return () -> true;
        } else {
          // if (DEBUG) {
          //   System.out.println("  target is same as current but term doesn't exist");
          // }
        }
        // validIndexPrefix = currentFrame.depth;
        // term.length = target.length;
        // return termExists;
      }

    } else {

      targetBeforeCurrentLength = -1;
      node = trieReader.root;

      // Empty string prefix must have an output (block) in the index!
      assert node.hasOutput();

      // if (DEBUG) {
      //   System.out.println("    no seek state; push root frame");
      // }

      currentFrame = staticFrame;

      // term.length = 0;
      targetUpto = 0;
      currentFrame = pushFrame(node, 0);
    }

    // if (DEBUG) {
    //   System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output +
    // " currentFrame.ord=" + currentFrame.ord + " targetBeforeCurrentLength=" +
    // targetBeforeCurrentLength);
    // }

    // We are done sharing the common prefix with the incoming target and where we are currently
    // seek'd; now continue walking the index:
    while (targetUpto < target.length) {

      final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

      final TrieReader.Node nextNode =
          trieReader.lookupChild(targetLabel, node, getNode(1 + targetUpto));

      if (nextNode == null) {

        // Index is exhausted
        // if (DEBUG) {
        //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " +
        // toHex(targetLabel));
        // }

        validIndexPrefix = currentFrame.prefixLength;
        // validIndexPrefix = targetUpto;

        currentFrame.scanToFloorFrame(target);

        if (!currentFrame.hasTerms) {
          termExists = false;
          term.setByteAt(targetUpto, (byte) targetLabel);
          term.setLength(1 + targetUpto);
          // if (DEBUG) {
          //   System.out.println("  FAST NOT_FOUND term=" + ToStringUtils.bytesRefToString(term));
          // }
          return null;
        }

        return getIOBooleanSupplier(target, prefetch);
      } else {
        // Follow this node
        node = nextNode;
        term.setByteAt(targetUpto, (byte) targetLabel);
        // Aggregate output as we go:

        // if (DEBUG) {
        //   System.out.println("    index: follow label=" + toHex(target.bytes[target.offset +
        // targetUpto]&0xff) + " node.output=" + node.output + " node.nfo=" + node.nextFinalOutput);
        // }
        targetUpto++;

        if (node.hasOutput()) {
          // if (DEBUG) System.out.println("    node is final!");
          currentFrame = pushFrame(node, targetUpto);
          // if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" +
          // currentFrame.hasTerms);
        }
      }
    }

    // validIndexPrefix = targetUpto;
    validIndexPrefix = currentFrame.prefixLength;

    currentFrame.scanToFloorFrame(target);

    // Target term is entirely contained in the index:
    if (!currentFrame.hasTerms) {
      termExists = false;
      term.setLength(targetUpto);
      // if (DEBUG) {
      //   System.out.println("  FAST NOT_FOUND term=" + ToStringUtils.bytesRefToString(term));
      // }
      return null;
    }

    return getIOBooleanSupplier(target, prefetch);
  }

  private IOBooleanSupplier getIOBooleanSupplier(BytesRef target, boolean prefetch)
      throws IOException {
    if (prefetch) {
      currentFrame.prefetchBlock();
    }

    return () -> {
      currentFrame.loadBlock();

      final SeekStatus result = currentFrame.scanToTerm(target, true);
      if (result == SeekStatus.FOUND) {
        // if (DEBUG) {
        //   System.out.println("  return FOUND term=" + term.utf8ToString() + " " + term);
        // }
        return true;
      } else {
        // if (DEBUG) {
        //   System.out.println("  got result " + result + "; return NOT_FOUND term=" +
        // term.utf8ToString());
        // }

        return false;
      }
    };
  }

  @Override
  public IOBooleanSupplier prepareSeekExact(BytesRef target) throws IOException {
    return prepareSeekExact(target, true);
  }

  @Override
  public boolean seekExact(BytesRef target) throws IOException {
    IOBooleanSupplier termExistsSupplier = prepareSeekExact(target, false);
    return termExistsSupplier != null && termExistsSupplier.get();
  }

  @Override
  public SeekStatus seekCeil(BytesRef target) throws IOException {

    term.grow(1 + target.length);

    assert clearEOF();

    // if (DEBUG) {
    //   System.out.println("\nBTTR.seekCeil seg=" + fr.parent.segment + " target=" +
    // fr.fieldInfo.name + ":" + ToStringUtils.bytesRefToString(target) + " current=" +
    // ToStringUtils.bytesRefToString(term) + " (exists?=" + termExists +
    // ") validIndexPrefix=  " + validIndexPrefix);
    //   printSeekState(System.out);
    // }

    TrieReader.Node node;
    int targetUpto;

    targetBeforeCurrentLength = currentFrame.ord;

    if (currentFrame != staticFrame) {

      // We are already seek'd; find the common
      // prefix of new seek term vs current term and
      // re-use the corresponding seek state.  For
      // example, if app first seeks to foobar, then
      // seeks to foobaz, we can re-use the seek state
      // for the first 5 bytes.

      // if (DEBUG) {
      // System.out.println("  re-use current seek state validIndexPrefix=" + validIndexPrefix);
      // }

      node = nodes[0];
      assert node.hasOutput();
      targetUpto = 0;

      SegmentTermsEnumFrame lastFrame = stack[0];
      assert validIndexPrefix <= term.length();

      final int targetLimit = Math.min(target.length, validIndexPrefix);

      int cmp = 0;

      // First compare up to valid seek frames:
      while (targetUpto < targetLimit) {
        cmp = (term.byteAt(targetUpto) & 0xFF) - (target.bytes[target.offset + targetUpto] & 0xFF);
        // if (DEBUG) {
        // System.out.println("    cycle targetUpto=" + targetUpto + " (vs limit=" + targetLimit +
        // ") cmp=" + cmp + " (targetLabel=" + (char) (target.bytes[target.offset + targetUpto]) +
        // " vs termLabel=" + (char) (term.byteAt(targetUpto)) + ")"   + " node.output=" +
        // node.output
        // + " output=" + output);
        // }
        if (cmp != 0) {
          break;
        }
        node = nodes[1 + targetUpto];
        assert node.label == (target.bytes[target.offset + targetUpto] & 0xFF)
            : "node.label="
                + (char) node.label
                + " targetLabel="
                + (char) (target.bytes[target.offset + targetUpto] & 0xFF);

        if (node.hasOutput()) {
          lastFrame = stack[1 + lastFrame.ord];
        }
        targetUpto++;
      }

      if (cmp == 0) {
        // Second compare the rest of the term, but
        // don't save node/output/frame:
        cmp =
            Arrays.compareUnsigned(
                term.bytes(),
                targetUpto,
                term.length(),
                target.bytes,
                target.offset + targetUpto,
                target.offset + target.length);
      }

      if (cmp < 0) {
        // Common case: target term is after current
        // term, ie, app is seeking multiple terms
        // in sorted order
        // if (DEBUG) {
        // System.out.println("  target is after current (shares prefixLen=" + targetUpto + ");
        // clear frame.scanned ord=" + lastFrame.ord);
        // }
        currentFrame = lastFrame;

      } else if (cmp > 0) {
        // Uncommon case: target term
        // is before current term; this means we can
        // keep the currentFrame but we must rewind it
        // (so we scan from the start)
        targetBeforeCurrentLength = 0;
        // if (DEBUG) {
        // System.out.println("  target is before current (shares prefixLen=" + targetUpto + ");
        // rewind frame ord=" + lastFrame.ord);
        // }
        currentFrame = lastFrame;
        currentFrame.rewind();
      } else {
        // Target is exactly the same as current term
        assert term.length() == target.length;
        if (termExists) {
          // if (DEBUG) {
          // System.out.println("  target is same as current; return FOUND");
          // }
          return SeekStatus.FOUND;
        } else {
          // if (DEBUG) {
          // System.out.println("  target is same as current but term doesn't exist");
          // }
        }
      }

    } else {

      targetBeforeCurrentLength = -1;
      node = nodes[0] = trieReader.root;

      // Empty string prefix must have an output (block) in the index!
      assert node.hasOutput();

      // if (DEBUG) {
      // System.out.println("    no seek state; push root frame");
      // }

      currentFrame = staticFrame;

      // term.length = 0;
      targetUpto = 0;
      currentFrame = pushFrame(node, 0);
    }

    // if (DEBUG) {
    // System.out.println("  start index loop targetUpto=" + targetUpto + " output=" + output +
    // " currentFrame.ord+1=" + currentFrame.ord + " targetBeforeCurrentLength=" +
    // targetBeforeCurrentLength);
    // }

    // We are done sharing the common prefix with the incoming target and where we are currently
    // seek'd; now continue walking the index:
    while (targetUpto < target.length) {

      final int targetLabel = target.bytes[target.offset + targetUpto] & 0xFF;

      final TrieReader.Node nextNode =
          trieReader.lookupChild(targetLabel, node, getNode(1 + targetUpto));

      if (nextNode == null) {

        // Index is exhausted
        // if (DEBUG) {
        //   System.out.println("    index: index exhausted label=" + ((char) targetLabel) + " " +
        // targetLabel);
        // }

        validIndexPrefix = currentFrame.prefixLength;
        // validIndexPrefix = targetUpto;

        currentFrame.scanToFloorFrame(target);

        currentFrame.loadBlock();

        // if (DEBUG) System.out.println("  now scanToTerm");
        final SeekStatus result = currentFrame.scanToTerm(target, false);
        if (result == SeekStatus.END) {
          term.copyBytes(target);
          termExists = false;

          if (next() != null) {
            // if (DEBUG) {
            // System.out.println("  return NOT_FOUND term=" +
            // ToStringUtils.bytesRefToString(term));
            // }
            return SeekStatus.NOT_FOUND;
          } else {
            // if (DEBUG) {
            // System.out.println("  return END");
            // }
            return SeekStatus.END;
          }
        } else {
          // if (DEBUG) {
          // System.out.println("  return " + result + " term=" +
          // ToStringUtils.bytesRefToString(term));
          // }
          return result;
        }
      } else {
        // Follow this node
        term.setByteAt(targetUpto, (byte) targetLabel);
        node = nextNode;

        // if (DEBUG) {
        // System.out.println("    index: follow label=" + (target.bytes[target.offset +
        // targetUpto]&0xff) + " node.output=" + node.output + " node.nfo=" + node.nextFinalOutput);
        // }
        targetUpto++;

        if (node.hasOutput()) {
          // if (DEBUG) System.out.println("    node is final!");
          currentFrame = pushFrame(node, targetUpto);
          // if (DEBUG) System.out.println("    curFrame.ord=" + currentFrame.ord + " hasTerms=" +
          // currentFrame.hasTerms);
        }
      }
    }

    // validIndexPrefix = targetUpto;
    validIndexPrefix = currentFrame.prefixLength;

    currentFrame.scanToFloorFrame(target);

    currentFrame.loadBlock();

    final SeekStatus result = currentFrame.scanToTerm(target, false);

    if (result == SeekStatus.END) {
      term.copyBytes(target);
      termExists = false;
      if (next() != null) {
        // if (DEBUG) {
        // System.out.println("  return NOT_FOUND term=" + term.get().utf8ToString() + " " + term);
        // }
        return SeekStatus.NOT_FOUND;
      } else {
        // if (DEBUG) {
        // System.out.println("  return END");
        // }
        return SeekStatus.END;
      }
    } else {
      return result;
    }
  }

  @SuppressWarnings("unused")
  private void printSeekState(PrintStream out) throws IOException {
    if (currentFrame == staticFrame) {
      out.println("  no prior seek");
    } else {
      out.println("  prior seek state:");
      int ord = 0;
      boolean isSeekFrame = true;
      while (true) {
        SegmentTermsEnumFrame f = getFrame(ord);
        assert f != null;
        final BytesRef prefix = new BytesRef(term.get().bytes, 0, f.prefixLength);
        if (f.nextEnt == -1) {
          out.println(
              "    frame "
                  + (isSeekFrame ? "(seek)" : "(next)")
                  + " ord="
                  + ord
                  + " fp="
                  + f.fp
                  + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "")
                  + " prefixLen="
                  + f.prefixLength
                  + " prefix="
                  + prefix
                  + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")"))
                  + " hasTerms="
                  + f.hasTerms
                  + " isFloor="
                  + f.isFloor
                  + " isLastInFloor="
                  + f.isLastInFloor
                  + " mdUpto="
                  + f.metaDataUpto
                  + " tbOrd="
                  + f.getTermBlockOrd());
        } else {
          out.println(
              "    frame "
                  + (isSeekFrame ? "(seek, loaded)" : "(next, loaded)")
                  + " ord="
                  + ord
                  + " fp="
                  + f.fp
                  + (f.isFloor ? (" (fpOrig=" + f.fpOrig + ")") : "")
                  + " prefixLen="
                  + f.prefixLength
                  + " prefix="
                  + prefix
                  + " nextEnt="
                  + f.nextEnt
                  + (f.nextEnt == -1 ? "" : (" (of " + f.entCount + ")"))
                  + " hasTerms="
                  + f.hasTerms
                  + " isFloor="
                  + f.isFloor
                  + " lastSubFP="
                  + f.lastSubFP
                  + " isLastInFloor="
                  + f.isLastInFloor
                  + " mdUpto="
                  + f.metaDataUpto
                  + " tbOrd="
                  + f.getTermBlockOrd());
        }
        assert !isSeekFrame || f.node != null : "isSeekFrame=" + isSeekFrame + " f.node=" + f.node;
        if (f.prefixLength > 0
            && isSeekFrame
            && f.node.label != (term.byteAt(f.prefixLength - 1) & 0xFF)) {
          out.println(
              "      broken seek state: node.label="
                  + (char) f.node.label
                  + " vs term byte="
                  + (char) (term.byteAt(f.prefixLength - 1) & 0xFF));
          throw new RuntimeException("seek state is broken");
        }

        TrieReader.Node node = trieReader.root;
        TrieReader.Node child = new TrieReader.Node();
        for (int i = 0; i < prefix.length; i++) {
          TrieReader.Node found =
              trieReader.lookupChild(prefix.bytes[i + prefix.offset] & 0xFF, node, child);
          if (found == null) {
            throw new RuntimeException("seek state is broken, prefix not exist in index");
          }
          node = child;
          child = new TrieReader.Node();
        }
        if (!node.hasOutput()) {
          out.println("      broken seek state: prefix is not final in index");
          throw new RuntimeException("seek state is broken");
        } else if (isSeekFrame && !f.isFloor) {
          if (f.fp != node.outputFp || f.hasTerms != node.hasTerms || f.isFloor != node.isFloor()) {
            out.println(
                "      broken seek state: output fp="
                    + node.outputFp
                    + ", hasTerms="
                    + node.hasTerms
                    + ", isFloor="
                    + node.isFloor()
                    + " doesn't match frame fp="
                    + f.fp
                    + ", hasTerms="
                    + f.hasTerms
                    + ", isFloor="
                    + f.isFloor);
            throw new RuntimeException("seek state is broken");
          }
        }

        if (f == currentFrame) {
          break;
        }
        if (f.prefixLength == validIndexPrefix) {
          isSeekFrame = false;
        }
        ord++;
      }
    }
  }

  /* Decodes only the term bytes of the next term.  If caller then asks for
  metadata, ie docFreq, totalTermFreq or pulls a D/&PEnum, we then (lazily)
  decode all metadata up to the current term. */
  @Override
  public BytesRef next() throws IOException {
    if (in == null) {
      // Fresh TermsEnum; seek to first term:
      final TrieReader.Node node = nodes[0] = trieReader.root;
      currentFrame = pushFrame(node, 0);
      currentFrame.loadBlock();
    }

    targetBeforeCurrentLength = currentFrame.ord;

    assert !eof;
    // if (DEBUG) {
    //   System.out.println("\nBTTR.next seg=" + fr.parent.segment + " term=" +
    // ToStringUtils.bytesRefToString(term) + " termExists?=" + termExists + " field=" +
    // fr.fieldInfo.name + " termBlockOrd=" + currentFrame.state.termBlockOrd +
    // " validIndexPrefix=" + validIndexPrefix);
    //   printSeekState(System.out);
    // }

    if (currentFrame == staticFrame) {
      // If seek was previously called and the term was
      // cached, or seek(TermState) was called, usually
      // caller is just going to pull a D/&PEnum or get
      // docFreq, etc.  But, if they then call next(),
      // this method catches up all internal state so next()
      // works properly:
      // if (DEBUG) System.out.println("  re-seek to pending term=" + term.utf8ToString() + " " +
      // term);
      final boolean result = seekExact(term.get());
      assert result;
    }

    // Pop finished blocks
    while (currentFrame.nextEnt == currentFrame.entCount) {
      if (!currentFrame.isLastInFloor) {
        // Advance to next floor block
        currentFrame.loadNextFloorBlock();
        break;
      } else {
        // if (DEBUG) System.out.println("  pop frame");
        if (currentFrame.ord == 0) {
          // if (DEBUG) System.out.println("  return null");
          assert setEOF();
          term.clear();
          validIndexPrefix = 0;
          currentFrame.rewind();
          termExists = false;
          return null;
        }
        final long lastFP = currentFrame.fpOrig;
        currentFrame = stack[currentFrame.ord - 1];

        if (currentFrame.nextEnt == -1 || currentFrame.lastSubFP != lastFP) {
          // We popped into a frame that's not loaded
          // yet or not scan'd to the right entry
          currentFrame.scanToFloorFrame(term.get());
          currentFrame.loadBlock();
          currentFrame.scanToSubBlock(lastFP);
        }

        // Note that the seek state (last seek) has been
        // invalidated beyond this depth
        validIndexPrefix = Math.min(validIndexPrefix, currentFrame.prefixLength);
        // if (DEBUG) {
        // System.out.println("  reset validIndexPrefix=" + validIndexPrefix);
        // }
      }
    }

    while (true) {
      if (currentFrame.next()) {
        // Push to new block:
        // if (DEBUG) System.out.println("  push frame");
        currentFrame = pushFrame(null, currentFrame.lastSubFP, term.length());
        // This is a "next" frame -- even if it's
        // floor'd we must pretend it isn't so we don't
        // try to scan to the right floor frame:
        currentFrame.loadBlock();
      } else {
        // if (DEBUG) System.out.println("  return term=" + ToStringUtils.bytesRefToString(term) +
        // " currentFrame.ord=" + currentFrame.ord);
        return term.get();
      }
    }
  }

  @Override
  public BytesRef term() {
    assert !eof;
    return term.get();
  }

  @Override
  public int docFreq() throws IOException {
    assert !eof;
    // if (DEBUG) System.out.println("BTR.docFreq");
    currentFrame.decodeMetaData();
    // if (DEBUG) System.out.println("  return " + currentFrame.state.docFreq);
    return currentFrame.state.docFreq;
  }

  @Override
  public long totalTermFreq() throws IOException {
    assert !eof;
    currentFrame.decodeMetaData();
    return currentFrame.state.totalTermFreq;
  }

  @Override
  public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
    assert !eof;
    // if (DEBUG) {
    // System.out.println("BTTR.docs seg=" + segment);
    // }
    currentFrame.decodeMetaData();
    // if (DEBUG) {
    // System.out.println("  state=" + currentFrame.state);
    // }
    return fr.parent.postingsReader.postings(fr.fieldInfo, currentFrame.state, reuse, flags);
  }

  @Override
  public ImpactsEnum impacts(int flags) throws IOException {
    assert !eof;
    // if (DEBUG) {
    // System.out.println("BTTR.docs seg=" + segment);
    // }
    currentFrame.decodeMetaData();
    // if (DEBUG) {
    // System.out.println("  state=" + currentFrame.state);
    // }
    return fr.parent.postingsReader.impacts(fr.fieldInfo, currentFrame.state, flags);
  }

  @Override
  public void seekExact(BytesRef target, TermState otherState) {
    // if (DEBUG) {
    //   System.out.println("BTTR.seekExact termState seg=" + segment + " target=" +
    // target.utf8ToString() + " " + target + " state=" + otherState);
    // }
    assert clearEOF();
    if (target.compareTo(term.get()) != 0 || !termExists) {
      assert otherState != null && otherState instanceof BlockTermState;
      currentFrame = staticFrame;
      currentFrame.state.copyFrom(otherState);
      term.copyBytes(target);
      currentFrame.metaDataUpto = currentFrame.getTermBlockOrd();
      assert currentFrame.metaDataUpto > 0;
      validIndexPrefix = 0;
    } else {
      // if (DEBUG) {
      //   System.out.println("  skip seek: already on target state=" + currentFrame.state);
      // }
    }
  }

  @Override
  public TermState termState() throws IOException {
    assert !eof;
    currentFrame.decodeMetaData();
    TermState ts = currentFrame.state.clone();
    // if (DEBUG) System.out.println("BTTR.termState seg=" + segment + " state=" + ts);
    return ts;
  }

  @Override
  public void seekExact(long ord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long ord() {
    throw new UnsupportedOperationException();
  }
}
