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
package org.apache.lucene.util.fst;

import static org.apache.lucene.util.fst.FST.ARCS_FOR_BINARY_SEARCH;
import static org.apache.lucene.util.fst.FST.ARCS_FOR_CONTINUOUS;
import static org.apache.lucene.util.fst.FST.ARCS_FOR_DIRECT_ADDRESSING;
import static org.apache.lucene.util.fst.FST.BIT_ARC_HAS_FINAL_OUTPUT;
import static org.apache.lucene.util.fst.FST.BIT_ARC_HAS_OUTPUT;
import static org.apache.lucene.util.fst.FST.BIT_FINAL_ARC;
import static org.apache.lucene.util.fst.FST.BIT_LAST_ARC;
import static org.apache.lucene.util.fst.FST.BIT_STOP_NODE;
import static org.apache.lucene.util.fst.FST.BIT_TARGET_NEXT;
import static org.apache.lucene.util.fst.FST.FINAL_END_NODE;
import static org.apache.lucene.util.fst.FST.NON_FINAL_END_NODE;
import static org.apache.lucene.util.fst.FST.VERSION_CURRENT;
import static org.apache.lucene.util.fst.FST.getNumPresenceBytes;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary output) from pre-sorted terms with
 * outputs. The FST becomes an FSA if you use NoOutputs. The FST is written on-the-fly into a
 * compact serialized format byte array, which can be saved to / loaded from a Directory or used
 * directly for traversal. The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
 *
 * <p>The parameterized type T is the output type. See the subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene 4.2). FSTs containing more than 2.1B
 * nodes are also now possible, however they cannot be packed.
 *
 * @lucene.experimental
 */
public class FSTCompiler<T> {

  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

  /**
   * @see #shouldExpandNodeWithFixedLengthArcs
   */
  static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

  /**
   * Maximum oversizing factor allowed for direct addressing compared to binary search when
   * expansion credits allow the oversizing. This factor prevents expansions that are obviously too
   * costly even if there are sufficient credits.
   *
   * @see #shouldExpandNodeWithDirectAddressing
   */
  private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

  private final NodeHash<T> dedupHash;
  final FST<T> fst;
  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  private final IntsRefBuilder lastInput = new IntsRefBuilder();

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  private UnCompiledNode<T>[] frontier;

  // Used for the BIT_TARGET_NEXT optimization (whereby
  // instead of storing the address of the target node for
  // a given arc, we mark a single bit noting that the next
  // node in the byte[] is the target node):
  long lastFrozenNode;

  // Reused temporarily while building the FST:
  int[] numBytesPerArc = new int[4];
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  long arcCount;
  long nodeCount;
  long binarySearchNodeCount;
  long directAddressingNodeCount;
  long continuousNodeCount;

  final boolean allowFixedLengthArcs;
  final float directAddressingMaxOversizingFactor;
  long directAddressingExpansionCredit;

  final BytesStore bytes;

  private FSTCompiler(
      FST.INPUT_TYPE inputType,
      double suffixRAMLimitMB,
      Outputs<T> outputs,
      boolean allowFixedLengthArcs,
      int bytesPageBits,
      float directAddressingMaxOversizingFactor) {
    this.allowFixedLengthArcs = allowFixedLengthArcs;
    this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
    bytes = new BytesStore(bytesPageBits);
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs
    bytes.writeByte((byte) 0);
    fst = new FST<>(new FST.FSTMetadata<>(inputType, outputs, null, -1, VERSION_CURRENT, 0), bytes);
    if (suffixRAMLimitMB < 0) {
      throw new IllegalArgumentException("ramLimitMB must be >= 0; got: " + suffixRAMLimitMB);
    } else if (suffixRAMLimitMB > 0) {
      dedupHash = new NodeHash<>(this, suffixRAMLimitMB);
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings({"rawtypes", "unchecked"})
    final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;
    for (int idx = 0; idx < frontier.length; idx++) {
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * Fluent-style constructor for FST {@link FSTCompiler}.
   *
   * <p>Creates an FST/FSA builder with all the possible tuning and construction tweaks. Read
   * parameter documentation carefully.
   */
  public static class Builder<T> {

    private final INPUT_TYPE inputType;
    private final Outputs<T> outputs;
    private double suffixRAMLimitMB = 32.0;
    private boolean allowFixedLengthArcs = true;
    private int bytesPageBits = 15;
    private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

    /**
     * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
     *     enumeration. Shorter types will consume less memory. Strings (character sequences) are
     *     represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
     * @param outputs The output type for each input sequence. Applies only if building an FST. For
     *     FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
     *     singleton output object.
     */
    public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
      this.inputType = inputType;
      this.outputs = outputs;
    }

    /**
     * The approximate maximum amount of RAM (in MB) to use holding the suffix cache, which enables
     * the FST to share common suffixes. Pass {@link Double#POSITIVE_INFINITY} to keep all suffixes
     * and create an exactly minimal FST. In this case, the amount of RAM actually used will be
     * bounded by the number of unique suffixes. If you pass a value smaller than the builder would
     * use, the least recently used suffixes will be discarded, thus reducing suffix sharing and
     * creating a non-minimal FST. In this case, the larger the limit, the closer the FST will be to
     * its true minimal size, with diminishing returns as you increase the limit. Pass {@code 0} to
     * disable suffix sharing entirely, but note that the resulting FST can be substantially larger
     * than the minimal FST.
     *
     * <p>Note that this is not a precise limit. The current implementation uses hash tables to map
     * the suffixes, and approximates the rough overhead (unused slots) in the hash table.
     *
     * <p>Default = {@code 32.0} MB.
     */
    public Builder<T> suffixRAMLimitMB(double mb) {
      if (mb < 0) {
        throw new IllegalArgumentException("suffixRAMLimitMB must be >= 0; got: " + mb);
      }
      this.suffixRAMLimitMB = mb;
      return this;
    }

    /**
     * Pass {@code false} to disable the fixed length arc optimization (binary search or direct
     * addressing) while building the FST; this will make the resulting FST smaller but slower to
     * traverse.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
      this.allowFixedLengthArcs = allowFixedLengthArcs;
      return this;
    }

    /**
     * How many bits wide to make each byte[] block in the BytesStore; if you know the FST will be
     * large then make this larger. For example 15 bits = 32768 byte pages.
     *
     * <p>Default = 15.
     */
    public Builder<T> bytesPageBits(int bytesPageBits) {
      this.bytesPageBits = bytesPageBits;
      return this;
    }

    /**
     * Overrides the default the maximum oversizing of fixed array allowed to enable direct
     * addressing of arcs instead of binary search.
     *
     * <p>Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
     * only binary search nodes will be created.
     *
     * <p>This factor does not determine whether to encode a node with a list of variable length
     * arcs or with fixed length arcs. It only determines the effective encoding of a node that is
     * already known to be encoded with fixed length arcs.
     *
     * <p>Default = 1.
     */
    public Builder<T> directAddressingMaxOversizingFactor(float factor) {
      this.directAddressingMaxOversizingFactor = factor;
      return this;
    }

    /** Creates a new {@link FSTCompiler}. */
    public FSTCompiler<T> build() throws IOException {
      return new FSTCompiler<>(
          inputType,
          suffixRAMLimitMB,
          outputs,
          allowFixedLengthArcs,
          bytesPageBits,
          directAddressingMaxOversizingFactor);
    }
  }

  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1 + nodeCount;
  }

  public long getArcCount() {
    return arcCount;
  }

  public long getMappedStateCount() {
    return dedupHash == null ? 0 : nodeCount;
  }

  private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
    final long node;
    long bytesPosStart = bytes.getPosition();
    if (dedupHash != null) {
      if (nodeIn.numArcs == 0) {
        node = addNode(nodeIn);
        lastFrozenNode = node;
      } else {
        node = dedupHash.add(nodeIn);
      }
    } else {
      node = addNode(nodeIn);
    }
    assert node != -2;

    long bytesPosEnd = bytes.getPosition();
    if (bytesPosEnd != bytesPosStart) {
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }

    nodeIn.clear();

    final CompiledNode fn = new CompiledNode();
    fn.node = node;
    return fn;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  long addNode(FSTCompiler.UnCompiledNode<T> nodeIn) throws IOException {
    // System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
    if (nodeIn.numArcs == 0) {
      if (nodeIn.isFinal) {
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;
      }
    }
    final long startAddress = bytes.getPosition();
    // System.out.println("  startAddr=" + startAddress);

    final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(nodeIn);
    if (doFixedLengthArcs) {
      // System.out.println("  fixed length arcs");
      if (numBytesPerArc.length < nodeIn.numArcs) {
        numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
        numLabelBytesPerArc = new int[numBytesPerArc.length];
      }
    }

    arcCount += nodeIn.numArcs;

    final int lastArc = nodeIn.numArcs - 1;

    long lastArcStart = bytes.getPosition();
    int maxBytesPerArc = 0;
    int maxBytesPerArcWithoutLabel = 0;
    for (int arcIdx = 0; arcIdx < nodeIn.numArcs; arcIdx++) {
      final FSTCompiler.Arc<T> arc = nodeIn.arcs[arcIdx];
      final FSTCompiler.CompiledNode target = (FSTCompiler.CompiledNode) arc.target;
      int flags = 0;
      // System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" +
      // target.node);

      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      if (lastFrozenNode == target.node && !doFixedLengthArcs) {
        // TODO: for better perf (but more RAM used) we
        // could avoid this except when arc is "near" the
        // last arc:
        flags += BIT_TARGET_NEXT;
      }

      if (arc.isFinal) {
        flags += BIT_FINAL_ARC;
        if (arc.nextFinalOutput != NO_OUTPUT) {
          flags += BIT_ARC_HAS_FINAL_OUTPUT;
        }
      } else {
        assert arc.nextFinalOutput == NO_OUTPUT;
      }

      boolean targetHasArcs = target.node > 0;

      if (!targetHasArcs) {
        flags += BIT_STOP_NODE;
      }

      if (arc.output != NO_OUTPUT) {
        flags += BIT_ARC_HAS_OUTPUT;
      }

      bytes.writeByte((byte) flags);
      long labelStart = bytes.getPosition();
      writeLabel(bytes, arc.label);
      int numLabelBytes = (int) (bytes.getPosition() - labelStart);

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + "
      // target=" + target.node + " pos=" + bytes.getPosition() + " output=" +
      // outputs.outputToString(arc.output));

      if (arc.output != NO_OUTPUT) {
        fst.outputs.write(arc.output, bytes);
        // System.out.println("    write output");
      }

      if (arc.nextFinalOutput != NO_OUTPUT) {
        // System.out.println("    write final output");
        fst.outputs.writeFinalOutput(arc.nextFinalOutput, bytes);
      }

      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        // System.out.println("    write target");
        bytes.writeVLong(target.node);
      }

      // just write the arcs "like normal" on first pass, but record how many bytes each one took
      // and max byte size:
      if (doFixedLengthArcs) {
        int numArcBytes = (int) (bytes.getPosition() - lastArcStart);
        numBytesPerArc[arcIdx] = numArcBytes;
        numLabelBytesPerArc[arcIdx] = numLabelBytes;
        lastArcStart = bytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
        maxBytesPerArcWithoutLabel =
            Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
        // System.out.println("    arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
      }
    }

    // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
    /*
     *
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     *
     * the one below just looks at #3
    if (doFixedLengthArcs) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
      }
    }
    */

    if (doFixedLengthArcs) {
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed byte size

      int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
      assert labelRange > 0;
      boolean continuousLabel = labelRange == nodeIn.numArcs;
      if (continuousLabel) {
        writeNodeForDirectAddressingOrContinuous(
            nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange, true);
        continuousNodeCount++;
      } else if (shouldExpandNodeWithDirectAddressing(
          nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
        writeNodeForDirectAddressingOrContinuous(
            nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange, false);
        directAddressingNodeCount++;
      } else {
        writeNodeForBinarySearch(nodeIn, startAddress, maxBytesPerArc);
        binarySearchNodeCount++;
      }
    }

    final long thisNodeAddress = bytes.getPosition() - 1;
    bytes.reverse(startAddress, thisNodeAddress);
    nodeCount++;
    return thisNodeAddress;
  }

  private void writeLabel(DataOutput out, int v) throws IOException {
    assert v >= 0 : "v=" + v;
    if (fst.metadata.inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255 : "v=" + v;
      out.writeByte((byte) v);
    } else if (fst.metadata.inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535 : "v=" + v;
      out.writeShort((short) v);
    } else {
      out.writeVInt(v);
    }
  }

  /**
   * Returns whether the given node should be expanded with fixed length arcs. Nodes will be
   * expanded depending on their depth (distance from the root node) and their number of arcs.
   *
   * <p>Nodes with fixed length arcs use more space, because they encode all arcs with a fixed
   * number of bytes, but they allow either binary search or direct addressing on the arcs (instead
   * of linear scan) on lookup by arc label.
   */
  private boolean shouldExpandNodeWithFixedLengthArcs(FSTCompiler.UnCompiledNode<T> node) {
    return allowFixedLengthArcs
        && ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH
                && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS)
            || node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);
  }

  /**
   * Returns whether the given node should be expanded with direct addressing instead of binary
   * search.
   *
   * <p>Prefer direct addressing for performance if it does not oversize binary search byte size too
   * much, so that the arcs can be directly addressed by label.
   *
   * @see FSTCompiler#getDirectAddressingMaxOversizingFactor()
   */
  private boolean shouldExpandNodeWithDirectAddressing(
      FSTCompiler.UnCompiledNode<T> nodeIn,
      int numBytesPerArc,
      int maxBytesPerArcWithoutLabel,
      int labelRange) {
    // Anticipate precisely the size of the encodings.
    int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
    int sizeForDirectAddressing =
        getNumPresenceBytes(labelRange)
            + numLabelBytesPerArc[0]
            + maxBytesPerArcWithoutLabel * nodeIn.numArcs;

    // Determine the allowed oversize compared to binary search.
    // This is defined by a parameter of FST Builder (default 1: no oversize).
    int allowedOversize = (int) (sizeForBinarySearch * getDirectAddressingMaxOversizingFactor());
    int expansionCost = sizeForDirectAddressing - allowedOversize;

    // Select direct addressing if either:
    // - Direct addressing size is smaller than binary search.
    //   In this case, increment the credit by the reduced size (to use it later).
    // - Direct addressing size is larger than binary search, but the positive credit allows the
    // oversizing.
    //   In this case, decrement the credit by the oversize.
    // In addition, do not try to oversize to a clearly too large node size
    // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
    if (expansionCost <= 0
        || (directAddressingExpansionCredit >= expansionCost
            && sizeForDirectAddressing
                <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
      directAddressingExpansionCredit -= expansionCost;
      return true;
    }
    return false;
  }

  private void writeNodeForBinarySearch(
      FSTCompiler.UnCompiledNode<T> nodeIn, long startAddress, int maxBytesPerArc) {
    // Build the header in a buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_BINARY_SEARCH)
        .writeVInt(nodeIn.numArcs)
        .writeVInt(maxBytesPerArc);
    int headerLen = fixedLengthArcsBuffer.getPosition();

    // Expand the arcs in place, backwards.
    long srcPos = bytes.getPosition();
    long destPos = startAddress + headerLen + nodeIn.numArcs * (long) maxBytesPerArc;
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      bytes.setPosition(destPos);
      for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
        destPos -= maxBytesPerArc;
        int arcLen = numBytesPerArc[arcIdx];
        srcPos -= arcLen;
        if (srcPos != destPos) {
          assert destPos > srcPos
              : "destPos="
                  + destPos
                  + " srcPos="
                  + srcPos
                  + " arcIdx="
                  + arcIdx
                  + " maxBytesPerArc="
                  + maxBytesPerArc
                  + " arcLen="
                  + arcLen
                  + " nodeIn.numArcs="
                  + nodeIn.numArcs;
          bytes.copyBytes(srcPos, destPos, arcLen);
        }
      }
    }

    // Write the header.
    bytes.writeBytes(startAddress, fixedLengthArcsBuffer.getBytes(), 0, headerLen);
  }

  private void writeNodeForDirectAddressingOrContinuous(
      FSTCompiler.UnCompiledNode<T> nodeIn,
      long startAddress,
      int maxBytesPerArcWithoutLabel,
      int labelRange,
      boolean continuous) {
    // Expand the arcs backwards in a buffer because we remove the labels.
    // So the obtained arcs might occupy less space. This is the reason why this
    // whole method is more complex.
    // Drop the label bytes since we can infer the label based on the arc index,
    // the presence bits, and the first label. Keep the first label.
    int headerMaxLen = 11;
    int numPresenceBytes = continuous ? 0 : getNumPresenceBytes(labelRange);
    long srcPos = bytes.getPosition();
    int totalArcBytes = numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
    int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
    byte[] buffer = fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
    // Copy the arcs to the buffer, dropping all labels except first one.
    for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
      bufferOffset -= maxBytesPerArcWithoutLabel;
      int srcArcLen = numBytesPerArc[arcIdx];
      srcPos -= srcArcLen;
      int labelLen = numLabelBytesPerArc[arcIdx];
      // Copy the flags.
      bytes.writeTo(srcPos, buffer, bufferOffset, 1);
      // Skip the label, copy the remaining.
      int remainingArcLen = srcArcLen - 1 - labelLen;
      if (remainingArcLen != 0) {
        bytes.writeTo(srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
      }
      if (arcIdx == 0) {
        // Copy the label of the first arc only.
        bufferOffset -= labelLen;
        bytes.writeTo(srcPos + 1, buffer, bufferOffset, labelLen);
      }
    }
    assert bufferOffset == headerMaxLen + numPresenceBytes;

    // Build the header in the buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(continuous ? ARCS_FOR_CONTINUOUS : ARCS_FOR_DIRECT_ADDRESSING)
        .writeVInt(labelRange) // labelRange instead of numArcs.
        .writeVInt(
            maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
    int headerLen = fixedLengthArcsBuffer.getPosition();

    // Write the header.
    bytes.setPosition(startAddress);
    bytes.writeBytes(fixedLengthArcsBuffer.getBytes(), 0, headerLen);

    // Write the presence bits
    if (continuous == false) {
      writePresenceBits(nodeIn);
      assert bytes.getPosition() == startAddress + headerLen + numPresenceBytes;
    }

    // Write the first label and the arcs.
    bytes.writeBytes(fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
    assert bytes.getPosition() == startAddress + headerLen + numPresenceBytes + totalArcBytes;
  }

  private void writePresenceBits(FSTCompiler.UnCompiledNode<T> nodeIn) {
    byte presenceBits = 1; // The first arc is always present.
    int presenceIndex = 0;
    int previousLabel = nodeIn.arcs[0].label;
    for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
      int label = nodeIn.arcs[arcIdx].label;
      assert label > previousLabel;
      presenceIndex += label - previousLabel;
      while (presenceIndex >= Byte.SIZE) {
        bytes.writeByte(presenceBits);
        presenceBits = 0;
        presenceIndex -= Byte.SIZE;
      }
      // Set the bit at presenceIndex to flag that the corresponding arc is present.
      presenceBits |= 1 << presenceIndex;
      previousLabel = label;
    }
    assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
    assert presenceBits != 0; // The last byte is not 0.
    assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
    bytes.writeByte(presenceBits);
  }

  private void freezeTail(int prefixLenPlus1) throws IOException {

    final int downTo = Math.max(1, prefixLenPlus1);

    for (int idx = lastInput.length(); idx >= downTo; idx--) {

      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parent = frontier[idx - 1];

      final T nextFinalOutput = node.output;

      // We "fake" the node as being final if it has no
      // outgoing arcs; in theory we could leave it
      // as non-final (the FST can represent this), but
      // FSTEnum, Util, etc., have trouble w/ non-final
      // dead-end states:

      // TODO: is node.numArcs == 0 always false?  we no longer prune any nodes from FST:
      final boolean isFinal = node.isFinal || node.numArcs == 0;

      // this node makes it and we now compile it.  first,
      // compile any targets that were previously
      // undecided:
      parent.replaceLast(
          lastInput.intAt(idx - 1),
          compileNode(node, 1 + lastInput.length() - idx),
          nextFinalOutput,
          isFinal);
    }
  }

  /**
   * Add the next input/output pair. The provided input must be sorted after the previous one
   * according to {@link IntsRef#compareTo}. It's also OK to add the same input twice in a row with
   * different outputs, as long as {@link Outputs} implements the {@link Outputs#merge} method. Note
   * that input is fully consumed after this method is returned (so caller is free to reuse), but
   * output is not. So if your outputs are changeable (eg {@link ByteSequenceOutputs} or {@link
   * IntSequenceOutputs}) then you cannot reuse across calls.
   */
  public void add(IntsRef input, T output) throws IOException {
    // De-dup NO_OUTPUT since it must be a singleton:
    if (output.equals(NO_OUTPUT)) {
      output = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0
        : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(output);

    // System.out.println("\nadd: " + input);
    if (input.length == 0) {
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].isFinal = true;
      setEmptyOutput(output);
      return;
    }

    // compare shared prefix length
    int pos1 = 0;
    int pos2 = input.offset;
    final int pos1Stop = Math.min(lastInput.length(), input.length);
    while (true) {
      if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1 + 1;

    if (frontier.length < input.length + 1) {
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
      for (int idx = frontier.length; idx < next.length; idx++) {
        next[idx] = new UnCompiledNode<>(this, idx);
      }
      frontier = next;
    }

    // minimize/compile states from previous input's
    // orphan'd suffix
    freezeTail(prefixLenPlus1);

    // init tail states for current input
    for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
      frontier[idx - 1].addArc(input.ints[input.offset + idx - 1], frontier[idx]);
    }

    final UnCompiledNode<T> lastNode = frontier[input.length];
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
      lastNode.isFinal = true;
      lastNode.output = NO_OUTPUT;
    }

    // push conflicting outputs forward, only as far as
    // needed
    for (int idx = 1; idx < prefixLenPlus1; idx++) {
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx - 1];

      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;
      final T wordSuffix;

      if (lastOutput != NO_OUTPUT) {
        commonOutputPrefix = fst.outputs.common(output, lastOutput);
        assert validOutput(commonOutputPrefix);
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);
        assert validOutput(wordSuffix);
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
        node.prependOutput(wordSuffix);
      } else {
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      output = fst.outputs.subtract(output, commonOutputPrefix);
      assert validOutput(output);
    }

    if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs
      lastNode.output = fst.outputs.merge(lastNode.output, output);
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1 - 1].setLastOutput(
          input.ints[input.offset + prefixLenPlus1 - 1], output);
    }

    // save last input
    lastInput.copyInts(input);
  }

  void setEmptyOutput(T v) {
    if (fst.metadata.emptyOutput != null) {
      fst.metadata.emptyOutput = fst.outputs.merge(fst.metadata.emptyOutput, v);
    } else {
      fst.metadata.emptyOutput = v;
    }
  }

  void finish(long newStartNode) {
    assert newStartNode <= bytes.getPosition();
    if (fst.metadata.startNode != -1) {
      throw new IllegalStateException("already finished");
    }
    if (newStartNode == FINAL_END_NODE && fst.metadata.emptyOutput != null) {
      newStartNode = 0;
    }
    fst.metadata.startNode = newStartNode;
    fst.metadata.numBytes = bytes.getPosition();
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST. NOTE: this will return null if nothing is accepted by the FST. */
  public FST<T> compile() throws IOException {

    final UnCompiledNode<T> root = frontier[0];

    // minimize nodes in the last word's suffix
    freezeTail(0);
    if (root.numArcs == 0) {
      if (fst.metadata.emptyOutput == null) {
        return null;
      }
    }

    // if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + "
    // root.output=" + root.output);
    finish(compileNode(root, lastInput.length()).node);
    bytes.finish();

    return fst;
  }

  /** Expert: holds a pending (seen but not yet serialized) arc. */
  static class Arc<T> {
    int label; // really an "unsigned" byte
    Node target;
    boolean isFinal;
    T output;
    T nextFinalOutput;
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  interface Node {
    boolean isCompiled();
  }

  public long fstRamBytesUsed() {
    return fst.ramBytesUsed();
  }

  public long fstSizeInBytes() {
    return bytes.getPosition();
  }

  static final class CompiledNode implements Node {
    long node;

    @Override
    public boolean isCompiled() {
      return true;
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) Node. */
  static final class UnCompiledNode<T> implements Node {
    final FSTCompiler<T> owner;
    int numArcs;
    Arc<T>[] arcs;
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    T output;
    boolean isFinal;

    /** This node's depth, starting from the automaton root. */
    final int depth;

    /**
     * @param depth The node's depth starting from the automaton root. Needed for LUCENE-2934 (node
     *     expansion based on conditions other than the fanout size).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    UnCompiledNode(FSTCompiler<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    @Override
    public boolean isCompiled() {
      return false;
    }

    void clear() {
      numArcs = 0;
      isFinal = false;
      output = owner.NO_OUTPUT;

      // We don't clear the depth here because it never changes
      // for nodes on the frontier (even when reused).
    }

    T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs - 1].label == labelToMatch;
      return arcs[numArcs - 1].output;
    }

    void addArc(int label, Node target) {
      assert label >= 0;
      assert numArcs == 0 || label > arcs[numArcs - 1].label
          : "arc[numArcs-1].label="
              + arcs[numArcs - 1].label
              + " new label="
              + label
              + " numArcs="
              + numArcs;
      if (numArcs == arcs.length) {
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs);
        for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
          newArcs[arcIdx] = new Arc<>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }

    void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target;
      // assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }

    void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs - 1].label;
      assert target == arcs[numArcs - 1].target;
      numArcs--;
    }

    void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);

      for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
        assert owner.validOutput(arcs[arcIdx].output);
      }

      if (isFinal) {
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }

  /**
   * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
   */
  static class FixedLengthArcsBuffer {

    // Initial capacity is the max length required for the header of a node with fixed length arcs:
    // header(byte) + numArcs(vint) + numBytes(vint)
    private byte[] bytes = new byte[11];
    private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

    /** Ensures the capacity of the internal byte array. Enlarges it if needed. */
    FixedLengthArcsBuffer ensureCapacity(int capacity) {
      if (bytes.length < capacity) {
        bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
        bado.reset(bytes);
      }
      return this;
    }

    FixedLengthArcsBuffer resetPosition() {
      bado.reset(bytes);
      return this;
    }

    FixedLengthArcsBuffer writeByte(byte b) {
      bado.writeByte(b);
      return this;
    }

    FixedLengthArcsBuffer writeVInt(int i) {
      try {
        bado.writeVInt(i);
      } catch (IOException e) { // Never thrown.
        throw new RuntimeException(e);
      }
      return this;
    }

    int getPosition() {
      return bado.getPosition();
    }

    /** Gets the internal byte array. */
    byte[] getBytes() {
      return bytes;
    }
  }
}
