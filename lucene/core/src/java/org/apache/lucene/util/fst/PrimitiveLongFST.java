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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PrimitiveLongFST.PrimitiveLongArc.BitTable;

/**
 * HACK!
 *
 * <p>A copy of {@link FST} but remove generics to work with primitive types and avoid
 * boxing-unboxing.
 *
 * @lucene.experimental
 */
public final class PrimitiveLongFST implements Accountable {

  final PrimitiveLongFSTMetadata metadata;

  /** Specifies allowed range of each int input label for this FST. */
  public enum INPUT_TYPE {
    BYTE1,
    BYTE2,
    BYTE4
  }

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(PrimitiveLongFST.class);

  static final int BIT_FINAL_ARC = 1 << 0;
  static final int BIT_LAST_ARC = 1 << 1;
  static final int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  static final int BIT_STOP_NODE = 1 << 3;

  /** This flag is set if the arc has an output. */
  public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

  static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  /**
   * Value of the arc flags to declare a node with fixed length (sparse) arcs designed for binary
   * search.
   */
  // We use this as a marker because this one flag is illegal by itself.
  public static final byte ARCS_FOR_BINARY_SEARCH = BIT_ARC_HAS_FINAL_OUTPUT;

  /**
   * Value of the arc flags to declare a node with fixed length dense arcs and bit table designed
   * for direct addressing.
   */
  static final byte ARCS_FOR_DIRECT_ADDRESSING = 1 << 6;

  /**
   * Value of the arc flags to declare a node with continuous arcs designed for pos the arc directly
   * with labelToPos - firstLabel. like {@link #ARCS_FOR_BINARY_SEARCH} we use flag combinations
   * that will not occur at the same time.
   */
  static final byte ARCS_FOR_CONTINUOUS = ARCS_FOR_DIRECT_ADDRESSING + ARCS_FOR_BINARY_SEARCH;

  // Increment version to change it
  private static final String FILE_FORMAT_NAME = "FST";
  private static final int VERSION_START = 6;
  private static final int VERSION_LITTLE_ENDIAN = 8;
  private static final int VERSION_CONTINUOUS_ARCS = 9;
  static final int VERSION_CURRENT = VERSION_CONTINUOUS_ARCS;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  static final long FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  static final long NON_FINAL_END_NODE = 0;

  /** If arc has this label then that arc is final/accepted */
  public static final int END_LABEL = -1;

  /**
   * A {@link BytesStore}, used during building, or during reading when the FST is very large (more
   * than 1 GB). If the FST is less than 1 GB then bytesArray is set instead.
   */
  private final FSTReader fstReader;

  public final PrimitiveLongFSTOutputs outputs;

  /** Represents a single arc. */
  public static final class PrimitiveLongArc {

    // *** Arc fields.

    private int label;

    private long output;

    private long target;

    private byte flags;

    private long nextFinalOutput;

    private long nextArc;

    private byte nodeFlags;

    // *** Fields for arcs belonging to a node with fixed length arcs.
    // So only valid when bytesPerArc != 0.
    // nodeFlags == ARCS_FOR_BINARY_SEARCH || nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

    private int bytesPerArc;

    private long posArcsStart;

    private int arcIdx;

    private int numArcs;

    // *** Fields for a direct addressing node. nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

    /**
     * Start position in the {@link BytesReader} of the presence bits for a direct addressing node,
     * aka the bit-table
     */
    private long bitTableStart;

    /** First label of a direct addressing node. */
    private int firstLabel;

    /**
     * Index of the current label of a direct addressing node. While {@link #arcIdx} is the current
     * index in the label range, {@link #presenceIndex} is its corresponding index in the list of
     * actually present labels. It is equal to the number of bits set before the bit at {@link
     * #arcIdx} in the bit-table. This field is a cache to avoid to count bits set repeatedly when
     * iterating the next arcs.
     */
    private int presenceIndex;

    /** Returns this */
    public PrimitiveLongArc copyFrom(PrimitiveLongArc other) {
      label = other.label();
      target = other.target();
      flags = other.flags();
      output = other.output();
      nextFinalOutput = other.nextFinalOutput();
      nextArc = other.nextArc();
      nodeFlags = other.nodeFlags();
      bytesPerArc = other.bytesPerArc();

      // Fields for arcs belonging to a node with fixed length arcs.
      // We could avoid copying them if bytesPerArc() == 0 (this was the case with previous code,
      // and the current code
      // still supports that), but it may actually help external uses of FST to have consistent arc
      // state, and debugging
      // is easier.
      posArcsStart = other.posArcsStart();
      arcIdx = other.arcIdx();
      numArcs = other.numArcs();
      bitTableStart = other.bitTableStart;
      firstLabel = other.firstLabel();
      presenceIndex = other.presenceIndex;

      return this;
    }

    boolean flag(int flag) {
      return PrimitiveLongFST.flag(flags, flag);
    }

    public boolean isLast() {
      return flag(BIT_LAST_ARC);
    }

    public boolean isFinal() {
      return flag(BIT_FINAL_ARC);
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append(" target=").append(target());
      b.append(" label=0x").append(Integer.toHexString(label()));
      if (flag(BIT_FINAL_ARC)) {
        b.append(" final");
      }
      if (flag(BIT_LAST_ARC)) {
        b.append(" last");
      }
      if (flag(BIT_TARGET_NEXT)) {
        b.append(" targetNext");
      }
      if (flag(BIT_STOP_NODE)) {
        b.append(" stop");
      }
      if (flag(BIT_ARC_HAS_OUTPUT)) {
        b.append(" output=").append(output());
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=").append(nextFinalOutput());
      }
      if (bytesPerArc() != 0) {
        b.append(" arcArray(idx=")
            .append(arcIdx())
            .append(" of ")
            .append(numArcs())
            .append(")")
            .append("(")
            .append(
                nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING
                    ? "da"
                    : nodeFlags() == ARCS_FOR_CONTINUOUS ? "cs" : "bs")
            .append(")");
      }
      return b.toString();
    }

    public int label() {
      return label;
    }

    public long output() {
      return output;
    }

    /** Ord/address to target node. */
    public long target() {
      return target;
    }

    public byte flags() {
      return flags;
    }

    public long nextFinalOutput() {
      return nextFinalOutput;
    }

    /**
     * Address (into the byte[]) of the next arc - only for list of variable length arc. Or
     * ord/address to the next node if label == {@link #END_LABEL}.
     */
    long nextArc() {
      return nextArc;
    }

    /** Where we are in the array; only valid if bytesPerArc != 0. */
    public int arcIdx() {
      return arcIdx;
    }

    /**
     * Node header flags. Only meaningful to check if the value is either {@link
     * #ARCS_FOR_BINARY_SEARCH} or {@link #ARCS_FOR_DIRECT_ADDRESSING} or {@link
     * #ARCS_FOR_CONTINUOUS} (other value when bytesPerArc == 0).
     */
    public byte nodeFlags() {
      return nodeFlags;
    }

    /** Where the first arc in the array starts; only valid if bytesPerArc != 0 */
    public long posArcsStart() {
      return posArcsStart;
    }

    /**
     * Non-zero if this arc is part of a node with fixed length arcs, which means all arcs for the
     * node are encoded with a fixed number of bytes so that we binary search or direct address. We
     * do when there are enough arcs leaving one node. It wastes some bytes but gives faster
     * lookups.
     */
    public int bytesPerArc() {
      return bytesPerArc;
    }

    /**
     * How many arcs; only valid if bytesPerArc != 0 (fixed length arcs). For a node designed for
     * binary search this is the array size. For a node designed for direct addressing, this is the
     * label range.
     */
    public int numArcs() {
      return numArcs;
    }

    /**
     * First label of a direct addressing node. Only valid if nodeFlags == {@link
     * #ARCS_FOR_DIRECT_ADDRESSING} or {@link #ARCS_FOR_CONTINUOUS}.
     */
    int firstLabel() {
      return firstLabel;
    }

    /**
     * Helper methods to read the bit-table of a direct addressing node. Only valid for {@link
     * PrimitiveLongArc} with {@link PrimitiveLongArc#nodeFlags()} == {@code
     * ARCS_FOR_DIRECT_ADDRESSING}.
     */
    static class BitTable {

      /** See {@link BitTableUtil#isBitSet(int, BytesReader)}. */
      static boolean isBitSet(int bitIndex, PrimitiveLongArc arc, BytesReader in)
          throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.isBitSet(bitIndex, in);
      }

      /**
       * See {@link BitTableUtil#countBits(int, BytesReader)}. The count of bit set is the number of
       * arcs of a direct addressing node.
       */
      static int countBits(PrimitiveLongArc arc, BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.countBits(getNumPresenceBytes(arc.numArcs()), in);
      }

      /** See {@link BitTableUtil#countBitsUpTo(int, BytesReader)}. */
      static int countBitsUpTo(int bitIndex, PrimitiveLongArc arc, BytesReader in)
          throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.countBitsUpTo(bitIndex, in);
      }

      /** See {@link BitTableUtil#nextBitSet(int, int, BytesReader)}. */
      static int nextBitSet(int bitIndex, PrimitiveLongArc arc, BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.nextBitSet(bitIndex, getNumPresenceBytes(arc.numArcs()), in);
      }

      /** See {@link BitTableUtil#previousBitSet(int, BytesReader)}. */
      static int previousBitSet(int bitIndex, PrimitiveLongArc arc, BytesReader in)
          throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.previousBitSet(bitIndex, in);
      }

      /** Asserts the bit-table of the provided {@link PrimitiveLongArc} is valid. */
      static boolean assertIsValid(PrimitiveLongArc arc, BytesReader in) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        // First bit must be set.
        assert isBitSet(0, arc, in);
        // Last bit must be set.
        assert isBitSet(arc.numArcs() - 1, arc, in);
        // No bit set after the last arc.
        assert nextBitSet(arc.numArcs() - 1, arc, in) == -1;
        return true;
      }
    }
  }

  private static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  private static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

  /**
   * Load a previously saved FST with a DataInput for metdata using an {@link OnHeapFSTStore} with
   * maxBlockBits set to {@link #DEFAULT_MAX_BLOCK_BITS}
   */
  public PrimitiveLongFST(PrimitiveLongFSTMetadata metadata, DataInput in) throws IOException {
    this(metadata, in, new OnHeapFSTStore(DEFAULT_MAX_BLOCK_BITS));
  }

  /**
   * Load a previously saved FST with a metdata object and a FSTStore. If using {@link
   * OnHeapFSTStore}, setting maxBlockBits allows you to control the size of the byte[] pages used
   * to hold the FST bytes.
   */
  public PrimitiveLongFST(PrimitiveLongFSTMetadata metadata, DataInput in, FSTStore fstStore)
      throws IOException {
    this(metadata, fstStore.init(in, metadata.numBytes));
  }

  /** Create the FST with a metadata object and a FSTReader. */
  PrimitiveLongFST(PrimitiveLongFSTMetadata metadata, FSTReader fstReader) {
    this.metadata = metadata;
    this.outputs = metadata.outputs;
    this.fstReader = fstReader;
  }

  /**
   * Read the FST metadata from DataInput
   *
   * @param metaIn the DataInput of the metadata
   * @param outputs the FST outputs
   * @return the FST metadata
   * @throws IOException if exception occurred during parsing
   */
  public static PrimitiveLongFSTMetadata readMetadata(
      DataInput metaIn, PrimitiveLongFSTOutputs outputs) throws IOException {
    // NOTE: only reads formats VERSION_START up to VERSION_CURRENT; we don't have
    // back-compat promise for FSTs (they are experimental), but we are sometimes able to offer it
    int version = CodecUtil.checkHeader(metaIn, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);
    Long emptyOutput;
    if (metaIn.readByte() == 1) {
      // accepts empty string
      // 1 KB blocks:
      BytesStore emptyBytes = new BytesStore(10);
      int numBytes = metaIn.readVInt();
      emptyBytes.copyBytes(metaIn, numBytes);

      // De-serialize empty-string output:
      BytesReader reader = emptyBytes.getReverseBytesReader();
      // NoOutputs uses 0 bytes when writing its output,
      // so we have to check here else BytesStore gets
      // angry:
      if (numBytes > 0) {
        reader.setPosition(numBytes - 1);
      }
      emptyOutput = outputs.readFinalOutput(reader);
    } else {
      emptyOutput = null;
    }
    INPUT_TYPE inputType;
    final byte t = metaIn.readByte();
    switch (t) {
      case 0:
        inputType = INPUT_TYPE.BYTE1;
        break;
      case 1:
        inputType = INPUT_TYPE.BYTE2;
        break;
      case 2:
        inputType = INPUT_TYPE.BYTE4;
        break;
      default:
        throw new CorruptIndexException("invalid input type " + t, metaIn);
    }
    long startNode = metaIn.readVLong();
    long numBytes = metaIn.readVLong();
    return new PrimitiveLongFSTMetadata(
        inputType, outputs, emptyOutput, startNode, version, numBytes);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + fstReader.ramBytesUsed();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(input=" + metadata.inputType + ",output=" + outputs;
  }

  public long numBytes() {
    return metadata.numBytes;
  }

  public long getEmptyOutput() {
    return metadata.emptyOutput.longValue();
  }

  public PrimitiveLongFSTMetadata getMetadata() {
    return metadata;
  }

  public void save(DataOutput metaOut, DataOutput out) throws IOException {
    saveMetadata(metaOut);
    fstReader.writeTo(out);
  }

  /**
   * Save the metadata to a DataOutput
   *
   * @param metaOut the DataOutput to save
   */
  public void saveMetadata(DataOutput metaOut) throws IOException {
    CodecUtil.writeHeader(metaOut, FILE_FORMAT_NAME, VERSION_CURRENT);

    // Accepts empty string
    metaOut.writeByte((byte) 1);

    if (metadata.emptyOutput != null) {
      // Serialize empty-string output:
      ByteBuffersDataOutput ros = new ByteBuffersDataOutput();
      outputs.writeFinalOutput(metadata.emptyOutput.longValue(), ros);
      byte[] emptyOutputBytes = ros.toArrayCopy();
      int emptyLen = emptyOutputBytes.length;

      // reverse
      final int stopAt = emptyLen / 2;
      int upto = 0;
      while (upto < stopAt) {
        final byte b = emptyOutputBytes[upto];
        emptyOutputBytes[upto] = emptyOutputBytes[emptyLen - upto - 1];
        emptyOutputBytes[emptyLen - upto - 1] = b;
        upto++;
      }
      metaOut.writeVInt(emptyLen);
      metaOut.writeBytes(emptyOutputBytes, 0, emptyLen);
    } else {
      metaOut.writeByte((byte) 0);
    }

    final byte t;
    if (metadata.inputType == INPUT_TYPE.BYTE1) {
      t = 0;
    } else if (metadata.inputType == INPUT_TYPE.BYTE2) {
      t = 1;
    } else {
      t = 2;
    }
    metaOut.writeByte(t);
    metaOut.writeVLong(metadata.startNode);
    metaOut.writeVLong(numBytes());
  }

  /** Writes an automaton to a file. */
  public void save(final Path path) throws IOException {
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
      DataOutput out = new OutputStreamDataOutput(os);
      save(out, out);
    }
  }

  /** Reads an automaton from a file. */
  public static PrimitiveLongFST read(Path path, PrimitiveLongFSTOutputs outputs)
      throws IOException {
    try (InputStream is = Files.newInputStream(path)) {
      DataInput in = new InputStreamDataInput(new BufferedInputStream(is));
      return new PrimitiveLongFST(readMetadata(in, outputs), in);
    }
  }

  /** Reads one BYTE1/2/4 label from the provided {@link DataInput}. */
  public int readLabel(DataInput in) throws IOException {
    final int v;
    if (metadata.inputType == INPUT_TYPE.BYTE1) {
      // Unsigned byte:
      v = in.readByte() & 0xFF;
    } else if (metadata.inputType == INPUT_TYPE.BYTE2) {
      // Unsigned short:
      if (metadata.version < VERSION_LITTLE_ENDIAN) {
        v = Short.reverseBytes(in.readShort()) & 0xFFFF;
      } else {
        v = in.readShort() & 0xFFFF;
      }
    } else {
      v = in.readVInt();
    }
    return v;
  }

  /** returns true if the node at this address has any outgoing arcs */
  public static boolean targetHasArcs(PrimitiveLongArc arc) {
    return arc.target() > 0;
  }

  /**
   * Gets the number of bytes required to flag the presence of each arc in the given label range,
   * one bit per arc.
   */
  static int getNumPresenceBytes(int labelRange) {
    assert labelRange >= 0;
    return (labelRange + 7) >> 3;
  }

  /**
   * Reads the presence bits of a direct-addressing node. Actually we don't read them here, we just
   * keep the pointer to the bit-table start and we skip them.
   */
  private void readPresenceBytes(PrimitiveLongArc arc, BytesReader in) throws IOException {
    assert arc.bytesPerArc() > 0;
    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
    arc.bitTableStart = in.getPosition();
    in.skipBytes(getNumPresenceBytes(arc.numArcs()));
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to the FST's start node */
  public PrimitiveLongArc getFirstArc(PrimitiveLongArc arc) {
    long NO_OUTPUT = outputs.getNoOutput();

    arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
    if (metadata.emptyOutput != null) {
      arc.nextFinalOutput = metadata.emptyOutput.longValue();
    }
    if (metadata.emptyOutput != null && metadata.emptyOutput.longValue() != NO_OUTPUT) {
      arc.flags = (byte) (arc.flags() | BIT_ARC_HAS_FINAL_OUTPUT);
    }

    arc.output = NO_OUTPUT;

    // If there are no nodes, ie, the FST only accepts the
    // empty string, then startNode is 0
    arc.target = metadata.startNode;
    return arc;
  }

  /**
   * Follows the <code>follow</code> arc and reads the last arc of its target; this changes the
   * provided <code>arc</code> (2nd arg) in-place and returns it.
   *
   * @return Returns the second argument (<code>arc</code>).
   */
  PrimitiveLongArc readLastTargetArc(PrimitiveLongArc follow, PrimitiveLongArc arc, BytesReader in)
      throws IOException {
    // System.out.println("readLast");
    if (!targetHasArcs(follow)) {
      // System.out.println("  end node");
      assert follow.isFinal();
      arc.label = END_LABEL;
      arc.target = FINAL_END_NODE;
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_LAST_ARC;
      arc.nodeFlags = arc.flags;
      return arc;
    } else {
      in.setPosition(follow.target());
      byte flags = arc.nodeFlags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH
          || flags == ARCS_FOR_DIRECT_ADDRESSING
          || flags == ARCS_FOR_CONTINUOUS) {
        // Special arc which is actually a node header for fixed length arcs.
        // Jump straight to end to find the last arc.
        arc.numArcs = in.readVInt();
        arc.bytesPerArc = in.readVInt();
        // System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
          readPresenceBytes(arc, in);
          arc.firstLabel = readLabel(in);
          arc.posArcsStart = in.getPosition();
          readLastArcByDirectAddressing(arc, in);
        } else if (flags == ARCS_FOR_BINARY_SEARCH) {
          arc.arcIdx = arc.numArcs() - 2;
          arc.posArcsStart = in.getPosition();
          readNextRealArc(arc, in);
        } else {
          arc.firstLabel = readLabel(in);
          arc.posArcsStart = in.getPosition();
          readLastArcByContinuous(arc, in);
        }
      } else {
        arc.flags = flags;
        // non-array: linear scan
        arc.bytesPerArc = 0;
        // System.out.println("  scan");
        while (!arc.isLast()) {
          // skip this arc:
          readLabel(in);
          if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            outputs.skipOutput(in);
          }
          if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            outputs.skipFinalOutput(in);
          }
          if (arc.flag(BIT_STOP_NODE)) {
          } else if (arc.flag(BIT_TARGET_NEXT)) {
          } else {
            readUnpackedNodeTarget(in);
          }
          arc.flags = in.readByte();
        }
        // Undo the byte flags we read:
        in.skipBytes(-1);
        arc.nextArc = in.getPosition();
        readNextRealArc(arc, in);
      }
      assert arc.isLast();
      return arc;
    }
  }

  private long readUnpackedNodeTarget(BytesReader in) throws IOException {
    return in.readVLong();
  }

  /**
   * Follow the <code>follow</code> arc and read the first arc of its target; this changes the
   * provided <code>arc</code> (2nd arg) in-place and returns it.
   *
   * @return Returns the second argument (<code>arc</code>).
   */
  public PrimitiveLongArc readFirstTargetArc(
      PrimitiveLongArc follow, PrimitiveLongArc arc, BytesReader in) throws IOException {
    // int pos = address;
    // System.out.println("    readFirstTarget follow.target=" + follow.target + " isFinal=" +
    // follow.isFinal());
    if (follow.isFinal()) {
      // Insert "fake" final first arc:
      arc.label = END_LABEL;
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_FINAL_ARC;
      if (follow.target() <= 0) {
        arc.flags |= BIT_LAST_ARC;
      } else {
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target();
      }
      arc.target = FINAL_END_NODE;
      arc.nodeFlags = arc.flags;
      // System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" +
      // arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      return readFirstRealTargetArc(follow.target(), arc, in);
    }
  }

  private void readFirstArcInfo(long nodeAddress, PrimitiveLongArc arc, final BytesReader in)
      throws IOException {
    in.setPosition(nodeAddress);

    byte flags = arc.nodeFlags = in.readByte();
    if (flags == ARCS_FOR_BINARY_SEARCH
        || flags == ARCS_FOR_DIRECT_ADDRESSING
        || flags == ARCS_FOR_CONTINUOUS) {
      // Special arc which is actually a node header for fixed length arcs.
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.arcIdx = -1;
      if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
        readPresenceBytes(arc, in);
        arc.firstLabel = readLabel(in);
        arc.presenceIndex = -1;
      } else if (flags == ARCS_FOR_CONTINUOUS) {
        arc.firstLabel = readLabel(in);
      }
      arc.posArcsStart = in.getPosition();
    } else {
      arc.nextArc = nodeAddress;
      arc.bytesPerArc = 0;
    }
  }

  public PrimitiveLongArc readFirstRealTargetArc(
      long nodeAddress, PrimitiveLongArc arc, final BytesReader in) throws IOException {
    readFirstArcInfo(nodeAddress, arc, in);
    return readNextRealArc(arc, in);
  }

  /**
   * Returns whether <code>arc</code>'s target points to a node in expanded format (fixed length
   * arcs).
   */
  boolean isExpandedTarget(PrimitiveLongArc follow, BytesReader in) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      in.setPosition(follow.target());
      byte flags = in.readByte();
      return flags == ARCS_FOR_BINARY_SEARCH
          || flags == ARCS_FOR_DIRECT_ADDRESSING
          || flags == ARCS_FOR_CONTINUOUS;
    }
  }

  /** In-place read; returns the arc. */
  public PrimitiveLongArc readNextArc(PrimitiveLongArc arc, BytesReader in) throws IOException {
    if (arc.label() == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc() <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc(), arc, in);
    } else {
      return readNextRealArc(arc, in);
    }
  }

  /** Peeks at next arc's label; does not alter arc. Do not call this if arc.isLast()! */
  int readNextArcLabel(PrimitiveLongArc arc, BytesReader in) throws IOException {
    assert !arc.isLast();

    if (arc.label() == END_LABEL) {
      // System.out.println("    nextArc fake " + arc.nextArc);
      // Next arc is the first arc of a node.
      // Position to read the first arc label.

      in.setPosition(arc.nextArc());
      byte flags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH
          || flags == ARCS_FOR_DIRECT_ADDRESSING
          || flags == ARCS_FOR_CONTINUOUS) {
        // System.out.println("    nextArc fixed length arc");
        // Special arc which is actually a node header for fixed length arcs.
        int numArcs = in.readVInt();
        in.readVInt(); // Skip bytesPerArc.
        if (flags == ARCS_FOR_BINARY_SEARCH) {
          in.readByte(); // Skip arc flags.
        } else if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
          in.skipBytes(getNumPresenceBytes(numArcs));
        } // Nothing to do for ARCS_FOR_CONTINUOUS
      }
    } else {
      switch (arc.nodeFlags()) {
        case ARCS_FOR_BINARY_SEARCH:
          // Point to next arc, -1 to skip arc flags.
          in.setPosition(arc.posArcsStart() - (1 + arc.arcIdx()) * (long) arc.bytesPerArc() - 1);
          break;
        case ARCS_FOR_DIRECT_ADDRESSING:
          // Direct addressing node. The label is not stored but rather inferred
          // based on first label and arc index in the range.
          assert BitTable.assertIsValid(arc, in);
          assert BitTable.isBitSet(arc.arcIdx(), arc, in);
          int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
          assert nextIndex != -1;
          return arc.firstLabel() + nextIndex;
        case ARCS_FOR_CONTINUOUS:
          return arc.firstLabel() + arc.arcIdx() + 1;
        default:
          // Variable length arcs - linear search.
          assert arc.bytesPerArc() == 0;
          // Arcs have variable length.
          // System.out.println("    nextArc real list");
          // Position to next arc, -1 to skip flags.
          in.setPosition(arc.nextArc() - 1);
          break;
      }
    }
    return readLabel(in);
  }

  public PrimitiveLongArc readArcByIndex(PrimitiveLongArc arc, final BytesReader in, int idx)
      throws IOException {
    assert arc.bytesPerArc() > 0;
    assert arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH;
    assert idx >= 0 && idx < arc.numArcs();
    in.setPosition(arc.posArcsStart() - idx * (long) arc.bytesPerArc());
    arc.arcIdx = idx;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /**
   * Reads a Continuous node arc, with the provided index in the label range.
   *
   * @param rangeIndex The index of the arc in the label range. It must be within the label range.
   */
  public PrimitiveLongArc readArcByContinuous(
      PrimitiveLongArc arc, final BytesReader in, int rangeIndex) throws IOException {
    assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
    in.setPosition(arc.posArcsStart() - rangeIndex * (long) arc.bytesPerArc());
    arc.arcIdx = rangeIndex;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /**
   * Reads a present direct addressing node arc, with the provided index in the label range.
   *
   * @param rangeIndex The index of the arc in the label range. It must be present. The real arc
   *     offset is computed based on the presence bits of the direct addressing node.
   */
  public PrimitiveLongArc readArcByDirectAddressing(
      PrimitiveLongArc arc, final BytesReader in, int rangeIndex) throws IOException {
    assert BitTable.assertIsValid(arc, in);
    assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
    assert BitTable.isBitSet(rangeIndex, arc, in);
    int presenceIndex = BitTable.countBitsUpTo(rangeIndex, arc, in);
    return readArcByDirectAddressing(arc, in, rangeIndex, presenceIndex);
  }

  /**
   * Reads a present direct addressing node arc, with the provided index in the label range and its
   * corresponding presence index (which is the count of presence bits before it).
   */
  private PrimitiveLongArc readArcByDirectAddressing(
      PrimitiveLongArc arc, final BytesReader in, int rangeIndex, int presenceIndex)
      throws IOException {
    in.setPosition(arc.posArcsStart() - presenceIndex * (long) arc.bytesPerArc());
    arc.arcIdx = rangeIndex;
    arc.presenceIndex = presenceIndex;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /**
   * Reads the last arc of a direct addressing node. This method is equivalent to call {@link
   * #readArcByDirectAddressing(PrimitiveLongArc, BytesReader, int)} with {@code rangeIndex} equal
   * to {@code arc.numArcs() - 1}, but it is faster.
   */
  public PrimitiveLongArc readLastArcByDirectAddressing(PrimitiveLongArc arc, final BytesReader in)
      throws IOException {
    assert BitTable.assertIsValid(arc, in);
    int presenceIndex = BitTable.countBits(arc, in) - 1;
    return readArcByDirectAddressing(arc, in, arc.numArcs() - 1, presenceIndex);
  }

  /** Reads the last arc of a continuous node. */
  public PrimitiveLongArc readLastArcByContinuous(PrimitiveLongArc arc, final BytesReader in)
      throws IOException {
    return readArcByContinuous(arc, in, arc.numArcs() - 1);
  }

  /** Never returns null, but you should never call this if arc.isLast() is true. */
  public PrimitiveLongArc readNextRealArc(PrimitiveLongArc arc, final BytesReader in)
      throws IOException {

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    switch (arc.nodeFlags()) {
      case ARCS_FOR_BINARY_SEARCH:
      case ARCS_FOR_CONTINUOUS:
        assert arc.bytesPerArc() > 0;
        arc.arcIdx++;
        assert arc.arcIdx() >= 0 && arc.arcIdx() < arc.numArcs();
        in.setPosition(arc.posArcsStart() - arc.arcIdx() * (long) arc.bytesPerArc());
        arc.flags = in.readByte();
        break;

      case ARCS_FOR_DIRECT_ADDRESSING:
        assert BitTable.assertIsValid(arc, in);
        assert arc.arcIdx() == -1 || BitTable.isBitSet(arc.arcIdx(), arc, in);
        int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
        return readArcByDirectAddressing(arc, in, nextIndex, arc.presenceIndex + 1);

      default:
        // Variable length arcs - linear search.
        assert arc.bytesPerArc() == 0;
        in.setPosition(arc.nextArc());
        arc.flags = in.readByte();
    }
    return readArc(arc, in);
  }

  /**
   * Reads an arc. <br>
   * Precondition: The arc flags byte has already been read and set; the given BytesReader is
   * positioned just after the arc flags byte.
   */
  private PrimitiveLongArc readArc(PrimitiveLongArc arc, BytesReader in) throws IOException {
    if (arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING || arc.nodeFlags() == ARCS_FOR_CONTINUOUS) {
      arc.label = arc.firstLabel() + arc.arcIdx();
    } else {
      arc.label = readLabel(in);
    }

    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
      arc.output = outputs.read(in);
    } else {
      arc.output = outputs.getNoOutput();
    }

    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
      arc.nextFinalOutput = outputs.readFinalOutput(in);
    } else {
      arc.nextFinalOutput = outputs.getNoOutput();
    }

    if (arc.flag(BIT_STOP_NODE)) {
      if (arc.flag(BIT_FINAL_ARC)) {
        arc.target = FINAL_END_NODE;
      } else {
        arc.target = NON_FINAL_END_NODE;
      }
      arc.nextArc = in.getPosition(); // Only useful for list.
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      arc.nextArc = in.getPosition(); // Only useful for list.
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (!arc.flag(BIT_LAST_ARC)) {
        if (arc.bytesPerArc() == 0) {
          // must scan
          seekToNextNode(in);
        } else {
          int numArcs =
              arc.nodeFlags == ARCS_FOR_DIRECT_ADDRESSING
                  ? BitTable.countBits(arc, in)
                  : arc.numArcs();
          in.setPosition(arc.posArcsStart() - arc.bytesPerArc() * (long) numArcs);
        }
      }
      arc.target = in.getPosition();
    } else {
      arc.target = readUnpackedNodeTarget(in);
      arc.nextArc = in.getPosition(); // Only useful for list.
    }
    return arc;
  }

  static PrimitiveLongArc readEndArc(PrimitiveLongArc follow, PrimitiveLongArc arc) {
    if (follow.isFinal()) {
      if (follow.target() <= 0) {
        arc.flags = PrimitiveLongFST.BIT_LAST_ARC;
      } else {
        arc.flags = 0;
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target();
      }
      arc.output = follow.nextFinalOutput();
      arc.label = PrimitiveLongFST.END_LABEL;
      return arc;
    } else {
      return null;
    }
  }

  // TODO: could we somehow [partially] tableize arc lookups
  // like automaton?

  /**
   * Finds an arc leaving the incoming arc, replacing the arc in place. This returns null if the arc
   * was not found, else the incoming arc.
   */
  public PrimitiveLongArc findTargetArc(
      int labelToMatch, PrimitiveLongArc follow, PrimitiveLongArc arc, BytesReader in)
      throws IOException {

    if (labelToMatch == END_LABEL) {
      if (follow.isFinal()) {
        if (follow.target() <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target();
        }
        arc.output = follow.nextFinalOutput();
        arc.label = END_LABEL;
        arc.nodeFlags = arc.flags;
        return arc;
      } else {
        return null;
      }
    }

    if (!targetHasArcs(follow)) {
      return null;
    }

    in.setPosition(follow.target());

    // System.out.println("fta label=" + (char) labelToMatch);

    byte flags = arc.nodeFlags = in.readByte();
    if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
      arc.numArcs = in.readVInt(); // This is in fact the label range.
      arc.bytesPerArc = in.readVInt();
      readPresenceBytes(arc, in);
      arc.firstLabel = readLabel(in);
      arc.posArcsStart = in.getPosition();

      int arcIndex = labelToMatch - arc.firstLabel();
      if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
        return null; // Before or after label range.
      } else if (!BitTable.isBitSet(arcIndex, arc, in)) {
        return null; // Arc missing in the range.
      }
      return readArcByDirectAddressing(arc, in, arcIndex);
    } else if (flags == ARCS_FOR_BINARY_SEARCH) {
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.posArcsStart = in.getPosition();

      // Array is sparse; do binary search:
      int low = 0;
      int high = arc.numArcs() - 1;
      while (low <= high) {
        // System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        // +1 to skip over flags
        in.setPosition(arc.posArcsStart() - (arc.bytesPerArc() * mid + 1));
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          arc.arcIdx = mid - 1;
          // System.out.println("    found!");
          return readNextRealArc(arc, in);
        }
      }
      return null;
    } else if (flags == ARCS_FOR_CONTINUOUS) {
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.firstLabel = readLabel(in);
      arc.posArcsStart = in.getPosition();
      int arcIndex = labelToMatch - arc.firstLabel();
      if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
        return null; // Before or after label range.
      }
      arc.arcIdx = arcIndex - 1;
      return readNextRealArc(arc, in);
    }

    // Linear scan
    readFirstArcInfo(follow.target(), arc, in);
    in.setPosition(arc.nextArc());
    while (true) {
      assert arc.bytesPerArc() == 0;
      flags = arc.flags = in.readByte();
      long pos = in.getPosition();
      int label = readLabel(in);
      if (label == labelToMatch) {
        in.setPosition(pos);
        return readArc(arc, in);
      } else if (label > labelToMatch) {
        return null;
      } else if (arc.isLast()) {
        return null;
      } else {
        if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
          outputs.skipOutput(in);
        }
        if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
          outputs.skipFinalOutput(in);
        }
        if (flag(flags, BIT_STOP_NODE) == false && flag(flags, BIT_TARGET_NEXT) == false) {
          readUnpackedNodeTarget(in);
        }
      }
    }
  }

  private void seekToNextNode(BytesReader in) throws IOException {

    while (true) {

      final int flags = in.readByte();
      readLabel(in);

      if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
        outputs.skipOutput(in);
      }

      if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        outputs.skipFinalOutput(in);
      }

      if (flag(flags, BIT_STOP_NODE) == false && flag(flags, BIT_TARGET_NEXT) == false) {
        readUnpackedNodeTarget(in);
      }

      if (flag(flags, BIT_LAST_ARC)) {
        return;
      }
    }
  }

  /** Returns a {@link BytesReader} for this FST, positioned at position 0. */
  public BytesReader getBytesReader() {
    return fstReader.getReverseBytesReader();
  }

  /** Represent the FST metadata */
  public static final class PrimitiveLongFSTMetadata {
    final INPUT_TYPE inputType;
    final PrimitiveLongFSTOutputs outputs;
    final int version;
    // if non-null, this FST accepts the empty string and
    // produces this output
    Long emptyOutput;
    long startNode;
    long numBytes;

    public PrimitiveLongFSTMetadata(
        INPUT_TYPE inputType,
        PrimitiveLongFSTOutputs outputs,
        Long emptyOutput,
        long startNode,
        int version,
        long numBytes) {
      this.inputType = inputType;
      this.outputs = outputs;
      this.emptyOutput = emptyOutput;
      this.startNode = startNode;
      this.version = version;
      this.numBytes = numBytes;
    }
  }

  public static class PrimitiveLongFSTOutputs {

    private static final long NO_OUTPUT = 0L;

    private static final PrimitiveLongFSTOutputs singleton = new PrimitiveLongFSTOutputs();

    private PrimitiveLongFSTOutputs() {}

    public static PrimitiveLongFSTOutputs getSingleton() {
      return singleton;
    }

    public long common(long output1, long output2) {
      assert valid(output1);
      assert valid(output2);
      if (output1 == NO_OUTPUT || output2 == NO_OUTPUT) {
        return NO_OUTPUT;
      } else {
        assert output1 > 0;
        assert output2 > 0;
        return Math.min(output1, output2);
      }
    }

    public long subtract(long output, long inc) {
      assert valid(output);
      assert valid(inc);
      assert output >= inc;

      if (inc == NO_OUTPUT) {
        return output;
      } else if (output == inc) {
        return NO_OUTPUT;
      } else {
        return output - inc;
      }
    }

    public long add(long prefix, long output) {
      assert valid(prefix);
      assert valid(output);
      if (prefix == NO_OUTPUT) {
        return output;
      } else if (output == NO_OUTPUT) {
        return prefix;
      } else {
        return prefix + output;
      }
    }

    public void write(long output, DataOutput out) throws IOException {
      assert valid(output);
      out.writeVLong(output);
    }

    public long read(DataInput in) throws IOException {
      long v = in.readVLong();
      if (v == 0) {
        return NO_OUTPUT;
      } else {
        return v;
      }
    }

    private boolean valid(long o) {
      assert o == NO_OUTPUT || o > 0 : "o=" + o;
      return true;
    }

    public long getNoOutput() {
      return NO_OUTPUT;
    }

    public String outputToString(long output) {
      return Long.toString(output);
    }

    public String toString() {
      return "PrimitiveLongFSTOutputs";
    }

    public long ramBytesUsed(Long output) {
      return RamUsageEstimator.sizeOf(output);
    }

    public void skipOutput(BytesReader in) throws IOException {
      read(in);
    }

    public void skipFinalOutput(BytesReader in) throws IOException {
      read(in);
    }

    public long readFinalOutput(BytesReader in) throws IOException {
      return read(in);
    }

    public void writeFinalOutput(long output, DataOutput out) throws IOException {
      write(output, out);
    }
  }

  public static long get(PrimitiveLongFST primitiveLongFST, BytesRef input) throws IOException {
    assert primitiveLongFST.metadata.inputType == PrimitiveLongFST.INPUT_TYPE.BYTE1;

    final BytesReader fstReader = primitiveLongFST.getBytesReader();

    // TODO: would be nice not to alloc this on every lookup
    final PrimitiveLongArc arc = primitiveLongFST.getFirstArc(new PrimitiveLongArc());

    // Accumulate output as we go
    long output = primitiveLongFST.outputs.getNoOutput();
    for (int i = 0; i < input.length; i++) {
      if (primitiveLongFST.findTargetArc(input.bytes[i + input.offset] & 0xFF, arc, arc, fstReader)
          == null) {
        return -1;
      }
      output = primitiveLongFST.outputs.add(output, arc.output());
    }

    if (arc.isFinal()) {
      return primitiveLongFST.outputs.add(output, arc.nextFinalOutput());
    } else {
      return -1;
    }
  }
}
