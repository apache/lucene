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
package org.apache.lucene.codecs.lucene104;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene103.blocktree.Lucene103BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene103.blocktree.Lucene103BlockTreeTermsWriter;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 10.4 postings format, which encodes postings in packed integer blocks for fast decode.
 *
 * <p>Basic idea:
 *
 * <ul>
 *   <li><b>Packed Blocks and VInt Blocks</b>:
 *       <p>In packed blocks, integers are encoded with the same bit width ({@link PackedInts packed
 *       format}): the block size (i.e. number of integers inside block) is fixed (currently 256).
 *       Additionally blocks that are all the same value are encoded in an optimized way.
 *       <p>In VInt blocks, integers are encoded as {@link DataOutput#writeVInt VInt}: the block
 *       size is variable.
 *   <li><b>Block structure</b>:
 *       <p>When the postings are long enough, Lucene104PostingsFormat will try to encode most
 *       integer data as a packed block.
 *       <p>Take a term with 259 documents as an example, the first 256 document ids are encoded as
 *       two packed blocks, while the remaining 3 are encoded as one VInt block.
 *       <p>Different kinds of data are always encoded separately into different packed blocks, but
 *       may possibly be interleaved into the same VInt block.
 *       <p>This strategy is applied to pairs: &lt;document number, frequency&gt;, &lt;position,
 *       payload length&gt;, &lt;position, offset start, offset length&gt;, and &lt;position,
 *       payload length, offsetstart, offset length&gt;.
 *   <li><b>Skipdata</b>:
 *       <p>Skipdata is interleaved with blocks on 2 levels. Level 0 skip data is interleaved
 *       between every packed block. Level 1 skip data is interleaved between every 32 packed
 *       blocks.
 *   <li><b>Positions, Payloads, and Offsets</b>:
 *       <p>A position is an integer indicating where the term occurs within one document. A payload
 *       is a blob of metadata associated with current position. An offset is a pair of integers
 *       indicating the tokenized start/end offsets for given term in current position: it is
 *       essentially a specialized payload.
 *       <p>When payloads and offsets are not omitted, numPositions==numPayloads==numOffsets
 *       (assuming a null payload contributes one count). As mentioned in block structure, it is
 *       possible to encode these three either combined or separately.
 *       <p>In all cases, payloads and offsets are stored together. When encoded as a packed block,
 *       position data is separated out as .pos, while payloads and offsets are encoded in .pay
 *       (payload metadata will also be stored directly in .pay). When encoded as VInt blocks, all
 *       these three are stored interleaved into the .pos (so is payload metadata).
 *       <p>With this strategy, the majority of payload and offset data will be outside .pos file.
 *       So for queries that require only position data, running on a full index with payloads and
 *       offsets, this reduces disk pre-fetches.
 * </ul>
 *
 * <p>Files and detailed format:
 *
 * <ul>
 *   <li><code>.tim</code>: <a href="#Termdictionary">Term Dictionary</a>
 *   <li><code>.tip</code>: <a href="#Termindex">Term Index</a>
 *   <li><code>.doc</code>: <a href="#Frequencies">Frequencies and Skip Data</a>
 *   <li><code>.pos</code>: <a href="#Positions">Positions</a>
 *   <li><code>.pay</code>: <a href="#Payloads">Payloads and Offsets</a>
 * </ul>
 *
 * <a id="Termdictionary"></a>
 *
 * <dl>
 *   <dd><b>Term Dictionary</b>
 *       <p>The .tim file contains the list of terms in each field along with per-term statistics
 *       (such as docfreq) and pointers to the frequencies, positions, payload and skip data in the
 *       .doc, .pos, and .pay files. See {@link Lucene103BlockTreeTermsWriter} for more details on
 *       the format.
 *       <p>NOTE: The term dictionary can plug into different postings implementations: the postings
 *       writer/reader are actually responsible for encoding and decoding the PostingsHeader and
 *       TermMetadata sections described here:
 *       <ul>
 *         <li>PostingsHeader --&gt; Header, PackedBlockSize
 *         <li>TermMetadata --&gt; (DocFPDelta|SingletonDocID), PosFPDelta?, PosVIntBlockFPDelta?,
 *             PayFPDelta?
 *         <li>Header, --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>PackedBlockSize, SingletonDocID --&gt; {@link DataOutput#writeVInt VInt}
 *         <li>DocFPDelta, PosFPDelta, PayFPDelta, PosVIntBlockFPDelta --&gt; {@link
 *             DataOutput#writeVLong VLong}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>Header is a {@link CodecUtil#writeIndexHeader IndexHeader} storing the version
 *             information for the postings.
 *         <li>PackedBlockSize is the fixed block size for packed blocks. In packed block, bit width
 *             is determined by the largest integer. Smaller block size result in smaller variance
 *             among width of integers hence smaller indexes. Larger block size result in more
 *             efficient bulk i/o hence better acceleration. This value should always be a multiple
 *             of 64, currently fixed as 256 as a tradeoff. It is also the skip interval used to
 *             accelerate {@link org.apache.lucene.index.PostingsEnum#advance(int)}.
 *         <li>DocFPDelta determines the position of this term's TermFreqs within the .doc file. In
 *             particular, it is the difference of file offset between this term's data and previous
 *             term's data (or zero, for the first term in the block).On disk it is stored as the
 *             difference from previous value in sequence.
 *         <li>PosFPDelta determines the position of this term's TermPositions within the .pos file.
 *             While PayFPDelta determines the position of this term's &lt;TermPayloads,
 *             TermOffsets?&gt; within the .pay file. Similar to DocFPDelta, it is the difference
 *             between two file positions (or neglected, for fields that omit payloads and offsets).
 *         <li>PosVIntBlockFPDelta determines the position of this term's last TermPosition in last
 *             pos packed block within the .pos file. It is synonym for PayVIntBlockFPDelta or
 *             OffsetVIntBlockFPDelta. This is actually used to indicate whether it is necessary to
 *             load following payloads and offsets from .pos instead of .pay. Every time a new block
 *             of positions are to be loaded, the PostingsReader will use this value to check
 *             whether current block is packed format or VInt. When packed format, payloads and
 *             offsets are fetched from .pay, otherwise from .pos. (this value is neglected when
 *             total number of positions i.e. totalTermFreq is less or equal to PackedBlockSize).
 *         <li>SingletonDocID is an optimization when a term only appears in one document. In this
 *             case, instead of writing a file pointer to the .doc file (DocFPDelta), and then a
 *             VIntBlock at that location, the single document ID is written to the term dictionary.
 *       </ul>
 * </dl>
 *
 * <a id="Termindex"></a>
 *
 * <dl>
 *   <dd><b>Term Index</b>
 *       <p>The .tip file contains an index into the term dictionary, so that it can be accessed
 *       randomly. See {@link Lucene103BlockTreeTermsWriter} for more details on the format.
 * </dl>
 *
 * <a id="Frequencies"></a>
 *
 * <dl>
 *   <dd><b>Frequencies and Skip Data</b>
 *       <p>The .doc file contains the lists of documents which contain each term, along with the
 *       frequency of the term in that document (except when frequencies are omitted: {@link
 *       IndexOptions#DOCS}). Skip data is saved at the end of each term's postings. The skip data
 *       is saved once for the entire postings list.
 *       <ul>
 *         <li>docFile(.doc) --&gt; Header, &lt;TermFreqs&gt;<sup>TermCount</sup>, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>TermFreqs --&gt; &lt;PackedBlock32&gt; <sup>PackedDocBlockNum/32</sup>, VIntBlock?
 *         <li>PackedBlock32 --&gt; Level1SkipData, &lt;PackedBlock&gt; <sup>32</sup>
 *         <li>PackedBlock --&gt; Level0SkipData, PackedDocDeltaBlock, PackedFreqBlock?
 *         <li>VIntBlock --&gt;
 *             &lt;DocDelta[,Freq?]&gt;<sup>DocFreq-PackedBlockSize*PackedDocBlockNum</sup>
 *         <li>Level1SkipData --&gt; DocDelta, DocFPDelta, Skip1NumBytes?, ImpactLength?, Impacts?,
 *             PosFPDelta?, NextPosUpto?, PayFPDelta?, NextPayByteUpto?
 *         <li>Level0SkipData --&gt; Skip0NumBytes, DocDelta, DocFPDelta, PackedBlockLength,
 *             ImpactLength?, Impacts?, PosFPDelta?, NextPosUpto?, PayFPDelta?, NextPayByteUpto?
 *         <li>PackedFreqBlock --&gt; {@link PackedInts PackedInts}, uses patching
 *         <li>PackedDocDeltaBlock --&gt; {@link PackedInts PackedInts}, does not use patching
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>PackedDocDeltaBlock is theoretically generated from two steps:
 *             <ol>
 *               <li>Calculate the difference between each document number and previous one, and get
 *                   a d-gaps list (for the first document, use absolute value);
 *               <li>For those d-gaps from first one to
 *                   PackedDocBlockNum*PackedBlockSize<sup>th</sup>, separately encode as packed
 *                   blocks.
 *             </ol>
 *             If frequencies are not omitted, PackedFreqBlock will be generated without d-gap step.
 *         <li>VIntBlock stores remaining d-gaps (along with frequencies when possible) with a
 *             format that encodes DocDelta and Freq:
 *             <p>DocDelta: if frequencies are indexed, this determines both the document number and
 *             the frequency. In particular, DocDelta/2 is the difference between this document
 *             number and the previous document number (or zero when this is the first document in a
 *             TermFreqs). When DocDelta is odd, the frequency is one. When DocDelta is even, the
 *             frequency is read as another VInt. If frequencies are omitted, DocDelta contains the
 *             gap (not multiplied by 2) between document numbers and no frequency information is
 *             stored.
 *             <p>For example, the TermFreqs for a term which occurs once in document seven and
 *             three times in document eleven, with frequencies indexed, would be the following
 *             sequence of VInts:
 *             <p>15, 8, 3
 *             <p>If frequencies were omitted ({@link IndexOptions#DOCS}) it would be this sequence
 *             of VInts instead:
 *             <p>7,4
 *         <li>PackedDocBlockNum is the number of packed blocks for current term's docids or
 *             frequencies. In particular, PackedDocBlockNum = floor(DocFreq/PackedBlockSize)
 *         <li>On skip data, DocDelta is the delta between the last doc of the previous block - or
 *             -1 if there is no previous block - and the last doc of this block. This helps know by
 *             how much the doc ID should be incremented in case the block gets skipped.
 *         <li>Skip0Length is the length of skip data at level 0. Encoding it is useful when skip
 *             data is never needed to quickly skip over skip data, e.g. if only using nextDoc(). It
 *             is also used when only the first fields of skip data are needed, in order to skip
 *             over remaining fields without reading them.
 *         <li>ImpactLength and Impacts are only stored if frequencies are indexed.
 *         <li>Since positions and payloads are also block encoded, the skip should skip to related
 *             block first, then fetch the values according to in-block offset. PosFPSkip and
 *             PayFPSkip record the file offsets of related block in .pos and .pay, respectively.
 *             While PosBlockOffset indicates which value to fetch inside the related block
 *             (PayBlockOffset is unnecessary since it is always equal to PosBlockOffset). Same as
 *             DocFPSkip, the file offsets are relative to the start of current term's TermFreqs,
 *             and stored as a difference sequence.
 *         <li>PayByteUpto indicates the start offset of the current payload. It is equivalent to
 *             the sum of the payload lengths in the current block up to PosBlockOffset
 *         <li>ImpactLength is the total length of CompetitiveFreqDelta and CompetitiveNormDelta
 *             pairs. CompetitiveFreqDelta and CompetitiveNormDelta are used to safely skip score
 *             calculation for uncompetitive documents; See {@link
 *             org.apache.lucene.codecs.CompetitiveImpactAccumulator} for more details.
 *       </ul>
 * </dl>
 *
 * <a id="Positions"></a>
 *
 * <dl>
 *   <dd><b>Positions</b>
 *       <p>The .pos file contains the lists of positions that each term occurs at within documents.
 *       It also sometimes stores part of payloads and offsets for speedup.
 *       <ul>
 *         <li>PosFile(.pos) --&gt; Header, &lt;TermPositions&gt; <sup>TermCount</sup>, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>TermPositions --&gt; &lt;PackedPosDeltaBlock&gt; <sup>PackedPosBlockNum</sup>,
 *             VIntBlock?
 *         <li>VIntBlock --&gt; &lt;PositionDelta[, PayloadLength?], PayloadData?, OffsetDelta?,
 *             OffsetLength?&gt;<sup>PosVIntCount</sup>
 *         <li>PackedPosDeltaBlock --&gt; {@link PackedInts PackedInts}
 *         <li>PositionDelta, OffsetDelta, OffsetLength --&gt; {@link DataOutput#writeVInt VInt}
 *         <li>PayloadData --&gt; {@link DataOutput#writeByte byte}<sup>PayLength</sup>
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>TermPositions are order by term (terms are implicit, from the term dictionary), and
 *             position values for each term document pair are incremental, and ordered by document
 *             number.
 *         <li>PackedPosBlockNum is the number of packed blocks for current term's positions,
 *             payloads or offsets. In particular, PackedPosBlockNum =
 *             floor(totalTermFreq/PackedBlockSize)
 *         <li>PosVIntCount is the number of positions encoded as VInt format. In particular,
 *             PosVIntCount = totalTermFreq - PackedPosBlockNum*PackedBlockSize
 *         <li>The procedure how PackedPosDeltaBlock is generated is the same as PackedDocDeltaBlock
 *             in chapter <a href="#Frequencies">Frequencies and Skip Data</a>.
 *         <li>PositionDelta is, if payloads are disabled for the term's field, the difference
 *             between the position of the current occurrence in the document and the previous
 *             occurrence (or zero, if this is the first occurrence in this document). If payloads
 *             are enabled for the term's field, then PositionDelta/2 is the difference between the
 *             current and the previous position. If payloads are enabled and PositionDelta is odd,
 *             then PayloadLength is stored, indicating the length of the payload at the current
 *             term position.
 *         <li>For example, the TermPositions for a term which occurs as the fourth term in one
 *             document, and as the fifth and ninth term in a subsequent document, would be the
 *             following sequence of VInts (payloads disabled):
 *             <p>4, 5, 4
 *         <li>PayloadData is metadata associated with the current term position. If PayloadLength
 *             is stored at the current position, then it indicates the length of this payload. If
 *             PayloadLength is not stored, then this payload has the same length as the payload at
 *             the previous position.
 *         <li>OffsetDelta/2 is the difference between this position's startOffset from the previous
 *             occurrence (or zero, if this is the first occurrence in this document). If
 *             OffsetDelta is odd, then the length (endOffset-startOffset) differs from the previous
 *             occurrence and an OffsetLength follows. Offset data is only written for {@link
 *             IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.
 *       </ul>
 * </dl>
 *
 * <a id="Payloads"></a>
 *
 * <dl>
 *   <dd><b>Payloads and Offsets</b>
 *       <p>The .pay file will store payloads and offsets associated with certain term-document
 *       positions. Some payloads and offsets will be separated out into .pos file, for performance
 *       reasons.
 *       <ul>
 *         <li>PayFile(.pay): --&gt; Header, &lt;TermPayloads?, TermOffsets?&gt;
 *             <sup>TermCount</sup>, Footer
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>TermPayloads --&gt; &lt;PackedPayLengthBlock, SumPayLength, PayData&gt;
 *             <sup>PackedPayBlockNum</sup>
 *         <li>TermOffsets --&gt; &lt;PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock&gt;
 *             <sup>PackedPayBlockNum</sup>
 *         <li>PackedPayLengthBlock, PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock --&gt;
 *             {@link PackedInts PackedInts}
 *         <li>SumPayLength --&gt; {@link DataOutput#writeVInt VInt}
 *         <li>PayData --&gt; {@link DataOutput#writeByte byte}<sup>SumPayLength</sup>
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>Notes:
 *       <ul>
 *         <li>The order of TermPayloads/TermOffsets will be the same as TermPositions, note that
 *             part of payload/offsets are stored in .pos.
 *         <li>The procedure how PackedPayLengthBlock and PackedOffsetLengthBlock are generated is
 *             the same as PackedFreqBlock in chapter <a href="#Frequencies">Frequencies and Skip
 *             Data</a>. While PackedStartDeltaBlock follows a same procedure as
 *             PackedDocDeltaBlock.
 *         <li>PackedPayBlockNum is always equal to PackedPosBlockNum, for the same term. It is also
 *             synonym for PackedOffsetBlockNum.
 *         <li>SumPayLength is the total length of payloads written within one block, should be the
 *             sum of PayLengths in one packed block.
 *         <li>PayLength in PackedPayLengthBlock is the length of each payload associated with the
 *             current position.
 *       </ul>
 * </dl>
 *
 * @lucene.experimental
 */
public final class Lucene104PostingsFormat extends PostingsFormat {

  /** Filename extension for some small metadata about how postings are encoded. */
  public static final String META_EXTENSION = "psm";

  /**
   * Filename extension for document number, frequencies, and skip data. See chapter: <a
   * href="#Frequencies">Frequencies and Skip Data</a>
   */
  public static final String DOC_EXTENSION = "doc";

  /** Filename extension for positions. See chapter: <a href="#Positions">Positions</a> */
  public static final String POS_EXTENSION = "pos";

  /**
   * Filename extension for payloads and offsets. See chapter: <a href="#Payloads">Payloads and
   * Offsets</a>
   */
  public static final String PAY_EXTENSION = "pay";

  /** Size of blocks. */
  public static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

  static final int BLOCK_MASK = BLOCK_SIZE - 1;

  /** We insert skip data on every block and every SKIP_FACTOR=32 blocks. */
  public static final int LEVEL1_FACTOR = 32;

  /** Total number of docs covered by level 1 skip data: 32 * 256 = 8,192 */
  public static final int LEVEL1_NUM_DOCS = LEVEL1_FACTOR * BLOCK_SIZE;

  static final int LEVEL1_MASK = LEVEL1_NUM_DOCS - 1;

  /**
   * Return the class that implements {@link ImpactsEnum} in this {@link PostingsFormat}. This is
   * internally used to help the JVM make good inlining decisions.
   *
   * @lucene.internal
   */
  public static Class<? extends ImpactsEnum> getImpactsEnumImpl() {
    return Lucene104PostingsReader.BlockPostingsEnum.class;
  }

  static final String TERMS_CODEC = "Lucene104PostingsWriterTerms";
  static final String META_CODEC = "Lucene104PostingsWriterMeta";
  static final String DOC_CODEC = "Lucene104PostingsWriterDoc";
  static final String POS_CODEC = "Lucene104PostingsWriterPos";
  static final String PAY_CODEC = "Lucene104PostingsWriterPay";

  static final int VERSION_START = 0;

  static final int VERSION_CURRENT = VERSION_START;

  private final int version;
  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /** Creates {@code Lucene104PostingsFormat} with default settings. */
  public Lucene104PostingsFormat() {
    this(
        Lucene103BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        Lucene103BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /**
   * Creates {@code Lucene104PostingsFormat} with custom values for {@code minBlockSize} and {@code
   * maxBlockSize} passed to block terms dictionary.
   *
   * @see
   *     Lucene103BlockTreeTermsWriter#Lucene103BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)
   */
  public Lucene104PostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    this(minTermBlockSize, maxTermBlockSize, VERSION_CURRENT);
  }

  /** Expert constructor that allows setting the version. */
  public Lucene104PostingsFormat(int minTermBlockSize, int maxTermBlockSize, int version) {
    super("Lucene104");
    if (version < VERSION_START || version > VERSION_CURRENT) {
      throw new IllegalArgumentException("Version out of range: " + version);
    }
    this.version = version;
    Lucene103BlockTreeTermsWriter.validateSettings(minTermBlockSize, maxTermBlockSize);
    this.minTermBlockSize = minTermBlockSize;
    this.maxTermBlockSize = maxTermBlockSize;
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene104PostingsWriter(state, version);
    try {
      return new Lucene103BlockTreeTermsWriter(
          state, postingsWriter, minTermBlockSize, maxTermBlockSize);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, postingsWriter);
      throw t;
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene104PostingsReader(state);
    try {
      return new Lucene103BlockTreeTermsReader(postingsReader, state);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, postingsReader);
      throw t;
    }
  }

  /**
   * Holds all state required for {@link Lucene104PostingsReader} to produce a {@link
   * org.apache.lucene.index.PostingsEnum} without re-seeking the terms dict.
   *
   * @lucene.internal
   */
  public static final class IntBlockTermState extends BlockTermState {
    /** file pointer to the start of the doc ids enumeration, in {@link #DOC_EXTENSION} file */
    public long docStartFP;

    /** file pointer to the start of the positions enumeration, in {@link #POS_EXTENSION} file */
    public long posStartFP;

    /** file pointer to the start of the payloads enumeration, in {@link #PAY_EXTENSION} file */
    public long payStartFP;

    /**
     * file offset for the last position in the last block, if there are more than {@link
     * ForUtil#BLOCK_SIZE} positions; otherwise -1
     *
     * <p>One might think to use total term frequency to track how many positions are left to read
     * as we decode the blocks, and decode the last block differently when num_left_positions &lt;
     * BLOCK_SIZE. Unfortunately this won't work since the tracking will be messed up when we skip
     * blocks as the skipper will only tell us new position offset (start of block) and number of
     * positions to skip for that block, without telling us how many positions it has skipped.
     */
    public long lastPosBlockOffset;

    /**
     * docid when there is a single pulsed posting, otherwise -1. freq is always implicitly
     * totalTermFreq in this case.
     */
    public int singletonDocID;

    /** Sole constructor. */
    public IntBlockTermState() {
      lastPosBlockOffset = -1;
      singletonDocID = -1;
    }

    @Override
    public IntBlockTermState clone() {
      IntBlockTermState other = new IntBlockTermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      IntBlockTermState other = (IntBlockTermState) _other;
      docStartFP = other.docStartFP;
      posStartFP = other.posStartFP;
      payStartFP = other.payStartFP;
      lastPosBlockOffset = other.lastPosBlockOffset;
      singletonDocID = other.singletonDocID;
    }

    @Override
    public String toString() {
      return super.toString()
          + " docStartFP="
          + docStartFP
          + " posStartFP="
          + posStartFP
          + " payStartFP="
          + payStartFP
          + " lastPosBlockOffset="
          + lastPosBlockOffset
          + " singletonDocID="
          + singletonDocID;
    }
  }
}
