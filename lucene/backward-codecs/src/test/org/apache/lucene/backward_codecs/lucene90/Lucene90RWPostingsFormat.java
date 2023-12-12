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
package org.apache.lucene.backward_codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.util.IOUtils;

public final class Lucene90RWPostingsFormat extends PostingsFormat {

  /** Size of blocks. */
  public static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

  /**
   * Expert: The maximum number of skip levels. Smaller values result in slightly smaller indexes,
   * but slower skipping in big posting lists.
   */
  static final int MAX_SKIP_LEVELS = 10;

  static final String TERMS_CODEC = "Lucene90PostingsWriterTerms";
  static final String DOC_CODEC = "Lucene90PostingsWriterDoc";
  static final String POS_CODEC = "Lucene90PostingsWriterPos";
  static final String PAY_CODEC = "Lucene90PostingsWriterPay";

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /** Creates {@code Lucene90RWPostingsFormat} with default settings. */
  public Lucene90RWPostingsFormat() {
    this(
        Lucene90BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
        Lucene90BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  public Lucene90RWPostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super("Lucene90");
    Lucene90BlockTreeTermsWriter.validateSettings(minTermBlockSize, maxTermBlockSize);
    this.minTermBlockSize = minTermBlockSize;
    this.maxTermBlockSize = maxTermBlockSize;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene90PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret =
          new Lucene90BlockTreeTermsWriter(
              state,
              postingsWriter,
              minTermBlockSize,
              maxTermBlockSize,
              Lucene90BlockTreeTermsReader.VERSION_START);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene90PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer ret = new Lucene90BlockTreeTermsReader(postingsReader, state);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }

  /**
   * Holds all state required for {@link Lucene90PostingsReader} to produce a {@link
   * org.apache.lucene.index.PostingsEnum} without re-seeking the terms dict.
   *
   * @lucene.internal
   */
  public static final class IntBlockTermState extends BlockTermState {
    /** file pointer to the start of the doc ids enumeration, in .doc file */
    public long docStartFP;

    /** file pointer to the start of the positions enumeration, in .pos file */
    public long posStartFP;

    /** file pointer to the start of the payloads enumeration, in .pay file */
    public long payStartFP;

    /**
     * file offset for the start of the skip list, relative to docStartFP, if there are more than
     * {@link ForUtil#BLOCK_SIZE} docs; otherwise -1
     */
    public long skipOffset;

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
      skipOffset = -1;
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
      skipOffset = other.skipOffset;
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
