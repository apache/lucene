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
package org.apache.lucene.codecs.lucene912;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.util.IOUtils;

public class Lucene912PostingsFormat extends PostingsFormat {

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
  public static final int BLOCK_SIZE_LOG2 = ForUtil.BLOCK_SIZE_LOG2;
  public static final int BLOCK_MASK = BLOCK_SIZE - 1;

  public static final int SKIP_FACTOR = 32;
  public static final int SKIP_TOTAL_SIZE = SKIP_FACTOR * BLOCK_SIZE;
  public static final int SKIP_MASK = SKIP_TOTAL_SIZE - 1;

  static final String TERMS_CODEC = "Lucene90PostingsWriterTerms";
  static final String DOC_CODEC = "Lucene912PostingsWriterDoc";
  static final String POS_CODEC = "Lucene912PostingsWriterPos";
  static final String PAY_CODEC = "Lucene912PostingsWriterPay";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  public Lucene912PostingsFormat() {
    super("Lucene912");
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene912PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret =
          new Lucene90BlockTreeTermsWriter(
              state, postingsWriter,
              Lucene90BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE,
              Lucene90BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
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
    PostingsReaderBase postingsReader = new Lucene912PostingsReader(state);
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
   * Holds all state required for {@link Lucene99PostingsReader} to produce a {@link
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

    /**
     * Impacts across the whole postings list.
     */
    public List<Impact> globalImpacts;

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
      globalImpacts = other.globalImpacts;
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
