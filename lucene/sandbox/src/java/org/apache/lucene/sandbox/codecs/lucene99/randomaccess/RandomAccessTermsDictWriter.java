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

package org.apache.lucene.sandbox.codecs.lucene99.randomaccess;

import java.io.IOException;
import org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/** Class to write the index files for one field. */
final class RandomAccessTermsDictWriter {
  /** externally provided * */
  private final IndexOptions indexOptions;
  private final DataOutput metaOutput;

  private final DataOutput indexOutput;

  private final TermDataOutputProvider termDataOutputProvider;

  /** Internal states below * */
  private final TermDataOutput[] termDataOutputPerType =
      new TermDataOutput[TermType.NUM_TOTAL_TYPES];

  private final TermsIndexBuilder termsIndexBuilder = new TermsIndexBuilder();

  private final TermDataWriter[] termDataWriterPerType =
      new TermDataWriter[TermType.NUM_TOTAL_TYPES];

  private final TermStatsTracker termStatsTracker;

  private BytesRef previousTerm;

  RandomAccessTermsDictWriter(
      int filedNumber,
      IndexOptions indexOptions,
      DataOutput metaOutput,
      DataOutput indexOutput,
      TermDataOutputProvider termDataOutputProvider) {
    this.indexOptions = indexOptions;
    this.metaOutput = metaOutput;
    this.indexOutput = indexOutput;
    this.termDataOutputProvider = termDataOutputProvider;
    this.termStatsTracker = new TermStatsTracker(filedNumber);
  }

  void add(BytesRef term, IntBlockTermState termState) throws IOException {
    TermType termType = TermType.fromTermState(termState);
    if (previousTerm == null) {
      // first term, which is also the minimum term
      termStatsTracker.setMinTerm(term);
    }
    termStatsTracker.recordTerm(termState);
    previousTerm = term;
    termsIndexBuilder.addTerm(term, termType);
    TermDataWriter termDataWriter = getTermDataWriterForType(termType);
    termDataWriter.addTermState(termState);
  }

  private TermDataWriter getTermDataWriterForType(TermType termType) throws IOException {
    if (termDataWriterPerType[termType.getId()] != null) {
      return termDataWriterPerType[termType.getId()];
    }

    TermDataOutput termDataOutput = getTermDataOutput(termType);
    TermDataWriter termDataWriter =
        new TermDataWriter(
            TermStateCodecImpl.getCodec(termType, indexOptions),
            termDataOutput.metadataOutput(),
            termDataOutput.dataOutput());
    termDataWriterPerType[termType.getId()] = termDataWriter;
    return termDataWriter;
  }

  private TermDataOutput getTermDataOutput(TermType termType) throws IOException {
    if (termDataOutputPerType[termType.getId()] == null) {
      termDataOutputPerType[termType.getId()] =
          termDataOutputProvider.getTermDataOutputForType(termType);
    }
    return termDataOutputPerType[termType.getId()];
  }

  void finish(int docCount) throws IOException {
    // finish up TermsStats for this field
    termStatsTracker.setMaxTerm(previousTerm);
    termStatsTracker.setDocCount(docCount);
    TermsStats termsStats = termStatsTracker.finish();
    // (1) Write field metadata
    termsStats.serialize(metaOutput);

    // (2) serialize the term index
    termsIndexBuilder.build().serialize(metaOutput, indexOutput);

    // (3) serialize information needed to decode per-TermType TermData
    // (3.1) number of unique TermTypes this field has
    int numTermTypesSeen = 0;
    for (var termDataWriter : termDataWriterPerType) {
      if (termDataWriter != null) {
        numTermTypesSeen += 1;
      }
    }
    metaOutput.writeByte((byte) numTermTypesSeen);

    // (3.2) (termType, metadataLength, dataLength) for each TermData
    for (int i = 0; i < termDataWriterPerType.length; i++) {
      var termDataWriter = termDataWriterPerType[i];
      if (termDataWriter != null) {
        termDataWriter.finish();
        metaOutput.writeByte((byte) i);
        metaOutput.writeVLong(termDataWriter.getTotalMetaDataBytesWritten());
        metaOutput.writeVLong(termDataWriter.getTotalDataBytesWritten());
      }
    }
  }

  record TermDataOutput(IndexOutput metadataOutput, IndexOutput dataOutput) {}

  @FunctionalInterface
  static interface TermDataOutputProvider {

    TermDataOutput getTermDataOutputForType(TermType termType) throws IOException;
  }

  static final class TermStatsTracker {
    final int fieldNumber;
    long size;
    long sumTotalTermFreq;
    long sumDocFreq;
    int docCount;
    BytesRef minTerm;
    BytesRef maxTerm;

    TermStatsTracker(int fieldNumber) {
      this.fieldNumber = fieldNumber;
    }

    void recordTerm(IntBlockTermState termState) {
      size += 1;
      sumDocFreq += termState.docFreq;
      sumTotalTermFreq += termState.totalTermFreq;
    }

    void setDocCount(int docCount) {
      this.docCount = docCount;
    }

    void setMinTerm(BytesRef minTerm) {
      this.minTerm = minTerm;
    }

    void setMaxTerm(BytesRef maxTerm) {
      this.maxTerm = maxTerm;
    }

    TermsStats finish() {
      assert docCount > 0 && minTerm != null && maxTerm != null;

      return new TermsStats(
          fieldNumber, size, sumTotalTermFreq, sumDocFreq, docCount, minTerm, maxTerm);
    }
  }
}
