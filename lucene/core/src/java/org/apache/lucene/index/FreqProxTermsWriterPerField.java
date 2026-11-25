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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  private FreqProxPostingsArray freqProxPostingsArray;
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  final boolean isTermDoc;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(
      FieldInvertState invertState,
      TermsHash termsHash,
      FieldInfo fieldInfo,
      TermsHashPerField nextPerField) {
    super(
        fieldInfo.getIndexOptions().subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) ? 2 : 1,
        termsHash.intPool,
        termsHash.bytePool,
        termsHash.termBytePool,
        termsHash.bytesUsed,
        nextPerField,
        fieldInfo.name,
        fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS);
    hasProx = indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    hasOffsets = indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    isTermDoc = fieldInfo.isTermDocField();
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    termFreqAtt = fieldState.termFreqAttribute;
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  void writeProx(int termID, int proxCode) {
    if (payloadAttribute == null) {
      writeVInt(1, proxCode << 1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        writeVInt(1, (proxCode << 1) | 1);
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode << 1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.setLastPosition(termID, fieldState.position);
  }

  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    final int lastStartOffset = freqProxPostingsArray.lastStartOffset(termID);
    assert startOffset - lastStartOffset >= 0;
    writeVInt(1, startOffset - lastStartOffset);
    writeVInt(1, endOffset - startOffset);
    freqProxPostingsArray.setLastStartOffset(termID, startOffset);
  }

  @Override
  void newTerm(final int termID, final int docID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;

    postings.setLastDocID(termID, docID);
    if (!hasFreq) {
      assert postings.hasFreqs() == false;
      postings.setLastDocCode(termID, docID);
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    } else {
      postings.setLastDocCode(termID, docID << 1);
      final int termFreq = getTermFreq();
      postings.setTermFreq(termID, termFreq);
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency = Math.max(termFreq, fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    final int lastDocID = postings.lastDocID(termID);
    assert !hasFreq || postings.termFreq(termID) > 0;

    if (!hasFreq) {
      assert postings.hasFreqs() == false;
      if (termFreqAtt != null && termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      if (docID != lastDocID) {
        // New document; now encode docCode for previous doc:
        assert docID > lastDocID;
        writeVInt(0, postings.lastDocCode(termID));
        postings.setLastDocCode(termID, docID - lastDocID);
        postings.setLastDocID(termID, docID);
        fieldState.uniqueTermCount++;
      }
    } else if (docID != lastDocID) {
      assert docID > lastDocID
          : "id: " + docID + " postings ID: " + lastDocID + " termID: " + termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      final int lastDocCode = postings.lastDocCode(termID);
      final int lastTermFreq = postings.termFreq(termID);
      if (1 == lastTermFreq) {
        writeVInt(0, lastDocCode | 1);
      } else {
        writeVInt(0, lastDocCode);
        writeVInt(0, lastTermFreq);
      }

      // Init freq for the current document
      final int termFreq = getTermFreq();
      postings.setTermFreq(termID, termFreq);
      fieldState.maxTermFrequency = Math.max(termFreq, fieldState.maxTermFrequency);
      postings.setLastDocCode(termID, (docID - lastDocID) << 1);
      postings.setLastDocID(termID, docID);
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.setLastStartOffset(termID, 0);
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    } else {
      if (isTermDoc) {
        throw new DuplicateTermException("field '" + getFieldName() + "' has duplicate term");
      }
      final int termFreq = Math.addExact(postings.termFreq(termID), getTermFreq());
      postings.setTermFreq(termID, termFreq);
      fieldState.maxTermFrequency = Math.max(fieldState.maxTermFrequency, termFreq);
      if (hasProx) {
        writeProx(termID, fieldState.position - postings.lastPosition(termID));
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt == null ? 1 : termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }
    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS);
    boolean hasProx = indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    boolean hasOffsets =
        indexOptions.subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  /**
   * Per-term state of the in-memory postings. The values of a term are stored next to each other in
   * a single {@code int[]}, so that the hot indexing path touches one or two cache lines per term
   * instead of one per value. The record of term {@code t} starts at {@code t * stride}, and only
   * holds the values that the field's index options need. Since index options are nested (offsets
   * imply positions, which imply freqs), each value has a fixed slot in the record.
   */
  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    /** Last docID where this term occurred. */
    private static final int LAST_DOC_ID = 0;

    /** Code for prior doc. */
    private static final int LAST_DOC_CODE = 1;

    /** # times this term occurs in the current doc, only if freqs are indexed. */
    private static final int TERM_FREQ = 2;

    /** Last position where this term occurred, only if positions are indexed. */
    private static final int LAST_POSITION = 3;

    /** Last startOffset where this term occurred, only if offsets are indexed. */
    private static final int LAST_START_OFFSET = 4;

    private final int stride;
    private final int[] termState;

    public FreqProxPostingsArray(
        int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      assert writeFreqs || writeProx == false;
      assert writeProx || writeOffsets == false;
      stride = 2 + (writeFreqs ? 1 : 0) + (writeProx ? 1 : 0) + (writeOffsets ? 1 : 0);
      termState = new int[Math.multiplyExact(size, stride)];
    }

    boolean hasFreqs() {
      return stride > TERM_FREQ;
    }

    boolean hasPositions() {
      return stride > LAST_POSITION;
    }

    boolean hasOffsets() {
      return stride > LAST_START_OFFSET;
    }

    int lastDocID(int termID) {
      return termState[termID * stride + LAST_DOC_ID];
    }

    int lastDocCode(int termID) {
      return termState[termID * stride + LAST_DOC_CODE];
    }

    int termFreq(int termID) {
      assert hasFreqs();
      return termState[termID * stride + TERM_FREQ];
    }

    int lastPosition(int termID) {
      assert hasPositions();
      return termState[termID * stride + LAST_POSITION];
    }

    int lastStartOffset(int termID) {
      assert hasOffsets();
      return termState[termID * stride + LAST_START_OFFSET];
    }

    void setLastDocID(int termID, int lastDocID) {
      termState[termID * stride + LAST_DOC_ID] = lastDocID;
    }

    void setLastDocCode(int termID, int lastDocCode) {
      termState[termID * stride + LAST_DOC_CODE] = lastDocCode;
    }

    void setTermFreq(int termID, int freq) {
      assert hasFreqs();
      termState[termID * stride + TERM_FREQ] = freq;
    }

    void setLastPosition(int termID, int lastPosition) {
      assert hasPositions();
      termState[termID * stride + LAST_POSITION] = lastPosition;
    }

    void setLastStartOffset(int termID, int lastStartOffset) {
      assert hasOffsets();
      termState[termID * stride + LAST_START_OFFSET] = lastStartOffset;
    }

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(size, hasFreqs(), hasPositions(), hasOffsets());
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;
      assert to.stride == stride;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(termState, 0, to.termState, 0, Math.multiplyExact(numToCopy, stride));
    }

    @Override
    int bytesPerPosting() {
      return ParallelPostingsArray.BYTES_PER_POSTING + stride * Integer.BYTES;
    }
  }
}
