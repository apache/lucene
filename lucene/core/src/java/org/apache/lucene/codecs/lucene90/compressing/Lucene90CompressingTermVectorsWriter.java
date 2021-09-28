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
package org.apache.lucene.codecs.lucene90.compressing;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.MatchingReaders;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link TermVectorsWriter} for {@link Lucene90CompressingTermVectorsFormat}.
 *
 * @lucene.experimental
 */
public final class Lucene90CompressingTermVectorsWriter extends TermVectorsWriter {

  static final String VECTORS_EXTENSION = "tvd";
  static final String VECTORS_INDEX_EXTENSION = "tvx";
  static final String VECTORS_META_EXTENSION = "tvm";
  static final String VECTORS_INDEX_CODEC_NAME = "Lucene90TermVectorsIndex";

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;
  static final int META_VERSION_START = 0;

  static final int PACKED_BLOCK_SIZE = 64;

  static final int POSITIONS = 0x01;
  static final int OFFSETS = 0x02;
  static final int PAYLOADS = 0x04;
  static final int FLAGS_BITS = DirectWriter.bitsRequired(POSITIONS | OFFSETS | PAYLOADS);

  private final String segment;
  private FieldsIndexWriter indexWriter;
  private IndexOutput metaStream, vectorsStream;

  private final CompressionMode compressionMode;
  private final Compressor compressor;
  private final int chunkSize;

  private long numChunks; // number of chunks
  private long numDirtyChunks; // number of incomplete compressed blocks written
  private long numDirtyDocs; // cumulative number of docs in incomplete chunks

  /** a pending doc */
  private class DocData {
    final int numFields;
    final Deque<FieldData> fields;
    final int posStart, offStart, payStart;

    DocData(int numFields, int posStart, int offStart, int payStart) {
      this.numFields = numFields;
      this.fields = new ArrayDeque<>(numFields);
      this.posStart = posStart;
      this.offStart = offStart;
      this.payStart = payStart;
    }

    FieldData addField(
        int fieldNum, int numTerms, boolean positions, boolean offsets, boolean payloads) {
      final FieldData field;
      if (fields.isEmpty()) {
        field =
            new FieldData(
                fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      } else {
        final FieldData last = fields.getLast();
        final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0);
        final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);
        final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0);
        field =
            new FieldData(
                fieldNum, numTerms, positions, offsets, payloads, posStart, offStart, payStart);
      }
      fields.add(field);
      return field;
    }
  }

  private DocData addDocData(int numVectorFields) {
    FieldData last = null;
    for (Iterator<DocData> it = pendingDocs.descendingIterator(); it.hasNext(); ) {
      final DocData doc = it.next();
      if (!doc.fields.isEmpty()) {
        last = doc.fields.getLast();
        break;
      }
    }
    final DocData doc;
    if (last == null) {
      doc = new DocData(numVectorFields, 0, 0, 0);
    } else {
      final int posStart = last.posStart + (last.hasPositions ? last.totalPositions : 0);
      final int offStart = last.offStart + (last.hasOffsets ? last.totalPositions : 0);
      final int payStart = last.payStart + (last.hasPayloads ? last.totalPositions : 0);
      doc = new DocData(numVectorFields, posStart, offStart, payStart);
    }
    pendingDocs.add(doc);
    return doc;
  }

  /** a pending field */
  private class FieldData {
    final boolean hasPositions, hasOffsets, hasPayloads;
    final int fieldNum, flags, numTerms;
    final int[] freqs, prefixLengths, suffixLengths;
    final int posStart, offStart, payStart;
    int totalPositions;
    int ord;

    FieldData(
        int fieldNum,
        int numTerms,
        boolean positions,
        boolean offsets,
        boolean payloads,
        int posStart,
        int offStart,
        int payStart) {
      this.fieldNum = fieldNum;
      this.numTerms = numTerms;
      this.hasPositions = positions;
      this.hasOffsets = offsets;
      this.hasPayloads = payloads;
      this.flags =
          (positions ? POSITIONS : 0) | (offsets ? OFFSETS : 0) | (payloads ? PAYLOADS : 0);
      this.freqs = new int[numTerms];
      this.prefixLengths = new int[numTerms];
      this.suffixLengths = new int[numTerms];
      this.posStart = posStart;
      this.offStart = offStart;
      this.payStart = payStart;
      totalPositions = 0;
      ord = 0;
    }

    void addTerm(int freq, int prefixLength, int suffixLength) {
      freqs[ord] = freq;
      prefixLengths[ord] = prefixLength;
      suffixLengths[ord] = suffixLength;
      ++ord;
    }

    void addPosition(int position, int startOffset, int length, int payloadLength) {
      if (hasPositions) {
        if (posStart + totalPositions == positionsBuf.length) {
          positionsBuf = ArrayUtil.grow(positionsBuf);
        }
        positionsBuf[posStart + totalPositions] = position;
      }
      if (hasOffsets) {
        if (offStart + totalPositions == startOffsetsBuf.length) {
          final int newLength = ArrayUtil.oversize(offStart + totalPositions, 4);
          startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
          lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
        }
        startOffsetsBuf[offStart + totalPositions] = startOffset;
        lengthsBuf[offStart + totalPositions] = length;
      }
      if (hasPayloads) {
        if (payStart + totalPositions == payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf);
        }
        payloadLengthsBuf[payStart + totalPositions] = payloadLength;
      }
      ++totalPositions;
    }
  }

  private int numDocs; // total number of docs seen
  private final Deque<DocData> pendingDocs; // pending docs
  private DocData curDoc; // current document
  private FieldData curField; // current field
  private final BytesRef lastTerm;
  private int[] positionsBuf, startOffsetsBuf, lengthsBuf, payloadLengthsBuf;
  private final ByteBuffersDataOutput termSuffixes; // buffered term suffixes
  private final ByteBuffersDataOutput payloadBytes; // buffered term payloads
  private final BlockPackedWriter writer;
  private final int maxDocsPerChunk; // hard limit on number of docs per chunk
  private final ByteBuffersDataOutput scratchBuffer = ByteBuffersDataOutput.newResettableInstance();

  /** Sole constructor. */
  Lucene90CompressingTermVectorsWriter(
      Directory directory,
      SegmentInfo si,
      String segmentSuffix,
      IOContext context,
      String formatName,
      CompressionMode compressionMode,
      int chunkSize,
      int maxDocsPerChunk,
      int blockShift)
      throws IOException {
    assert directory != null;
    this.segment = si.name;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;
    this.maxDocsPerChunk = maxDocsPerChunk;

    numDocs = 0;
    pendingDocs = new ArrayDeque<>();
    termSuffixes = ByteBuffersDataOutput.newResettableInstance();
    payloadBytes = ByteBuffersDataOutput.newResettableInstance();
    lastTerm = new BytesRef(ArrayUtil.oversize(30, 1));

    boolean success = false;
    try {
      metaStream =
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_META_EXTENSION),
              context);
      CodecUtil.writeIndexHeader(
          metaStream,
          VECTORS_INDEX_CODEC_NAME + "Meta",
          VERSION_CURRENT,
          si.getId(),
          segmentSuffix);
      assert CodecUtil.indexHeaderLength(VECTORS_INDEX_CODEC_NAME + "Meta", segmentSuffix)
          == metaStream.getFilePointer();

      vectorsStream =
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, VECTORS_EXTENSION), context);
      CodecUtil.writeIndexHeader(
          vectorsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix)
          == vectorsStream.getFilePointer();

      indexWriter =
          new FieldsIndexWriter(
              directory,
              segment,
              segmentSuffix,
              VECTORS_INDEX_EXTENSION,
              VECTORS_INDEX_CODEC_NAME,
              si.getId(),
              blockShift,
              context);

      metaStream.writeVInt(PackedInts.VERSION_CURRENT);
      metaStream.writeVInt(chunkSize);
      writer = new BlockPackedWriter(vectorsStream, PACKED_BLOCK_SIZE);

      positionsBuf = new int[1024];
      startOffsetsBuf = new int[1024];
      lengthsBuf = new int[1024];
      payloadLengthsBuf = new int[1024];

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaStream, vectorsStream, indexWriter, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(metaStream, vectorsStream, indexWriter);
    } finally {
      metaStream = null;
      vectorsStream = null;
      indexWriter = null;
    }
  }

  @Override
  public void startDocument(int numVectorFields) throws IOException {
    curDoc = addDocData(numVectorFields);
  }

  @Override
  public void finishDocument() throws IOException {
    // append the payload bytes of the doc after its terms
    payloadBytes.copyTo(termSuffixes);
    payloadBytes.reset();
    ++numDocs;
    if (triggerFlush()) {
      flush(false);
    }
    curDoc = null;
  }

  @Override
  public void startField(
      FieldInfo info, int numTerms, boolean positions, boolean offsets, boolean payloads)
      throws IOException {
    curField = curDoc.addField(info.number, numTerms, positions, offsets, payloads);
    lastTerm.length = 0;
  }

  @Override
  public void finishField() throws IOException {
    curField = null;
  }

  @Override
  public void startTerm(BytesRef term, int freq) throws IOException {
    assert freq >= 1;
    final int prefix;
    if (lastTerm.length == 0) {
      // no previous term: no bytes to write
      prefix = 0;
    } else {
      prefix = StringHelper.bytesDifference(lastTerm, term);
    }
    curField.addTerm(freq, prefix, term.length - prefix);
    termSuffixes.writeBytes(term.bytes, term.offset + prefix, term.length - prefix);
    // copy last term
    if (lastTerm.bytes.length < term.length) {
      lastTerm.bytes = new byte[ArrayUtil.oversize(term.length, 1)];
    }
    lastTerm.offset = 0;
    lastTerm.length = term.length;
    System.arraycopy(term.bytes, term.offset, lastTerm.bytes, 0, term.length);
  }

  @Override
  public void addPosition(int position, int startOffset, int endOffset, BytesRef payload)
      throws IOException {
    assert curField.flags != 0;
    curField.addPosition(
        position, startOffset, endOffset - startOffset, payload == null ? 0 : payload.length);
    if (curField.hasPayloads && payload != null) {
      payloadBytes.writeBytes(payload.bytes, payload.offset, payload.length);
    }
  }

  private boolean triggerFlush() {
    return termSuffixes.size() >= chunkSize || pendingDocs.size() >= maxDocsPerChunk;
  }

  private void flush(boolean force) throws IOException {
    assert force != triggerFlush();
    final int chunkDocs = pendingDocs.size();
    assert chunkDocs > 0 : chunkDocs;
    numChunks++;
    if (force) {
      numDirtyChunks++; // incomplete: we had to force this flush
      numDirtyDocs += pendingDocs.size();
    }
    // write the index file
    indexWriter.writeIndex(chunkDocs, vectorsStream.getFilePointer());

    final int docBase = numDocs - chunkDocs;
    vectorsStream.writeVInt(docBase);
    final int dirtyBit = force ? 1 : 0;
    vectorsStream.writeVInt((chunkDocs << 1) | dirtyBit);

    // total number of fields of the chunk
    final int totalFields = flushNumFields(chunkDocs);

    if (totalFields > 0) {
      // unique field numbers (sorted)
      final int[] fieldNums = flushFieldNums();
      // offsets in the array of unique field numbers
      flushFields(totalFields, fieldNums);
      // flags (does the field have positions, offsets, payloads?)
      flushFlags(totalFields, fieldNums);
      // number of terms of each field
      flushNumTerms(totalFields);
      // prefix and suffix lengths for each field
      flushTermLengths();
      // term freqs - 1 (because termFreq is always >=1) for each term
      flushTermFreqs();
      // positions for all terms, when enabled
      flushPositions();
      // offsets for all terms, when enabled
      flushOffsets(fieldNums);
      // payload lengths for all terms, when enabled
      flushPayloadLengths();

      // compress terms and payloads and write them to the output
      //
      // TODO: We could compress in the slices we already have in the buffer (min/max slice
      // can be set on the buffer itself).
      byte[] content = termSuffixes.toArrayCopy();
      compressor.compress(content, 0, content.length, vectorsStream);
    }

    // reset
    pendingDocs.clear();
    curDoc = null;
    curField = null;
    termSuffixes.reset();
  }

  private int flushNumFields(int chunkDocs) throws IOException {
    if (chunkDocs == 1) {
      final int numFields = pendingDocs.getFirst().numFields;
      vectorsStream.writeVInt(numFields);
      return numFields;
    } else {
      writer.reset(vectorsStream);
      int totalFields = 0;
      for (DocData dd : pendingDocs) {
        writer.add(dd.numFields);
        totalFields += dd.numFields;
      }
      writer.finish();
      return totalFields;
    }
  }

  /** Returns a sorted array containing unique field numbers */
  private int[] flushFieldNums() throws IOException {
    SortedSet<Integer> fieldNums = new TreeSet<>();
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        fieldNums.add(fd.fieldNum);
      }
    }

    final int numDistinctFields = fieldNums.size();
    assert numDistinctFields > 0;
    final int bitsRequired = PackedInts.bitsRequired(fieldNums.last());
    final int token = (Math.min(numDistinctFields - 1, 0x07) << 5) | bitsRequired;
    vectorsStream.writeByte((byte) token);
    if (numDistinctFields - 1 >= 0x07) {
      vectorsStream.writeVInt(numDistinctFields - 1 - 0x07);
    }
    final PackedInts.Writer writer =
        PackedInts.getWriterNoHeader(
            vectorsStream, PackedInts.Format.PACKED, fieldNums.size(), bitsRequired, 1);
    for (Integer fieldNum : fieldNums) {
      writer.add(fieldNum);
    }
    writer.finish();

    int[] fns = new int[fieldNums.size()];
    int i = 0;
    for (Integer key : fieldNums) {
      fns[i++] = key;
    }
    return fns;
  }

  private void flushFields(int totalFields, int[] fieldNums) throws IOException {
    scratchBuffer.reset();
    final DirectWriter writer =
        DirectWriter.getInstance(
            scratchBuffer, totalFields, DirectWriter.bitsRequired(fieldNums.length - 1));
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        final int fieldNumIndex = Arrays.binarySearch(fieldNums, fd.fieldNum);
        assert fieldNumIndex >= 0;
        writer.add(fieldNumIndex);
      }
    }
    writer.finish();
    vectorsStream.writeVLong(scratchBuffer.size());
    scratchBuffer.copyTo(vectorsStream);
  }

  private void flushFlags(int totalFields, int[] fieldNums) throws IOException {
    // check if fields always have the same flags
    boolean nonChangingFlags = true;
    int[] fieldFlags = new int[fieldNums.length];
    Arrays.fill(fieldFlags, -1);
    outer:
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
        assert fieldNumOff >= 0;
        if (fieldFlags[fieldNumOff] == -1) {
          fieldFlags[fieldNumOff] = fd.flags;
        } else if (fieldFlags[fieldNumOff] != fd.flags) {
          nonChangingFlags = false;
          break outer;
        }
      }
    }

    if (nonChangingFlags) {
      // write one flag per field num
      vectorsStream.writeVInt(0);
      scratchBuffer.reset();
      final DirectWriter writer =
          DirectWriter.getInstance(scratchBuffer, fieldFlags.length, FLAGS_BITS);
      for (int flags : fieldFlags) {
        assert flags >= 0;
        writer.add(flags);
      }
      writer.finish();
      vectorsStream.writeVInt(Math.toIntExact(scratchBuffer.size()));
      scratchBuffer.copyTo(vectorsStream);
    } else {
      // write one flag for every field instance
      vectorsStream.writeVInt(1);
      scratchBuffer.reset();
      final DirectWriter writer = DirectWriter.getInstance(scratchBuffer, totalFields, FLAGS_BITS);
      for (DocData dd : pendingDocs) {
        for (FieldData fd : dd.fields) {
          writer.add(fd.flags);
        }
      }
      writer.finish();
      vectorsStream.writeVInt(Math.toIntExact(scratchBuffer.size()));
      scratchBuffer.copyTo(vectorsStream);
    }
  }

  private void flushNumTerms(int totalFields) throws IOException {
    int maxNumTerms = 0;
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        maxNumTerms |= fd.numTerms;
      }
    }
    final int bitsRequired = DirectWriter.bitsRequired(maxNumTerms);
    vectorsStream.writeVInt(bitsRequired);
    scratchBuffer.reset();
    final DirectWriter writer = DirectWriter.getInstance(scratchBuffer, totalFields, bitsRequired);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        writer.add(fd.numTerms);
      }
    }
    writer.finish();
    vectorsStream.writeVInt(Math.toIntExact(scratchBuffer.size()));
    scratchBuffer.copyTo(vectorsStream);
  }

  private void flushTermLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.prefixLengths[i]);
        }
      }
    }
    writer.finish();
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.suffixLengths[i]);
        }
      }
    }
    writer.finish();
  }

  private void flushTermFreqs() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        for (int i = 0; i < fd.numTerms; ++i) {
          writer.add(fd.freqs[i] - 1);
        }
      }
    }
    writer.finish();
  }

  private void flushPositions() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPositions) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            int previousPosition = 0;
            for (int j = 0; j < fd.freqs[i]; ++j) {
              final int position = positionsBuf[fd.posStart + pos++];
              writer.add(position - previousPosition);
              previousPosition = position;
            }
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }

  private void flushOffsets(int[] fieldNums) throws IOException {
    boolean hasOffsets = false;
    long[] sumPos = new long[fieldNums.length];
    long[] sumOffsets = new long[fieldNums.length];
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        hasOffsets |= fd.hasOffsets;
        if (fd.hasOffsets && fd.hasPositions) {
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            sumPos[fieldNumOff] += positionsBuf[fd.posStart + fd.freqs[i] - 1 + pos];
            sumOffsets[fieldNumOff] += startOffsetsBuf[fd.offStart + fd.freqs[i] - 1 + pos];
            pos += fd.freqs[i];
          }
          assert pos == fd.totalPositions;
        }
      }
    }

    if (!hasOffsets) {
      // nothing to do
      return;
    }

    final float[] charsPerTerm = new float[fieldNums.length];
    for (int i = 0; i < fieldNums.length; ++i) {
      charsPerTerm[i] =
          (sumPos[i] <= 0 || sumOffsets[i] <= 0) ? 0 : (float) ((double) sumOffsets[i] / sumPos[i]);
    }

    // start offsets
    for (int i = 0; i < fieldNums.length; ++i) {
      vectorsStream.writeInt(Float.floatToRawIntBits(charsPerTerm[i]));
    }

    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if ((fd.flags & OFFSETS) != 0) {
          final int fieldNumOff = Arrays.binarySearch(fieldNums, fd.fieldNum);
          final float cpt = charsPerTerm[fieldNumOff];
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            int previousPos = 0;
            int previousOff = 0;
            for (int j = 0; j < fd.freqs[i]; ++j) {
              final int position = fd.hasPositions ? positionsBuf[fd.posStart + pos] : 0;
              final int startOffset = startOffsetsBuf[fd.offStart + pos];
              writer.add(startOffset - previousOff - (int) (cpt * (position - previousPos)));
              previousPos = position;
              previousOff = startOffset;
              ++pos;
            }
          }
        }
      }
    }
    writer.finish();

    // lengths
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if ((fd.flags & OFFSETS) != 0) {
          int pos = 0;
          for (int i = 0; i < fd.numTerms; ++i) {
            for (int j = 0; j < fd.freqs[i]; ++j) {
              writer.add(
                  lengthsBuf[fd.offStart + pos++] - fd.prefixLengths[i] - fd.suffixLengths[i]);
            }
          }
          assert pos == fd.totalPositions;
        }
      }
    }
    writer.finish();
  }

  private void flushPayloadLengths() throws IOException {
    writer.reset(vectorsStream);
    for (DocData dd : pendingDocs) {
      for (FieldData fd : dd.fields) {
        if (fd.hasPayloads) {
          for (int i = 0; i < fd.totalPositions; ++i) {
            writer.add(payloadLengthsBuf[fd.payStart + i]);
          }
        }
      }
    }
    writer.finish();
  }

  @Override
  public void finish(int numDocs) throws IOException {
    if (!pendingDocs.isEmpty()) {
      flush(true);
    }
    if (numDocs != this.numDocs) {
      throw new RuntimeException(
          "Wrote " + this.numDocs + " docs, finish called with numDocs=" + numDocs);
    }
    indexWriter.finish(numDocs, vectorsStream.getFilePointer(), metaStream);
    metaStream.writeVLong(numChunks);
    metaStream.writeVLong(numDirtyChunks);
    metaStream.writeVLong(numDirtyDocs);
    CodecUtil.writeFooter(metaStream);
    CodecUtil.writeFooter(vectorsStream);
  }

  @Override
  public void addProx(int numProx, DataInput positions, DataInput offsets) throws IOException {
    assert (curField.hasPositions) == (positions != null);
    assert (curField.hasOffsets) == (offsets != null);

    if (curField.hasPositions) {
      final int posStart = curField.posStart + curField.totalPositions;
      if (posStart + numProx > positionsBuf.length) {
        positionsBuf = ArrayUtil.grow(positionsBuf, posStart + numProx);
      }
      int position = 0;
      if (curField.hasPayloads) {
        final int payStart = curField.payStart + curField.totalPositions;
        if (payStart + numProx > payloadLengthsBuf.length) {
          payloadLengthsBuf = ArrayUtil.grow(payloadLengthsBuf, payStart + numProx);
        }
        for (int i = 0; i < numProx; ++i) {
          final int code = positions.readVInt();
          if ((code & 1) != 0) {
            // This position has a payload
            final int payloadLength = positions.readVInt();
            payloadLengthsBuf[payStart + i] = payloadLength;
            payloadBytes.copyBytes(positions, payloadLength);
          } else {
            payloadLengthsBuf[payStart + i] = 0;
          }
          position += code >>> 1;
          positionsBuf[posStart + i] = position;
        }
      } else {
        for (int i = 0; i < numProx; ++i) {
          position += (positions.readVInt() >>> 1);
          positionsBuf[posStart + i] = position;
        }
      }
    }

    if (curField.hasOffsets) {
      final int offStart = curField.offStart + curField.totalPositions;
      if (offStart + numProx > startOffsetsBuf.length) {
        final int newLength = ArrayUtil.oversize(offStart + numProx, 4);
        startOffsetsBuf = ArrayUtil.growExact(startOffsetsBuf, newLength);
        lengthsBuf = ArrayUtil.growExact(lengthsBuf, newLength);
      }
      int lastOffset = 0, startOffset, endOffset;
      for (int i = 0; i < numProx; ++i) {
        startOffset = lastOffset + offsets.readVInt();
        endOffset = startOffset + offsets.readVInt();
        lastOffset = endOffset;
        startOffsetsBuf[offStart + i] = startOffset;
        lengthsBuf[offStart + i] = endOffset - startOffset;
      }
    }

    curField.totalPositions += numProx;
  }

  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP =
      Lucene90CompressingTermVectorsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;

  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (
        @SuppressWarnings("unused")
        SecurityException ignored) {
    }
    BULK_MERGE_ENABLED = v;
  }

  private void copyChunks(
      final MergeState mergeState,
      final CompressingTermVectorsSub sub,
      final int fromDocID,
      final int toDocID)
      throws IOException {
    final Lucene90CompressingTermVectorsReader reader =
        (Lucene90CompressingTermVectorsReader) mergeState.termVectorsReaders[sub.readerIndex];
    assert reader.getVersion() == VERSION_CURRENT;
    assert reader.getChunkSize() == chunkSize;
    assert reader.getCompressionMode() == compressionMode;
    assert !tooDirty(reader);
    assert mergeState.liveDocs[sub.readerIndex] == null;

    int docID = fromDocID;
    final FieldsIndex index = reader.getIndexReader();

    // copy docs that belong to the previous chunk
    while (docID < toDocID && reader.isLoaded(docID)) {
      addAllDocVectors(reader.get(docID++), mergeState);
    }

    if (docID >= toDocID) {
      return;
    }
    // copy chunks
    long fromPointer = index.getStartPointer(docID);
    final long toPointer =
        toDocID == sub.maxDoc ? reader.getMaxPointer() : index.getStartPointer(toDocID);
    if (fromPointer < toPointer) {
      // flush any pending chunks
      if (!pendingDocs.isEmpty()) {
        flush(true);
      }
      final IndexInput rawDocs = reader.getVectorsStream();
      rawDocs.seek(fromPointer);
      do {
        // iterate over each chunk. we use the vectors index to find chunk boundaries,
        // read the docstart + doccount from the chunk header (we write a new header, since doc
        // numbers will change),
        // and just copy the bytes directly.
        // read header
        final int base = rawDocs.readVInt();
        if (base != docID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", docID=" + docID, rawDocs);
        }

        final int code = rawDocs.readVInt();
        final int bufferedDocs = code >>> 1;

        // write a new index entry and new header for this chunk.
        indexWriter.writeIndex(bufferedDocs, vectorsStream.getFilePointer());
        vectorsStream.writeVInt(numDocs); // rebase
        vectorsStream.writeVInt(code);
        docID += bufferedDocs;
        numDocs += bufferedDocs;
        if (docID > toDocID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", count=" + bufferedDocs + ", toDocID=" + toDocID,
              rawDocs);
        }

        // copy bytes until the next chunk boundary (or end of chunk data).
        // using the stored fields index for this isn't the most efficient, but fast enough
        // and is a source of redundancy for detecting bad things.
        final long end;
        if (docID == sub.maxDoc) {
          end = reader.getMaxPointer();
        } else {
          end = index.getStartPointer(docID);
        }
        vectorsStream.copyBytes(rawDocs, end - rawDocs.getFilePointer());
        ++numChunks;
        boolean dirtyChunk = (code & 1) != 0;
        if (dirtyChunk) {
          numDirtyChunks++;
          numDirtyDocs += bufferedDocs;
        }
        fromPointer = end;
      } while (fromPointer < toPointer);
    }
    // copy leftover docs that don't form a complete chunk
    assert reader.isLoaded(docID) == false;
    while (docID < toDocID) {
      addAllDocVectors(reader.get(docID++), mergeState);
    }
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    final int numReaders = mergeState.termVectorsReaders.length;
    final MatchingReaders matchingReaders = new MatchingReaders(mergeState);
    final List<CompressingTermVectorsSub> subs = new ArrayList<>(numReaders);
    for (int i = 0; i < numReaders; i++) {
      final TermVectorsReader reader = mergeState.termVectorsReaders[i];
      if (reader != null) {
        reader.checkIntegrity();
      }
      final boolean bulkMerge = canPerformBulkMerge(mergeState, matchingReaders, i);
      subs.add(new CompressingTermVectorsSub(mergeState, bulkMerge, i));
    }
    int docCount = 0;
    final DocIDMerger<CompressingTermVectorsSub> docIDMerger =
        DocIDMerger.of(subs, mergeState.needsIndexSort);
    CompressingTermVectorsSub sub = docIDMerger.next();
    while (sub != null) {
      assert sub.mappedDocID == docCount : sub.mappedDocID + " != " + docCount;
      if (sub.canPerformBulkMerge) {
        final int fromDocID = sub.docID;
        int toDocID = fromDocID;
        final CompressingTermVectorsSub current = sub;
        while ((sub = docIDMerger.next()) == current) {
          ++toDocID;
          assert sub.docID == toDocID;
        }
        ++toDocID; // exclusive bound
        copyChunks(mergeState, current, fromDocID, toDocID);
        docCount += toDocID - fromDocID;
      } else {
        final TermVectorsReader reader = mergeState.termVectorsReaders[sub.readerIndex];
        final Fields vectors = reader != null ? reader.get(sub.docID) : null;
        addAllDocVectors(vectors, mergeState);
        ++docCount;
        sub = docIDMerger.next();
      }
    }
    finish(docCount);
    return docCount;
  }

  /**
   * Returns true if we should recompress this reader, even though we could bulk merge compressed
   * data
   *
   * <p>The last chunk written for a segment is typically incomplete, so without recompressing, in
   * some worst-case situations (e.g. frequent reopen with tiny flushes), over time the compression
   * ratio can degrade. This is a safety switch.
   */
  boolean tooDirty(Lucene90CompressingTermVectorsReader candidate) {
    // A segment is considered dirty only if it has enough dirty docs to make a full block
    // AND more than 1% blocks are dirty.
    return candidate.getNumDirtyDocs() > maxDocsPerChunk
        && candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }

  private boolean canPerformBulkMerge(
      MergeState mergeState, MatchingReaders matchingReaders, int readerIndex) {
    if (mergeState.termVectorsReaders[readerIndex]
        instanceof Lucene90CompressingTermVectorsReader) {
      final Lucene90CompressingTermVectorsReader reader =
          (Lucene90CompressingTermVectorsReader) mergeState.termVectorsReaders[readerIndex];
      return BULK_MERGE_ENABLED
          && matchingReaders.matchingReaders[readerIndex]
          && reader.getCompressionMode() == compressionMode
          && reader.getChunkSize() == chunkSize
          && reader.getVersion() == VERSION_CURRENT
          && reader.getPackedIntsVersion() == PackedInts.VERSION_CURRENT
          && mergeState.liveDocs[readerIndex] == null
          && !tooDirty(reader);
    }
    return false;
  }

  private static class CompressingTermVectorsSub extends DocIDMerger.Sub {
    final int maxDoc;
    final int readerIndex;
    final boolean canPerformBulkMerge;
    int docID = -1;

    CompressingTermVectorsSub(MergeState mergeState, boolean canPerformBulkMerge, int readerIndex) {
      super(mergeState.docMaps[readerIndex]);
      this.maxDoc = mergeState.maxDocs[readerIndex];
      this.readerIndex = readerIndex;
      this.canPerformBulkMerge = canPerformBulkMerge;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return positionsBuf.length
        + startOffsetsBuf.length
        + lengthsBuf.length
        + payloadLengthsBuf.length
        + termSuffixes.ramBytesUsed()
        + payloadBytes.ramBytesUsed()
        + lastTerm.bytes.length
        + scratchBuffer.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return List.of(termSuffixes, payloadBytes);
  }
}
