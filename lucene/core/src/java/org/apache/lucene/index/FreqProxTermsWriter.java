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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.TimSorter;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.packed.PackedInts;

final class FreqProxTermsWriter extends TermsHash {

  FreqProxTermsWriter(
      final IntBlockPool.Allocator intBlockAllocator,
      final ByteBlockPool.Allocator byteBlockAllocator,
      Counter bytesUsed,
      TermsHash termVectors) {
    super(intBlockAllocator, byteBlockAllocator, bytesUsed, termVectors);
  }

  private void applyDeletes(SegmentWriteState state, Fields fields) throws IOException {
    // Process any pending Term deletes for this newly
    // flushed segment:
    if (state.segUpdates != null && state.segUpdates.deleteTerms.size() > 0) {

      BufferedUpdates.DeletedTerms segDeletes = state.segUpdates.deleteTerms;
      FrozenBufferedUpdates.TermDocsIterator iterator =
          new FrozenBufferedUpdates.TermDocsIterator(fields, true);

      segDeletes.forEachOrdered(
          (term, docId) -> {
            DocIdSetIterator postings = iterator.nextTerm(term.field(), term.bytes());
            if (postings != null) {
              assert docId < PostingsEnum.NO_MORE_DOCS;
              int doc;
              while ((doc = postings.nextDoc()) < docId) {
                if (state.liveDocs == null) {
                  state.liveDocs = new FixedBitSet(state.segmentInfo.maxDoc());
                  state.liveDocs.set(0, state.segmentInfo.maxDoc());
                }
                if (state.liveDocs.get(doc)) {
                  state.delCountOnFlush++;
                  state.liveDocs.clear(doc);
                }
              }
            }
          });
    }
  }

  @Override
  public void flush(
      Map<String, TermsHashPerField> fieldsToFlush,
      final SegmentWriteState state,
      Sorter.DocMap sortMap,
      NormsProducer norms)
      throws IOException {
    super.flush(fieldsToFlush, state, sortMap, norms);

    // Gather all fields that saw any postings:
    List<FreqProxTermsWriterPerField> allFields = new ArrayList<>();

    for (TermsHashPerField f : fieldsToFlush.values()) {
      final FreqProxTermsWriterPerField perField = (FreqProxTermsWriterPerField) f;
      if (perField.getNumTerms() > 0) {
        perField.sortTerms();
        assert perField.indexOptions != IndexOptions.NONE;
        allFields.add(perField);
      }
    }

    if (!state.fieldInfos.hasPostings()) {
      assert allFields.isEmpty();
      return;
    }

    // Sort by field name
    CollectionUtil.introSort(allFields);

    Fields fields = new FreqProxFields(allFields);
    applyDeletes(state, fields);
    if (sortMap != null) {
      final Sorter.DocMap docMap = sortMap;
      final FieldInfos infos = state.fieldInfos;
      fields =
          new FilterLeafReader.FilterFields(fields) {

            @Override
            public Terms terms(final String field) throws IOException {
              Terms terms = in.terms(field);
              if (terms == null) {
                return null;
              } else {
                return new SortingTerms(terms, infos.fieldInfo(field).getIndexOptions(), docMap);
              }
            }
          };
    }

    try (FieldsConsumer consumer =
        state.segmentInfo.getCodec().postingsFormat().fieldsConsumer(state)) {
      consumer.write(fields, norms);
    }
  }

  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new FreqProxTermsWriterPerField(
        invertState, this, fieldInfo, nextTermsHash.addField(invertState, fieldInfo));
  }

  static class SortingTerms extends FilterLeafReader.FilterTerms {

    private final Sorter.DocMap docMap;
    private final IndexOptions indexOptions;

    SortingTerms(final Terms in, IndexOptions indexOptions, final Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new SortingTermsEnum(in.iterator(), docMap, indexOptions);
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new SortingTermsEnum(in.intersect(compiled, startTerm), docMap, indexOptions);
    }
  }

  private static class SortingTermsEnum extends FilterLeafReader.FilterTermsEnum {

    final Sorter.DocMap docMap; // pkg-protected to avoid synthetic accessor methods
    private final IndexOptions indexOptions;

    SortingTermsEnum(final TermsEnum in, Sorter.DocMap docMap, IndexOptions indexOptions) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, final int flags) throws IOException {

      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0
          && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        final PostingsEnum inReuse;
        final SortingPostingsEnum wrapReuse;
        if (reuse != null && reuse instanceof SortingPostingsEnum) {
          // if we're asked to reuse the given DocsEnum and it is Sorting, return
          // the wrapped one, since some Codecs expect it.
          wrapReuse = (SortingPostingsEnum) reuse;
          inReuse = wrapReuse.getWrapped();
        } else {
          wrapReuse = new SortingPostingsEnum();
          inReuse = reuse;
        }

        final PostingsEnum inDocsAndPositions = in.postings(inReuse, flags);
        // we ignore the fact that positions/offsets may be stored but not asked for,
        // since this code is expected to be used during addIndexes which will
        // ask for everything. if that assumption changes in the future, we can
        // factor in whether 'flags' says offsets are not required.
        final boolean storePositions =
            indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        final boolean storeOffsets =
            indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        wrapReuse.reset(docMap, inDocsAndPositions, storePositions, storeOffsets);
        return wrapReuse;
      }

      final PostingsEnum inReuse;
      final SortingDocsEnum wrapReuse;
      if (reuse != null && reuse instanceof SortingDocsEnum) {
        // if we're asked to reuse the given DocsEnum and it is Sorting, return
        // the wrapped one, since some Codecs expect it.
        wrapReuse = (SortingDocsEnum) reuse;
        inReuse = wrapReuse.getWrapped();
      } else {
        wrapReuse = new SortingDocsEnum();
        inReuse = reuse;
      }

      final PostingsEnum inDocs = in.postings(inReuse, flags);
      wrapReuse.reset(docMap, inDocs);
      return wrapReuse;
    }
  }

  static class SortingDocsEnum extends PostingsEnum {

    private final LSBRadixSorter sorter;
    private PostingsEnum in;
    private int[] docs = IntsRef.EMPTY_INTS;
    private int docIt;
    private int upTo;

    SortingDocsEnum() {
      sorter = new LSBRadixSorter();
    }

    void reset(Sorter.DocMap docMap, PostingsEnum in) throws IOException {
      this.in = in;
      int i = 0;
      for (int doc = in.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = in.nextDoc()) {
        if (docs.length <= i) {
          docs = ArrayUtil.grow(docs);
        }
        docs[i++] = docMap.oldToNew(doc);
      }
      upTo = i;
      if (docs.length == upTo) {
        docs = ArrayUtil.grow(docs);
      }
      docs[upTo] = DocIdSetIterator.NO_MORE_DOCS;
      final int maxDoc = docMap.size();
      final int numBits = PackedInts.bitsRequired(Math.max(0, maxDoc - 1));
      // Even though LSBRadixSorter cannot take advantage of partial ordering like TimSorter it is
      // often still faster for nearly-sorted inputs.
      sorter.sort(numBits, docs, upTo);
      docIt = -1;
    }

    PostingsEnum getWrapped() {
      return in;
    }

    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docs[docIt];
    }

    @Override
    public int nextDoc() throws IOException {
      return docs[++docIt];
    }

    @Override
    public long cost() {
      return upTo;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }
  }

  static class SortingPostingsEnum extends PostingsEnum {

    /**
     * A {@link TimSorter} which sorts two parallel arrays of doc IDs and offsets in one go. Everyti
     * me a doc ID is 'swapped', its corresponding offset is swapped too.
     */
    private static final class DocOffsetSorter extends TimSorter {

      private int[] docs;
      private long[] offsets;
      private int[] tmpDocs;
      private long[] tmpOffsets;

      public DocOffsetSorter(int numTempSlots) {
        super(numTempSlots);
        this.tmpDocs = IntsRef.EMPTY_INTS;
        this.tmpOffsets = LongsRef.EMPTY_LONGS;
      }

      public void reset(int[] docs, long[] offsets) {
        this.docs = docs;
        this.offsets = offsets;
      }

      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }

      @Override
      protected void swap(int i, int j) {
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;

        long tmpOffset = offsets[i];
        offsets[i] = offsets[j];
        offsets[j] = tmpOffset;
      }

      @Override
      protected void copy(int src, int dest) {
        docs[dest] = docs[src];
        offsets[dest] = offsets[src];
      }

      @Override
      protected void save(int i, int len) {
        if (tmpDocs.length < len) {
          tmpDocs = new int[ArrayUtil.oversize(len, Integer.BYTES)];
          tmpOffsets = new long[tmpDocs.length];
        }
        System.arraycopy(docs, i, tmpDocs, 0, len);
        System.arraycopy(offsets, i, tmpOffsets, 0, len);
      }

      @Override
      protected void restore(int i, int j) {
        docs[j] = tmpDocs[i];
        offsets[j] = tmpOffsets[i];
      }

      @Override
      protected int compareSaved(int i, int j) {
        return tmpDocs[i] - docs[j];
      }
    }

    private DocOffsetSorter sorter;
    private int[] docs = IntsRef.EMPTY_INTS;
    private long[] offsets = LongsRef.EMPTY_LONGS;
    private int upto;

    private ByteBuffersDataInput postingInput;
    private PostingsEnum in;
    private boolean storePositions, storeOffsets;

    private int docIt;
    private int pos;
    private int startOffset;
    private int endOffset;
    private final BytesRef payload = new BytesRef();
    private int currFreq;

    private final ByteBuffersDataOutput buffer = ByteBuffersDataOutput.newResettableInstance();

    void reset(Sorter.DocMap docMap, PostingsEnum in, boolean storePositions, boolean storeOffsets)
        throws IOException {
      this.in = in;
      this.storePositions = storePositions;
      this.storeOffsets = storeOffsets;
      if (sorter == null) {
        final int numTempSlots = docMap.size() / 8;
        sorter = new DocOffsetSorter(numTempSlots);
      }
      docIt = -1;
      startOffset = -1;
      endOffset = -1;

      buffer.reset();
      int doc;
      int i = 0;
      while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (i == docs.length) {
          final int newLength = ArrayUtil.oversize(i + 1, 4);
          docs = ArrayUtil.growExact(docs, newLength);
          offsets = ArrayUtil.growExact(offsets, newLength);
        }
        docs[i] = docMap.oldToNew(doc);
        offsets[i] = buffer.size();
        addPositions(in, buffer);
        i++;
      }
      upto = i;
      sorter.reset(docs, offsets);
      sorter.sort(0, upto);

      this.postingInput = buffer.toDataInput();
    }

    private void addPositions(final PostingsEnum in, final DataOutput out) throws IOException {
      int freq = in.freq();
      out.writeVInt(freq);
      if (storePositions) {
        int previousPosition = 0;
        int previousEndOffset = 0;
        for (int i = 0; i < freq; i++) {
          final int pos = in.nextPosition();
          final BytesRef payload = in.getPayload();
          // The low-order bit of token is set only if there is a payload, the
          // previous bits are the delta-encoded position.
          final int token = (pos - previousPosition) << 1 | (payload == null ? 0 : 1);
          out.writeVInt(token);
          previousPosition = pos;
          if (storeOffsets) { // don't encode offsets if they are not stored
            final int startOffset = in.startOffset();
            final int endOffset = in.endOffset();
            out.writeVInt(startOffset - previousEndOffset);
            out.writeVInt(endOffset - startOffset);
            previousEndOffset = endOffset;
          }
          if (payload != null) {
            out.writeVInt(payload.length);
            out.writeBytes(payload.bytes, payload.offset, payload.length);
          }
        }
      }
    }

    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docIt >= upto ? NO_MORE_DOCS : docs[docIt];
    }

    @Override
    public int endOffset() throws IOException {
      return endOffset;
    }

    @Override
    public int freq() throws IOException {
      return currFreq;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return payload.length == 0 ? null : payload;
    }

    @Override
    public int nextDoc() throws IOException {
      if (++docIt >= upto) return DocIdSetIterator.NO_MORE_DOCS;
      postingInput.seek(offsets[docIt]);
      currFreq = postingInput.readVInt();
      // reset variables used in nextPosition
      pos = 0;
      endOffset = 0;
      return docs[docIt];
    }

    @Override
    public int nextPosition() throws IOException {
      if (storePositions == false) {
        return -1;
      }
      final int token = postingInput.readVInt();
      pos += token >>> 1;
      if (storeOffsets) {
        startOffset = endOffset + postingInput.readVInt();
        endOffset = startOffset + postingInput.readVInt();
      }
      if ((token & 1) != 0) {
        payload.offset = 0;
        payload.length = postingInput.readVInt();
        if (payload.length > payload.bytes.length) {
          payload.bytes = new byte[ArrayUtil.oversize(payload.length, 1)];
        }
        postingInput.readBytes(payload.bytes, 0, payload.length);
      } else {
        payload.length = 0;
      }
      return pos;
    }

    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    /** Returns the wrapped {@link PostingsEnum}. */
    PostingsEnum getWrapped() {
      return in;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }
}
