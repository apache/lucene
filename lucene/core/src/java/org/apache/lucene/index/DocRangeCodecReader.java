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

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Exposes only documents in {@code [start, end)} of the wrapped reader, by treating everything
 * outside that range as deleted. Used by {@link IndexWriter} to build one output of a partitioned
 * merge (see {@link MergePolicy.OneMerge#getDocRangePartitions(java.util.List)}).
 *
 * <p>Documents outside the range map to {@code -1} in the resulting {@link MergeState.DocMap},
 * which is what lets the existing delete carry-over logic route each concurrently-arriving delete
 * to exactly the one output that owns the document, with no additional bookkeeping.
 */
final class DocRangeCodecReader extends FilterCodecReader {

  private final Bits liveDocs;
  private final int numDocs;
  private final int start;
  private final int end;

  DocRangeCodecReader(CodecReader in, int start, int end) {
    super(in);
    this.start = start;
    this.end = end;
    assert start >= 0 && end <= in.maxDoc() && start <= end
        : "bad range [" + start + "," + end + ") maxDoc=" + in.maxDoc();
    // An output can legitimately own no document in this reader: a key missing here makes two cuts
    // land on the same offset.
    final Bits existing = in.getLiveDocs();
    this.liveDocs = new RangeLiveDocs(existing, start, end, in.maxDoc());
    if (existing == null) {
      this.numDocs = end - start;
    } else {
      int live = 0;
      for (int doc = start; doc < end; doc++) {
        if (existing.get(doc)) {
          live++;
        }
      }
      this.numDocs = live;
    }
  }

  /**
   * The documents of one output: those of the wrapped reader, restricted to {@code [start, end)}.
   *
   * <p>A view rather than a copy. A partitioned merge builds one of these for every input of every
   * output, and the ranges of one input partition its documents, so materialising a bit per
   * document would cost the whole segment's worth of bits once per output.
   */
  private static final class RangeLiveDocs implements Bits {
    private final Bits liveDocs;
    private final int start;
    private final int end;
    private final int maxDoc;

    RangeLiveDocs(Bits liveDocs, int start, int end, int maxDoc) {
      this.liveDocs = liveDocs;
      this.start = start;
      this.end = end;
      this.maxDoc = maxDoc;
    }

    @Override
    public boolean get(int index) {
      return index >= start && index < end && (liveDocs == null || liveDocs.get(index));
    }

    @Override
    public int length() {
      return maxDoc;
    }

    @Override
    public void applyMask(FixedBitSet bitSet, int offset) {
      // Clear what the range excludes, then let the reader's own deletions clear the rest. Both
      // ends are clamped into the window, so a window entirely outside the range clears fully.
      final int length = bitSet.length();
      final int from = Math.max(0, Math.min(length, start - offset));
      if (from > 0) {
        bitSet.clear(0, from);
      }
      final int to = Math.max(0, Math.min(length, end - offset));
      if (to < length) {
        bitSet.clear(to, length);
      }
      if (liveDocs != null) {
        liveDocs.applyMask(bitSet, offset);
      }
    }
  }

  /**
   * Postings are read through the terms dictionary, which a merge walks in term order and cannot
   * seek by document, so an output owning no document here would read the whole dictionary to
   * discard all of it. A null producer is what a reader with no postings looks like, and the merge
   * already skips those.
   */
  @Override
  public FieldsProducer getPostingsReader() {
    return start == end ? null : in.getPostingsReader();
  }

  /** Points are traversed in value order, so the same applies. */
  @Override
  public PointsReader getPointsReader() {
    return start == end ? null : in.getPointsReader();
  }

  @Override
  public Bits getLiveDocs() {
    return liveDocs;
  }

  @Override
  public int numDocs() {
    return numDocs;
  }

  /**
   * Doc values restricted to the range rather than merely masked.
   *
   * <p>Masking is enough for correctness but not for cost: a merge reads a field's values with
   * {@link DocValuesIterator#nextDoc()} and discards whatever the document map sends to {@code -1},
   * having already decoded it. Each output of a partitioned merge would therefore read every
   * document's values to keep its own share, and k outputs would read the segment k times. Seeking
   * to the range instead makes the outputs together read it once.
   */
  @Override
  public DocValuesProducer getDocValuesReader() {
    final DocValuesProducer values = in.getDocValuesReader();
    if (values == null) {
      return null;
    }
    return new DocRangeDocValuesProducer(values, start, end);
  }

  /** Norms restricted to the range, for the same reason as the doc values above. */
  @Override
  public NormsProducer getNormsReader() {
    final NormsProducer norms = in.getNormsReader();
    if (norms == null) {
      return null;
    }
    return new DocRangeNormsProducer(norms, start, end);
  }

  /** Vector values restricted to the range, for the same reason as the doc values above. */
  @Override
  public KnnVectorsReader getVectorReader() {
    final KnnVectorsReader vectors = in.getVectorReader();
    if (vectors == null) {
      return null;
    }
    return new DocRangeKnnVectorsReader(vectors, start, end);
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }
}
