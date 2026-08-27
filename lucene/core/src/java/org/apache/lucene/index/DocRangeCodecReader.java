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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
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
    FixedBitSet bits = new FixedBitSet(in.maxDoc());
    if (start < end) {
      // An output can legitimately own no document in this reader -- a key
      // missing here makes two cuts land on the same offset -- and
      // FixedBitSet#set rejects an empty range starting at maxDoc.
      bits.set(start, end);
    }
    Bits existing = in.getLiveDocs();
    if (existing != null) {
      existing.applyMask(bits, 0);
    }
    this.liveDocs = bits;
    this.numDocs = bits.cardinality();
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
