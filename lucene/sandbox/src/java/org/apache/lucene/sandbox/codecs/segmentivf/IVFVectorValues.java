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

package org.apache.lucene.sandbox.codecs.segmentivf;

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_FINE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * A field's vectors as decoded from their fine codes: normalized, and with the rotation undone.
 * They are the quantized vectors, not the originals, which SegmentIVF does not store.
 *
 * <p>Slices, buffers and the ordinal map are created on first use: iterator-only users such as
 * {@code FieldExistsQuery} need none of them.
 */
final class IVFVectorValues extends FloatVectorValues {
  private final IVFFieldReader field;
  private RandomAccessInput codes;
  private byte[] code;
  private float[] rotated;
  private float[] value;

  // The field's ordinal-to-document map, fetched once: taking the field's lock for every document
  // serialized all searcher threads on one lock per segment.
  private int[] ordToDoc;

  IVFVectorValues(IVFFieldReader field) {
    this.field = field;
  }

  private RandomAccessInput codes() throws IOException {
    if (codes == null) {
      codes = field.section(SECTION_FINE);
    }
    return codes;
  }

  private int[] ordToDoc() {
    if (ordToDoc == null) {
      try {
        ordToDoc = field.ordToDoc();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return ordToDoc;
  }

  /**
   * Writes ordinal {@code ord} into {@code staged}'s chunk at {@code offset} as document {@code
   * doc}: its fine code, and the coarse query that staging needs for routing, derived from the fine
   * code.
   */
  void copyRow(int ord, int doc, SegmentIVFVectorsWriter.StagedVectors staged, int offset)
      throws IOException {
    FineCodec fine = field.fine;
    byte[] dest = staged.chunk;
    codes().readBytes((long) ord * fine.recordBytes, dest, offset, fine.codeBytes);
    BitUtil.VH_LE_INT.set(dest, offset + staged.docOffset, doc);
    float[] vector = rotatedBuffer();
    fine.decode(dest, offset, vector);
    VectorUtil.l2normalize(vector, false);
    staged.coarse.encodeCentroid(vector, dest, offset + staged.queryOffset);
  }

  private float[] rotatedBuffer() {
    if (rotated == null) {
      rotated = new float[field.dim];
    }
    return rotated;
  }

  @Override
  public int dimension() {
    return field.dim;
  }

  @Override
  public int size() {
    return field.count;
  }

  @Override
  public float[] vectorValue(int ord) throws IOException {
    float[] rotatedValue = rotatedValue(ord);
    if (value == null) {
      value = new float[field.dim];
    }
    field.rotation.inverseRotate(rotatedValue, value);
    return value;
  }

  /** Ordinal {@code ord}'s vector in the rotated space the codes live in. */
  private float[] rotatedValue(int ord) throws IOException {
    FineCodec fine = field.fine;
    if (code == null) {
      code = new byte[fine.codeBytes];
    }
    codes().readBytes((long) ord * fine.recordBytes, code, 0, fine.codeBytes);
    float[] vector = rotatedBuffer();
    fine.decode(code, 0, vector);
    VectorUtil.l2normalize(vector, false);
    return vector;
  }

  @Override
  public int ordToDoc(int ord) {
    return field.dense() ? ord : ordToDoc()[ord];
  }

  @Override
  public FloatVectorValues copy() {
    return new IVFVectorValues(field);
  }

  /**
   * Ordinals follow document order, so a sparse field advances by binary search. The inherited
   * sparse iterator advances one document at a time, which made a {@code FieldExistsQuery}
   * leapfrogged against the probed cells walk the whole segment per query.
   */
  @Override
  public DocIndexIterator iterator() {
    if (field.dense()) {
      return createDenseIterator();
    }
    int[] docs = ordToDoc();
    int count = field.count;
    return new DocIndexIterator() {
      private int ord = -1;
      private int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int index() {
        return ord;
      }

      @Override
      public int nextDoc() {
        return moveTo(ord + 1);
      }

      @Override
      public int advance(int target) {
        int from = ord + 1;
        if (from >= count) {
          return moveTo(count);
        }
        int found = Arrays.binarySearch(docs, from, count, target);
        return moveTo(found < 0 ? -1 - found : found);
      }

      private int moveTo(int newOrd) {
        ord = newOrd;
        doc = newOrd < count ? docs[newOrd] : NO_MORE_DOCS;
        return doc;
      }

      @Override
      public long cost() {
        return count;
      }
    };
  }

  /**
   * An exact scorer over the fine codes. The rotation is orthogonal, so the query is rotated once
   * instead of undoing the rotation of every scored vector. Codes are scored in place with the
   * rerank's query and score conversion ({@link FineCodec.Query}), from the query rotated as the
   * rerank rotates it, so a document scores the same whether its segment was searched exactly or
   * approximately.
   */
  @Override
  public VectorScorer scorer(float[] target) throws IOException {
    if (target.length != field.dim) {
      throw new IllegalArgumentException(
          "vector query dimension: "
              + target.length
              + " differs from field dimension: "
              + field.dim);
    }
    if (field.count == 0) {
      return null;
    }
    IVFVectorValues values = new IVFVectorValues(field);
    DocIndexIterator iterator = values.iterator();
    float[] query = new float[field.dim];
    field.rotation.rotate(target, query);
    return values.new InPlaceScorer(query, iterator);
  }

  /**
   * Scores fine codes in place with the rerank kernel and the rerank's score conversion: no decode,
   * and no copy from a mapped section.
   */
  private final class InPlaceScorer implements VectorScorer {
    private final FineCodec.Query query;
    private final DocIndexIterator iterator;
    private final long[] offset = new long[1];
    private final float[] oneScore = new float[1];

    InPlaceScorer(float[] rotatedQuery, DocIndexIterator iterator) {
      this.query = field.fine.query(rotatedQuery, field.similarity);
      this.iterator = iterator;
    }

    /** The score of the current document's code, read in place when mapped. */
    @Override
    public float score() throws IOException {
      int ord = iterator.index();
      FineCodec fine = field.fine;
      long at = (long) ord * fine.recordBytes;
      MemorySegment source = field.fineSegment;
      if (source != null) {
        offset[0] = at;
      } else {
        if (code == null) {
          code = new byte[fine.codeBytes];
        }
        codes().readBytes(at, code, 0, fine.codeBytes);
        source = MemorySegment.ofArray(code);
        offset[0] = 0;
      }
      query.score(source, offset, 1, oneScore);
      return oneScore[0];
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }
  }
}
