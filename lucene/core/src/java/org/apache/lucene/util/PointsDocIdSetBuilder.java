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
package org.apache.lucene.util;

import java.io.IOException;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * A builder of {@link DocIdSet}s for {@link PointValues}. At first it uses a sparse structure to
 * gather documents, and then upgrades to a non-sparse bit set once enough hits match.
 *
 * <p>To add documents, you first need to call {@link #grow} with the number of points that are to
 * be visited in order to reserve space, and then call {@link BulkAdder#add(int)} on the returned
 * {@link BulkAdder}.
 *
 * <p>See {@link DocIdSetBuilder} if you are not working with {@link PointValues}
 *
 * @lucene.internal
 */
public final class PointsDocIdSetBuilder {

  /**
   * Utility class to efficiently add many docs in one go.
   *
   * @see PointsDocIdSetBuilder#grow
   */
  public abstract static class BulkAdder {
    public abstract void add(int doc);

    public void add(DocIdSetIterator iterator) throws IOException {
      int docID;
      while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        add(docID);
      }
    }
  }

  private static class FixedBitSetAdder extends BulkAdder {
    final FixedBitSet bitSet;

    FixedBitSetAdder(FixedBitSet bitSet) {
      this.bitSet = bitSet;
    }

    @Override
    public void add(int doc) {
      bitSet.set(doc);
    }

    @Override
    public void add(DocIdSetIterator iterator) throws IOException {
      bitSet.or(iterator);
    }
  }

  private static class BufferAdder extends BulkAdder {
    final Buffers buffer;

    BufferAdder(Buffers buffer) {
      this.buffer = buffer;
    }

    @Override
    public void add(int doc) {
      buffer.addDoc(doc);
    }
  }

  private final int maxDoc;
  // pkg-private for testing
  final double numValuesPerDoc;
  Buffers buffers;
  private FixedBitSet bitSet;
  private long counter = -1;
  private BulkAdder adder;

  /**
   * Create a {@link PointsDocIdSetBuilder} instance that is optimized for accumulating docs that
   * match the given {@link PointValues}.
   */
  public PointsDocIdSetBuilder(int maxDoc, PointValues values) {
    this.maxDoc = maxDoc;
    final long valueCount = values.size();
    final int docCount = values.getDocCount();
    final boolean multivalued = docCount != valueCount;
    if (docCount <= 0 || valueCount < 0) {
      // assume one value per doc, this means the cost will be overestimated
      // if the docs are actually multi-valued
      this.numValuesPerDoc = 1;
    } else {
      // otherwise compute from index stats
      this.numValuesPerDoc = (double) valueCount / docCount;
    }
    assert numValuesPerDoc >= 1 : "valueCount=" + valueCount + " docCount=" + docCount;
    this.buffers = new Buffers(maxDoc, multivalued);
    this.adder = new BufferAdder(buffers);
    this.bitSet = null;
  }

  /**
   * Reserve space and return a {@link BulkAdder} object that can be used to visit up to {@code
   * numPoints} points.
   */
  public BulkAdder grow(long numPoints) {
    if (bitSet == null) {
      final int numDocs = (int) Math.min(Integer.MAX_VALUE, numPoints);
      if (buffers.ensureBufferCapacity(numDocs) == false) {
        upgradeToBitSet();
        counter += numPoints;
      }
    } else {
      counter += numPoints;
    }
    return adder;
  }

  private void upgradeToBitSet() {
    assert bitSet == null;
    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    this.counter = buffers.toBitSet(bitSet);
    this.bitSet = bitSet;
    this.buffers = null;
    this.adder = new FixedBitSetAdder(bitSet);
  }

  /** Build a {@link DocIdSet} from the accumulated doc IDs. */
  public DocIdSet build() {
    try {
      if (bitSet != null) {
        assert counter >= 0;
        final long cost = Math.round(counter / numValuesPerDoc);
        return new BitDocIdSet(bitSet, cost);
      } else {
        return buffers.toDocIdSet();
      }
    } finally {
      this.buffers = null;
      this.bitSet = null;
    }
  }
}
