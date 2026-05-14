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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link DocIdSetIterator} for numeric doc values range queries that batch-evaluates values for
 * MAYBE blocks. Instead of checking one doc at a time through a {@link
 * org.apache.lucene.search.TwoPhaseIterator}, this iterator reads values in a tight loop and sets
 * bits directly in a {@link FixedBitSet}, enabling the {@link DenseConjunctionBulkScorer} to use
 * the faster bitset intersection path.
 *
 * <p>This is used for dense single-valued numeric fields with a skip index.
 */
public final class BatchDocValuesRangeIterator extends DocIdSetIterator {

  private final SkipBlockRangeIterator blockIterator;
  private final NumericDocValues values;
  private final long minValue;
  private final long maxValue;
  private int doc = -1;

  public BatchDocValuesRangeIterator(
      NumericDocValues values, DocValuesSkipper skipper, long minValue, long maxValue) {
    this.blockIterator = new SkipBlockRangeIterator(skipper, minValue, maxValue);
    this.values = values;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    // Use the block iterator to skip NO blocks
    int blockDoc = blockIterator.docID();
    if (blockDoc < target) {
      blockDoc = blockIterator.advance(target);
    }
    if (blockDoc == NO_MORE_DOCS) {
      return doc = NO_MORE_DOCS;
    }

    // For YES blocks, all docs match — find the first doc with a value
    if (blockIterator.getMatch() == SkipBlockRangeIterator.Match.YES) {
      return doc = blockDoc;
    }

    // For YES_IF_PRESENT blocks, all values are in range so we only need to check which docs have
    // the value.
    if (blockIterator.getMatch() == SkipBlockRangeIterator.Match.YES_IF_PRESENT) {
      int docToCheck = Math.max(target, blockDoc);
      int currentBlockEnd = blockIterator.blockEnd();
      while (true) {
        if (values.advanceExact(docToCheck)) {
          return doc = docToCheck;
        }
        docToCheck++;
        if (docToCheck >= currentBlockEnd) {
          blockDoc = blockIterator.advance(docToCheck);
          if (blockDoc == NO_MORE_DOCS) {
            return doc = NO_MORE_DOCS;
          }
          docToCheck = blockDoc;
          SkipBlockRangeIterator.Match nextMatch = blockIterator.getMatch();
          if (nextMatch == SkipBlockRangeIterator.Match.YES) {
            return doc = docToCheck;
          }
          if (nextMatch != SkipBlockRangeIterator.Match.YES_IF_PRESENT) {
            // This is a MAYBE block, break out of the loop.
            break;
          }
          currentBlockEnd = blockIterator.blockEnd();
        }
      }
      // If we reached here, it means that we are now pointing to a MAYBE block
    }

    // For MAYBE blocks, scan forward to find a matching doc
    int docToCheck = Math.max(target, blockDoc);
    // Use the actual block boundary (not docIDRunEnd which returns doc+1 for MAYBE blocks)
    int currentBlockEnd = blockIterator.blockEnd();
    while (docToCheck != NO_MORE_DOCS) {
      if (values.advanceExact(docToCheck)) {
        // If we landed in a YES_IF_PRESENT block, skip the range check
        if (blockIterator.getMatch() == SkipBlockRangeIterator.Match.YES_IF_PRESENT) {
          return doc = docToCheck;
        }
        long v = values.longValue();
        if (v >= minValue && v <= maxValue) {
          return doc = docToCheck;
        }
      }
      docToCheck++;
      // Check if we've left the current block
      if (docToCheck >= currentBlockEnd) {
        // Move to next matching block
        blockDoc = blockIterator.advance(docToCheck);
        if (blockDoc == NO_MORE_DOCS) {
          return doc = NO_MORE_DOCS;
        }
        docToCheck = blockDoc;
        if (blockIterator.getMatch() == SkipBlockRangeIterator.Match.YES) {
          return doc = docToCheck;
        }
        currentBlockEnd = blockIterator.blockEnd();
      }
    }
    return doc = NO_MORE_DOCS;
  }

  @Override
  public long cost() {
    return values.cost();
  }

  @Override
  public int docIDRunEnd() throws IOException {
    return blockIterator.docIDRunEnd();
  }

  @Override
  public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
    while (doc < upTo) {
      // Advance block iterator if needed
      if (blockIterator.docID() < doc) {
        blockIterator.advance(doc);
      }
      if (blockIterator.docID() >= upTo || blockIterator.docID() == NO_MORE_DOCS) {
        doc = blockIterator.docID() == NO_MORE_DOCS ? NO_MORE_DOCS : blockIterator.docID();
        return;
      }

      int blockStart = Math.max(doc, blockIterator.docID());
      SkipBlockRangeIterator.Match match = blockIterator.getMatch();

      // For YES/YES_IF_PRESENT: docIDRunEnd() respects multi-level block promotion.
      // For MAYBE: docIDRunEnd() returns doc+1 (conservative), but we need the actual
      // block boundary to bulk-evaluate the whole block with rangeIntoBitSet().
      int blockEnd =
          match == SkipBlockRangeIterator.Match.MAYBE
              ? Math.min(upTo, blockIterator.blockEnd())
              : Math.min(upTo, blockIterator.docIDRunEnd());

      switch (match) {
        case YES:
          // All docs in this range match — set all bits
          bitSet.set(blockStart - offset, blockEnd - offset);
          break;

        case YES_IF_PRESENT:
          // All values in this block are in range, but the field is sparse so some docs
          // may not have a value. No range check needed here.
          for (int d = blockStart; d < blockEnd; d++) {
            if (values.advanceExact(d)) {
              bitSet.set(d - offset);
            }
          }
          break;

        case MAYBE:
          // Use rangeIntoBitSet — SIMD bulk evaluation for the full block.
          // For dense fields, this bypasses advanceExact() overhead entirely.
          values.rangeIntoBitSet(blockStart, blockEnd, minValue, maxValue, bitSet, offset);
          break;
      }

      // Move past this block
      doc = blockEnd;
      if (doc < upTo) {
        blockIterator.advance(doc);
        if (blockIterator.docID() == NO_MORE_DOCS) {
          doc = NO_MORE_DOCS;
          return;
        }
        doc = blockIterator.docID();
      }
    }
  }
}
