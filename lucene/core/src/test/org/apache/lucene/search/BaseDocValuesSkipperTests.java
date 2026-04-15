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
import org.apache.lucene.tests.util.LuceneTestCase;

public abstract class BaseDocValuesSkipperTests extends LuceneTestCase {

  /**
   * Fake numeric doc values so that: - docs 0-256 all match - docs in 256-512 are all greater than
   * queryMax - docs in 512-768 are all less than queryMin - docs in 768-1024 have some docs that
   * match the range, others not - docs in 1024-2048 follow a similar pattern as docs in 0-1024
   * except that not all docs have a - value
   */
  protected static NumericDocValues docValues(long queryMin, long queryMax) {
    return new NumericDocValues() {

      int doc = -1;

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
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
        if (target < 1024) {
          // dense up to 1024
          return doc = target;
        } else if (doc < 2047) {
          // 50% docs have a value up to 2048
          return doc = target + (target & 1);
        } else {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
      }

      @Override
      public long longValue() throws IOException {
        int d = doc % 1024;
        if (d < 128) {
          return (queryMin + queryMax) >> 1;
        } else if (d < 256) {
          return queryMax + 1;
        } else if (d < 512) {
          return queryMin - 1;
        } else {
          return switch ((d / 2) % 3) {
            case 0 -> queryMin - 1;
            case 1 -> queryMax + 1;
            case 2 -> (queryMin + queryMax) >> 1;
            default -> throw new AssertionError();
          };
        }
      }

      @Override
      public long cost() {
        return 42;
      }
    };
  }

  /**
   * Fake skipper over a NumericDocValues field built by an equivalent call to {@link
   * #docValues(long, long)}
   */
  protected static DocValuesSkipper docValuesSkipper(
      long queryMin, long queryMax, boolean doLevels) {
    return new DocValuesSkipper() {

      int doc = -1;

      @Override
      public void advance(int target) throws IOException {
        doc = target;
      }

      @Override
      public int numLevels() {
        return doLevels ? 3 : 1;
      }

      @Override
      public int minDocID(int level) {
        int rangeLog = 9 - numLevels() + level;

        // the level is the log2 of the interval
        if (doc < 0) {
          return -1;
        } else if (doc >= 2048) {
          return DocIdSetIterator.NO_MORE_DOCS;
        } else {
          int mask = (1 << rangeLog) - 1;
          // prior multiple of 2^level
          return doc & ~mask;
        }
      }

      @Override
      public int maxDocID(int level) {
        int rangeLog = 9 - numLevels() + level;

        int minDocID = minDocID(level);
        return switch (minDocID) {
          case -1 -> -1;
          case DocIdSetIterator.NO_MORE_DOCS -> DocIdSetIterator.NO_MORE_DOCS;
          default -> minDocID + (1 << rangeLog) - 1;
        };
      }

      @Override
      @SuppressWarnings("DuplicateBranches")
      public long minValue(int level) {
        int d = doc % 1024;
        if (d < 128) {
          return queryMin;
        } else if (d < 256) {
          return queryMax + 1;
        } else if (d < 768) {
          return queryMin - 1;
        } else {
          return queryMin - 1;
        }
      }

      @Override
      public long maxValue(int level) {
        int d = doc % 1024;
        if (d < 128) {
          return queryMax;
        } else if (d < 256) {
          return queryMax + 1;
        } else if (d < 768) {
          return queryMin - 1;
        } else {
          return queryMax + 1;
        }
      }

      @Override
      public int docCount(int level) {
        int rangeLog = 9 - numLevels() + level;

        if (doc < 1024) {
          return 1 << rangeLog;
        } else {
          // half docs have a value
          return 1 << rangeLog >> 1;
        }
      }

      @Override
      public long minValue() {
        return Long.MIN_VALUE;
      }

      @Override
      public long maxValue() {
        return Long.MAX_VALUE;
      }

      @Override
      public int docCount() {
        return 1024 + 1024 / 2;
      }
    };
  }
}
