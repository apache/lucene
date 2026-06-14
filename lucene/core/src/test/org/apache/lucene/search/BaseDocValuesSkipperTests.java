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

  // Dense up to 1024+64=1088, sparse (even docs only) from 1088 onward.
  // This means block [1024,1087] is dense YES while [1088,1151] is sparse YES_IF_PRESENT,
  // both in the same level-1 block [1024,1151] — exercising the docIDRunEnd density check.
  static final int DENSE_END = 1088;

  /**
   * Fake numeric doc values: value pattern repeats every 1024 docs (d%1024 in [0,128) matches,
   * [128,256) is above queryMax, [256,512) is below queryMin, [512,1024) is a mix). Docs 0 through
   * {@link #DENSE_END}-1 are dense (all have values); docs from {@link #DENSE_END} onward are
   * sparse (only even docs have a value).
   */
  protected static NumericDocValues docValues(long queryMin, long queryMax) {
    return new NumericDocValues() {

      int doc = -1;

      @Override
      public boolean advanceExact(int target) throws IOException {
        int advanced = advance(target);
        return advanced == target;
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
        if (target >= 2048) {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        } else if (target < DENSE_END) {
          return doc = target;
        } else {
          // sparse: round up to even
          int d = target + (target & 1);
          return doc = (d >= 2048) ? DocIdSetIterator.NO_MORE_DOCS : d;
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
   * #docValues(long, long)}. Dense up to doc {@link #DENSE_END}, sparse (even docs only) after
   * that. This means block [1024,1087] is dense YES while [1088,1151] is sparse YES_IF_PRESENT,
   * both in the same level-1 block — exercising the docIDRunEnd density check.
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
      public long minValue(int level) {
        int dStart = minDocID(level) % 1024;
        int dEnd = maxDocID(level) % 1024;
        long min = Long.MAX_VALUE;
        if (dStart <= 127 && dEnd >= 0) min = Math.min(min, queryMin);
        if (dStart <= 255 && dEnd >= 128) min = Math.min(min, queryMax + 1);
        if (dStart <= 511 && dEnd >= 256) min = Math.min(min, queryMin - 1);
        if (dEnd >= 512) min = Math.min(min, queryMin - 1);
        return min;
      }

      @Override
      public long maxValue(int level) {
        int dStart = minDocID(level) % 1024;
        int dEnd = maxDocID(level) % 1024;
        long max = Long.MIN_VALUE;
        if (dStart <= 127 && dEnd >= 0) max = Math.max(max, queryMax);
        if (dStart <= 255 && dEnd >= 128) max = Math.max(max, queryMax + 1);
        if (dStart <= 511 && dEnd >= 256) max = Math.max(max, queryMin - 1);
        if (dEnd >= 512) max = Math.max(max, queryMax + 1);
        return max;
      }

      @Override
      public int docCount(int level) {
        int rangeLog = 9 - numLevels() + level;
        int blockSize = 1 << rangeLog;
        int minDoc = minDocID(level);
        if (minDoc < 0 || minDoc >= 2048) {
          return 0;
        }
        if (minDoc >= DENSE_END) {
          return blockSize / 2;
        }
        // Block starts in dense region — count dense + sparse portions
        int denseCount = Math.min(blockSize, DENSE_END - minDoc);
        int sparseCount = (blockSize - denseCount) / 2;
        return denseCount + sparseCount;
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
        return DENSE_END + (2048 - DENSE_END) / 2; // 1088 + 480 = 1568
      }

      @Override
      public int maxValueCount() {
        return 1;
      }
    };
  }
}
