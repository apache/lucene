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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToLongFunction;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;

class NumericFieldReaderContextComparator implements Comparator<LeafReaderContext> {

  private final Map<Integer, Long> cachedSortValues = new HashMap<>();
  private final String field;
  private final boolean reverse;
  private final Long missingValue;
  private final ToLongFunction<byte[]> pointDecoder;

  public NumericFieldReaderContextComparator(
      String field, Long missingValue, boolean reverse, ToLongFunction<byte[]> pointDecoder) {
    this.field = field;
    this.missingValue = missingValue;
    this.reverse = reverse;
    this.pointDecoder = pointDecoder;
  }

  @Override
  public int compare(LeafReaderContext o1, LeafReaderContext o2) {
    return reverse
        ? Long.compare(getSortValue(o2), getSortValue(o1))
        : Long.compare(getSortValue(o1), getSortValue(o2));
  }

  private long getSortValue(LeafReaderContext ctx) {
    if (cachedSortValues.containsKey(ctx.ord) == false) {
      cachedSortValues.put(ctx.ord, loadSortValue(ctx));
    }
    return cachedSortValues.get(ctx.ord);
  }

  private long loadSortValue(LeafReaderContext ctx) {
    LeafReader reader = ctx.reader();
    try {
      DocValuesSkipper skipper = reader.getDocValuesSkipper(field);
      if (skipper != null) {
        if (skipper.docCount() == reader.maxDoc() || missingValue == null) {
          return reverse ? skipper.maxValue() : skipper.minValue();
        }
        if (reverse) {
          return Math.max(skipper.maxValue(), missingValue);
        } else {
          return Math.min(skipper.minValue(), missingValue);
        }
      }
      PointValues pointValues = reader.getPointValues(field);
      if (pointValues != null) {
        if (pointValues.getDocCount() == reader.maxDoc() || missingValue == null) {
          if (reverse) {
            return pointDecoder.applyAsLong(pointValues.getMaxPackedValue());
          } else {
            return pointDecoder.applyAsLong(pointValues.getMinPackedValue());
          }
        }
        if (reverse) {
          return Math.max(pointDecoder.applyAsLong(pointValues.getMaxPackedValue()), missingValue);
        } else {
          return Math.min(pointDecoder.applyAsLong(pointValues.getMinPackedValue()), missingValue);
        }
      }
    } catch (IOException _) {
      return reverse ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    return reverse ? Long.MAX_VALUE : Long.MIN_VALUE;
  }
}
