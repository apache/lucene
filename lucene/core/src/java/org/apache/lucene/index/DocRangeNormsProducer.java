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
import org.apache.lucene.codecs.NormsProducer;

/**
 * Norms restricted to {@code [start, end)} rather than merely masked.
 *
 * <p>Masking is enough for correctness but not for cost: merging a field's norms walks the column
 * with {@link NumericDocValues#nextDoc()} and drops whatever the document map sends to {@code -1},
 * having already read it. Each output of a partitioned merge would therefore read the whole column,
 * and k outputs would read it k times. Seeking to the range instead makes the outputs together read
 * it once.
 */
final class DocRangeNormsProducer extends NormsProducer {

  private final NormsProducer in;
  private final int start;
  private final int end;

  DocRangeNormsProducer(NormsProducer in, int start, int end) {
    this.in = in;
    this.start = start;
    this.end = end;
  }

  @Override
  public NumericDocValues getNorms(FieldInfo field) throws IOException {
    final NumericDocValues values = in.getNorms(field);
    return new FilterNumericDocValues(values) {
      private int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        // The first call seeks to the range; the rest walk inside it.
        return doc = clamp(doc < start ? values.advance(start) : values.nextDoc());
      }

      @Override
      public int advance(int target) throws IOException {
        return doc = clamp(values.advance(Math.max(target, start)));
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        doc = target;
        return target >= start && target < end && values.advanceExact(target);
      }

      private int clamp(int d) {
        return d >= end ? NumericDocValues.NO_MORE_DOCS : d;
      }
    };
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    in.checkIntegrity(merge);
  }

  @Override
  public void close() throws IOException {
    // The wrapped producer is owned by the reader this narrows, not by the narrowing.
  }
}
