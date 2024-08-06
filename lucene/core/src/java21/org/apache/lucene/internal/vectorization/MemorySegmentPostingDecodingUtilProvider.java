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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.function.Function;
import org.apache.lucene.codecs.lucene912.PostingDecodingUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;

public class MemorySegmentPostingDecodingUtilProvider
    implements Function<IndexInput, PostingDecodingUtil> {

  public MemorySegmentPostingDecodingUtilProvider() {}

  @Override
  public PostingDecodingUtil apply(IndexInput in) {
    if (in instanceof MemorySegmentAccessInput msai) {
      try {
        MemorySegment ms = msai.segmentSliceOrNull(0, in.length());
        if (ms != null) {
          try {
            return new MemorySegmentPostingDecodingUtil(in, ms);
          } catch (
              @SuppressWarnings("unused")
              UnsupportedOperationException e) {
            // long species width is incompatible with the PostingDecodingUtil contract
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return new DefaultPostingDecodingUtil(in);
  }
}
