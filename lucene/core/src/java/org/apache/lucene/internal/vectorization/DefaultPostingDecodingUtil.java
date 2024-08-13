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
import org.apache.lucene.store.IndexInput;

final class DefaultPostingDecodingUtil extends PostingDecodingUtil {

  protected final IndexInput in;

  public DefaultPostingDecodingUtil(IndexInput in) {
    this.in = in;
  }

  @Override
  public void splitLongs(
      int count, long[] b, int bShift, long bMask, long[] c, int cIndex, long cMask)
      throws IOException {
    assert count <= 64;
    in.readLongs(c, cIndex, count);
    // The below loop is auto-vectorized
    for (int i = 0; i < count; ++i) {
      b[i] = (c[cIndex + i] >>> bShift) & bMask;
      c[cIndex + i] &= cMask;
    }
  }
}
