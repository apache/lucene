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

import java.util.Locale;

/**
 * Word2Vec unit composed by a term with the associated vector
 *
 * @lucene.experimental
 */
public record TermAndVector(BytesRef term, float[] vector) {

  public int size() {
    return vector.length;
  }

  /** Return a {@link TermAndVector} whose vector is normalized according to the L2 norm. */
  public TermAndVector normalizeVector() {
    float[] vector = this.vector.clone();
    VectorUtil.l2normalize(vector);
    return new TermAndVector(term, vector);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(this.term.utf8ToString());
    builder.append(" [");
    if (vector.length > 0) {
      for (int i = 0; i < vector.length - 1; i++) {
        builder.append(String.format(Locale.ROOT, "%.3f,", vector[i]));
      }
      builder.append(String.format(Locale.ROOT, "%.3f]", vector[vector.length - 1]));
    }
    return builder.toString();
  }
}
