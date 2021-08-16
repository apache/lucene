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
package org.apache.lucene.demo.knn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Arrays;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;

/**
 * Looks up each tokens in a dictionary, and sums the token vectors. Unrecognized tokens are
 * ignored. The resulting vector is normalized to unit length.
 */
public final class KnnVectorDictFilter extends TokenFilter {

  private final KnnVectorDict dict;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final float[] scratchFloats;
  private final float[] result;
  private final byte[] scratchBytes;
  private final FloatBuffer scratchBuffer;

  /**
   * sole constructor
   *
   * @param input the input token stream to filter.
   * @param dict a token to vector dictionary, used to look up the token vectors.
   */
  public KnnVectorDictFilter(TokenStream input, KnnVectorDict dict) {
    super(input);
    this.dict = dict;
    result = new float[dict.getDimension()];
    scratchBytes = new byte[dict.getDimension() * Float.BYTES];
    scratchBuffer = ByteBuffer.wrap(scratchBytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
    scratchFloats = new float[dict.getDimension()];
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken() == false) {
      return false;
    }
    BytesRef term = new BytesRef(termAtt.toString());
    dict.get(term, scratchBytes);
    scratchBuffer.position(0);
    scratchBuffer.get(scratchFloats);
    VectorUtil.add(result, scratchFloats);
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    Arrays.fill(result, 0);
  }

  @Override
  public void end() throws IOException {
    super.end();
    VectorUtil.l2normalize(result, false);
  }

  /**
   * Get the vector computed from the input
   *
   * @return the resultant sum of the vectors of each term.
   */
  public float[] getResult() {
    return result;
  }
}
