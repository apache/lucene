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

package org.apache.lucene.codecs.lucene94;

import java.io.IOException;
import org.apache.lucene.index.FilterVectorValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;

/** convert from floating point to single byte */
public class CompressingVectorValues extends FilterVectorValues {

  private final BytesRef binaryValue;

  /** @param in the wrapped values */
  protected CompressingVectorValues(VectorValues in) {
    super(in);
    binaryValue = new BytesRef(in.dimension());
    binaryValue.length = in.dimension();
  }

  @Override
  public BytesRef binaryValue() throws IOException {
    float[] floats = vectorValue();
    byte[] bytes = binaryValue.bytes;
    for (int i = 0; i < dimension(); i++) {
      bytes[i] = (byte) floats[i];
    }
    return binaryValue;
  }
}
