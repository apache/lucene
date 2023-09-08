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
package org.apache.lucene.codecs.lucene98;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;

/** The quantization state, includes lower and upper quantile */
class QuantizationState implements Accountable {
  private final float lowerQuantile, upperQuantile;

  static QuantizationState fromInput(IndexInput input) throws IOException {
    return new QuantizationState(
        Float.intBitsToFloat(input.readInt()), Float.intBitsToFloat(input.readInt()));
  }

  QuantizationState(float lowerQuantile, float upperQuantile) {
    this.lowerQuantile = lowerQuantile;
    this.upperQuantile = upperQuantile;
  }

  float getLowerQuantile() {
    return lowerQuantile;
  }

  float getUpperQuantile() {
    return upperQuantile;
  }

  void writeToOutput(IndexOutput output) throws IOException {
    output.writeInt(Float.floatToIntBits(lowerQuantile));
    output.writeInt(Float.floatToIntBits(upperQuantile));
  }

  @Override
  public long ramBytesUsed() {
    return Float.BYTES * 2;
  }
}
