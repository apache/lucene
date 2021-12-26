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
package org.apache.lucene.util.packed;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Class for writing packed longs to be blockly read from Directory. Longs can be read on-the-fly
 * via {@link BlockReader}.
 *
 * <p>Unlike PackedInts, it optimizes for read i/o operations and supports &gt; 2B values. Example
 * usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = DirectWriter.bitsRequired(100); // values up to and including 100
 *   IndexOutput output = dir.createOutput("packed", IOContext.DEFAULT);
 *   BlockWriter writer = new BlockWriter(output, bitsPerValue);
 *   for (int i = 0; i &lt; numberOfValues; i++) {
 *     writer.add(value);
 *   }
 *   writer.finish();
 *   output.close();
 * </pre>
 *
 * @see BlockReader
 */
public class BlockWriter {

  public static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

  public static int bitsRequired(long maxValue) {
    return DirectWriter.bitsRequired(maxValue);
  }

  public static int unsignedBitsRequired(long maxValue) {
    return DirectWriter.unsignedBitsRequired(maxValue);
  }

  static final int[] SUPPORTED_BITS_PER_VALUE = DirectWriter.SUPPORTED_BITS_PER_VALUE;

  private final int bpv;
  private final long[] buffer;
  private final DataOutput output;
  private final ForUtil forUtil = new ForUtil();
  private int bufferIndex = 0;
  private boolean finished = false;

  public BlockWriter(DataOutput output, int bpv) {
    this.output = output;
    this.bpv = bpv;
    this.buffer = new long[BLOCK_SIZE];
  }

  public void add(long l) throws IOException {
    assert !finished;
    buffer[bufferIndex++] = l;
    if (bufferIndex == BLOCK_SIZE) {
      forUtil.encode(buffer, bpv, output);
      bufferIndex = 0;
    }
  }

  public void finish() throws IOException {
    assert !finished;
    if (bufferIndex > 0) {
      writeRemainder();
    }
    bufferIndex = 0;
    finished = true;
  }

  private void writeRemainder() throws IOException {
    if (bufferIndex == 0) {
      return;
    }
    forUtil.encode(buffer, bpv, output);
//    DirectWriter directWriter = DirectWriter.getInstance(output, bufferIndex, bpv);
//    for (int i=0; i<bufferIndex; i++) {
//      directWriter.add(buffer[i]);
//    }
//    directWriter.finish();
  }
}
