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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class to read the results of a query from the DPU results array The purpose of this class is to
 * be able to read the results of a query while abstracting out the fact that the results are
 * scattered across the DPU results array.
 */
public class DpuResultsInput extends DpuResultsReader {

  final int nbDpus;
  final ByteBuffer[] dpuQueryResultsAddr;
  final ByteBuffer[] dpuResults;
  final int queryId;
  int currDpuId;
  int currByteIndex;
  int byteIndexEnd;

  DpuResultsInput(
      int nbDpus, ByteBuffer[] dpuResults, ByteBuffer[] dpuQueryResultsAddr, int queryId) {
    this.nbDpus = nbDpus;
    this.dpuResults = dpuResults;
    this.dpuQueryResultsAddr = dpuQueryResultsAddr;
    this.queryId = queryId;
    this.currDpuId = -1;
    this.byteIndexEnd = 0;
    this.currByteIndex = 0;
  }

  private void nextDpu() throws IOException {

    this.currDpuId++;
    if (this.currDpuId >= this.nbDpus) throw new IOException("No more DPU results");

    this.currByteIndex = 0;
    if (queryId > 0)
      this.currByteIndex =
          dpuQueryResultsAddr[currDpuId].getInt((queryId - 1) * Integer.BYTES) * Integer.BYTES * 2;

    this.byteIndexEnd =
        dpuQueryResultsAddr[currDpuId].getInt(queryId * Integer.BYTES) * Integer.BYTES * 2;
  }

  private boolean endOfDpuBuffer() {
    return this.currByteIndex >= this.byteIndexEnd;
  }

  @Override
  public byte readByte() throws IOException {
    while (endOfDpuBuffer()) nextDpu();
    return dpuResults[currDpuId].get(currByteIndex++);
  }

  @Override
  public int readInt() throws IOException {
    while (endOfDpuBuffer()) nextDpu();
    int val = dpuResults[currDpuId].getInt(currByteIndex);
    currByteIndex += 4;
    return val;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {

    if (len <= (byteIndexEnd - currByteIndex)) {
      System.arraycopy(dpuResults[currDpuId], currByteIndex, b, offset, len);
      currByteIndex += len;
    } else {
      int nbBytesToCopy = byteIndexEnd - currByteIndex;
      if (nbBytesToCopy > 0)
        System.arraycopy(dpuResults[currDpuId], currByteIndex, b, offset, nbBytesToCopy);
      nextDpu();
      readBytes(b, offset + nbBytesToCopy, len - nbBytesToCopy);
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {

    if (numBytes <= (byteIndexEnd - currByteIndex)) {
      currByteIndex += numBytes;
    } else {
      int nbBytesToSkip = byteIndexEnd - currByteIndex;
      if (nbBytesToSkip > 0) currByteIndex += nbBytesToSkip;
      nextDpu();
      skipBytes(numBytes - nbBytesToSkip);
    }
  }

  @Override
  public boolean eof() {

    while ((this.currDpuId + 1 < this.nbDpus) && endOfDpuBuffer()) {
      try {
        nextDpu();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return (this.currDpuId + 1 == this.nbDpus) && endOfDpuBuffer();
  }
}
