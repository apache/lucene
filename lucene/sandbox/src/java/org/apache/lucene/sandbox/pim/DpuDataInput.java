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
import org.apache.lucene.store.DataInput;

/**
 * DataInput to read results coming from the DPUs. The results are scattered over several
 * ByteBuffers, one for each DPU, and each ByteBuffer contains results of several queries (but
 * results are sorted by queries, and results of a query in one DPU are sorted by docId).
 */
public class DpuDataInput extends DataInput {

  private final int nbDpus;
  private final ByteBuffer[] dpuResults;
  private int currDpuId;
  private final int[] currByteIndex;
  private final int[] lastByteIndex;

  DpuDataInput(
      int nbDpus,
      ByteBuffer[] dpuResults,
      ByteBuffer[] dpuQueryResultsAddr,
      int queryId,
      int queryResultByteSize) {

    this.nbDpus = nbDpus;
    this.dpuResults = dpuResults;
    this.currDpuId = 0;
    currByteIndex = new int[dpuResults.length];
    lastByteIndex = new int[dpuResults.length];
    for (int i = 0; i < dpuResults.length; ++i) {
      currByteIndex[i] = 0;
      if (queryId > 0)
        currByteIndex[i] =
            dpuQueryResultsAddr[i].getInt((queryId - 1) * Integer.BYTES) * queryResultByteSize;
      this.lastByteIndex[i] =
          dpuQueryResultsAddr[i].getInt(queryId * Integer.BYTES) * queryResultByteSize;
    }
  }

  /** jump to first DPU */
  public void beginDpu() {
    this.currDpuId = 0;
  }

  /**
   * @return true if there are no more data to be read for the current DPU
   */
  public boolean eof() {
    return currByteIndex[currDpuId] >= lastByteIndex[currDpuId];
  }

  /**
   * @return true if this is the last DPU
   */
  public boolean lastDpu() {
    return currDpuId == nbDpus - 1;
  }

  /** jump to next DPU */
  public void nextDpu() {
    currDpuId++;
  }

  /**
   * Read the docId of the current result, but do not increase the position of the buffer
   *
   * @return the docId of the current result
   */
  public int peekDocID() {
    return dpuResults[currDpuId].getInt(currByteIndex[currDpuId]);
  }

  /**
   * Read the docId of the current result, and increase the position of the buffer
   *
   * @return the docId of the current result
   */
  public int readDocId() throws IOException {
    return readInt();
  }

  @Override
  public int readInt() throws IOException {
    int val = dpuResults[currDpuId].getInt(currByteIndex[currDpuId]);
    currByteIndex[currDpuId] += Integer.BYTES;
    return val;
  }

  @Override
  public byte readByte() throws IOException {
    return dpuResults[currDpuId].get(currByteIndex[currDpuId]++);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {

    if (len > (lastByteIndex[currDpuId] - currByteIndex[currDpuId]))
      throw new IOException("Read passed DpuDataInput current DPU results");
    System.arraycopy(dpuResults[currDpuId], currByteIndex[currDpuId], b, offset, len);
    currByteIndex[currDpuId] += len;
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {

    if (numBytes > (lastByteIndex[currDpuId] - currByteIndex[currDpuId]))
      throw new IOException("Read passed DpuDataInput current DPU results");
    currByteIndex[currDpuId] += numBytes;
  }
}
