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

import com.upmem.dpu.DpuException;
import com.upmem.dpu.DpuProgramInfo;
import com.upmem.dpu.DpuSet;
import com.upmem.dpu.DpuSymbol;
import com.upmem.dpu.DpuSystem;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;

class DpuSystemExecutor implements PimQueriesExecutor {
  static final int INDEX_CACHE_SIZE = 1 << 16;
  static final int MAX_DPU_INDEX_SIZE = 48 << 20;
  static final boolean PARALLEL_INDEX_LOAD = true;
  static final int INIT_SG_RETURN_CAPACITY = 2;
  private final byte[] queryBatchBuffer;
  private final DpuSystem dpuSystem;
  private final DpuProgramInfo dpuProgramInfo;
  private final ByteArrayOutputStream dpuStream;
  private final byte[] dpuQueryOffsetInBatch;
  private final int[] dpuIdOffset;
  private int nbLuceneSegments;
  private SGReturnPool sgReturnPool;

  /*
   * Static load of the DPU jni library specific to PIM lucene
   * This contains the native (C) method to transfer results from
   * DPUs with scatter/gather (to reorder the results per query and lucene segments)
   */
  static {
    System.loadLibrary("dpuLucene");
  }

  /**
   * Transfer queries results from the DPU
   *
   * @param nr_queries the number of queries in the batch sent to the DPU
   * @param nr_segments the number of lucene segments
   * @return an SGReturn object, containing the queries results ordered by query id and lucene
   *     segment id
   */
  native int sgXferResults(int nr_queries, int nr_segments, SGReturnPool.SGReturn results);

  DpuSystemExecutor(int numDpusToAlloc) throws DpuException {
    queryBatchBuffer = new byte[DpuConstants.dpuQueryBatchByteSize];
    // allocate DPUs, load the program, allocate space for DPU results
    dpuStream = new ByteArrayOutputStream();
    try {
      dpuSystem =
          DpuSystem.allocate(
              numDpusToAlloc, "sgXferEnable=true", new PrintStream(dpuStream, true, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    dpuProgramInfo = dpuSystem.load(DpuConstants.dpuProgramPath);
    dpuQueryOffsetInBatch = new byte[DpuConstants.dpuQueryMaxBatchSize * Integer.BYTES];
    dpuIdOffset = new int[dpuSystem.dpus().size()];
    int cnt = 0;
    for (int i = 0; i < dpuSystem.ranks().size(); ++i) {
      dpuIdOffset[i] = cnt;
      cnt += dpuSystem.ranks().get(i).dpus().size();
    }
    nbLuceneSegments = 0;
  }

  @Override
  public void setPimIndex(PimIndexInfo pimIndexInfo) throws DpuException, IOException {

    if (pimIndexInfo.getNumDpus() > DpuConstants.nrDpus) {
      throw new DpuException(
          "ERROR: index contains to many DPUs "
              + pimIndexInfo.getNumDpus()
              + " > "
              + DpuConstants.nrDpus);
    }

    if (pimIndexInfo.getNumSegments() > DpuConstants.dpuMaxNbLuceneSegments) {
      throw new DpuException(
          "ERROR: index contains too many Lucene segments "
              + pimIndexInfo.getNumSegments()
              + " > "
              + DpuConstants.dpuMaxNbLuceneSegments);
    }

    // TODO Debug further parallel load. It crashes in the jni layer of the DPU load API
    if (PARALLEL_INDEX_LOAD) {

      DpuSymbol indexSymbol = dpuProgramInfo.get(DpuConstants.dpuIndexVarName);

      // parallel transfer of the index on each DPU rank
      // loop over each DPU of the rank, read INDEX_CACHE_SIZE bytes
      // Then transfer to the DPUs of the rank
      // loop until all DPUs have received all their index
      dpuSystem
          .async()
          .call(
              (DpuSet set, int rankId) -> {
                if (dpuIdOffset[rankId] >= pimIndexInfo.getNumDpus()) return;

                try {
                  ByteBuffer[] indexBuffers = new ByteBuffer[set.dpus().size()];
                  byte[][] indexBuffersArrays = new byte[set.dpus().size()][INDEX_CACHE_SIZE];
                  for (int i = 0; i < indexBuffers.length; ++i) {
                    indexBuffers[i] = ByteBuffer.allocateDirect(INDEX_CACHE_SIZE);
                    indexBuffers[i].order(ByteOrder.LITTLE_ENDIAN);
                  }
                  long[] dpuIndexPos = new long[set.dpus().size()];
                  int cnt = 0;
                  IndexInput in = pimIndexInfo.getFileInput();
                  while (true) {
                    boolean dpuActive = false;
                    int readSizeSet = 0;
                    for (int i = 0; i < set.dpus().size(); ++i) {
                      if (dpuIdOffset[rankId] + i >= pimIndexInfo.getNumDpus()) {
                        indexBuffers[i] = null;
                        continue;
                      }
                      long indexSize = pimIndexInfo.seekToDpu(in, dpuIdOffset[rankId] + i);
                      if (indexSize > MAX_DPU_INDEX_SIZE)
                        throw new DpuException(
                            "ERROR: index of DPU" + i + " is too large, size=" + indexSize);
                      int readSize =
                          readIndexBufferForDpu(
                              in, dpuIndexPos[i], indexSize, indexBuffersArrays[i]);
                      if (readSize != 0) {
                        dpuActive = true;
                        dpuIndexPos[i] += readSize;
                        indexBuffers[i].clear();
                        indexBuffers[i].put(indexBuffersArrays[i]);
                        if (readSize > readSizeSet) readSizeSet = readSize;
                      }
                    }
                    if (!dpuActive) break;

                    DpuSymbol sym = indexSymbol.getSymbolWithOffset(cnt * INDEX_CACHE_SIZE);
                    set.copy(sym, indexBuffers, ((readSizeSet + 7) >> 3) << 3);
                    cnt++;
                  }
                  in.close();
                } catch (IOException e) {
                  throw new DpuException(e.getMessage());
                }
              });
    }

    // send a value to all DPU indicating whether an index
    // was loaded in it or not (there can be more dpus allocated in the PIM system than
    // DPUs needed by the index loaded).
    byte[][] indexLoaded = new byte[DpuConstants.nrDpus][Integer.BYTES];
    ByteBuffer b = ByteBuffer.allocate(Integer.BYTES);
    b.order(ByteOrder.LITTLE_ENDIAN);
    b.putInt(1);
    for (int i = 0; i < pimIndexInfo.getNumDpus(); ++i) {

      if (!PARALLEL_INDEX_LOAD) {
        IndexInput in = pimIndexInfo.getFileInput();
        long dpuIndexSize = pimIndexInfo.seekToDpu(in, i);
        byte[] data = new byte[((Math.toIntExact(dpuIndexSize) + 7) >> 3) << 3];
        in.readBytes(data, 0, Math.toIntExact(dpuIndexSize));
        // TODO alignment on 8 bytes ?
        dpuSystem.dpus().get(i).copy(DpuConstants.dpuIndexVarName, data);
      }

      indexLoaded[i] = b.array();
    }
    dpuSystem.copy(DpuConstants.dpuIndexLoadedVarName, indexLoaded);

    nbLuceneSegments = pimIndexInfo.getNumSegments();
    sgReturnPool =
        new SGReturnPool(
            INIT_SG_RETURN_CAPACITY, getInitSgResultsByteSize(pimIndexInfo.getNumDocs()));
  }

  private int getInitSgResultsByteSize(int numDocs) {

    int docPerQuery = Math.toIntExact((long) (numDocs * 0.01));
    if (docPerQuery < 1000) docPerQuery = 1000;
    return DpuConstants.dpuQueryMaxBatchSize * docPerQuery * 8;
  }

  private int readIndexBufferForDpu(IndexInput in, long dpuIndexPos, long indexSize, byte[] buffer)
      throws IOException {

    if (buffer == null) return 0;
    if (dpuIndexPos >= indexSize) return 0;
    int readSize = INDEX_CACHE_SIZE;
    if (readSize > (indexSize - dpuIndexPos)) {
      readSize = Math.toIntExact(indexSize - dpuIndexPos);
    }
    in.skipBytes(dpuIndexPos);
    in.readBytes(buffer, 0, readSize);
    return readSize;
  }

  /**
   * Broadcast an integer to the DpuSystem
   *
   * @param varName the variable name in DPU code
   * @param val the integer value to transfer
   * @throws DpuException if there is an issue with the copy
   */
  private void copyIntToDpus(String varName, int val) throws DpuException {

    // TODO alignment on 8 bytes
    ByteBuffer b = ByteBuffer.allocate(4);
    b.order(ByteOrder.LITTLE_ENDIAN);
    b.putInt(val);
    dpuSystem.async().copy(varName, b.array());
  }

  /**
   * Align the input integer to the next power of 8
   *
   * @param v the integer to align
   * @return the smallest value multiple of 8 and >= v
   */
  private static int AlignTo8(int v) {
    return ((v + 7) >> 3) << 3;
  }

  @Override
  public void executeQueries(List<PimSystemManager.QueryBuffer> queryBuffers) throws DpuException {

    // 1) send queries to PIM
    sendQueriesToPIM(queryBuffers);

    // System.out.println(">> Launching DPUs");
    // 2) launch DPUs (program should be loaded on PimSystemManager Index load (only once)
    dpuSystem.async().exec(null);

    if (DpuConstants.DEBUG_DPU) {
      dpuSystem.async().sync();
      for (int i = 0; i < dpuSystem.ranks().size(); ++i) {
        dpuSystem.ranks().get(i).log();
      }
    }

    // 3) results transfer from DPUs to CPU
    //    Call native API which performs scatter/gather transfer
    SGReturnPool.SGReturn results = sgReturnPool.get(queryBuffers.size(), nbLuceneSegments);
    int resSize = sgXferResults(queryBuffers.size(), nbLuceneSegments, results);
    if (resSize > 0) {
      // buffer passed to the JNI layer was to small, request a larger one from the pool and
      // call again
      results.endReading(queryBuffers.size() * nbLuceneSegments);
      results = sgReturnPool.get(queryBuffers.size(), nbLuceneSegments, resSize);
      if (sgXferResults(queryBuffers.size(), nbLuceneSegments, results) > 0)
        throw new DpuException("Error in sg transfer results buffer allocation");
    }
    results.queriesIndices.order(ByteOrder.LITTLE_ENDIAN);
    results.byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    results.segmentsIndices.order(ByteOrder.LITTLE_ENDIAN);

    // 4) barrier to wait for transfers to be finished
    // TODO sg transfer in asynchronous mode ?
    dpuSystem.async().sync();

    // 5) Update the results map for the client threads to read their results
    for (int q = 0, size = queryBuffers.size(); q < size; ++q) {
      PimSystemManager.QueryBuffer buffer = queryBuffers.get(q);
      buffer.addResults(
          new DpuExecutorSGResultsReader(
              buffer.query, results, q, buffer.query.getResultByteSize(), nbLuceneSegments));
    }
  }

  private void sendQueriesToPIM(List<PimSystemManager.QueryBuffer> queryBuffers)
      throws DpuException {
    // TODO: Here we could sort queryBuffers to group the queries by type before
    // sending them. Just need to make QueryBuffer implement Comparable and compare
    // on the query type.
    // queryBuffers.sort(Comparator.naturalOrder());

    // TODO: use Scatter Gather approach to send query buffers separately?
    int batchLength = 0;
    ByteArrayDataOutput out = new ByteArrayDataOutput(dpuQueryOffsetInBatch);
    for (PimSystemManager.QueryBuffer queryBuffer : queryBuffers) {
      if (batchLength + queryBuffer.length > DpuConstants.dpuQueryBatchByteSize) {
        // size is too large, the DPU cannot support this
        // TODO should not just throw but handle this by reducing the size of the batch
        // for instance doing two execution of DPU
        throw new DpuException(
            "Error: batch size too large for DPU size="
                + batchLength
                + " max="
                + DpuConstants.dpuQueryBatchByteSize);
      }
      System.arraycopy(queryBuffer.bytes, 0, queryBatchBuffer, batchLength, queryBuffer.length);
      out.writeInt(batchLength);
      batchLength += queryBuffer.length;
    }

    dpuSystem
        .async()
        .copy(DpuConstants.dpuQueryBatchVarName, queryBatchBuffer, 0, AlignTo8(batchLength), 0);
    dpuSystem
        .async()
        .copy(
            DpuConstants.dpuQueryOffsetInBatchVarName,
            dpuQueryOffsetInBatch,
            0,
            AlignTo8(queryBuffers.size() * Integer.BYTES),
            0);
    copyIntToDpus(DpuConstants.dpuNbQueryInBatchVarName, queryBuffers.size());
    copyIntToDpus(DpuConstants.dpuNbByteInBatchVarName, batchLength);
  }

  @Override
  public void dumpDpuStream() {
    if (DpuConstants.DEBUG_DPU) {
      System.out.println("Printing DPU stream");
      System.out.println(dpuStream.toString());
    }
  }
}
