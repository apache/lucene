package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuException;
import com.upmem.dpu.DpuSet;
import com.upmem.dpu.DpuSystem;
import com.upmem.dpu.DpuSymbol;
import com.upmem.dpu.DpuProgramInfo;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class DpuSystemExecutor implements PimQueriesExecutor {
    static final int QUERY_BATCH_BUFFER_CAPACITY = 1 << 11;
    static final int INDEX_CACHE_SIZE = 1 << 16;
    static final int MAX_DPU_INDEX_SIZE = 48 << 20;
    static final boolean PARALLEL_INDEX_LOAD = true;
    private final byte[] queryBatchBuffer;
    private final DpuSystem dpuSystem;
    private final DpuProgramInfo dpuProgramInfo;
    private final ByteArrayOutputStream dpuStream;
    private final byte[] dpuQueryOffsetInBatch;
    private final int[] dpuIdOffset;
    private final byte[] dpuQueryTmp = new byte[8];
    private int nbDpusInIndex;
    private final Lock dpuPrintLock = new ReentrantLock();

    DpuSystemExecutor() throws DpuException {
        queryBatchBuffer = new byte[QUERY_BATCH_BUFFER_CAPACITY];
        // allocate DPUs, load the program, allocate space for DPU results
        dpuStream = new ByteArrayOutputStream();
        dpuSystem = DpuSystem.allocate(DpuConstants.nrDpus, "", new PrintStream(dpuStream));
        dpuProgramInfo = dpuSystem.load(DpuConstants.dpuProgramPath);
        dpuQueryOffsetInBatch = new byte[DpuConstants.dpuQueryMaxBatchSize * Integer.BYTES];
        dpuIdOffset = new int[dpuSystem.dpus().size()];
        int cnt = 0;
        for (int i = 0; i < dpuSystem.ranks().size(); ++i) {
            dpuIdOffset[i] = cnt;
            cnt += dpuSystem.ranks().get(i).dpus().size();
        }
        nbDpusInIndex = 0;
    }

    @Override
    public void setPimIndex(PimIndexInfo pimIndexInfo) throws DpuException, IOException {

        if(pimIndexInfo.getNumDpus() > DpuConstants.nrDpus) {
            throw new DpuException("ERROR: index contains to many DPUs "
                    + pimIndexInfo.getNumDpus() + " > " + DpuConstants.nrDpus);
        }
        if(pimIndexInfo.getNumSegments() > 1) {
            throw new DpuException("ERROR: only one segment supported for index\n");
        }

        //TODO Debug further parallel load. It crashes in the jni layer of the DPU load API
        if(PARALLEL_INDEX_LOAD) {

          DpuSymbol indexSymbol = dpuProgramInfo.get(DpuConstants.dpuIndexVarName);

          // parallel transfer of the index on each DPU rank
          // loop over each DPU of the rank, read INDEX_CACHE_SIZE bytes
          // Then transfer to the DPUs of the rank
          // loop until all DPUs have received all their index
          dpuSystem.async().call((DpuSet set, int rankId) -> {

            if(dpuIdOffset[rankId] >= pimIndexInfo.getNumDpus())
              return;

            try {
              ByteBuffer[] indexBuffers = new ByteBuffer[set.dpus().size()];
              byte[][] indexBuffersArrays = new byte[set.dpus().size()][INDEX_CACHE_SIZE];
              for(int i = 0; i < indexBuffers.length; ++i) {
                indexBuffers[i] = ByteBuffer.allocateDirect(INDEX_CACHE_SIZE);
                indexBuffers[i].order(ByteOrder.LITTLE_ENDIAN);
              }
              long[] dpuIndexPos =  new long[set.dpus().size()];
              int cnt = 0;
              IndexInput in = pimIndexInfo.getFileInput(0);
              while(true) {
                boolean dpuActive = false;
                int readSizeSet = 0;
                for(int i = 0; i < set.dpus().size(); ++i) {
                  if(dpuIdOffset[rankId] + i >= pimIndexInfo.getNumDpus()) {
                    indexBuffers[i] = null;
                    continue;
                  }
                  long indexSize = pimIndexInfo.seekToDpu(in, dpuIdOffset[rankId] + i);
                  if(indexSize > MAX_DPU_INDEX_SIZE)
                    throw new DpuException("ERROR: index of DPU" + i + " is too large, size=" + indexSize);
                  int readSize = readIndexBufferForDpu(in, dpuIndexPos[i], indexSize, indexBuffersArrays[i]);
                  if(readSize != 0) {
                    dpuActive = true;
                    dpuIndexPos[i] += readSize;
                    indexBuffers[i].clear();
                    indexBuffers[i].put(indexBuffersArrays[i]);
                    if(readSize > readSizeSet)
                        readSizeSet = readSize;
                  }
                }
                if(!dpuActive) break;

                DpuSymbol sym = indexSymbol.getSymbolWithOffset(cnt * INDEX_CACHE_SIZE);
                set.copy(sym, indexBuffers, ((readSizeSet + 7) >> 3) << 3);
                cnt++;
              }
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
        for(int i = 0; i < pimIndexInfo.getNumDpus(); ++i) {

          if(!PARALLEL_INDEX_LOAD) {
            IndexInput in = pimIndexInfo.getFileInput(0);
            long dpuIndexSize = pimIndexInfo.seekToDpu(in, i);
            byte[] data = new byte[((Math.toIntExact(dpuIndexSize) + 7) >> 3) << 3];                                                                                
            in.readBytes(data, 0, Math.toIntExact(dpuIndexSize));
            //TODO alignment on 8 bytes ?
            dpuSystem.dpus().get(i).copy(DpuConstants.dpuIndexVarName, data);
          }

          indexLoaded[i] = b.array();
        }
        dpuSystem.copy(DpuConstants.dpuIndexLoadedVarName, indexLoaded);

        nbDpusInIndex = pimIndexInfo.getNumDpus();
    }

    private int readIndexBufferForDpu(IndexInput in, long dpuIndexPos, long indexSize,
                                      byte[] buffer) throws IOException {

        if(buffer == null)
            return 0;
        if(dpuIndexPos >= indexSize)
            return 0;
        int readSize = INDEX_CACHE_SIZE;
        if(readSize > (indexSize - dpuIndexPos)) {
            readSize = Math.toIntExact(indexSize - dpuIndexPos);
        }
        in.skipBytes(dpuIndexPos);
        in.readBytes(buffer, 0, readSize);
        return readSize;
    }

    public void executeQueries(ByteBufferBoundedQueue.ByteBuffers queryBatch, PimSystemManager.ResultReceiver resultReceiver)
            throws DpuException {

        // 1) send queries to PIM
        sendQueriesToPIM(queryBatch);

        // 2) launch DPUs (program should be loaded on PimSystemManager Index load (only once)
        System.out.println(">> Launching DPUs");
        dpuSystem.async().exec(null);

        if (DpuConstants.DEBUG_DPU) {
            dpuSystem.async().call((s, i) -> {
                try {
                    dpuPrintLock.lock();
                    s.log();
                }
                finally {
                    dpuPrintLock.unlock();
                }
            });
        }

        // 3) results transfer from DPUs to CPU
        // first get the meta-data (index of query results in results array for each DPU)
        // This meta-data has one integer per query in the batch
        ByteBuffer[] dpuQueryResultsAddr = new ByteBuffer[dpuSystem.dpus().size()];
        for(int i = 0; i < dpuQueryResultsAddr.length; ++i) {
            dpuQueryResultsAddr[i] = ByteBuffer.allocateDirect(AlignTo8(queryBatch.getNbElems() * Integer.BYTES));
            dpuQueryResultsAddr[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        dpuSystem.async().copy(dpuQueryResultsAddr, DpuConstants.dpuResultsIndexVarName,
                AlignTo8(queryBatch.getNbElems() * Integer.BYTES));

        // then transfer the results
        // use a callback to transfer a minimal number of results per rank
        assert queryBatch.getNbElems() != 0;
        final int lastQueryIndex = (queryBatch.getNbElems() - 1) * Integer.BYTES;
        ByteBuffer[] dpuResults = new ByteBuffer[dpuSystem.dpus().size()];
        ByteBuffer[][] dpuResultsPerRank = new ByteBuffer[dpuSystem.ranks().size()][];
        dpuSystem.async().call(
                (DpuSet set, int rankId) -> {
                    // find the max byte size of results for DPUs in this rank
                    int resultsSize = 0;
                    for (int i = 0; i < set.dpus().size(); ++i) {
                        int dpuResultsSize = dpuQueryResultsAddr[dpuIdOffset[rankId] + i].getInt(lastQueryIndex);
                        if (dpuResultsSize > resultsSize)
                            resultsSize = dpuResultsSize;
                    }
                    assert resultsSize >= 0;
                    if(resultsSize == 0)
                      return;

                    // allocate the memory to transfer results
                    dpuResultsPerRank[rankId] = new ByteBuffer[set.dpus().size()];
                    for (int i = 0; i < set.dpus().size(); ++i) {
                        dpuResults[dpuIdOffset[rankId] + i] =
                                ByteBuffer.allocateDirect(resultsSize * Integer.BYTES * 2);
                        dpuResults[dpuIdOffset[rankId] + i].order(ByteOrder.LITTLE_ENDIAN);
                        dpuResultsPerRank[rankId][i] = dpuResults[dpuIdOffset[rankId] + i];
                    }

                    // perform the transfer for this rank
                    set.copy(dpuResultsPerRank[rankId], DpuConstants.dpuResultsBatchVarName,
                        resultsSize * Integer.BYTES * 2);
                }
        );

        // 4) barrier to wait for all transfers to be finished
        dpuSystem.async().sync();

        // 5) Update the results map for the client threads to read their results
        resultReceiver.startResultBatch();
        try {
            for (int q = 0; q < queryBatch.getNbElems(); ++q) {
                resultReceiver.addResults(queryBatch.getUniqueIdOf(q),
                        new DpuResultsInput(nbDpusInIndex, dpuResults, dpuQueryResultsAddr, q));
            }
        } finally {
            resultReceiver.endResultBatch();
        }

    }

    /**
     * Broadcast an integer to the DpuSystem
     * @param varName the variable name in DPU code
     * @param val the integer value to transfer
     * @throws DpuException if there is an issue with the copy
     */
    private void copyIntToDpus(String varName, int val) throws DpuException {

        //TODO alignment on 8 bytes
        ByteBuffer b = ByteBuffer.allocate(4);
        b.order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(val);
        dpuSystem.async().copy(varName, b.array());
    }

    /**
     * Align the input integer to the next power of 8
     * @param v the integer to align
     * @return the smallest value multiple of 8 and >= v
     */
    static private int AlignTo8(int v) {
        return ((v + 7) >> 3) << 3;
    }

    private void sendQueriesToPIM(ByteBufferBoundedQueue.ByteBuffers queryBatch) throws DpuException {

        // if the query is too big for the limit on DPU, throw an exception
        // The query would have to be handled by the CPU
        if (queryBatch.getSize() > DpuConstants.dpuQueryBatchByteSize)
            throw new DpuException("Query too big: size=" + queryBatch.getSize() + " limit=" + DpuConstants.dpuQueryBatchByteSize);

        // prepare the array with query offsets in the query batch array
        int length = 0;
        ByteArrayDataOutput out = new ByteArrayDataOutput(dpuQueryOffsetInBatch);
        for(int i = 0; i < queryBatch.getNbElems(); ++i) {
            out.writeInt(length);
            length += queryBatch.getSizeOf(i);
        }

        // there is a special case when the byte buffer slice spans ends and beginning of the byte buffer
        if (queryBatch.isSplitted()) {
            int firstSliceNbElems = queryBatch.getBuffer().length - queryBatch.getStartIndex();
            int secondSliceNbElems = queryBatch.getSize() - firstSliceNbElems;
            dpuSystem.async().copy(DpuConstants.dpuQueryBatchVarName, queryBatch.getBuffer(), queryBatch.getStartIndex(),
                    AlignTo8(firstSliceNbElems), 0);
            int firstDisalign = AlignTo8(firstSliceNbElems) - firstSliceNbElems;
            dpuSystem.async().copy(DpuConstants.dpuQueryBatchVarName, queryBatch.getBuffer(),
                    firstDisalign,
                    AlignTo8(secondSliceNbElems), AlignTo8(firstSliceNbElems));
            if(firstDisalign != 0) {
                // here we need to handle the alignment issue by sending the 8 bytes in between
                send8bytesSplitCase(queryBatch);
            }
        } else {
            dpuSystem.async().copy(DpuConstants.dpuQueryBatchVarName, queryBatch.getBuffer(), queryBatch.getStartIndex(),
                    AlignTo8(queryBatch.getSize()), 0);
        }
        dpuSystem.async().copy(DpuConstants.dpuQueryOffsetInBatchVarName, dpuQueryOffsetInBatch,
                0, AlignTo8(queryBatch.getNbElems() * Integer.BYTES), 0);
        copyIntToDpus(DpuConstants.dpuNbQueryInBatchVarName, queryBatch.getNbElems());
        copyIntToDpus(DpuConstants.dpuNbByteInBatchVarName, queryBatch.getSize());
    }

    private void send8bytesSplitCase(ByteBufferBoundedQueue.ByteBuffers queryBatch) throws DpuException {
        int firstSliceNbElems = queryBatch.getBuffer().length - queryBatch.getStartIndex();
        int firstDisalign = AlignTo8(firstSliceNbElems) - firstSliceNbElems;
        int nbFirst = 8 - firstDisalign;
        int i = 0;
        for(; i < nbFirst;  ++i) {
            dpuQueryTmp[i] = queryBatch.getBuffer()[queryBatch.getBuffer().length - nbFirst + i];
        }
        for(; i < 8; ++i) {
            dpuQueryTmp[i] = queryBatch.getBuffer()[i - nbFirst];
        }
        dpuSystem.async().copy(DpuConstants.dpuQueryBatchVarName, dpuQueryTmp,
                0, 8, firstSliceNbElems & (Integer.MAX_VALUE & ~7));
    }

    @Override
    public void executeQueries(List<PimSystemManager2.QueryBuffer> queryBuffers)
            throws DpuException {

        // 1) send queries to PIM
        sendQueriesToPIM(queryBuffers);

        System.out.println(">> Launching DPUs");
        // 2) launch DPUs (program should be loaded on PimSystemManager Index load (only once)
        dpuSystem.async().exec(null);

        if (DpuConstants.DEBUG_DPU) {
            dpuSystem.async().call((s, i) -> {
                try {
                    dpuPrintLock.lock();
                    s.log();
                }
                finally {
                    dpuPrintLock.unlock();
                }
            });
        }

        // 3) results transfer from DPUs to CPU
        // first get the meta-data (index of query results in results array for each DPU)
        // This meta-data has one integer per query in the batch
        ByteBuffer[] dpuQueryResultsAddr = new ByteBuffer[dpuSystem.dpus().size()];
        for(int i = 0; i < dpuQueryResultsAddr.length; ++i) {
            dpuQueryResultsAddr[i] = ByteBuffer.allocateDirect(AlignTo8(queryBuffers.size() * Integer.BYTES));
            dpuQueryResultsAddr[i].order(ByteOrder.LITTLE_ENDIAN);
        }
        dpuSystem.async().copy(dpuQueryResultsAddr, DpuConstants.dpuResultsIndexVarName,
                AlignTo8(queryBuffers.size() * Integer.BYTES));

        // then transfer the results
        // use a callback to transfer a minimal number of results per rank
        assert queryBuffers.size() != 0;
        final int lastQueryIndex = (queryBuffers.size() - 1) * Integer.BYTES;
        ByteBuffer[] dpuResults = new ByteBuffer[dpuSystem.dpus().size()];
        ByteBuffer[][] dpuResultsPerRank = new ByteBuffer[dpuSystem.ranks().size()][];
        dpuSystem.async().call(
                (DpuSet set, int rankId) -> {
                    // find the max byte size of results for DPUs in this rank
                    int resultsSize = 0;
                    for (int i = 0; i < set.dpus().size(); ++i) {
                        int dpuResultsSize = dpuQueryResultsAddr[dpuIdOffset[rankId] + i].getInt(lastQueryIndex);
                        if (dpuResultsSize > resultsSize)
                            resultsSize = dpuResultsSize;
                    }
                    assert resultsSize >= 0;
                    if(resultsSize == 0)
                      return;

                    // allocate the memory to transfer results
                    dpuResultsPerRank[rankId] = new ByteBuffer[set.dpus().size()];
                    for (int i = 0; i < set.dpus().size(); ++i) {
                        dpuResults[dpuIdOffset[rankId] + i] =
                                ByteBuffer.allocateDirect(resultsSize * Integer.BYTES * 2);
                        dpuResults[dpuIdOffset[rankId] + i].order(ByteOrder.LITTLE_ENDIAN);
                        dpuResultsPerRank[rankId][i] = dpuResults[dpuIdOffset[rankId] + i];
                    }

                    // perform the transfer for this rank
                    set.copy(dpuResultsPerRank[rankId], DpuConstants.dpuResultsBatchVarName,
                        resultsSize * Integer.BYTES * 2);
                }
        );

        // 4) barrier to wait for all transfers to be finished
        dpuSystem.async().sync();

        // 5) Update the results map for the client threads to read their results
        for (int q = 0, size = queryBuffers.size(); q < size; ++q) {
            queryBuffers.get(q).addResults(new DpuResultsInput(nbDpusInIndex, dpuResults, dpuQueryResultsAddr, q));
        }
    }

    private void sendQueriesToPIM(List<PimSystemManager2.QueryBuffer> queryBuffers) throws DpuException {
        //TODO: Here we could sort queryBuffers to group the queries by type before
        // sending them. Just need to make QueryBuffer implement Comparable and compare
        // on the query type.
        //queryBuffers.sort(Comparator.naturalOrder());

        //TODO: use Scatter Gather approach to send query buffers separately?
        int batchLength = 0;
        ByteArrayDataOutput out = new ByteArrayDataOutput(dpuQueryOffsetInBatch);
        for (PimSystemManager2.QueryBuffer queryBuffer : queryBuffers) {
            System.arraycopy(queryBuffer.bytes, 0, queryBatchBuffer, batchLength, queryBuffer.length);
            out.writeInt(batchLength);
            batchLength += queryBuffer.length;
        }
        dpuSystem.async().copy(DpuConstants.dpuQueryBatchVarName, queryBatchBuffer,
                0, AlignTo8(batchLength), 0);
        dpuSystem.async().copy(DpuConstants.dpuQueryOffsetInBatchVarName, dpuQueryOffsetInBatch,
                0, AlignTo8(queryBuffers.size() * Integer.BYTES), 0);
        copyIntToDpus(DpuConstants.dpuNbQueryInBatchVarName, queryBuffers.size());
        copyIntToDpus(DpuConstants.dpuNbByteInBatchVarName, batchLength);
    }

    public void dumpDpuStream() {
        System.out.println("Printing DPU stream");
        System.out.println(dpuStream.toString());
    }
}
