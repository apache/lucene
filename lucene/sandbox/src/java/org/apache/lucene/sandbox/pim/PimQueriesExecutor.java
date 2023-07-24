package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuException;

import java.io.IOException;
import java.util.List;

interface PimQueriesExecutor {

    void setPimIndex(PimIndexInfo pimIndexInfo) throws IOException, DpuException;

    void executeQueries(ByteBufferBoundedQueue.ByteBuffers queryBatch, PimSystemManager.ResultReceiver resultReceiver)
            throws IOException, DpuException;

    void executeQueries(List<PimSystemManager2.QueryBuffer> queryBuffers)
            throws IOException, DpuException;

    default void dumpDpuStream() {}
}
