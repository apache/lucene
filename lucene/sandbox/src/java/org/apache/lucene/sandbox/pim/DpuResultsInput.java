package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BitUtil;

import java.io.IOException;

/**
 * Class to read the results of a query from the DPU results array
 * The purpose of this class is to be able to read the results of a query
 * while abstracting out the fact that the results are scattered across the DPU results array.
 */
public class DpuResultsInput extends DataInput {

    byte[][] dpuQueryResultsAddr;
    byte[][] dpuResults;
    int queryId;
    int currDpuId;
    int currByteIndex;
    int byteIndexEnd;

    DpuResultsInput(byte[][] dpuResults, byte[][] dpuQueryResultsAddr, int queryId) {
        this.dpuResults = dpuResults;
        this.dpuQueryResultsAddr = dpuQueryResultsAddr;
        this.queryId = queryId;
        this.currDpuId = 0;
    }

    private void nextDpu() throws IOException {

        this.currDpuId++;

        if (currDpuId >= dpuResults.length)
            throw new IOException("No more DPU results");

        this.currByteIndex = (int) BitUtil.VH_LE_INT.get(
                dpuQueryResultsAddr[currDpuId], queryId * Integer.BYTES);
        this.byteIndexEnd = (int) BitUtil.VH_LE_INT.get(
                dpuQueryResultsAddr[currDpuId], (queryId + 1) * Integer.BYTES);
    }

    private boolean endOfDpuBuffer() {
        return this.currByteIndex >= this.byteIndexEnd;
    }

    @Override
    public byte readByte() throws IOException {
        while (endOfDpuBuffer())
            nextDpu();
        return dpuResults[currDpuId][currByteIndex++];
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
            if (nbBytesToSkip > 0)
                currByteIndex += nbBytesToSkip;
            nextDpu();
            skipBytes(numBytes - nbBytesToSkip);
        }
    }
}
