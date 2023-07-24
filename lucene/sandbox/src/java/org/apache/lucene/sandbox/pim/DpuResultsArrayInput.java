package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.ByteArrayDataInput;

import java.io.IOException;

/**
 * Read the DPU results using a ByteArrayDataInput (used for the software simulator)
 */
public class DpuResultsArrayInput extends DpuResultsReader {

    private ByteArrayDataInput in;

    DpuResultsArrayInput(ByteArrayDataInput in) {
        this.in = in;
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readBytes(b, offset, len);
    }

    @Override
    public void skipBytes(long numBytes) throws IOException {
        in.skipBytes(numBytes);
    }

    @Override
    public boolean eof() {
        return in.eof();
    }
}
