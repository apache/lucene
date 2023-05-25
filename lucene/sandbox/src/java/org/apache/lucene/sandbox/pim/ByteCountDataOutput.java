package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * A dummy DataOutput class that just counts how many bytes
 * have been written.
 **/
public class ByteCountDataOutput extends DataOutput {
    private Long byteCount;

    ByteCountDataOutput() {
        this.byteCount = 0L;
    }

    void reset() {
        this.byteCount = 0L;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        byteCount++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        byteCount += length;
    }

    Long getByteCount() {
        return byteCount;
    }
}
