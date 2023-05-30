package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * A dummy DataOutput class that just counts how many bytes
 * have been written.
 **/
public class ByteCountDataOutput extends DataOutput {
    private Long byteCount;

    /**
     * Default constructor
     */
    ByteCountDataOutput() {
        this.byteCount = 0L;
    }

    /**
     * Constructor
     * Syntactic sugar for ByteCountDataOutput out; out.writeVInt(val);
     * @param val the int value for which bytes to be written will be counted
     */
    ByteCountDataOutput(int val) {
        this();
        try {
            writeVInt(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructor
     * Syntactic sugar for ByteCountDataOutput out; out.writeVLong(val);
     * @param val the long value for which bytes to be written will be counted
     */
    ByteCountDataOutput(long val) {
        this();
        try {
            writeVLong(val);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
