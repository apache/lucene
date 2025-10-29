/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.jvector;

import io.github.jbellis.jvector.disk.IndexWriter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * JVectorRandomAccessWriter is a wrapper around IndexOutput that implements RandomAccessWriter.
 * Note: This is not thread safe!
 */
@Log4j2
public class JVectorIndexWriter implements IndexWriter {
    private final IndexOutput indexOutputDelegate;

    public JVectorIndexWriter(IndexOutput indexOutputDelegate) {
        this.indexOutputDelegate = indexOutputDelegate;
    }

    @Override
    public long position() throws IOException {
        return indexOutputDelegate.getFilePointer();
    }

    @Override
    public void close() throws IOException {
        indexOutputDelegate.close();
    }

    @Override
    public void write(int b) throws IOException {
        indexOutputDelegate.writeByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        indexOutputDelegate.writeBytes(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        indexOutputDelegate.writeBytes(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        indexOutputDelegate.writeByte((byte) (v ? 1 : 0));
    }

    @Override
    public void writeByte(int v) throws IOException {
        indexOutputDelegate.writeByte((byte) v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        indexOutputDelegate.writeShort((short) v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        throw new UnsupportedOperationException("JVectorRandomAccessWriter does not support writing chars");
    }

    @Override
    public void writeInt(int v) throws IOException {
        indexOutputDelegate.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        indexOutputDelegate.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        indexOutputDelegate.writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException("JVectorIndexWriter does not support writing String as bytes");
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException("JVectorIndexWriter does not support writing chars");
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("JVectorIndexWriter does not support writing UTF strings");
    }
}
