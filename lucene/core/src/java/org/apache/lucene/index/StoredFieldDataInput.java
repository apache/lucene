package org.apache.lucene.index;

import org.apache.lucene.store.DataInput;

import java.util.Objects;

public class StoredFieldDataInput {

    private final DataInput in;
    private final int length;

    public StoredFieldDataInput(DataInput in, int length) {
        this.in = Objects.requireNonNull(in);
        this.length = length;
    }

    public DataInput getDataInput() {
        return in;
    }

    public int getLength() {
        return length;
    }
}
