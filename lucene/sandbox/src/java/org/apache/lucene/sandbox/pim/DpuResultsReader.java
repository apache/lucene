package org.apache.lucene.sandbox.pim;

import org.apache.lucene.store.DataInput;

/**
 * Base abstract class for reading DPU results
 * This is just a DataInput which also defines the eof method
 */
public abstract class DpuResultsReader extends DataInput {
    public abstract boolean eof();
}
