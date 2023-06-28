package org.apache.lucene.sandbox.pim;

class DpuConstants {
    // TODO some of the constants defined here should be common with the DPU code
    // encoding of query types for PIM
    static final byte PIM_PHRASE_QUERY_TYPE = (byte)1;
    static final String dpuQueryBatchVarName = "query_batch";
    static final String dpuResultsBatchVarName = "results_batch";
    static final String dpuResultsIndexVarName = "results_index";
    static final String dpuIndexVarName = "__sys_used_mram_end";
    static final int dpuQueryBatchByteSize = 1 << 18;
    static final int dpuResultsMaxByteSize = 1 << 13;
    static final int nrDpus = 2048;
    static final String dpuProgramPath = "../dpu/pim_index_searcher.dpu";
}
