package org.apache.lucene.index;

public class MergeTaskWrapper {
    private final Runnable mergeTask;
    private final IndexWriter sourceWriter;
    private final long mergeSize;      // For priority-based scheduling

    public MergeTaskWrapper(Runnable mergeTask, IndexWriter sourceWriter, long mergeSize) {
        this.mergeTask = mergeTask;
        this.sourceWriter = sourceWriter;
        this.mergeSize = mergeSize;
    }

    public Runnable getMergeTask() {
        return mergeTask;
    }

    public IndexWriter getSourceWriter() {
        return sourceWriter;
    }

    public long getMergeSize() {
        return mergeSize;
    }
}