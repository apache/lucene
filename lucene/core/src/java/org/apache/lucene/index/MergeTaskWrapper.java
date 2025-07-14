package org.apache.lucene.index;

public class MergeTaskWrapper {
    private final Runnable mergeTask;
    private final Object sourceWriter; // Can be IndexWriter or its ID
    private final long mergeSize;      // For priority-based scheduling

    public MergeTaskWrapper(Runnable mergeTask, Object sourceWriter, long mergeSize) {
        this.mergeTask = mergeTask;
        this.sourceWriter = sourceWriter;
        this.mergeSize = mergeSize;
    }

    public Runnable getMergeTask() {
        return mergeTask;
    }

    public Object getSourceWriter() {
        return sourceWriter;
    }

    public long getMergeSize() {
        return mergeSize;
    }
}