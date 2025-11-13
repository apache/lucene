package org.apache.lucene.index;

import java.util.concurrent.Future;

/**
 * Package-private wrapper that *is* the Runnable for a single merge.
 * (Prototype: minimal fields; will be extended later for metrics / priority.)
 */
final class MergeTaskWrapper implements Runnable {
    
    final SharedMergeScheduler owner;
    final long mergeSizeBytes;       // size hint (may be used later for prioritization)
    Runnable delegate;         // calls mergeSource.merge(...)
    volatile boolean started = false;
    volatile boolean finished = false;
    volatile boolean aborted = false;
    volatile Future<?> future;       // will be set after submission (for possible cancellation later)

    MergeTaskWrapper(SharedMergeScheduler owner, Runnable delegate, long mergeSizeBytes) {
        this.owner = owner;
        this.mergeSizeBytes = mergeSizeBytes;
        this.delegate = delegate;
    }

    public void setRunnable(Runnable delegate) {
        this.delegate = delegate;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }


    @Override
    public void run() {
        started = true;
        try {
            delegate.run();
        } finally {
            finished = true;
            owner.onTaskFinished(this);
        }
    }

    /** Attempt to cancel if still queued (not started). */
    boolean cancelIfNotStarted() {
        Future<?> f = future;
        return !started && f != null && f.cancel(false);
    }

    public void markAborted() {
        this.aborted = true;
    }

    public boolean isAborted() {
        return aborted;
    }
}

