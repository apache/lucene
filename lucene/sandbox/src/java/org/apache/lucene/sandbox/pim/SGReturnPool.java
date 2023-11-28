package org.apache.lucene.sandbox.pim;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class SGReturnPool {

    private ArrayList<ByteBuffer> bufferPool;
    private ReentrantLock poolLock = new ReentrantLock();

    public SGReturnPool(int initCapacity) {

        bufferPool = new ArrayList<>();
        for(int i = 0; i < initCapacity; ++i) {
            bufferPool.add(ByteBuffer.allocateDirect(DpuConstants.dpuResultsMaxByteSize));
        }
    }

    public SGReturn get(int nr_queries, int nr_segments) {

        ByteBuffer buffer = null;
        boolean allocate = false;
        try {
            poolLock.lock();
            if (bufferPool.isEmpty()) {
                allocate = true;
            } else {
                buffer = bufferPool.remove(0);
            }
        } finally {
            poolLock.unlock();
        }
        if(allocate)
            buffer = ByteBuffer.allocateDirect(DpuConstants.dpuResultsMaxByteSize);

        return new SGReturn(
                buffer,
                ByteBuffer.allocateDirect(nr_queries * Integer.BYTES),
                ByteBuffer.allocateDirect(nr_segments * nr_queries * Integer.BYTES),
                nr_queries,
                this);
    }

    public void release(SGReturn sgReturn) {

        try {
            poolLock.lock();
            bufferPool.add(sgReturn.byteBuffer);
        } finally {
            poolLock.unlock();
        }
    }

    public class SGReturn {
        public final ByteBuffer byteBuffer;
        public final ByteBuffer queriesIndices;
        public final ByteBuffer segmentsIndices;

        private int nbReaders;
        private ReentrantLock lock = new ReentrantLock();
        private final SGReturnPool myPool;

        private SGReturn(ByteBuffer byteBuffer, ByteBuffer queriesIndices, ByteBuffer segmentsIndices,
                        int nbReaders, SGReturnPool myPool) {
            this.byteBuffer = byteBuffer;
            this.queriesIndices = queriesIndices;
            this.segmentsIndices = segmentsIndices;
            this.nbReaders = nbReaders;
            this.myPool = myPool;
        }

        public void endReading() {

            boolean dealloc = false;
            try {
                lock.lock();
                nbReaders--;
                if(nbReaders == 0) {
                    dealloc = true;
                }
            } finally {
                lock.unlock();
            }
            if(dealloc) {
                myPool.release(this);
            }
        }
    };
}
