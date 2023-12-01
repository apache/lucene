package org.apache.lucene.sandbox.pim;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

public class SGReturnPool {

    private TreeMap<Integer, ArrayList<ByteBuffer>> bufferPool;
    private ReentrantLock poolLock = new ReentrantLock();
    private final int initByteSize;

    public SGReturnPool(int initCapacity, int initByteSize) {

        bufferPool = new TreeMap<>();
        ArrayList<ByteBuffer> list = new ArrayList<>();
        for(int i = 0; i < initCapacity; ++i) {
             list.add(ByteBuffer.allocateDirect(initByteSize));
        }
        bufferPool.put(initByteSize, list);
        this.initByteSize = initByteSize;
    }

    public SGReturn get(int nr_queries, int nr_segments) {
        return get(nr_queries, nr_segments, 0);
    }

    /**
     * Default get, returns buffer of smallest size larger than min size requested
     * @param nr_queries
     * @param nr_segments
     * @param minResultsByteSize
     * @return
     */
    public SGReturn get(int nr_queries, int nr_segments, int minResultsByteSize) {

        ByteBuffer buffer = null;
        boolean allocate = false;
        try {
            poolLock.lock();
            if (bufferPool.isEmpty()) {
                allocate = true;
            } else {
                var target = bufferPool.ceilingEntry(minResultsByteSize);
                if(target == null)
                    allocate = true;
                else {
                    buffer = target.getValue().remove(0);
                    if (target.getValue().isEmpty())
                        bufferPool.remove(target.getKey());
                }
            }
        } finally {
            poolLock.unlock();
        }
        if(allocate) {
            if(minResultsByteSize == 0)
                buffer = ByteBuffer.allocateDirect(nextPowerOf2(initByteSize));
            else
                buffer = ByteBuffer.allocateDirect(nextPowerOf2(minResultsByteSize));
        }

        return new SGReturn(
                buffer,
                ByteBuffer.allocateDirect(nr_queries * Integer.BYTES),
                ByteBuffer.allocateDirect(nr_segments * nr_queries * Integer.BYTES),
                nr_queries * nr_segments,
                this);
    }

    public void release(SGReturn sgReturn) {

        try {
            poolLock.lock();
            int size = sgReturn.byteBuffer.capacity();
            ArrayList<ByteBuffer> list = bufferPool.get(size);
            if(list == null) {
                list = new ArrayList<>();
            }
            list.add(sgReturn.byteBuffer);
            bufferPool.put(size, list);
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

        public void endReading(int cnt) {

            boolean dealloc = false;
            try {
                lock.lock();
                nbReaders-=cnt;
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

        public void endReading() {
            endReading(1);
        }
    };

    static private int nextPowerOf2(int n)
    {
        int p = 1;
        if (n > 0 && (n & (n - 1)) == 0)
            return n;

        while (p < n)
            p <<= 1;

        return p;
    }
}
