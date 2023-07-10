package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.sandbox.pim.DpuSystemExecutor.QUERY_BATCH_BUFFER_CAPACITY;

/**
 * PimSystemManager implementation 2
 * TODO currently this uses a software model to answer queries, not the PIM HW.
 */
public class PimSystemManager2 implements PimSystemManager {

    private static class SingletonHolder {
        static final PimSystemManager2 INSTANCE = new PimSystemManager2();
    }

    private static final boolean USE_SOFTWARE_MODEL = true;

    // TODO: Should there be a queue per query type, with a different max number of queries?
    private static final int MAX_NUM_QUERIES = 128;

    private final ThreadLocal<QueryBuffer> threadQueryBuffer = ThreadLocal.withInitial(QueryBuffer::new);

    private final PimQueriesExecutor queriesExecutor;
    private final BlockingQueue<QueryBuffer> queryQueue;
    private final QueryRunner queryRunner;
    private volatile boolean indexLoaded;
    private PimIndexInfo pimIndexInfo;

    private PimSystemManager2() {
        if (USE_SOFTWARE_MODEL) {
            queriesExecutor = new DpuSystemSimulator();
        } else {
            try {
                queriesExecutor = new DpuSystemExecutor();
            } catch (DpuException e) {
                throw new RuntimeException(e);
            }
        }
        queryQueue = new ArrayBlockingQueue<>(MAX_NUM_QUERIES);
        queryRunner = new QueryRunner();
        pimIndexInfo = null;
        Thread t = new Thread(queryRunner, getClass().getSimpleName() + "-" + queryRunner.getClass().getSimpleName());
        t.setDaemon(true);
        t.start();
    }

    /**
     * Returns the singleton.
     */
    public static PimSystemManager2 get() {
        return SingletonHolder.INSTANCE;
    }

    @Override
    public boolean isReady(LeafReaderContext context) {
        return indexLoaded;
    }

    @Override
    public boolean isQuerySupported(Query query) {
        return query instanceof PimQuery;
    }

    @Override
    public boolean loadPimIndex(Directory pimDirectory) throws IOException {
        if (!indexLoaded) {
            synchronized (this) {
                if (!indexLoaded) {
                    pimIndexInfo = readPimIndexInfo(pimDirectory);
                    queriesExecutor.setPimIndex(pimIndexInfo);
                    indexLoaded = true;
                    return true;
                }
            }
        }
        return false;
    }

    public int getNbDpus() {
        synchronized (PimSystemManager1.class) {
            if(indexLoaded) {
                return pimIndexInfo.getNumDpus();
            }
        }
        return 0;
    }

    private static PimIndexInfo readPimIndexInfo(Directory pimDirectory) throws IOException {
        IndexInput infoInput = pimDirectory.openInput("pimIndexInfo", IOContext.DEFAULT);
        byte[] bytes = new byte[(int) infoInput.length()];
        infoInput.readBytes(bytes, 0, bytes.length);
        infoInput.close();
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
        PimIndexInfo pimIndexInfo;
        try {
            pimIndexInfo = (PimIndexInfo) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        objectInputStream.close();
        pimIndexInfo.setPimDir(pimDirectory);
        return pimIndexInfo;
    }

    @Override
    public boolean unloadPimIndex() {
        if (indexLoaded) {
            synchronized (this) {
                if (indexLoaded) {
                    indexLoaded = false;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void shutDown() {
        queryRunner.stop();
    }

    @Override
    public <QueryType extends Query & PimQuery> List<PimMatch> search(LeafReaderContext context,
                                                                      QueryType query,
                                                                      LeafSimScorer scorer)
            throws PimQueryQueueFullException, InterruptedException, IOException {
        assert isQuerySupported(query);
        QueryBuffer queryBuffer = threadQueryBuffer.get().reset();
        writeQueryToPim(query, context.ord, queryBuffer);
        if (!queryQueue.offer(queryBuffer)) {
            throw new PimQueryQueueFullException();
            //TODO: or return null?
        }
        try {
            DataInput results = queryBuffer.waitForResults();
            return getMatches(query, results, scorer);
        }
        finally {
            queryBuffer.releaseResults();
        }
    }

    private <QueryType extends Query & PimQuery> void writeQueryToPim(QueryType query, int leafIdx, QueryBuffer queryBuffer) {
        try {
            queryBuffer.writeVInt(leafIdx);
            queryBuffer.writeByte(DpuConstants.PIM_PHRASE_QUERY_TYPE); //TODO: this should depend on QueryType.
            query.writeToPim(queryBuffer);
        } catch (IOException e) {
            // Will not be thrown for QueryBuffer.
            throw new RuntimeException(e);
        }
    }

    private <QueryType extends Query & PimQuery> List<PimMatch> getMatches(QueryType query,
                                                                           DataInput input,
                                                                           LeafSimScorer scorer) throws IOException {
        List<PimMatch> matches = null;
        int nbResults = input.readVInt();
        for (int i = 0; i < nbResults; ++i) {
            PimMatch m = query.readResult(input, scorer);
            if (m != null) {
                if (matches == null) {
                    matches = new ArrayList<>(nbResults);
                }
                matches.add(m);
            }
        }
        return matches == null ? Collections.emptyList() : matches;
    }

    static class QueryBuffer extends DataOutput {

        final BlockingQueue<DataInput> resultQueue = new LinkedBlockingQueue<>();
        byte[] bytes = new byte[128];
        int length;
        private Runnable releaseResults;

        QueryBuffer reset() {
            length = 0;
            return this;
        }

        public DataInput getDataInput() {
            return new ByteArrayDataInput(bytes, 0, length);
        }

        DataInput waitForResults() throws InterruptedException {
            return resultQueue.take();
        }

        void addResults(DataInput results, Runnable releaseResults) {

            this.releaseResults = releaseResults;
            resultQueue.add(results);
        }

        void releaseResults() {
            releaseResults.run();
        }

        @Override
        public void writeByte(byte b) {
            if (length >= bytes.length) {
                bytes = ArrayUtil.grow(bytes);
            }
            bytes[length++] = b;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            if (this.length + length > bytes.length) {
                bytes = ArrayUtil.grow(bytes, this.length + length);
            }
            System.arraycopy(b, offset, bytes, this.length, length);
            this.length += length;
        }

        @Override
        public void writeShort(short i) {
            if (length + Short.BYTES > bytes.length) {
                bytes = ArrayUtil.grow(bytes, length + Short.BYTES);
            }
            BitUtil.VH_LE_SHORT.set(bytes, length, i);
            length += Short.BYTES;
        }

        @Override
        public void writeInt(int i) {
            if (length + Short.BYTES > bytes.length) {
                bytes = ArrayUtil.grow(bytes, length + Integer.BYTES);
            }
            BitUtil.VH_LE_INT.set(bytes, length, i);
            length += Integer.BYTES;
        }

        @Override
        public void writeLong(long i) {
            if (length + Short.BYTES > bytes.length) {
                bytes = ArrayUtil.grow(bytes, length + Long.BYTES);
            }
            BitUtil.VH_LE_LONG.set(bytes, length, i);
            length += Long.BYTES;
        }
    }

    /**
     * Checks regularly the query input queue, executes a batch of queries, and pushes the results.
     */
    private class QueryRunner implements Runnable {

        private static final long WAIT_FOR_BATCH_NS = 0;

        volatile boolean stop;

        public void stop() {
            stop = true;
            // Add any QueryBuffer to the queue to make sure it stops waiting if it is empty.
            queryQueue.offer(threadQueryBuffer.get());
        }

        @Override
        public void run() {
            try {
                runInner();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (DpuException e) {
                throw new UncheckedDpuException(e);
            }
        }

        private void runInner() throws InterruptedException, IOException, DpuException {
            List<QueryBuffer> batchQueryBuffers = new ArrayList<>(MAX_NUM_QUERIES);
            QueryBuffer pendingQueryBuffer = null;
            while (!stop) {
                // Drain the QueryBuffers from the queue.
                // Wait for the first QueryBuffer to be available.
                QueryBuffer queryBuffer = pendingQueryBuffer == null ?
                        queryQueue.take() : pendingQueryBuffer;
                if (stop) {
                    break;
                }
                assert queryBuffer.length < QUERY_BATCH_BUFFER_CAPACITY;
                batchQueryBuffers.add(queryBuffer);
                long bufferSize = queryBuffer.length;
                long startTimeNs = System.nanoTime();
                while (bufferSize < QUERY_BATCH_BUFFER_CAPACITY) {
                    // Wait some time to give a chance to accumulate more queries and send
                    // a larger batch to DPUs. This is a throughput oriented strategy.
                    long timeout = Math.max(WAIT_FOR_BATCH_NS - (System.nanoTime() - startTimeNs), 0L);
                    queryBuffer = queryQueue.poll(timeout, TimeUnit.NANOSECONDS);
                    if (queryBuffer == null) {
                        break;
                    }
                    if (bufferSize + queryBuffer.length > QUERY_BATCH_BUFFER_CAPACITY) {
                        pendingQueryBuffer = queryBuffer;
                        break;
                    }
                    batchQueryBuffers.add(queryBuffer);
                    bufferSize += queryBuffer.length;
                }
                assert bufferSize <= QUERY_BATCH_BUFFER_CAPACITY;

                // Send the query batch to the DPUs, launch, get results.
                queriesExecutor.executeQueries(batchQueryBuffers);
                batchQueryBuffers.clear();
            }
        }
    }
}
