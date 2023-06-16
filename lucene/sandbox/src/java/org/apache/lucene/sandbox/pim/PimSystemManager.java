package org.apache.lucene.sandbox.pim;

import com.upmem.dpu.DpuSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayCircularDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.DataInput;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.ObjectInputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.upmem.dpu.DpuSystem;
import com.upmem.dpu.DpuException;

/**
 * PimSystemManager
 * Singleton class used to manage the PIM system and offload Lucene queries to it.
 * TODO currently this uses a software model to answer queries, not the PIM HW.
 */
public final class PimSystemManager {

    private static PimSystemManager instance;

    final int BYTE_BUFFER_QUEUE_LOG2_BYTE_SIZE = 11;
    final int QUERY_BATCH_SIZE = 128;

    private boolean isIndexLoaded;
    private boolean isIndexBeingLoaded;
    private PimIndexInfo pimIndexInfo;
    private ByteBufferBoundedQueue queryBuffer;

    private static final boolean debug = false;
    private static final boolean use_software_model = true;
    // for the moment, the PIM index search is performed on CPU
    // using this class, no PIM HW involved
    private PimIndexSearcher pimSearcher;

    private final Lock queryLock = new ReentrantLock();
    private final Condition queryPushedCond  = queryLock.newCondition();
    private final ReentrantReadWriteLock resultsLock = new ReentrantReadWriteLock();
    private final Lock resultsPushedLock = new ReentrantLock();
    private final Condition resultsPushedCond = resultsPushedLock.newCondition();

    private TreeMap<Integer, DataInput> queryResultsMap;
    private PimManagerThread pimThread;
    private ByteArrayOutputStream dpuStream = new ByteArrayOutputStream();

    private DpuSystem dpuSystem;
    private byte[][] dpuQueryResultsAddr;
    private byte[][] dpuResults;
    private byte[][][] dpuResultsPerRank;
    private int[] dpuIdOffset;
    private final Lock resultBufferLock = new ReentrantLock();
    private final Condition resultBufferCond = resultBufferLock.newCondition();

    private PimSystemManager() {
        isIndexLoaded = false;
        isIndexBeingLoaded = false;
        pimIndexInfo = null;
        pimSearcher = null;
        try {
            queryBuffer = new ByteBufferBoundedQueue(BYTE_BUFFER_QUEUE_LOG2_BYTE_SIZE);
        } catch (ByteBufferBoundedQueue.BufferLog2SizeTooLargeException e) {
            throw new RuntimeException(e);
        }
        queryResultsMap = new TreeMap<>();
        pimThread = new PimManagerThread(queryBuffer);
        pimThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> PimSystemManager.shutDown()));

        if(!use_software_model) {
            // allocate DPUs, load the program, allocate space for DPU results
            try {
                dpuSystem = DpuSystem.allocate(nrDpus, "sgXferEnable=true", new PrintStream(dpuStream));
                dpuSystem.load(dpuProgramPath);
                dpuQueryResultsAddr = new byte[dpuSystem.dpus().size()][dpuQueryBatchByteSize];
                dpuResults = new byte[dpuSystem.dpus().size()][dpuResultsMaxByteSize];
                dpuResultsPerRank = new byte[dpuSystem.ranks().size()][][];
                dpuIdOffset = new int[dpuSystem.dpus().size()];
                int cnt = 0;
                for(int i = 0; i < dpuSystem.ranks().size(); ++i) {
                    dpuResultsPerRank[i] = new byte[dpuSystem.ranks().get(i).dpus().size()][];
                    dpuIdOffset[i] = cnt;
                    for(int j = 0; j < dpuSystem.ranks().get(i).dpus().size(); ++j) {
                        dpuResultsPerRank[i][j] = dpuResults[cnt++];
                    }
                }
            } catch (DpuException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Singleton accessor
     *
     * @return unique PimSystemManager instance
     */
    public static PimSystemManager get() {
        //lazy initialization on first call
        if (instance == null) {
            //synchronized block for thread safety
            synchronized (PimSystemManager.class) {
                if (instance == null) {
                    // if instance is null, initialize
                    instance = new PimSystemManager();
                }
            }
        }
        return instance;
    }

    /**
     * Load the pim index unless one is already loaded
     *
     * @param pimDirectory the directory containing the PIM index
     * @return true if the index was successfully loaded
     */
    public boolean loadPimIndex(Directory pimDirectory) throws IOException {

        if (!isIndexLoaded && !isIndexBeingLoaded) {
            boolean loadSuccess = false;
            //synchronized block for thread safety
            synchronized (PimSystemManager.class) {
                if (!isIndexLoaded && !isIndexBeingLoaded) {
                    getPimInfoFromDir(pimDirectory);
                    isIndexBeingLoaded = true;
                    loadSuccess = true;
                }
            }
            if (loadSuccess) {
                // the calling thread has succeeded loading the PIM Index
                transferPimIndex();
                synchronized (PimSystemManager.class) {
                    isIndexBeingLoaded = false;
                    isIndexLoaded = true;
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Load the pim index and can force load if an index is already loaded
     *
     * @param pimDirectory the directory containing the PIM index
     * @param force when true, unload the currently loaded index to force load the new one
     * @return true if the index was successfully loaded
     */
    public boolean loadPimIndex(Directory pimDirectory, boolean force) throws IOException {

        if(force)
            unloadPimIndex();
        return loadPimIndex(pimDirectory);
    }

    /**
     * Unload the PIM index if currently loaded
     *
     * @return true if the index has been unloaded
     */
    public boolean unloadPimIndex() {

        if (isIndexLoaded) {
            //synchronized block for thread safety
            synchronized (PimSystemManager.class) {
                if (isIndexLoaded) {
                    // set the boolean variable to false,
                    // authorizing for a new load that will
                    // overwrite current PIM Index
                    isIndexLoaded = false;
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @return true if an index is currently loaded in the PIM system
     */
    public boolean isIndexLoaded() {
        synchronized (PimSystemManager.class) {
            return isIndexLoaded;
        }
    }

    /**
     * @return number of dpus used by the index if an index is currently loaded
     * in the PIM system and zero otherwise
     */
    public int getNbDpus() {
        synchronized (PimSystemManager.class) {
            if(isIndexLoaded) {
                return pimIndexInfo.getNumDpus();
            }
        }
        return 0;
    }

    /**
     * NOT IMPLEMENTED
     * Tells whether the current PIM index loaded is
     * the right one to answer queries for the LeafReaderContext object
     * TODO implement this, returns always true
     *
     * @param context
     * @return
     */
    public boolean isReady(LeafReaderContext context) {
        //TODO check if the PIM system has the correct index loaded
        //need to find a way to correlate this context with the PimIndexInfo
        return isIndexLoaded;
    }

    /**
     * Information on which query types are supported by the PIM system
     *
     * @param query the input query
     * @return true if the query is supported by the PIM system
     */
    public boolean isQuerySupported(Query query) {
        // for the moment support only PimPhraseQuery
        if (query instanceof PimPhraseQuery)
            return true;
        else
            return false;
    }

    /**
     * Kill the thread created by this singleton to
     * handle the interface with the PIM HW
     */
    public static void shutDown() {

        if (instance != null) {
            //synchronized block for thread safety
            synchronized (PimSystemManager.class) {
                if (instance != null) {
                    if(instance.pimThread != null)
                        instance.pimThread.interrupt();
                }
            }
        }
    }

    /** Custom Exception to be thrown when the PimSystemManager query queue is full */
    public static final class PimQueryQueueFullException extends Exception {

        public PimQueryQueueFullException() {
            super(
                    "PimSystemManager query queue is full");
        }
    }

    /**
     * Queries are sent in batches to the PIM system
     * This call will push the query into a submit queue, and wait
     * for the results to be available
     * <p>
     * It is the responsibility of the caller to make sure that an
     * index was previously successfully loaded with a call to loadPimIndex
     * returning true, and that no unloadPimIndex method was called
     *
     * @param context the leafReaderContext to search
     * @param query   the query to execute
     * @param scorer  the scorer to use to score the results
     * @return the list of matches
     */
    public <QueryType extends Query & PimQuery>  List<PimMatch> search(LeafReaderContext context,
                                              QueryType query, LeafSimScorer scorer) throws PimQueryQueueFullException {

        if (!isQuerySupported(query))
            return null;

        try {
            // 1) push query in a queue
            // first request a buffer of the correct size
            ByteCountDataOutput countOutput = new ByteCountDataOutput();
            writeQueryToPim(countOutput, query, context.ord);
            final int byteSize = Math.toIntExact(countOutput.getByteCount());
            var queryOutput = queryBuffer.add(byteSize);
            // unique id identifying the query in the queue
            int id = queryOutput.getUniqueId();

            // write leaf id, query type, then write query
            writeQueryToPim(queryOutput, query, context.ord);

            // 2) signal condition variable to wake up thread which handle queries to DPUs
            queryLock.lock();
            try {
                if (debug)
                    System.out.println("Signal query");
                queryPushedCond.signal();
            } finally {
                queryLock.unlock();
            }
            if (debug)
                System.out.println("Waiting for result");

            // 3) wait on condition variable until new results were collected
            // 4) check if the result is present, if not wait again on condition variable
            resultsPushedLock.lock();
            try {
                while (!queryResultsAvailable(id))
                    resultsPushedCond.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                resultsPushedLock.unlock();
            }

            if (debug)
                System.out.println("Reading result");

            // results are available
            return getQueryMatches(query, id, scorer);

        } catch (ByteBufferBoundedQueue.InsufficientSpaceInQueueException e) {
            // not enough capacity in queue
            // throw an exception that the user should catch, and
            // issue the query later or use the CPU instead of PIM system
            throw new PimQueryQueueFullException();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Check if the results of a query are ready (i.e., returned by the PIM system)
     * @param id the id of the query
     * @return true if the results are present
     */
    private boolean queryResultsAvailable(int id) {

        resultsLock.readLock().lock();
        try {
            return queryResultsMap.get(id) != null;
        }
        finally {
            resultsLock.readLock().unlock();
        }
    }

    /**
     * Returns the results of a query by interpreting data returned by the PIM system and
     * creating a list of PimMatch objects.
     * @param q the query
     * @param id the id of the query (specific to this PimManager)
     * @param scorer the scorer to be used to score results obtained from the PIM system
     * @return the list of matches for this query
     * @param <QueryType> a type that is both a Query and a PimQuery
     * @throws IOException
     */
    private <QueryType extends Query & PimQuery> List<PimMatch> getQueryMatches(
            QueryType q, int id, LeafSimScorer scorer) throws IOException {

        resultsLock.readLock().lock();
        DataInput resultsReader;
        try {
            resultsReader = queryResultsMap.get(id);
        }
        finally {
            resultsLock.readLock().unlock();
        }
        assert resultsReader != null;

        List<PimMatch> matches = getMatches(q, resultsReader, scorer);

        // remove results array from the map
        resultsLock.writeLock().lock();
        try {
            queryResultsMap.remove(id);
        }
        finally {
            resultsLock.writeLock().unlock();
        }
        return matches;
    }

    /**
     * Used by method getQueryMatches
     */
    private <QueryType extends Query & PimQuery> List<PimMatch> getMatches(
            QueryType q, DataInput input, LeafSimScorer scorer) throws IOException {

        List<PimMatch> matches = new ArrayList<>();

        // 1) read number of results
        int nbResults = input.readVInt();

        // 2) loop and call readResult (specialized on query type, return a PimMatch)
        for(int i = 0; i < nbResults; ++i) {
            PimMatch m = q.readResult(input, scorer);
            if(m != null)
                matches.add(m);
        }
        return matches;
    }

    /**
     * Write the query as a byte array in the PIM system format
     * @param output the output where to write the query
     * @param query the query to be written
     * @param leafIdx the leaf id
     * @param <QueryType> a type that is both a Query and a PimQuery
     * @throws IOException
     */
    private <QueryType extends Query & PimQuery>
    void writeQueryToPim(DataOutput output, QueryType query, int leafIdx) throws IOException {

        output.writeVInt(leafIdx);
        output.writeByte(PIM_PHRASE_QUERY_TYPE);
        query.writeToPim(output);
    }

    /**
     * Copy the PIM index to the PIM system
     */
    private void transferPimIndex() {
        // TODO load index to PIM system
        // create a new PimIndexSearcher for this index
        // TODO copy the PIM index files here to mimic transfer
        // to DPU and be safe searching it while the index is overwritten
        // Lock the pim index to avoid it to be overwritten ?
        if(use_software_model)
            pimSearcher = new PimIndexSearcher(pimIndexInfo);
        else {
            // TODO copy the PIM index in each DPU
        }
    }

    /**
     * Read from the PIM directory the information about the PIM index
     * @param pimDirectory the directory containing the PIM index
     * @throws IOException
     */
    private void getPimInfoFromDir(Directory pimDirectory) throws IOException {

        IndexInput infoInput = pimDirectory.openInput("pimIndexInfo", IOContext.DEFAULT);
        byte[] bytes = new byte[(int) infoInput.length()];
        infoInput.readBytes(bytes, 0, bytes.length);
        infoInput.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream
                = new ObjectInputStream(bais);
        try {
            pimIndexInfo = (PimIndexInfo) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        objectInputStream.close();
        pimIndexInfo.setPimDir(pimDirectory);
    }

    /**
     * PIM Manager thread
     * This thread is responsible for regularly checking the query input queue, executing a batch of queries,
     * and pushing the results in a TreeMap.
     */
    private class PimManagerThread implements Runnable {

        private Thread worker;
        private AtomicBoolean running = new AtomicBoolean(false);
        ByteBufferBoundedQueue buffer;
        static final int minNbQuery = 8;
        static final int waitForBatchNanoTime = 0;
        //static final int waitForBatchNanoTime = 10000;

        PimManagerThread(ByteBufferBoundedQueue buffer) {
            this.buffer = buffer;
        }

        public void start() {
            worker = new Thread(this);
            worker.start();
        }

        public void interrupt() {
            running.set(false);
            if(worker != null && worker.isAlive())
                worker.interrupt();
        }

        @Override
        public void run() {

            running.set(true);

            while (running.get()) {

                try {
                    // wait for a query to be pushed
                    ByteBufferBoundedQueue.ByteBuffers slice = buffer.peekMany(QUERY_BATCH_SIZE);
                    while (slice.getNbElems() == 0) {
                        buffer.release();
                        // found no query, need to wait for one
                        // take the lock, verify that still no query and wait on condition variable
                        queryLock.lock();
                        try {
                            slice = buffer.peekMany(QUERY_BATCH_SIZE);
                            if(slice.getNbElems() == 0) {
                                buffer.release();
                                if(debug)
                                    System.out.println("Waiting for query");
                                queryPushedCond.await();
                            }
                        } finally {
                            queryLock.unlock();
                        }
                        if(slice.getNbElems() == 0) {
                            buffer.release();
                            slice = buffer.peekMany(QUERY_BATCH_SIZE);
                        }
                    }

                    // if the number of queries is under a minimum threshold, wait a bit more
                    // to give a second chance to accumulate more queries and send a larger batch to DPUs
                    // this is a throughput oriented strategy
                    if (waitForBatchNanoTime != 0 && slice.getNbElems() < minNbQuery) {
                        buffer.release();
                        if(debug)
                            System.out.println("Handling query but waiting a bit more");
                        Thread.sleep(0, waitForBatchNanoTime);
                        slice = buffer.peekMany(QUERY_BATCH_SIZE);
                    }
                    if(debug)
                        System.out.println("Handling query");

                    // TODO send the query batch to the DPUs, launch, get results
                    // for now use software model instead
                    if (use_software_model) {
                        ByteArrayCircularDataInput input = new ByteArrayCircularDataInput(slice.getBuffer(),
                                slice.getStartIndex(), slice.getSize());

                        for (int q = 0; q < slice.getNbElems(); ++q) {

                            // rebuild a query object for PimIndexSearcher
                            int segment = input.readVInt();
                            byte type = input.readByte();
                            assert type == PIM_PHRASE_QUERY_TYPE;
                            int fieldSz = input.readVInt();
                            byte[] fieldBytes = new byte[fieldSz];
                            input.readBytes(fieldBytes, 0, fieldSz);
                            BytesRef field = new BytesRef(fieldBytes);
                            PimPhraseQuery.Builder builder = new PimPhraseQuery.Builder();
                            int nbTerms = input.readVInt();
                            for (int i = 0; i < nbTerms; ++i) {
                                int termByteSize = input.readVInt();
                                byte[] termBytes = new byte[termByteSize];
                                input.readBytes(termBytes, 0, termByteSize);
                                builder.add(new Term(field.utf8ToString(), new BytesRef(termBytes)));
                            }

                            // use PimIndexSearcher to handle the query (software model)
                            List<PimMatch> matches = pimSearcher.searchPhrase(segment, builder.build());

                            // write the results in the results queue
                            ByteCountDataOutput countOut = new ByteCountDataOutput();
                            countOut.writeVInt(matches.size());
                            for (PimMatch m : matches) {
                                countOut.writeVInt(m.docId);
                                countOut.writeVInt((int) m.score);
                            }
                            byte[] matchesByteArr = new byte[Math.toIntExact(countOut.getByteCount())];
                            ByteArrayDataOutput byteOut = new ByteArrayDataOutput(matchesByteArr);
                            byteOut.writeVInt(matches.size());
                            for (PimMatch m : matches) {
                                byteOut.writeVInt(m.docId);
                                byteOut.writeVInt((int) m.score);
                            }

                            resultsLock.writeLock().lock();
                            try {
                                queryResultsMap.put(slice.getUniqueIdOf(q), new ByteArrayDataInput(matchesByteArr));
                            } finally {
                                resultsLock.writeLock().unlock();
                            }
                        }
                    }
                    else {
                        // TODO send the query batch to the DPUs, launch, get results
                        executeQueriesOnPIM(slice);
                    }

                    resultsPushedLock.lock();
                    try {
                        // signal client threads that some results are available
                        if(debug)
                            System.out.println("Signal results, nb res:" + queryResultsMap.size());
                        resultsPushedCond.signalAll();
                    } finally {
                        resultsPushedLock.unlock();
                    }

                    // remove the slice handled
                    buffer.remove();
                } catch (InterruptedException e) {
                    // interrupted, return
                    Thread.currentThread().interrupt();
                    return;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ByteBufferBoundedQueue.ParallelPeekException e) {
                    throw new RuntimeException(e);
                } catch (DpuException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void executeQueriesOnPIM(ByteBufferBoundedQueue.ByteBuffers queryBatch) throws DpuException {

        // 1) send queries to PIM
        sendQueriesToPIM(queryBatch);

        // 2) launch DPUs (program should be loaded on PimSystemManager Index load (only once)
        dpuSystem.async().exec();

        // 3) results transfer from DPUs to CPU
        // first get the meta-data (index of query results in results array for each DPU)
        // This meta-data has one integer per query in the batch
        dpuSystem.async().copy(dpuQueryResultsAddr, dpuResultsIndexVarName,
                queryBatch.getNbElems() * Integer.BYTES);

        // then transfer the results
        // use a callback to transfer a minimal number of results per rank
        final int batchSize = queryBatch.getNbElems() * Integer.BYTES;
        dpuSystem.async().call(
                (DpuSet set, int rankId) -> {
                    // find the max byte size of results for DPUs in this rank
                    int resultsSize = 0;
                    for(int i = 0; i < set.dpus().size(); ++i) {
                        int dpuResultsSize = (int) BitUtil.VH_LE_INT.get(
                                dpuQueryResultsAddr[dpuIdOffset[rankId] + i], batchSize);
                        if(dpuResultsSize > resultsSize)
                            resultsSize = dpuResultsSize;
                    }
                    // perform the transfer for this rank
                    set.copy(dpuResultsPerRank[rankId], dpuResultsBatchVarName, resultsSize);
                }
        );

        // 4) barrier to wait for all transfers to be finished
        dpuSystem.async().sync();

        // 5) Update the results map for the client threads to read their results
        resultsLock.writeLock().lock();
        try {
            for(int q = 0; q < queryBatch.getNbElems(); ++q) {
                queryResultsMap.put(queryBatch.getUniqueIdOf(q),
                        new DpuResultsInput(dpuResults, dpuQueryResultsAddr, q));
            }
        } finally {
            resultsLock.writeLock().unlock();
        }

    }

    private void sendQueriesToPIM(ByteBufferBoundedQueue.ByteBuffers queryBatch) throws DpuException {

        // if the query is too big for the limit on DPU, throw an exception
        // The query would have to be handled by the CPU
        if(queryBatch.getSize() > dpuQueryBatchByteSize)
            throw new DpuException("Query too big: size=" + queryBatch.getSize() + " limit=" +dpuQueryBatchByteSize);

        // there is a special case when the byte buffer slice spans ends and beginning of the byte buffer
        if(queryBatch.isSplitted()) {
            int firstSliceNbElems = queryBatch.getBuffer().length - queryBatch.getStartIndex();
            int secondSliceNbElems = queryBatch.getSize() - firstSliceNbElems;
            dpuSystem.async().copy(dpuQueryBatchVarName, queryBatch.getBuffer(), queryBatch.getStartIndex(),
                    firstSliceNbElems, 0);
            dpuSystem.async().copy(dpuQueryBatchVarName, queryBatch.getBuffer(), 0,
                    secondSliceNbElems, firstSliceNbElems);
        }
        else {
            dpuSystem.async().copy(dpuQueryBatchVarName, queryBatch.getBuffer(), queryBatch.getStartIndex(),
                    queryBatch.getSize(), 0);
        }
    }

    /**
     * Class to read the results of a query from the DPU results array
     * The purpose of this class is to be able to read the results of a query
     * while abstracting out the fact that the results are scattered across the DPU results array.
     */
    public static class DpuResultsInput extends DataInput {

        byte[][] dpuQueryResultsAddr;
        byte[][] dpuResults;
        int queryId;
        int currDpuId;
        int currByteIndex;
        int byteIndexEnd;

        DpuResultsInput(byte[][] dpuResults, byte[][] dpuQueryResultsAddr, int queryId) {
            this.dpuResults = dpuResults;
            this.dpuQueryResultsAddr = dpuQueryResultsAddr;
            this.queryId = queryId;
            this.currDpuId = 0;
        }

        private void nextDpu() throws IOException{

            this.currDpuId++;

            if(currDpuId >= dpuResults.length)
                throw new IOException("No more DPU results");

            this.currByteIndex = (int) BitUtil.VH_LE_INT.get(
                    dpuQueryResultsAddr[currDpuId], queryId * Integer.BYTES);
            this.byteIndexEnd = (int) BitUtil.VH_LE_INT.get(
                    dpuQueryResultsAddr[currDpuId], (queryId + 1) * Integer.BYTES);
        }

        private boolean endOfDpuBuffer() {
            return this.currByteIndex >= this.byteIndexEnd;
        }

        @Override
        public byte readByte() throws IOException {
            while(endOfDpuBuffer())
                nextDpu();
            return dpuResults[currDpuId][currByteIndex++];
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {

            if(len <= (byteIndexEnd - currByteIndex)) {
                System.arraycopy(dpuResults[currDpuId], currByteIndex, b, offset, len);
                currByteIndex += len;
            }
            else {
                int nbBytesToCopy = byteIndexEnd - currByteIndex;
                if(nbBytesToCopy > 0)
                    System.arraycopy(dpuResults[currDpuId], currByteIndex, b, offset, nbBytesToCopy);
                nextDpu();
                readBytes(b, offset + nbBytesToCopy, len - nbBytesToCopy);
            }
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {

            if(numBytes <= (byteIndexEnd - currByteIndex)) {
                currByteIndex += numBytes;
            }
            else {
                int nbBytesToSkip = byteIndexEnd - currByteIndex;
                if(nbBytesToSkip > 0)
                    currByteIndex += nbBytesToSkip;
                nextDpu();
                skipBytes(numBytes - nbBytesToSkip);
            }
        }
    }


    // TODO some of the constants defined here should be common with the DPU code
    // encoding of query types for PIM
    private static final byte PIM_PHRASE_QUERY_TYPE = (byte)1;
    private static final String dpuQueryBatchVarName = "query_batch";
    private static final String dpuResultsBatchVarName = "results_batch";
    private static final String dpuResultsIndexVarName = "results_index";
    private static final int dpuQueryBatchByteSize = 1 << 18;
    private static final int dpuResultsMaxByteSize = 1 << 13;

    private static final int nrDpus = 2048;
    private static final String dpuProgramPath = "../dpu/pim_index_searcher.dpu";
}
