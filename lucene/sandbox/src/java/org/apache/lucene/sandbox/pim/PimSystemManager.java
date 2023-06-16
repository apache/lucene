package org.apache.lucene.sandbox.pim;

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
public class PimSystemManager {

    private static PimSystemManager instance;

    final int BYTE_BUFFER_QUEUE_LOG2_BYTE_SIZE = 11;
    final int MAX_QUERY_ID = 1 << BYTE_BUFFER_QUEUE_LOG2_BYTE_SIZE;
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
    final Condition queryPushedCond  = queryLock.newCondition();
    private final ReentrantReadWriteLock resultsLock = new ReentrantReadWriteLock();
    private final Lock resultsPushedLock = new ReentrantLock();
    final Condition resultsPushedCond = resultsPushedLock.newCondition();
    final Lock idLock = new ReentrantLock();
    private int queryId;


    // Note: let's assume we will use scatter/gather DPU->CPU transfers
    // And all results for the same query will be in a continuous array in memory after the transfer
    private TreeMap<Integer, byte[]> queryResultsMap;
    private PimManagerThread pimThread;
    private ByteArrayOutputStream dpuStream = new ByteArrayOutputStream();
    private DpuSystem dpuSystem;
    private Byte[][] dpuNbResults;
    private Byte[][] dpuResults;

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
        queryId = 0;
        queryResultsMap = new TreeMap<>();
        pimThread = new PimManagerThread(queryBuffer);
        pimThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> PimSystemManager.shutDown()));

        if(!use_software_model) {
            // allocate DPUs, load the program, allocate space for DPU results
            try {
                dpuSystem = DpuSystem.allocate(nrDpus, "sgXferEnable=true", new PrintStream(dpuStream));
                dpuSystem.load(dpuProgramPath);
                dpuNbResults = new Byte[nrDpus][dpuQueryBatchByteSize];
                dpuResults = new Byte[dpuQueryBatchByteSize][dpuQueryResultsByteSize];
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
            int id = getQueryId();
            ByteCountDataOutput countOutput = new ByteCountDataOutput();
            writeQueryToPim(countOutput, query, id, context.ord);
            final int byteSize = Math.toIntExact(countOutput.getByteCount());
            var queryOutput = queryBuffer.add(byteSize);

            // write query id, leaf id, query type, then write query
            writeQueryToPim(queryOutput, query, id, context.ord);

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
        byte[] resultsArr;
        try {
            resultsArr = queryResultsMap.get(id);
        }
        finally {
            resultsLock.readLock().unlock();
        }
        assert resultsArr != null;

        List<PimMatch> matches = getMatches(q, resultsArr, scorer);

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
            QueryType q, byte[] resultsArr, LeafSimScorer scorer) throws IOException {

        List<PimMatch> matches = new ArrayList<>();
        ByteArrayDataInput input = new ByteArrayDataInput(resultsArr);

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
     * @param id the id of the query
     * @param leafIdx the leaf id
     * @param <QueryType> a type that is both a Query and a PimQuery
     * @throws IOException
     */
    private <QueryType extends Query & PimQuery>
    void writeQueryToPim(DataOutput output, QueryType query, int id, int leafIdx) throws IOException {

        output.writeVInt(id);
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
     * Allocate an id for a new query
     * @return the query id
     */
    private int getQueryId() {
        idLock.lock();
        int id = -1;
        try {
            id = queryId++;
            if(queryId > MAX_QUERY_ID) {
                // since the MAX_QUERY_ID is defined as being equal to
                // the number of bytes in the queue, and a query is minimum 1 byte,
                // query with id 0 is already handled whenever this condition happens
                queryId = 0;
            }
        }
        finally {
            idLock.unlock();
        }
        return id;
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
                            int id = input.readVInt();
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
                                queryResultsMap.put(id, matchesByteArr);
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

        // 3) scatter-gather transfer for the rank
        //TODO first get the meta-data (nb of results per DPU for each query of the batch) and then the results

        // 4) dpu_sync()
        dpuSystem.async().sync();

        // 5) once transfer is finished, all queries have their results ready to be added in the treeMap
        // Do it in this thread as the batch will be of small size
        resultsLock.writeLock().lock();
        try {
            //queryResultsMap.put(id, matches);
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


    // TODO some of the constants defined here should be common with the DPU code
    // encoding of query types for PIM
    private static final byte PIM_PHRASE_QUERY_TYPE = (byte)1;
    private static final String dpuQueryBatchVarName = "query_batch";
    private static final int dpuQueryBatchByteSize = 1 << 18;
    private static final int dpuQueryResultsByteSize = 1 << 18;

    private static final int nrDpus = 2048;
    private static final String dpuProgramPath = "../dpu/pim_index_searcher.dpu";
}
