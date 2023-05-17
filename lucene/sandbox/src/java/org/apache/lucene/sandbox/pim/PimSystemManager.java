package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;

import java.util.List;

/**
 * @class PimSystemManager
 * Singleton class used to manage the PIM system and offload
 * queries to it.
 * TODO currently use a software model to answer queries, not
 * the real PIM HW
 */
public class PimSystemManager {

    private static PimSystemManager instance;

    private boolean isIndexLoaded;
    private PimIndexInfo pimIndexInfo;
    private final PimConfig pimConfig = new PimConfig();

    // for the moment, the PIM index search is performed on CPU
    // using this class, no PIM HW involved
    PimIndexSearcher pimSearcher;

    private PimSystemManager() {
        isIndexLoaded = false;
        pimIndexInfo = null;
        pimSearcher = null;
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
     * @param pimIndexInfo the PIM index to load
     * @return true if the index was successfully loaded
     */
    public boolean loadPimIndex(PimIndexInfo pimIndexInfo) {

        if (!isIndexLoaded) {
            boolean loadSuccess = false;
            //synchronized block for thread safety
            synchronized (PimSystemManager.class) {
                if (!isIndexLoaded) {
                    this.pimIndexInfo = pimIndexInfo;
                    isIndexLoaded = true;
                    loadSuccess = true;
                }
            }
            if (loadSuccess) {
                // the calling thread has succeeded loading the PIM Index
                transferPimIndex();
            }
        }
        return false;
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
     * Base method to search a query using the PIM system
     * The base method just returns null and should be overloaded to support
     * specific query types
     *
     * @param context the leafReaderContext to search
     * @param query   the input query
     * @param scorer  a LeafSimScorer object used to score the matches
     * @return the list of matches
     */
    public synchronized List<PimMatch> search(LeafReaderContext context,
                                              Query query, LeafSimScorer scorer) {
        // unsupported query
        return null;
    }

    /**
     * in the PIM system, only one search can happen at a time
     * (or a batch of searches, to come later)
     * so this method needs to be declared as synchronized
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
    public synchronized List<PimMatch> search(LeafReaderContext context,
                                              PimPhraseQuery query, LeafSimScorer scorer) {
        return pimSearcher.searchPhrase(context.ord, query, scorer);
    }

    private void transferPimIndex() {
        // TODO load index to PIM system
        // create a new PimIndexSearcher for this index
        // TODO copy the PIM index files here to mimic transfer
        // to DPU and be safe searching it while the index is overwritten
        pimSearcher = new PimIndexSearcher(pimIndexInfo);
    }
}
