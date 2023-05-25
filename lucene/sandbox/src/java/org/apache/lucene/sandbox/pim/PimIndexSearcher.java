package org.apache.lucene.sandbox.pim;

import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * class PimIndexSearcher
 * Implement term and phrase search on a PIM index.
 * The PIM index is intended to be loaded in PIM memory and
 * searched by the PIM Hardware. Hence, this class purpose
 * is only to test the index correctness.
 **/
public class PimIndexSearcher implements Closeable {

    ArrayList<DPUIndexSearcher> searchers;
    PimIndexInfo pimIndexInfo;
    boolean addStartDoc;

    /**
     * Constructor
     *
     * @param pimIndexInfo Object containing the info on the PIM index
     */
    PimIndexSearcher(PimIndexInfo pimIndexInfo) {
        this(pimIndexInfo, false);
    }

    /**
     * Constructor
     *
     * @param pimIndexInfo Object containing the info on the PIM index
     * @param addStartDoc true if the document IDs need to be shifted with
     *                    the segment start doc ID
     */
    PimIndexSearcher(PimIndexInfo pimIndexInfo, boolean addStartDoc) {

        this.pimIndexInfo = pimIndexInfo;
        this.addStartDoc = addStartDoc;
        searchers = new ArrayList<>();
        for (int i = 0; i < pimIndexInfo.getNumDpus(); ++i) {
            searchers.add(new DPUIndexSearcher(pimIndexInfo, i));
        }
    }

    /**
     * Search a term in PIM index
     *
     * @param field  the field to be searched
     * @param term   the term to be searched
     * @param scorer the scorer to be used for each match
     * @return the list of matches with document ID and score
     */
    ArrayList<PimMatch> searchTerm(BytesRef field, BytesRef term, LeafSimScorer scorer) {

        ArrayList<PimMatch> results = new ArrayList<>();
        int nbSegments = pimIndexInfo.getNumSegments();
        for (int leafIdx = 0; leafIdx < nbSegments; ++leafIdx) {
            results.addAll(searchTerm(leafIdx, field, term, scorer));
        }
        return results;
    }

    /**
     * Search a term in PIM index in a given segment
     *
     * @param leafIdx the segment number
     * @param field   the field to be searched
     * @param term    the term to be searched
     * @param scorer  the scorer to be used for each match
     * @return the list of matches with document ID and score
     */
    ArrayList<PimMatch> searchTerm(int leafIdx, BytesRef field, BytesRef term, LeafSimScorer scorer) {

        ArrayList<PimMatch> results = new ArrayList<>();
        int finalLeafIdx = leafIdx;
        searchers.forEach((s) -> {
            s.switchToNewSegment(finalLeafIdx);
            var matches = s.SearchTerm(field, term, scorer);
            if (matches != null)
                results.addAll(matches);
            try {
                s.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return results;
    }

    /**
     * Search a term in PIM index without scorer
     * In this case the score is just the frequency
     *
     * @param field the field to be searched
     * @param term  the term to be searched
     * @return the list of matches with document ID and frequency
     */
    ArrayList<PimMatch> searchTerm(BytesRef field, BytesRef term) {

        try {
            return searchTerm(field, term,
                    new LeafSimScorer(
                            new Similarity.SimScorer() {
                                @Override
                                public float score(float freq, long norm) {
                                    return freq;
                                }
                            },
                            null,
                            field.utf8ToString(),
                            false
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Search a phrase in PIM index
     *
     * @param query  the PIM phrase query
     * @param scorer the LeafSimScorer to be used to score each match
     * @return a list of matches with doc ID and score
     */
    ArrayList<PimMatch> searchPhrase(PimPhraseQuery query, LeafSimScorer scorer) {

        ArrayList<PimMatch> results = new ArrayList<>();
        int nbSegments = pimIndexInfo.getNumSegments();
        for (int leafIdx = 0; leafIdx < nbSegments; ++leafIdx) {
            results.addAll(searchPhrase(leafIdx, query, scorer));
        }
        return results;
    }

    /**
     * Search a phrase in PIM index in one segment
     *
     * @param leafIdx the segment number
     * @param query   the PIM phrase query
     * @param scorer  the LeafSimScorer to be used to score each match
     * @return a list of matches with doc ID and score
     */
    ArrayList<PimMatch> searchPhrase(int leafIdx, PimPhraseQuery query, LeafSimScorer scorer) {

        ArrayList<PimMatch> results = new ArrayList<>();
        searchers.forEach((s) -> {
            s.switchToNewSegment(leafIdx);
            var matches = s.SearchPhrase(query, scorer);
            if (matches != null)
                results.addAll(matches);
            try {
                s.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return results;
    }

    /**
     * Search a phrase in PIM index without scoring
     * In this case the score is just the frequency
     *
     * @param query the PIM phrase query
     * @return a list of matches with doc ID and freq
     */
    ArrayList<PimMatch> searchPhrase(PimPhraseQuery query) {

        try {
            return searchPhrase(query,
                    new LeafSimScorer(
                            new Similarity.SimScorer() {
                                @Override
                                public float score(float freq, long norm) {
                                    return freq;
                                }
                            },
                            null,
                            query.getField(),
                            false
                    )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        for (DPUIndexSearcher s : searchers) {
            s.close();
        }
    }

    /**
     * @class DPUIndexSearcher
     * Search for a term or phrase in the index of a DPU
     */
    private class DPUIndexSearcher implements Closeable {

        final int dpuId;
        final PimIndexInfo pimIndexInfo;
        int startDoc;
        IndexInput fieldTableInput;
        IndexInput blockTableInput;
        IndexInput blocksInput;
        IndexInput postingsInput;

        BytesRefToDataBlockTreeMap fieldTableTree;
        BytesRefToDataBlockTreeMap blockTableTree;

        DPUIndexSearcher(PimIndexInfo pimIndexInfo, int dpuId) {
            this.dpuId = dpuId;
            this.pimIndexInfo = pimIndexInfo;
            this.startDoc = 0;
        }

        void switchToNewSegment(int leafIdx) {

            try {
                openFilesInput(pimIndexInfo, leafIdx);
                // create field table
                this.fieldTableTree = BytesRefToDataBlockTreeMap.read(fieldTableInput);
            } catch (EOFException e) {
                // it may be that the file is empty if the DPU was assigned no docs
                // in this case this searcher will always return null for searchTerm/searchPhrase
                this.fieldTableTree = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void openFilesInput(PimIndexInfo pimIndexInfo, int leafIdx) throws IOException {

            startDoc = addStartDoc ? pimIndexInfo.getStartDoc(leafIdx) : 0;
            fieldTableInput = pimIndexInfo.getFieldFileInput(leafIdx, dpuId);
            blockTableInput = pimIndexInfo.getBlockTableFileInput(leafIdx, dpuId);
            blocksInput = pimIndexInfo.getBlocksFileInput(leafIdx, dpuId);
            postingsInput = pimIndexInfo.getPostingsFileInput(leafIdx, dpuId);
        }

        ArrayList<PimMatch> SearchTerm(BytesRef field, BytesRef term, LeafSimScorer scorer) {

            // get the postings for this term
            BytesRefToDataBlockTreeMap.SearchResult termPostings = getTermPostings(field, term);

            if (termPostings == null)
                return null;

            ArrayList<PimMatch> results = new ArrayList<>();

            // read the postings and fill in the results array
            // TODO there is no scoring done for now, just put the frequency as the score
            try {
                postingsInput.seek(termPostings.block.address);
                DocumentIterator docIt = new DocumentIterator(postingsInput, termPostings.byteSize);
                int doc = docIt.Next();
                while (doc >= 0) {
                    results.add(new PimMatch(doc + startDoc,
                            scorer.score(doc, docIt.getFreq())));
                    doc = docIt.Next();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return results;
        }

        ArrayList<PimMatch> SearchPhrase(PimPhraseQuery query, LeafSimScorer scorer) {

            // get the postings address of each term in the phrase query
            BytesRefToDataBlockTreeMap.SearchResult[] termPostingBlocks =
                    new BytesRefToDataBlockTreeMap.SearchResult[query.getTerms().length];
            IndexInput[] termPostings = new IndexInput[query.getTerms().length];
            BytesRef field = new BytesRef(query.getField());
            for (int i = 0; i < termPostingBlocks.length; ++i) {
                termPostingBlocks[i] = getTermPostings(field, query.getTerms()[i].bytes());
                if (termPostingBlocks[i] == null)
                    return null;

                // create multiple readers of the postings file
                // in order to read the postings of phrase terms in parallel
                termPostings[i] = postingsInput.clone();
                try {
                    termPostings[i].seek(termPostingBlocks[i].block.address);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // the search for exact phrase is done in two main steps
            // 1) find the next document which contains all terms of the phrase
            // 2) try to find an alignment of positions that forms the exact phrase in the document
            ArrayList<PimMatch> results = new ArrayList<>();

            try {
                assert termPostings.length > 0;
                int[] currDoc = new int[termPostings.length];
                Arrays.fill(currDoc, -1);
                DocumentIterator[] docIt = new DocumentIterator[termPostings.length];
                for (int i = 0; i < termPostings.length; ++i)
                    docIt[i] = new DocumentIterator(termPostings[i], termPostingBlocks[i].byteSize);

                while (true) {

                    int searchDoc = docIt[0].Next(0);
                    if (searchDoc < 0)
                        return results;
                    currDoc[0] = searchDoc;
                    int maxDoc = currDoc[0];

                    // document search
                    while (true) {
                        for (int i = 0; i < termPostings.length; ++i) {
                            if (currDoc[i] != searchDoc) {
                                currDoc[i] = docIt[i].Next(searchDoc);
                                if (currDoc[i] < 0) {
                                    // no more docs to check, we are done
                                    return results;
                                }
                                if (currDoc[i] > maxDoc) {
                                    maxDoc = currDoc[i];
                                }
                            }
                        }
                        // if maxDoc == searchDoc, then a document is found
                        // otherwise continue the loop and start searching from maxDoc
                        if (maxDoc == searchDoc)
                            break;
                        assert maxDoc > searchDoc;
                        searchDoc = maxDoc;
                    }

                    // found a document, perform the positions alignment
                    int[] currPos = new int[termPostings.length];
                    int[] searchPos = new int[termPostings.length];
                    Arrays.fill(currPos, -1);
                    PositionsIterator[] posIt = new PositionsIterator[termPostings.length];
                    for (int i = 0; i < termPostings.length; ++i) {
                        posIt[i] = new PositionsIterator(termPostings[i], docIt[i].getNbPositionsForDoc());
                    }

                    searchPos[0] = posIt[0].Next(0);
                    if (searchPos[0] < 0) continue;
                    currPos[0] = searchPos[0];
                    extendSearchPositions(searchPos);
                    boolean endPositions = false;
                    int nbPositionsMatch = 0;
                    while (true) {
                        int nbMatches = 0;
                        int maxPos = 0;
                        for (int i = 0; i < termPostings.length; ++i) {
                            if (currPos[i] != searchPos[i]) {
                                currPos[i] = posIt[i].Next(searchPos[i]);
                                if (currPos[i] < 0) {
                                    // no more positions to check, we are done with this doc
                                    endPositions = true;
                                    break;
                                } else if (currPos[i] == searchPos[i]) {
                                    nbMatches++;
                                } else if (currPos[i] > maxPos + i) {
                                    maxPos = currPos[i] - i;
                                }
                            }
                            else
                                nbMatches++;
                        }
                        if (endPositions)
                            break;
                        if (nbMatches == termPostings.length) {
                            // found a match, increment the number of position matches
                            // and continue the search from first term next position
                            nbPositionsMatch++;
                            searchPos[0] = posIt[0].Next(0);
                            if (searchPos[0] < 0) {
                                // no more positions
                                break;
                            }
                            currPos[0] = searchPos[0];
                        } else {
                            // no match at this position
                            // start searching from maxPos
                            searchPos[0] = maxPos;
                        }
                        extendSearchPositions(searchPos);
                    }
                    // end looking for positions of the matching document
                    // add the result if positions matches were found
                    if (nbPositionsMatch > 0) {
                        results.add(new PimMatch(searchDoc + startDoc,
                                scorer.score(searchDoc, nbPositionsMatch)));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void extendSearchPositions(int[] searchPos) {
            for (int i = 1; i < searchPos.length; ++i)
                searchPos[i] = searchPos[0] + i;
        }

        // method to find the address of where to read the postings of a given term in a given field
        // first lookup the field in the field table, then the term in the term block table
        private BytesRefToDataBlockTreeMap.SearchResult getTermPostings(BytesRef field, BytesRef term) {

            // case of empty index for this DPU
            if (this.fieldTableTree == null)
                return null;

            // first search for the field in the field table
            BytesRefToDataBlockTreeMap.SearchResult fieldResult = fieldTableTree.SearchForBlock(field);

            if (fieldResult == null)
                return null;

            // search for the block table for this field and read it
            blockTableTree = null;
            try {
                blockTableInput.seek(fieldResult.block.address);
                blockTableTree = BytesRefToDataBlockTreeMap.read(blockTableInput);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (blockTableTree == null)
                return null;

            // search for the right block where to find the term
            BytesRefToDataBlockTreeMap.SearchResult termResult = blockTableTree.SearchForBlock(term);

            if (termResult == null)
                return null;

            // start reading at the address of the block to find the term (if present)
            // and the address to its posting list
            long postingAddress = -1L;
            long postingByteSize = 0L;
            try {
                blocksInput.seek(termResult.block.address);
                //special case where the first term of the block
                //is the one searched
                if (term.compareTo(termResult.block.bytesRef) == 0) {
                    // the posting address is the first VLong
                    postingAddress = blocksInput.readVLong();
                    postingByteSize = blocksInput.readVLong();
                } else {
                    // ignore first term posting info
                    blocksInput.readVLong();
                    blocksInput.readVLong();
                    while (blocksInput.getFilePointer() < (termResult.block.address + termResult.byteSize)) {
                        // read term
                        int termLength = blocksInput.readVInt();
                        byte[] termBytes = new byte[termLength];
                        blocksInput.readBytes(termBytes, 0, termLength);

                        // compare term to the one searched
                        int cmp = term.compareTo(new BytesRef(termBytes));
                        if (cmp == 0) {
                            // found term, save posting list address
                            postingAddress = blocksInput.readVLong();
                            postingByteSize = blocksInput.readVLong();
                            break;
                        }
                        if (cmp < 0) {
                            // this means the term searched is not present
                            break;
                        }
                        // skip current term posting address / byte size
                        blocksInput.readVLong();
                        blocksInput.readVLong();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (postingAddress < 0)
                return null;

            return new BytesRefToDataBlockTreeMap.SearchResult(
                    new BytesRefToDataBlockTreeMap.Block(term, postingAddress), (int) postingByteSize);
        }

        /**
         * @class Iterator
         * abstract base class for doc and position iterator classes
         ***/
        private static abstract class Iterator {

            public abstract int Next() throws IOException;

            // iterate to the next value that is no smaller
            // than the target value
            int Next(int target) throws IOException {

                int next = Next();
                while (next >= 0 && next < target)
                    next = Next();
                return next;
            }
        }

        /**
         * @class DocumentIterator
         * class used to iterate over documents in the posting list
         **/
        private static class DocumentIterator extends Iterator {

            private IndexInput postingInput;
            private final long endPointer;
            private int lastDoc;
            private long nextDocPointer;
            private long nbPositions;
            private int freq;

            /**
             * @param postingInput the IndexInput where to read the postings
             * @param byteSize     the size in bytes of the postings for the term we want to find the docs
             ***/
            DocumentIterator(IndexInput postingInput, long byteSize) {
                this.postingInput = postingInput;
                this.endPointer = postingInput.getFilePointer() + byteSize;
                this.lastDoc = 0;
                this.nextDocPointer = -1;
                this.nbPositions = -1;
                this.freq = -1;
            }

            public int Next() throws IOException {

                // first skip the necessary number of bytes
                // to reach the next doc
                if (nextDocPointer > 0) {
                    this.postingInput.seek(nextDocPointer);
                }

                // stop if this is the end of the posting list for the term
                if (postingInput.getFilePointer() >= endPointer) {
                    nextDocPointer = -1;
                    nbPositions = -1;
                    return -1;
                }

                // decode doc, freq and byte size
                int deltaDoc = postingInput.readVInt();
                lastDoc += deltaDoc;
                freq = postingInput.readZInt();
                if (freq == 0) {
                    nbPositions = postingInput.readVInt();
                    nextDocPointer = postingInput.readVLong();
                    nextDocPointer += postingInput.getFilePointer();
                } else if (freq < 0) {
                    nbPositions = -freq;
                    nextDocPointer = postingInput.readShort();
                    nextDocPointer += postingInput.getFilePointer();
                } else {
                    nbPositions = freq;
                    nextDocPointer = postingInput.readByte();
                    nextDocPointer += postingInput.getFilePointer();
                }
                return lastDoc;
            }

            long getNbPositionsForDoc() {
                return nbPositions;
            }

            int getFreq() {
                return freq;
            }
        }

        /**
         * @class PositionsIterator
         * class used to iterate over positions in the posting list
         **/
        private static class PositionsIterator extends Iterator {

            private IndexInput postingInput;
            private long nbPositions;
            private int lastPos;

            /**
             * @param postingInput the IndexInput where to read the postings
             * @param nbPositions  the number of positions to read
             ***/
            PositionsIterator(IndexInput postingInput, long nbPositions) {
                assert nbPositions > 0;
                this.postingInput = postingInput;
                this.nbPositions = nbPositions;
                this.lastPos = 0;
            }

            public int Next() throws IOException {
                if (nbPositions == 0)
                    return -1;
                nbPositions--;
                lastPos += postingInput.readVInt();
                return lastPos;
            }
        }

        @Override
        public void close() throws IOException {

            if (fieldTableInput != null)
                fieldTableInput.close();
            if (blockTableInput != null)
                blockTableInput.close();
            if (blocksInput != null)
                blocksInput.close();
            if (postingsInput != null)
                postingsInput.close();
        }
    }
}
