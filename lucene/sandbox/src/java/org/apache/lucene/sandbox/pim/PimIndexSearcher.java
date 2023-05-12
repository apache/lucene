package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @class PimIndexSearcher
 * Implement term search on a PIM index.
 * The PIM index is intended to be loaded in PIM memory and
 * searched by the PIM Hardware. Hence, this class purpose
 * is only to test the index correctness.
 **/
public class PimIndexSearcher implements Closeable  {

    ArrayList<DPUIndexSearcher> searchers;
    Directory dir;

    PimIndexSearcher(Directory dir, Directory pimDir, PimConfig config) {

        this.dir = dir;
        searchers = new ArrayList<>();
        for(int i = 0; i < config.getNumDpus(); ++i) {
            searchers.add(new DPUIndexSearcher(dir, pimDir, i));
        }
    }

    ArrayList<PimMatch> SearchTerm(BytesRef field, BytesRef term) {

        ArrayList<PimMatch> results = new ArrayList<>();
        int nbSegments = GetNumberOfSegments();
        for(int leafIdx = 0; leafIdx < nbSegments; ++leafIdx) {
            int finalLeafIdx = leafIdx;
            searchers.forEach((s) -> {
                s.switchToNewSegment(finalLeafIdx);
                var matches = s.SearchTerm(field, term);
                if (matches != null)
                    results.addAll(matches);
                try {
                    s.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return results;
    }

    ArrayList<PimMatch> SearchPhrase(PimPhraseQuery query) {

        ArrayList<PimMatch> results = new ArrayList<>();
        int nbSegments = GetNumberOfSegments();
        for(int leafIdx = 0; leafIdx < nbSegments; ++leafIdx) {
            int finalLeafIdx = leafIdx;
            searchers.forEach((s) -> {
                s.switchToNewSegment(finalLeafIdx);
                var matches = s.SearchPhrase(query);
                if (matches != null)
                    results.addAll(matches);
                try {
                    s.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        for (DPUIndexSearcher s : searchers) {
            s.close();
        }
    }

    private int GetNumberOfSegments() {

        try (IndexReader indexReader = DirectoryReader.open(dir)) {
            List<LeafReaderContext> leaves = indexReader.leaves();
            return leaves.size();
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class DPUIndexSearcher implements Closeable {

        int dpuId;
        Directory dir;
        Directory pimDir;
        IndexInput fieldTableInput;
        IndexInput blockTableInput;
        IndexInput blocksInput;
        IndexInput postingsInput;

        PimTreeBasedTermTable fieldTableTree;
        PimTreeBasedTermTable blockTableTree;
        int startDoc;

        DPUIndexSearcher(Directory dir, Directory pimDir, int dpuId) {
            this.dpuId = dpuId;
            this.dir = dir;
            this.pimDir = pimDir;
        }

        void switchToNewSegment(int leafIdx) {

            try {
                openFilesInput(dir, pimDir, leafIdx);
                // create field table
                this.fieldTableTree = PimTreeBasedTermTable.read(fieldTableInput);
            } catch (EOFException e) {
                // it may be that the file is empty if the DPU was assigned no docs
                // in this case this searcher will always return null for searchTerm/searchPhrase
                this.fieldTableTree = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void openFilesInput(Directory dir, Directory pimDir, int leafIdx) throws IOException {

            SegmentInfos segmentInfos = SegmentInfos.readCommit(dir,
                    SegmentInfos.getLastCommitSegmentsFileName(dir));

            SegmentCommitInfo segmentCommitInfo = segmentInfos.info(leafIdx);
            startDoc = 0;
            for(int i = 0; i < leafIdx && i < segmentInfos.size(); ++i) {
                startDoc += segmentInfos.info(i).info.maxDoc();
            }

            String fieldFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_FIELD_INDEX_EXTENSION);
            fieldTableInput = pimDir.openInput(fieldFileName, IOContext.DEFAULT);

            String blockTablesFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION);
            blockTableInput = pimDir.openInput(blockTablesFileName, IOContext.DEFAULT);

            String blocksFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_BLOCK_INDEX_EXTENSION);
            blocksInput = pimDir.openInput(blocksFileName, IOContext.DEFAULT);

            String postingsFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_POSTINGS_INDEX_EXTENSION);
            postingsInput = pimDir.openInput(postingsFileName, IOContext.DEFAULT);
        }

        ArrayList<PimMatch> SearchTerm(BytesRef field, BytesRef term) {

            // search for the right block where to find the term
            PimTreeBasedTermTable.Block termPostings = getTermPostings(field, term);

            if(termPostings == null)
                return null;

            ArrayList<PimMatch> results = new ArrayList<>();

            // read the postings
            try {
                postingsInput.seek(termPostings.address);
                DocumentIterator docIt = new DocumentIterator(postingsInput, termPostings.byteSize);
                int doc = docIt.Next();
                while(doc >= 0) {
                    results.add(new PimMatch(startDoc + doc, docIt.getFreq()));
                    doc = docIt.Next();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return results;
        }

        ArrayList<PimMatch> SearchPhrase(PimPhraseQuery query) {

            // lookup the term blocks where to find the phrase terms (in block table)
            PimTreeBasedTermTable.Block[] termPostingBlocks = new PimTreeBasedTermTable.Block[query.getTerms().length];
            IndexInput[] termPostings =  new IndexInput[query.getTerms().length];
            BytesRef field = new BytesRef(query.getField());
            for(int i = 0; i < termPostingBlocks.length; ++i) {
                termPostingBlocks[i] = getTermPostings(field, query.getTerms()[i].bytes());
                if(termPostingBlocks[i] == null)
                    return null;

                // create multiple readers of the postings file
                // in order to read the postings of phrase terms in parallel
                termPostings[i] = postingsInput.clone();
                try {
                    termPostings[i].seek(termPostingBlocks[i].address);
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
                for(int i = 0; i < termPostings.length; ++i)
                    docIt[i] = new DocumentIterator(termPostings[i], termPostingBlocks[i].byteSize);

                while(true) {

                    int searchDoc = docIt[0].Next(0);
                    if(searchDoc < 0)
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
                    for(int i = 0; i < termPostings.length; ++i) {
                        posIt[i] = new PositionsIterator(termPostings[i], docIt[i].getNbPositionsForDoc());
                    }

                    searchPos[0] = posIt[0].Next(0);
                    if(searchPos[0] < 0) continue;
                    currPos[0] = searchPos[0];
                    extendSearchPositions(searchPos);
                    boolean endPositions = false;
                    while (true) {
                        int nbMatches = 1;
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
                                }
                                else if(currPos[i] > maxPos + i) {
                                    maxPos = currPos[i] - i;
                                }
                            }
                        }
                        if (endPositions)
                            break;
                        if(nbMatches == termPostings.length) {
                            // found a match, store it
                            // and continue the search from first term next position
                            results.add(new PimMatch(startDoc + searchDoc, searchPos[0]));
                            searchPos[0] = posIt[0].Next(0);
                            if(searchPos[0] < 0) {
                                // no more positions
                                break;
                            }
                            currPos[0] = searchPos[0];
                        }
                        else {
                            // no match at this position
                            // start searching from maxPos
                            searchPos[0] = maxPos;
                        }
                        extendSearchPositions(searchPos);
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
        private PimTreeBasedTermTable.Block getTermPostings(BytesRef field, BytesRef term) {

            // case of empty index for this DPU
            if(this.fieldTableTree == null)
                return null;

            // first search for the field in the field table
            PimTreeBasedTermTable.Block fieldBlock = fieldTableTree.SearchForBlock(field);

            if(fieldBlock == null)
                return null;

            // search for the block table for this field and read it
            blockTableTree = null;
            try {
                blockTableInput.seek(fieldBlock.address);
                blockTableTree = PimTreeBasedTermTable.read(blockTableInput);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if(blockTableTree == null)
                return null;

            // search for the right block where to find the term
            PimTreeBasedTermTable.Block termBlock = blockTableTree.SearchForBlock(term);

            if(termBlock == null)
                return null;

            // start reading at the address of the block to find the term (if present)
            // and the address to its posting list
            long postingAddress = -1L;
            long postingByteSize = 0L;
            try {
                blocksInput.seek(termBlock.address);
                //special case where the first term of the block
                //is the one searched
                if(term.compareTo(termBlock.term) == 0) {
                    // the posting address is the first VLong
                    postingAddress = blocksInput.readVLong();
                    postingByteSize = blocksInput.readVLong();
                }
                else {
                    // ignore first term posting info
                    blocksInput.readVLong();
                    blocksInput.readVLong();
                    while (blocksInput.getFilePointer() < (termBlock.address + termBlock.byteSize)) {
                        // read term
                        int termLength = blocksInput.readVInt();
                        byte[] termBytes = new byte[termLength];
                        blocksInput.readBytes(termBytes, 0, termLength);

                        // compare term to the one searched
                        int cmp = term.compareTo(new BytesRef(termBytes));
                        if(cmp == 0) {
                            // found term, save posting list address
                            postingAddress = blocksInput.readVLong();
                            postingByteSize = blocksInput.readVLong();
                            break;
                        }
                        if(cmp < 0) {
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

            if(postingAddress < 0)
                return null;

            return new PimTreeBasedTermTable.Block(term, postingAddress, postingByteSize);
        }

        /**
         * abstract base class for doc and position iterator classes
         ***/
        private static abstract class Iterator {

            public abstract int Next() throws IOException;

            // iterate to the next value that is no smaller
            // than the target value
            int Next(int target) throws IOException {

                int next = Next();
                while(next >= 0 && next < target)
                    next = Next();
                return next;
            }
        }

        /**
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
             * @param byteSize the size in bytes of the postings for the term we want to find the docs
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
                if(nextDocPointer > 0) {
                    this.postingInput.seek(nextDocPointer);
                }

                // stop if this is the end of the posting list for the term
                if(postingInput.getFilePointer() >= endPointer) {
                    nextDocPointer = -1;
                    nbPositions = -1;
                    return -1;
                }

                // decode doc, freq and byte size
                int deltaDoc = postingInput.readVInt();
                lastDoc += deltaDoc;
                freq = postingInput.readZInt();
                if(freq == 0) {
                    nbPositions = postingInput.readVInt();
                    nextDocPointer = postingInput.readVLong();
                    nextDocPointer += postingInput.getFilePointer();
                }
                else if(freq < 0) {
                    nbPositions = -freq;
                    nextDocPointer = postingInput.readShort();
                    nextDocPointer += postingInput.getFilePointer();
                }
                else {
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
         * class used to iterate over positions in the posting list
         **/
        private static class PositionsIterator extends Iterator {

            private IndexInput postingInput;
            private long nbPositions;
            private int lastPos;

            /**
             * @param postingInput the IndexInput where to read the postings
             * @param nbPositions the number of positions to read
             ***/
            PositionsIterator(IndexInput postingInput, long nbPositions) {
                assert nbPositions > 0;
                this.postingInput = postingInput;
                this.nbPositions = nbPositions;
                this.lastPos = 0;
            }

            public int Next() throws IOException {
                if(nbPositions == 0)
                    return -1;
                nbPositions--;
                lastPos += postingInput.readVInt();
                return lastPos;
            }
        }

        @Override
        public void close() throws IOException {

            if(fieldTableInput != null)
                fieldTableInput.close();
            if(blockTableInput != null)
                blockTableInput.close();
            if(blocksInput != null)
                blocksInput.close();
            if(postingsInput != null)
                postingsInput.close();
        }
    }

    public static final String DPU_TERM_FIELD_INDEX_EXTENSION = "dpuf";
    public static final String DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION = "dpub";
    public static final String DPU_TERM_BLOCK_INDEX_EXTENSION = "dput";
    public static final String DPU_TERM_POSTINGS_INDEX_EXTENSION = "dpup";
}
