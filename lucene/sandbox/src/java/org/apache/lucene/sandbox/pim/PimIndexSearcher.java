package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @class PimIndexSearcher
 * Implement term search on a PIM index.
 * The PIM index is intended to be loaded in PIM memory and
 * searched by the PIM Hardware. Hence, this class purpose
 * is only to test the index correctness.
 **/
public class PimIndexSearcher implements Closeable  {

    ArrayList<DPUIndexSearcher> searchers;

    PimIndexSearcher(Directory dir, PimConfig config) {

        searchers = new ArrayList<>();
        for(int i = 0; i < config.getNumDpus(); ++i) {
            searchers.add(new DPUIndexSearcher(dir, i));
        }
    }

    ArrayList<PimMatch> SearchTerm(BytesRef field, BytesRef term) {

        ArrayList<PimMatch> results = new ArrayList<>();
        searchers.forEach((s) -> {
            var matches = s.SearchTerm(field, term);
            if(matches != null)
                results.addAll(matches);
        });
        return results;
    }

    @Override
    public void close() throws IOException {
        for (DPUIndexSearcher s : searchers) {
            s.close();
        }
    }

    private class DPUIndexSearcher implements Closeable {

        int dpuId;
        IndexInput fieldTableInput;
        IndexInput blockTableInput;
        IndexInput blocksInput;
        IndexInput postingsInput;

        PimTreeBasedTermTable fieldTableTree;
        PimTreeBasedTermTable blockTableTree;

        DPUIndexSearcher(Directory dir, int dpuId) {

            this.dpuId = dpuId;
            try {
                openFilesInput(dir);
                // create field table
                this.fieldTableTree = PimTreeBasedTermTable.read(fieldTableInput);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void openFilesInput(Directory dir) throws IOException {

            SegmentInfos segmentInfos = SegmentInfos.readCommit(dir,
                    SegmentInfos.getLastCommitSegmentsFileName(dir));

            //TODO for the moment assume only one segment
            SegmentCommitInfo segmentCommitInfo = segmentInfos.info(0);

            String fieldFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_FIELD_INDEX_EXTENSION);
            fieldTableInput = dir.openInput(fieldFileName, IOContext.DEFAULT);

            String blockTablesFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION);
            blockTableInput = dir.openInput(blockTablesFileName, IOContext.DEFAULT);

            String blocksFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_BLOCK_INDEX_EXTENSION);
            blocksInput = dir.openInput(blocksFileName, IOContext.DEFAULT);

            String postingsFileName =
                    IndexFileNames.segmentFileName(
                            segmentCommitInfo.info.name, Integer.toString(dpuId), DPU_TERM_POSTINGS_INDEX_EXTENSION);
            postingsInput = dir.openInput(postingsFileName, IOContext.DEFAULT);
        }

        ArrayList<PimMatch> SearchTerm(BytesRef field, BytesRef term) {

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

            ArrayList<PimMatch> results = new ArrayList<>();

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

            // read the postings
            try {
                postingsInput.seek(postingAddress);
                int doc = 0;
                while (postingsInput.getFilePointer() < (postingAddress + postingByteSize)) {
                    int deltaDoc = postingsInput.readVInt();
                    doc += deltaDoc;
                    int freq = postingsInput.readZInt();
                    long numBytesPos = 0L;
                    if(freq == 0) {
                        freq = postingsInput.readVInt();
                        numBytesPos = postingsInput.readVLong();
                    }
                    else if(freq < 0) {
                        freq = -freq;
                        numBytesPos = postingsInput.readShort();
                    }
                    else {
                        numBytesPos = postingsInput.readByte();
                    }

                    results.add(new PimMatch(doc, freq));

                    // TODO read positions for the term in this doc
                    // For the moment just skip
                    postingsInput.skipBytes(numBytesPos);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return results;
        }

        @Override
        public void close() throws IOException {

            fieldTableInput.close();
            blockTableInput.close();
            blocksInput.close();
            postingsInput.close();
        }
    }

    public static final String DPU_TERM_FIELD_INDEX_EXTENSION = "dpuf";
    public static final String DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION = "dpub";
    public static final String DPU_TERM_BLOCK_INDEX_EXTENSION = "dput";
    public static final String DPU_TERM_POSTINGS_INDEX_EXTENSION = "dpup";
}
