package org.apache.lucene.sandbox.pim;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.Serializable;

/**
 * class PimIndexInfo
 * Class to hold the information to be passed from
 * the PimIndexWriter to the PimSystemManager that loads the index
 * to PIM.
 */
public class PimIndexInfo implements Serializable {

    transient Directory pimDir;
    final int numDpus;
    final int numSegments;
    final String segmentCommitName[];
    final int startDoc[];

    PimIndexInfo(Directory pimDir, int nbDpus, SegmentInfos segmentInfos) {

        this.pimDir = pimDir;
        this.numSegments = segmentInfos.size();
        this.numDpus = nbDpus;
        segmentCommitName = new String[numSegments];
        startDoc = new int[numSegments];

        for (int i = 0; i < numSegments; ++i) {
            SegmentCommitInfo segmentCommitInfo = segmentInfos.info(i);
            segmentCommitName[i] = segmentCommitInfo.info.name;
            if (i < numSegments - 1) {
                startDoc[i + 1] = startDoc[i] + segmentCommitInfo.info.maxDoc();
                System.out.println("startDoc of " + i + 1 + ": " + startDoc[i + 1]);
            }
        }
    }

    public int getNumDpus() {
        return numDpus;
    }

    public int getNumSegments() {
        return numSegments;
    }

    public int getStartDoc(int leafIdx) {
        return startDoc[leafIdx];
    }

    public void setPimDir(Directory pimDir) {
        this.pimDir = pimDir;
    }

    public IndexInput getFieldFileInput(int leafIdx, int dpuId) throws IOException {

        return getFileInput(leafIdx, dpuId, DPU_TERM_FIELD_INDEX_EXTENSION);
    }

    public IndexInput getBlockTableFileInput(int leafIdx, int dpuId) throws IOException {

        return getFileInput(leafIdx, dpuId, DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION);
    }

    public IndexInput getBlocksFileInput(int leafIdx, int dpuId) throws IOException {

        return getFileInput(leafIdx, dpuId, DPU_TERM_BLOCK_INDEX_EXTENSION);
    }

    public IndexInput getPostingsFileInput(int leafIdx, int dpuId) throws IOException {

        return getFileInput(leafIdx, dpuId, DPU_TERM_POSTINGS_INDEX_EXTENSION);
    }

    private IndexInput getFileInput(int leafIdx, int dpuId, String ext) throws IOException {

        String fileName =
                IndexFileNames.segmentFileName(
                        segmentCommitName[leafIdx], Integer.toString(dpuId), ext);
        return pimDir.openInput(fileName, IOContext.DEFAULT);
    }

    private static final String DPU_TERM_FIELD_INDEX_EXTENSION = "dpuf";
    private static final String DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION = "dpub";
    private static final String DPU_TERM_BLOCK_INDEX_EXTENSION = "dput";
    private static final String DPU_TERM_POSTINGS_INDEX_EXTENSION = "dpup";
}
