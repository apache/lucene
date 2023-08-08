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
    final int numDpuSegments;
    final String segmentCommitName[];
    final int startDoc[];

    /**
     * Constructor
     * @param pimDir the PIM index directory
     * @param nbDpus the number of DPUs
     * @param segmentInfos SegmentInfos of the Lucene index
     */
    PimIndexInfo(Directory pimDir, int nbDpus, int numDpuSegments, SegmentInfos segmentInfos) {

        this.pimDir = pimDir;
        this.numSegments = segmentInfos.size();
        this.numDpus = nbDpus;
        this.numDpuSegments = numDpuSegments;
        segmentCommitName = new String[numSegments];
        startDoc = new int[numSegments];

        for (int i = 0; i < numSegments; ++i) {
            SegmentCommitInfo segmentCommitInfo = segmentInfos.info(i);
            segmentCommitName[i] = segmentCommitInfo.info.name;
            if (i < numSegments - 1) {
                startDoc[i + 1] = startDoc[i] + segmentCommitInfo.info.maxDoc();
            }
        }
    }

    /**
     * @return number of dpus
     */
    public int getNumDpus() {
        return numDpus;
    }

    /**
     * @return number of segments
     */
    public int getNumSegments() {
        return numSegments;
    }

    /**
     * @return number of DPU segments
     */
    public int getNumDpuSegments() {
        return numDpuSegments;
    }

    /**
     * @param leafIdx segment id
     * @return start doc ID (offset) for the segment
     */
    public int getStartDoc(int leafIdx) {
        return startDoc[leafIdx];
    }

    /**
     * Set the PIM index directory
     * @param pimDir the directory
     */
    public void setPimDir(Directory pimDir) {
        this.pimDir = pimDir;
    }

    /**
     * Get a slice of the PIM index IndexInput, pointing to the field table of the given DPU
     * @param in the IndexInput for the PIM index
     * @param dpuId the dpu ID
     * @return a slice of IndexInput
     * @throws IOException
     */
    public IndexInput getFieldFileInput(IndexInput in, int dpuId) throws IOException {

        seekToDpu(in, dpuId);
        in.readByte(); // nb DPU segments
        long fieldSize = in.readVLong();
        if(fieldSize == 0) {
            // empty DPU, no docs were added
            return null;
        }
        in.readVLong();
        in.readVLong();
        return in.slice("fieldInput", in.getFilePointer(), fieldSize);
    }

    /**
     * Get a slice of the PIM index IndexInput, pointing to the block table of the given DPU
     * @param in the IndexInput for the PIM index
     * @param dpuId the dpu ID
     * @return a slice of IndexInput
     * @throws IOException
     */
    public IndexInput getBlockTableFileInput(IndexInput in, int dpuId) throws IOException {

        seekToDpu(in, dpuId);
        in.readByte(); // nb DPU segments
        long blockTableOffset = in.readVLong();
        if(blockTableOffset == 0) {
            // empty DPU, no docs were added
            return null;
        }
        long blockTableLength = in.readVLong() - blockTableOffset;
        in.readVLong();
        return in.slice("blockTableInput", in.getFilePointer() + blockTableOffset, blockTableLength);
    }

    /**
     * Get a slice of the PIM index IndexInput, pointing to the block list of the given DPU
     * @param in the IndexInput for the PIM index
     * @param dpuId the dpu ID
     * @return a slice of IndexInput
     * @throws IOException
     */
    public IndexInput getBlocksFileInput(IndexInput in, int dpuId) throws IOException {

        seekToDpu(in, dpuId);
        in.readByte(); // nb segments
        in.readVLong();
        long blockListOffset = in.readVLong();
        if(blockListOffset == 0) {
            // empty DPU, no docs were added
            return null;
        }
        long blockListSize = in.readVLong() - blockListOffset;
        return in.slice("blockListInput", in.getFilePointer() + blockListOffset, blockListSize);
    }

    /**
     * Get a slice of the PIM index IndexInput, pointing to the postings of the given DPU
     * @param in the IndexInput for the PIM index
     * @param dpuId the dpu ID
     * @return a slice of IndexInput
     * @throws IOException
     */
    public IndexInput getPostingsFileInput(IndexInput in, int dpuId) throws IOException {

        seekToDpu(in, dpuId);
        in.readByte(); // nb segments
        in.readVLong();
        in.readVLong();
        long postingsOffset = in.readVLong();
        if(postingsOffset == 0)  {
            // empty DPU, no docs were added
            return null;
        }
        return in.slice("postingsInput", in.getFilePointer() + postingsOffset,
                in.length() - (in.getFilePointer() + postingsOffset));
    }

    /**
     * Get an IndexInput for the PIM index of the given segment
     * @param leafIdx the segment ID
     * @return the IndexInput object
     * @throws IOException
     */
    public IndexInput getFileInput(int leafIdx) throws IOException {

        if(leafIdx >= segmentCommitName.length)
            return null;
        String fileName =
                IndexFileNames.segmentFileName(
                        segmentCommitName[leafIdx], Integer.toString(numDpus), DPU_INDEX_COMPOUND_EXTENSION);
        return pimDir.openInput(fileName, IOContext.DEFAULT);
    }

    /**
     * Set the IndexInput object to point to data for the given DPU
     * @param in the IndexInput for the PIM index
     * @param dpuId the DPU id
     * @return the size in bytes of the DPU index
     * @throws IOException
     */
    public long seekToDpu(IndexInput in, int dpuId) throws IOException {

        // seek the right place where to find the information
        in.seek(0);
        int nbDpus = in.readVInt();
        assert nbDpus == numDpus;
        long dpuAddr = 0;
        long nextDpuAddr = in.length();
        boolean next = false;
        for(int i = 0; i < nbDpus; ++i) {
            if(next) {
                nextDpuAddr = in.readVLong();
                next = false;
            }
            else if(i == dpuId) {
                dpuAddr = in.readVLong();
                next = true;
            }
            else
                in.readVLong();
        }

        in.skipBytes(dpuAddr);

        if(next)
          return in.length() - in.getFilePointer();
        else
          return nextDpuAddr - dpuAddr;
    }

    public static final String DPU_INDEX_COMPOUND_EXTENSION = "dpuc";
}
