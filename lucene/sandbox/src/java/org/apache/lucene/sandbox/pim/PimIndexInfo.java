/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * class PimIndexInfo Class to hold the information to be passed from the PimIndexWriter to the
 * PimSystemManager that loads the index to PIM.
 */
public class PimIndexInfo {

  Directory pimDir;
  int numDpus;
  int numSegments;
  int numDpuSegments;
  int numDocs;
  String segmentCommitName;
  int startDoc[];
  byte segmentId[][];
  byte segmentCommitId[][];

  /**
   * Constructor
   *
   * @param pimDir the PIM index directory
   * @param nbDpus the number of DPUs
   * @param segmentInfos SegmentInfos of the Lucene index
   */
  PimIndexInfo(Directory pimDir, int nbDpus, int numDpuSegments, SegmentInfos segmentInfos) {

    this.pimDir = pimDir;
    this.numSegments = segmentInfos.size();
    this.numDpus = nbDpus;
    this.numDpuSegments = numDpuSegments;
    segmentCommitName = segmentInfos.getSegmentsFileName();
    startDoc = new int[numSegments];
    segmentId = new byte[numSegments][];
    segmentCommitId = new byte[numSegments][];

    numDocs = 0;
    for (int i = 0; i < numSegments; ++i) {
      SegmentCommitInfo segmentCommitInfo = segmentInfos.info(i);
      segmentId[i] = segmentCommitInfo.info.getId();
      segmentCommitId[i] = segmentCommitInfo.getId();
      if (i + 1 < numSegments) startDoc[i + 1] = startDoc[i] + segmentCommitInfo.info.maxDoc();
      numDocs += segmentCommitInfo.info.maxDoc();
    }
  }

  /**
   * get the offset for documents in the index leaf (first document id of the leaf)
   *
   * @param leafId the id of the leaf
   * @return first document id of the leaf
   */
  public int getStartDoc(int leafId) {
    assert leafId < numSegments;
    return startDoc[leafId];
  }

  private PimIndexInfo() {
    this.pimDir = null;
    this.numDpus = 0;
    this.numSegments = 0;
    this.numDpuSegments = 0;
    this.segmentCommitName = null;
    this.startDoc = null;
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
   * @return number of documents in the index
   */
  public int getNumDocs() {
    return numDocs;
  }

  /**
   * Set the PIM index directory
   *
   * @param pimDir the directory
   */
  public void setPimDir(Directory pimDir) {
    this.pimDir = pimDir;
  }

  /**
   * Get a slice of the PIM index IndexInput, pointing to the field table of the given DPU
   *
   * @param in the IndexInput for the PIM index
   * @param dpuId the dpu ID
   * @return a slice of IndexInput
   * @throws IOException if failed to read the IndexInput
   */
  public IndexInput getFieldFileInput(IndexInput in, int dpuId) throws IOException {

    seekToDpu(in, dpuId);
    skipSegmentInfo(in);
    long fieldSize = in.readVLong();
    if (fieldSize == 0) {
      // empty DPU, no docs were added
      return null;
    }
    in.readVLong();
    in.readVLong();
    return in.slice("fieldInput", in.getFilePointer(), fieldSize);
  }

  /**
   * Get a slice of the PIM index IndexInput, pointing to the block table of the given DPU
   *
   * @param in the IndexInput for the PIM index
   * @param dpuId the dpu ID
   * @return a slice of IndexInput
   * @throws IOException if failed to read the IndexInput
   */
  public IndexInput getBlockTableFileInput(IndexInput in, int dpuId) throws IOException {

    seekToDpu(in, dpuId);
    skipSegmentInfo(in);
    long blockTableOffset = in.readVLong();
    if (blockTableOffset == 0) {
      // empty DPU, no docs were added
      return null;
    }
    long blockTableLength = in.readVLong() - blockTableOffset;
    in.readVLong();
    return in.slice("blockTableInput", in.getFilePointer() + blockTableOffset, blockTableLength);
  }

  /**
   * Get a slice of the PIM index IndexInput, pointing to the block list of the given DPU
   *
   * @param in the IndexInput for the PIM index
   * @param dpuId the dpu ID
   * @return a slice of IndexInput
   * @throws IOException if failed to read the IndexInput
   */
  public IndexInput getBlocksFileInput(IndexInput in, int dpuId) throws IOException {

    seekToDpu(in, dpuId);
    skipSegmentInfo(in);
    in.readVLong();
    long blockListOffset = in.readVLong();
    if (blockListOffset == 0) {
      // empty DPU, no docs were added
      return null;
    }
    long blockListSize = in.readVLong() - blockListOffset;
    return in.slice("blockListInput", in.getFilePointer() + blockListOffset, blockListSize);
  }

  /**
   * Get a slice of the PIM index IndexInput, pointing to the postings of the given DPU
   *
   * @param in the IndexInput for the PIM index
   * @param dpuId the dpu ID
   * @return a slice of IndexInput
   * @throws IOException if failed to read the IndexInput
   */
  public IndexInput getPostingsFileInput(IndexInput in, int dpuId) throws IOException {

    seekToDpu(in, dpuId);
    skipSegmentInfo(in);
    in.readVLong();
    in.readVLong();
    long postingsOffset = in.readVLong();
    if (postingsOffset == 0) {
      // empty DPU, no docs were added
      return null;
    }
    return in.slice(
        "postingsInput",
        in.getFilePointer() + postingsOffset,
        in.length() - (in.getFilePointer() + postingsOffset));
  }

  /**
   * Get an IndexInput for the PIM index
   *
   * @return the IndexInput object
   * @throws IOException if failed to open the IndexInput
   */
  public IndexInput getFileInput() throws IOException {

    String fileName =
        IndexFileNames.segmentFileName(
            "dpu_" + segmentCommitName, Integer.toString(numDpus), DPU_INDEX_COMPOUND_EXTENSION);
    return pimDir.openInput(fileName, IOContext.DEFAULT);
  }

  /**
   * Set the IndexInput object to point to data for the given DPU
   *
   * @param in the IndexInput for the PIM index
   * @param dpuId the DPU id
   * @return the size in bytes of the DPU index
   * @throws IOException if failed to read the IndexInput
   */
  public long seekToDpu(IndexInput in, int dpuId) throws IOException {

    // seek the right place where to find the information
    in.seek(0);
    int nbDpus = in.readVInt();
    assert nbDpus == numDpus;
    long dpuAddr = 0;
    long nextDpuAddr = in.length();
    boolean next = false;
    for (int i = 0; i < nbDpus; ++i) {
      if (next) {
        nextDpuAddr = in.readVLong();
        next = false;
      } else if (i == dpuId) {
        dpuAddr = in.readVLong();
        next = true;
      } else in.readVLong();
    }

    in.skipBytes(dpuAddr);

    if (next) return in.length() - in.getFilePointer();
    else return nextDpuAddr - dpuAddr;
  }

  public void writeExternal(IndexOutput out) throws IOException {

    out.writeInt(numDpus);
    out.writeInt(numSegments);
    out.writeInt(numDpuSegments);
    out.writeInt(numDocs);
    out.writeString(segmentCommitName);

    for (int i = 0; i < startDoc.length; ++i) {
      out.writeInt(startDoc[i]);
    }
    for (int i = 0; i < segmentId.length; ++i) {
      out.writeInt(segmentId[i].length);
      out.writeBytes(segmentId[i], segmentId[i].length);
    }
    for (int i = 0; i < segmentCommitId.length; ++i) {
      out.writeInt(segmentCommitId[i].length);
      out.writeBytes(segmentCommitId[i], segmentCommitId[i].length);
    }
  }

  public static PimIndexInfo readExternal(IndexInput in) throws IOException {

    PimIndexInfo info = new PimIndexInfo();
    info.numDpus = in.readInt();
    info.numSegments = in.readInt();
    info.numDpuSegments = in.readInt();
    info.numDocs = in.readInt();
    info.startDoc = new int[info.numSegments];
    info.segmentId = new byte[info.numSegments][];
    info.segmentCommitId = new byte[info.numSegments][];
    info.segmentCommitName = in.readString();

    for (int i = 0; i < info.startDoc.length; ++i) {
      info.startDoc[i] = in.readInt();
    }
    for (int i = 0; i < info.segmentId.length; ++i) {
      int size = in.readInt();
      info.segmentId[i] = new byte[size];
      in.readBytes(info.segmentId[i], 0, info.segmentId[i].length);
    }
    for (int i = 0; i < info.segmentCommitId.length; ++i) {
      int size = in.readInt();
      info.segmentCommitId[i] = new byte[size];
      in.readBytes(info.segmentCommitId[i], 0, info.segmentCommitId[i].length);
    }
    return info;
  }

  private void skipSegmentInfo(IndexInput in) throws IOException {
    in.readByte(); // nb DPU segments
    in.readByte(); // nb lucene segments
    int nbBytesMaxDoc = in.readVInt();
    in.skipBytes(nbBytesMaxDoc);
  }

  public static final String DPU_INDEX_COMPOUND_EXTENSION = "dpuc";
}
