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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Extends {@link IndexWriter} to build the term indexes for each DPU after each commit. The term
 * indexes for DPUs are split by the Lucene internal docId, so that each DPU receives the term index
 * for an exclusive set of docIds, and for each index segment.
 *
 * <p>The PIM index for one DPU consists in four parts:
 *
 * <p>1) A field table (BytesRefToDataBlockTreeMap object written to disk) The field table
 * associates to each field the address where to find the field's term block table
 *
 * <p>2) A list of term block table for each field (BytesRefToDataBlockTreeMap object written to
 * disk) The term block table is used to find the block where a particular term should be searched.
 * For instance, if we have the following sorted term set:
 *
 * <p>Apache, Lucene, Search, Table, Term, Tree
 *
 * <p>And this set is split into two blocks of size 3, then the term block table has 2 elements, one
 * for term "Apache" and one for term "Table". A search for the terms "Lucene" or "Search" will
 * return a pointer to "Apache", from where a linear scan can be done to find the right term in the
 * block.
 *
 * <p>3) A block list Each block is a list of terms of a small and configurable size, which is meant
 * to be scanned linearly after finding the block's start term in the block table. Each term in a
 * block is associated to an address pointing to the term's postings list.
 *
 * <p>4) The postings lists The postings list of a term contains the list of docIDs and positions
 * where the term appears. The docIDs and positions are delta-encoded.
 *
 * <p>The set of docIds assigned to a DPU are also split into a number of DPU segments specified by
 * the PimConfig. Each DPU segment is assigned a static range of docIds of constant size. For
 * instance, if there is a total of 1024 documents in the index, and the number of DPU segments is
 * 8, the first DPU segment is assigned the range [0,128[, the second the range [128,256[ etc. When
 * a docId is assigned to a particular DPU, it is put in the correct segment based on the range. For
 * instance, if the DPU 0 is assigned docIds 8, 72, 224, 756, it will have 3 non-empty segments, the
 * first one, the second one and the sixth one. The posting list of each term is separated in
 * segments, and the postings information for a term contains a pointer to jump to a given segment.
 * Using DPU segments enable different HW threads of the DPU to work on different DPU segments in
 * parallel for a given query, improving the overall load balancing.
 */
public class PimIndexWriter extends IndexWriter {

  public static final String DPU_TERM_FIELD_INDEX_EXTENSION = "dpuf";
  public static final String DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION = "dpub";
  public static final String DPU_TERM_BLOCK_INDEX_EXTENSION = "dput";
  public static final String DPU_TERM_POSTINGS_INDEX_EXTENSION = "dpup";
  public static final String DPU_INDEX_COMPOUND_EXTENSION = "dpuc";

  private final PimConfig pimConfig;
  private final Directory pimDirectory;
  private static final boolean ENABLE_STATS = false;
  private static final boolean DEBUG_INDEX = false;
  private PimIndexInfo pimIndexInfo;

  public PimIndexWriter(
      Directory directory,
      Directory pimDirectory,
      IndexWriterConfig indexWriterConfig,
      PimConfig pimConfig)
      throws IOException {
    super(directory, indexWriterConfig);
    this.pimConfig = pimConfig;
    this.pimDirectory = pimDirectory;
  }

  PimIndexInfo getPimIndexInfo() {
    return pimIndexInfo;
  }

  @Override
  protected void doAfterCommit() throws IOException {

    if (DEBUG_INDEX) System.out.println("Creating PIM index...");
    long start = System.nanoTime();
    SegmentInfos segmentInfos =
        SegmentInfos.readCommit(
            getDirectory(), SegmentInfos.getLastCommitSegmentsFileName(getDirectory()));

    int totalDoc = segmentInfos.totalMaxDoc();
    int nbDocPerDPUSegment = (int) Math.ceil((double) (totalDoc) / pimConfig.getNumDpuSegments());
    try (IndexReader indexReader = DirectoryReader.open(getDirectory())) {
      List<LeafReaderContext> leaves = indexReader.leaves();
      // Iterate on segments.
      // There will be a different term index sub-part per segment and per DPU.
      for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
        LeafReaderContext leafReaderContext = leaves.get(leafIdx);
        LeafReader reader = leafReaderContext.reader();
        SegmentCommitInfo segmentCommitInfo = segmentInfos.info(leafIdx);
        if (DEBUG_INDEX)
          System.out.println(
              "segment="
                  + new BytesRef(segmentCommitInfo.getId())
                  + " "
                  + segmentCommitInfo.info.name
                  + " leafReader ord="
                  + leafReaderContext.ord
                  + " maxDoc="
                  + segmentCommitInfo.info.maxDoc()
                  + " delCount="
                  + segmentCommitInfo.getDelCount());
        // Create a DpuTermIndexes that will build the term index for each DPU separately.
        try (DpuTermIndexes dpuTermIndexes =
            new DpuTermIndexes(segmentCommitInfo, nbDocPerDPUSegment)) {
          for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            // For each field in the term index.
            // There will be a different term index sub-part per segment, per field, and per DPU.
            reader.getLiveDocs(); // TODO: remove as useless. We are going to let Core Lucene handle
            // the live docs.
            Terms terms = reader.terms(fieldInfo.name);
            if (terms != null) {
              int docCount = terms.getDocCount();
              if (DEBUG_INDEX) System.out.println("  " + docCount + " docs");
              TermsEnum termsEnum = terms.iterator();
              // Send the term enum to DpuTermIndexes.
              // DpuTermIndexes separates the term docs according to the docId range split per DPU.
              dpuTermIndexes.writeTerms(fieldInfo, termsEnum);
            }
          }
        }
      }
      // successfully updated the PIM index, register it
      writePimIndexInfo(segmentInfos, pimConfig.getNumDpuSegments());
    }
    if (DEBUG_INDEX)
      System.out.printf(
          "\nPIM index creation took %.2f secs\n", (System.nanoTime() - start) * 1e-9);
  }

  private void writePimIndexInfo(SegmentInfos segmentInfos, int numDpuSegments) throws IOException {

    pimIndexInfo = new PimIndexInfo(pimDirectory, pimConfig.nbDpus, numDpuSegments, segmentInfos);
    Set<String> fileNames = Set.of(pimDirectory.listAll());
    if (fileNames.contains("pimIndexInfo")) {
      pimDirectory.deleteFile("pimIndexInfo");
    }
    IndexOutput infoOutput = pimDirectory.createOutput("pimIndexInfo", IOContext.DEFAULT);
    try {
      pimIndexInfo.writeExternal(infoOutput);
    } finally {
      infoOutput.close();
    }
  }

  private class DpuTermIndexes implements Closeable {

    private int dpuForDoc[];
    String commitName;
    private final DpuTermIndex[] termIndexes;
    private PostingsEnum postingsEnum;
    private final ByteBuffersDataOutput posBuffer;
    private FieldInfo fieldInfo;
    private final int nbDocPerDpuSegment;
    PriorityQueue<DpuIndexSize> dpuPrQ;
    static final int blockSize = 8;

    DpuTermIndexes(SegmentCommitInfo segmentCommitInfo, int nbDocPerDpuSegment) throws IOException {
      // TODO: The num docs per DPU could be
      // 1- per field
      // 2- adapted to the doc size, but it would require to keep the doc size info
      //    at indexing time, in a new file. Then here we could target (sumDocSizes / numDpus)
      //    per DPU.
      dpuForDoc = new int[segmentCommitInfo.info.maxDoc()];
      Arrays.fill(dpuForDoc, -1);
      termIndexes = new DpuTermIndex[pimConfig.getNumDpus()];
      this.nbDocPerDpuSegment = nbDocPerDpuSegment;

      if (DEBUG_INDEX) {
        System.out.println("Directory " + getDirectory() + " --------------");
        for (String fileNames : getDirectory().listAll()) {
          System.out.println(fileNames);
        }
        System.out.println("---------");
      }

      commitName = segmentCommitInfo.info.name;

      Set<String> fileNames = Set.of(pimDirectory.listAll());
      for (int i = 0; i < termIndexes.length; i++) {
        termIndexes[i] =
            new DpuTermIndex(
                i,
                createIndexOutput(DPU_TERM_FIELD_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(DPU_TERM_BLOCK_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(DPU_TERM_POSTINGS_INDEX_EXTENSION, i, fileNames),
                blockSize);
      }
      posBuffer = ByteBuffersDataOutput.newResettableInstance();

      dpuPrQ =
          new PriorityQueue<>(pimConfig.getNumDpus()) {
            @Override
            protected boolean lessThan(DpuIndexSize a, DpuIndexSize b) {
              return a.byteSize < b.byteSize;
            }
          };
      for (int i = pimConfig.getNumDpus() - 1; i >= 0; --i) {
        dpuPrQ.add(new DpuIndexSize(i, 0));
      }
    }

    private IndexOutput createIndexOutput(String ext, int dpuIndex, Set<String> fileNames)
        throws IOException {
      String indexName =
          IndexFileNames.segmentFileName(commitName, Integer.toString(dpuIndex), ext);
      if (fileNames.contains(indexName)) {
        pimDirectory.deleteFile(indexName);
      }
      return pimDirectory.createOutput(indexName, IOContext.DEFAULT);
    }

    private IndexInput createIndexInput(String ext, int dpuIndex) throws IOException {
      String indexName =
          IndexFileNames.segmentFileName(commitName, Integer.toString(dpuIndex), ext);
      IndexInput in = pimDirectory.openInput(indexName, IOContext.DEFAULT);
      in.seek(0);
      return in;
    }

    private void deleteDpuIndex(int dpuIndex) throws IOException {
      pimDirectory.deleteFile(
          IndexFileNames.segmentFileName(
              commitName, Integer.toString(dpuIndex), DPU_TERM_FIELD_INDEX_EXTENSION));
      pimDirectory.deleteFile(
          IndexFileNames.segmentFileName(
              commitName, Integer.toString(dpuIndex), DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION));
      pimDirectory.deleteFile(
          IndexFileNames.segmentFileName(
              commitName, Integer.toString(dpuIndex), DPU_TERM_BLOCK_INDEX_EXTENSION));
      pimDirectory.deleteFile(
          IndexFileNames.segmentFileName(
              commitName, Integer.toString(dpuIndex), DPU_TERM_POSTINGS_INDEX_EXTENSION));
    }

    void writeTerms(FieldInfo fieldInfo, TermsEnum termsEnum) throws IOException {

      this.fieldInfo = fieldInfo;
      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.resetForNextField();
      }

      while (termsEnum.next() != null) {
        BytesRef term = termsEnum.term();
        for (DpuTermIndex termIndex : termIndexes) {
          termIndex.resetForNextTerm();
        }
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.POSITIONS);
        int doc;
        while ((doc = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          int dpuIndex = getDpuForDoc(doc);
          DpuTermIndex termIndex = termIndexes[dpuIndex];
          termIndex.writeTermIfAbsent(term);
          termIndex.writeDoc(doc);
        }
      }

      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.endField();
      }
    }

    private static class DpuIndexSize {
      int dpuIndex;
      long byteSize;

      DpuIndexSize(int dpuIndex, long byteSize) {
        this.dpuIndex = dpuIndex;
        this.byteSize = byteSize;
      }
    }

    int getDpuForDoc(int doc) {
      if (dpuForDoc[doc] < 0) {
        int dpuIndex = getDpuWithSmallerIndex();
        dpuForDoc[doc] = dpuIndex;
        addDpuIndexSize(dpuIndex, termIndexes[dpuIndex].getIndexSize());
      }
      return dpuForDoc[doc];
    }

    int getDpuWithSmallerIndex() {

      DpuIndexSize dpu = dpuPrQ.pop();
      return dpu.dpuIndex;
    }

    void addDpuIndexSize(int dpuIndex, long byteSize) {
      dpuPrQ.add(new DpuIndexSize(dpuIndex, byteSize));
    }

    @Override
    public void close() throws IOException {

      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.close();
      }

      // On close, merge all DPU indexes into one compound file
      long numTerms = 0, numBlockTableBytes = 0, numBlockBytes = 0, numPostingBytes = 0;
      long numBytesIndex = 0;
      long[] dpuIndexAddr = new long[pimConfig.getNumDpus()];
      long[] dpuIndexBlockTableAddr = new long[pimConfig.getNumDpus()];
      long[] dpuIndexBlockListAddr = new long[pimConfig.getNumDpus()];
      long[] dpuIndexPostingsAddr = new long[pimConfig.getNumDpus()];
      dpuIndexAddr[0] = 0;

      for (DpuTermIndex termIndex : termIndexes) {
        if (ENABLE_STATS) {
          numTerms += termIndex.numTerms;
          numBlockTableBytes += termIndex.getInfo().blockTableSize;
          numBlockBytes += termIndex.getInfo().blockListSize;
          numPostingBytes += termIndex.getInfo().postingsSize;
          numBytesIndex += termIndex.getInfo().totalSize;
        }

        int i = termIndex.dpuIndex;
        dpuIndexBlockTableAddr[i] = termIndex.getInfo().fieldTableSize;
        dpuIndexBlockListAddr[i] = dpuIndexBlockTableAddr[i] + termIndex.getInfo().blockTableSize;
        dpuIndexPostingsAddr[i] = dpuIndexBlockListAddr[i] + termIndex.getInfo().blockListSize;

        if (i + 1 < dpuIndexAddr.length)
          dpuIndexAddr[i + 1] =
              dpuIndexAddr[i]
                  + termIndex.getInfo().totalSize
                  + new ByteCountDataOutput(dpuIndexBlockTableAddr[i]).getByteCount()
                  + new ByteCountDataOutput(dpuIndexBlockListAddr[i]).getByteCount()
                  + new ByteCountDataOutput(dpuIndexPostingsAddr[i]).getByteCount()
                  + 1; // byte specifiying the number of segments
      }

      Set<String> fileNames = Set.of(pimDirectory.listAll());
      IndexOutput compoundOutput =
          createIndexOutput(DPU_INDEX_COMPOUND_EXTENSION, pimConfig.getNumDpus(), fileNames);
      compoundOutput.writeVInt(pimConfig.getNumDpus());
      for (int i = 0; i < pimConfig.getNumDpus(); ++i) {
        compoundOutput.writeVLong(dpuIndexAddr[i]);
      }
      long offset = compoundOutput.getFilePointer();
      for (int i = 0; i < pimConfig.getNumDpus(); ++i) {
        // write offset to each section
        assert compoundOutput.getFilePointer() == dpuIndexAddr[i] + offset;
        compoundOutput.writeByte(
            (byte)
                (Integer.BYTES * 8
                    - Integer.numberOfLeadingZeros(pimConfig.getNumDpuSegments() - 1)));
        compoundOutput.writeVLong(dpuIndexBlockTableAddr[i]);
        compoundOutput.writeVLong(dpuIndexBlockListAddr[i]);
        compoundOutput.writeVLong(dpuIndexPostingsAddr[i]);
        IndexInput in = createIndexInput(DPU_TERM_FIELD_INDEX_EXTENSION, i);
        copyIndex(in, compoundOutput);
        in.close();
        in = createIndexInput(DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION, i);
        copyIndex(in, compoundOutput);
        in.close();
        in = createIndexInput(DPU_TERM_BLOCK_INDEX_EXTENSION, i);
        copyIndex(in, compoundOutput);
        in.close();
        in = createIndexInput(DPU_TERM_POSTINGS_INDEX_EXTENSION, i);
        copyIndex(in, compoundOutput);
        in.close();
        deleteDpuIndex(i);
      }
      compoundOutput.close();

      if (DEBUG_INDEX) {
        IndexInput in = createIndexInput(DPU_INDEX_COMPOUND_EXTENSION, pimConfig.getNumDpus());
        // skip number of DPUs and offsets
        in.readVInt();
        in.readVLong();
        System.out.println("\n------------ FULL PIM INDEX -------------");
        System.out.printf("[%d]=", in.length() - in.getFilePointer());
        while (in.getFilePointer() < in.length()) {
          System.out.printf("0x%x", in.readByte());
          if (in.getFilePointer() + 1 < in.length()) System.out.print(", ");
          else System.out.println();
          if ((in.getFilePointer() % 20) == 0) System.out.println();
        }
        in.close();
      }

      if (ENABLE_STATS) {
        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrintWriter p = new PrintWriter(statsOut, true);
        p.println("\n------------ FULL PIM INDEX STATS -------------");
        p.println("\n#TOTAL " + termIndexes.length + " DPUS");
        p.println("#terms for DPUS       : " + numTerms);
        p.println("#bytes block tables   : " + numBlockTableBytes);
        p.println("#bytes block files    : " + numBlockBytes);
        p.println("#bytes postings files : " + numPostingBytes);
        p.println("#bytes index          : " + numBytesIndex);
        System.out.println(statsOut);
      }
    }

    static final int nbBytesCopy = 1 << 10;
    static final byte[] bufferCopy = new byte[nbBytesCopy];

    static void copyIndex(IndexInput in, IndexOutput out) throws IOException {

      while (in.getFilePointer() + nbBytesCopy < in.length()) {
        in.readBytes(bufferCopy, 0, bufferCopy.length);
        out.writeBytes(bufferCopy, 0, bufferCopy.length);
      }
      if (in.getFilePointer() < in.length()) {
        int length = Math.toIntExact(in.length() - in.getFilePointer());
        in.readBytes(bufferCopy, 0, length);
        out.writeBytes(bufferCopy, 0, length);
      }
    }

    static ByteCountDataOutput outByteCount = new ByteCountDataOutput();

    private static int numBytesToEncode(int value) {
      try {
        outByteCount.reset();
        outByteCount.writeVInt(value);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return Math.toIntExact(outByteCount.getByteCount());
    }

    /**
     * @class DpuTermIndex Write a DPU index where terms and their posting lists are stored in
     *     blocks of fixed size. A block table contains the first term of each block and the address
     *     to jump to the block. Hence, for search it is sufficient to find the right block from the
     *     block table and to scan the block to find the term.
     */
    private class DpuTermIndex {

      final int dpuIndex;
      final ArrayList<BytesRefToDataBlockTreeMap.Block> fieldList;
      boolean fieldWritten;
      boolean termWritten;
      int doc;
      long numTerms;
      long lastTermPostingAddress;
      int nbDpuSegmentsWritten;
      final ByteBuffersDataOutput segmentSkipBuffer;
      /**
       * number of terms stored in one block of the index. each block is scanned linearly for
       * searching a term.
       */
      int blockCapacity;

      int currBlockSz;
      ArrayList<BytesRefToDataBlockTreeMap.Block> blockList;
      IndexOutput fieldTableOutput;
      IndexOutput blocksTableOutput;
      IndexOutput blocksOutput;
      IndexOutput postingsOutput;

      ByteArrayOutputStream statsOut;
      IndexInfo info;

      DpuTermIndex(
          int dpuIndex,
          IndexOutput fieldTableOutput,
          IndexOutput blockTablesOutput,
          IndexOutput blocksOutput,
          IndexOutput postingsOutput,
          int blockCapacity) {
        this.dpuIndex = dpuIndex;
        this.fieldList = new ArrayList<>();
        this.fieldWritten = false;
        this.termWritten = false;
        this.doc = 0;
        this.numTerms = 0;
        this.lastTermPostingAddress = -1L;
        this.nbDpuSegmentsWritten = 0;
        this.segmentSkipBuffer = ByteBuffersDataOutput.newResettableInstance();
        this.blockCapacity = blockCapacity;
        this.currBlockSz = 0;
        this.blockList = new ArrayList<>();
        this.fieldTableOutput = fieldTableOutput;
        this.blocksTableOutput = blockTablesOutput;
        this.blocksOutput = blocksOutput;
        this.postingsOutput = postingsOutput;
        if (ENABLE_STATS) {
          this.statsOut = new ByteArrayOutputStream();
        }
        this.info = null;
      }

      void resetForNextField() {

        fieldWritten = false;
        lastTermPostingAddress = -1L;
        nbDpuSegmentsWritten = 0;
      }

      void resetForNextTerm() {
        termWritten = false;
        doc = 0;
      }

      void writeTermIfAbsent(BytesRef term) throws IOException {

        // Do not write the bytes of the first term of a block
        // since they are already written in the block table
        if (currBlockSz != 0) {
          if (!termWritten) {

            // write byte size of the last term's last segment postings (if any)
            // and write the segment skip info
            finishDpuSegmentsInfo();
            this.lastTermPostingAddress = postingsOutput.getFilePointer();

            // first check if the current block exceeds its capacity
            // if yes, start a new block
            if (currBlockSz == blockCapacity) {
              blockList.add(
                  new BytesRefToDataBlockTreeMap.Block(
                      BytesRef.deepCopyOf(term), blocksOutput.getFilePointer()));
              currBlockSz = 0;
            } else {

              // TODO: delta-prefix the term bytes (see UniformSplit).
              blocksOutput.writeVInt(term.length);
              blocksOutput.writeBytes(term.bytes, term.offset, term.length);
            }

            termWritten = true;
            numTerms++;

            // write pointer to the posting list for this term (in posting file)
            blocksOutput.writeVLong(postingsOutput.getFilePointer());
            currBlockSz++;
          }
        } else {
          if (blockList.size() == 0) {
            // this is the first term of the first block
            // save the address and term
            blockList.add(
                new BytesRefToDataBlockTreeMap.Block(
                    BytesRef.deepCopyOf(term), blocksOutput.getFilePointer()));
          }
          // Do not write the first term
          // but the parent method should act as if it was written
          termWritten = true;
          currBlockSz++;

          // write byte size of the last term's last segment postings (if any)
          // and write the segment skip info
          finishDpuSegmentsInfo();
          this.lastTermPostingAddress = postingsOutput.getFilePointer();

          // write pointer to the posting list for this term (in posting file)
          blocksOutput.writeVLong(postingsOutput.getFilePointer());
        }

        if (!fieldWritten) {
          fieldList.add(
              new BytesRefToDataBlockTreeMap.Block(
                  new BytesRef(fieldInfo.getName()), blocksTableOutput.getFilePointer()));
          fieldWritten = true;
        }
      }

      private int getDpuSegment(int doc) {
        return doc / nbDocPerDpuSegment;
      }

      void writeDoc(int doc) throws IOException {

        int lastDocDpuSegment = getDpuSegment(this.doc);
        int newDocDpuSegment = getDpuSegment(doc);

        if (lastDocDpuSegment != newDocDpuSegment) {
          // should have already written a term when reaching here
          assert (this.lastTermPostingAddress >= 0);
          segmentSkipBuffer.writeVLong(
              postingsOutput.getFilePointer() - this.lastTermPostingAddress);
          this.lastTermPostingAddress = postingsOutput.getFilePointer();
          nbDpuSegmentsWritten++;
          lastDocDpuSegment++;

          // write size of zero for all segments that were skipped
          while (lastDocDpuSegment != newDocDpuSegment) {
            segmentSkipBuffer.writeVLong(0);
            lastDocDpuSegment++;
            nbDpuSegmentsWritten++;
          }

          // write the new doc without delta encoding
          this.doc = 0;
        }
        int deltaDoc = doc - this.doc;
        assert deltaDoc > 0 || doc == 0 && deltaDoc == 0;
        postingsOutput.writeVInt(deltaDoc);
        this.doc = doc;
        int freq = postingsEnum.freq();
        assert freq > 0;
        // System.out.print("    doc=" + doc + " dpu=" + dpuIndex + " freq=" + freq);
        int previousPos = 0;
        for (int i = 0; i < freq; i++) {
          // TODO: If freq is large (>= 128) then it could be possible to better
          //  encode positions (see PForUtil).
          int pos = postingsEnum.nextPosition();
          int deltaPos = pos - previousPos;
          previousPos = pos;
          posBuffer.writeVInt(deltaPos);
          // System.out.print(" pos=" + pos);
        }
        int numBytesPos = Math.toIntExact(posBuffer.size());
        // The sign bit of freq defines how the offset to the next doc is encoded:
        // freq > 0 => offset encoded on 1 byte
        // freq < 0 => offset encoded on 2 bytes
        // freq = 0 => write real freq and offset encoded on variable length
        assert freq > 0;
        // System.out.print(" numBytesPos=" + numBytesPos + " numBytesToEncode=" +
        // numBytesToEncode(numBytesPos));
        switch (numBytesToEncode(numBytesPos)) {
          case 1 -> {
            postingsOutput.writeZInt(freq);
            postingsOutput.writeByte((byte) numBytesPos);
          }
          case 2 -> {
            postingsOutput.writeZInt(-freq);
            postingsOutput.writeShort((short) numBytesPos);
          }
          default -> {
            postingsOutput.writeZInt(0);
            postingsOutput.writeVInt(freq);
            postingsOutput.writeVLong(numBytesPos);
          }
        }
        posBuffer.copyTo(postingsOutput);
        posBuffer.reset();
        // System.out.println();
      }

      void finishDpuSegmentsInfo() throws IOException {
        if (this.lastTermPostingAddress >= 0) {
          segmentSkipBuffer.writeVLong(
              postingsOutput.getFilePointer() - this.lastTermPostingAddress);
          nbDpuSegmentsWritten++;
          for (int i = nbDpuSegmentsWritten; i < pimConfig.getNumDpuSegments(); i++)
            segmentSkipBuffer.writeVLong(0);
          blocksOutput.writeVLong(segmentSkipBuffer.size());
          segmentSkipBuffer.copyTo(blocksOutput);

          segmentSkipBuffer.reset();
          nbDpuSegmentsWritten = 0;
        }
      }

      void writeBlockTable() throws IOException {

        if (blockList.size() == 0) return;

        BytesRefToDataBlockTreeMap table =
            new BytesRefToDataBlockTreeMap(
                new BytesRefToDataBlockTreeMap.BlockList(blockList, blocksOutput.getFilePointer()));
        table.write(blocksTableOutput);

        if (ENABLE_STATS) {

          PrintWriter p = new PrintWriter(statsOut, true);

          if (fieldList.size() < 2) {
            p.println("\n------------- DPU" + dpuIndex + " PIM INDEX STATS -------------");
          }
          p.println(
              "#terms block table    : "
                  + blockList.size()
                  + " (field "
                  + fieldInfo.getName()
                  + ")");
          p.println("#bytes block table    : " + blocksTableOutput.getFilePointer());
          p.println("#bytes block file     : " + blocksOutput.getFilePointer());
          p.println("#bytes postings file  : " + postingsOutput.getFilePointer());
        }

        // reset internal parameters
        currBlockSz = 0;
        blockList = new ArrayList<>();
      }

      void writeFieldTable() throws IOException {

        if (fieldList.size() == 0) return;

        // NOTE: the fields are not read in sorted order in Lucene index
        // sorting them here, otherwise the binary search in block table cannot work
        Collections.sort(fieldList, (b1, b2) -> b1.bytesRef.compareTo(b2.bytesRef));

        BytesRefToDataBlockTreeMap table =
            new BytesRefToDataBlockTreeMap(
                new BytesRefToDataBlockTreeMap.BlockList(
                    fieldList, blocksTableOutput.getFilePointer()));
        table.write(fieldTableOutput);

        if (ENABLE_STATS) {
          PrintWriter p = new PrintWriter(statsOut, true);
          p.println("#fields               : " + fieldList.size());
          p.println("#bytes field table    : " + fieldTableOutput.getFilePointer());
        }
      }

      void endField() {

        // at the end of a field,
        // write the block table
        try {
          // write byte size of the postings of the last term (if any) and write the segment skip
          // info
          finishDpuSegmentsInfo();
          writeBlockTable();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      void close() throws IOException {

        // System.out.println("Writing field table dpu:" + dpuIndex + " nb fields " +
        // fieldList.size());
        writeFieldTable();
        if (ENABLE_STATS) {
          PrintWriter p = new PrintWriter(statsOut, true);
          p.println("\n#TOTAL DPU" + dpuIndex);
          p.println("#terms for DPU        : " + numTerms);
          p.println("#bytes block table    : " + blocksTableOutput.getFilePointer());
          p.println("#bytes block file     : " + blocksOutput.getFilePointer());
          p.println("#bytes postings file  : " + postingsOutput.getFilePointer());
          p.println("#bytes index          : " + getIndexSize());
          System.out.println(statsOut.toString());
        }
        info = new IndexInfo(this);
        fieldTableOutput.close();
        blocksTableOutput.close();
        blocksOutput.close();
        postingsOutput.close();
      }

      public static class IndexInfo {
        long fieldTableSize;
        long blockTableSize;
        long blockListSize;
        long postingsSize;
        long totalSize;

        IndexInfo(DpuTermIndex termIndex) {
          this.fieldTableSize = termIndex.fieldTableOutput.getFilePointer();
          this.blockTableSize = termIndex.blocksTableOutput.getFilePointer();
          this.blockListSize = termIndex.blocksOutput.getFilePointer();
          this.postingsSize = termIndex.postingsOutput.getFilePointer();
          this.totalSize =
              this.fieldTableSize + this.blockTableSize + this.blockListSize + this.postingsSize;
        }
      }

      IndexInfo getInfo() {
        return info;
      }

      private long getIndexSize() {
        return fieldTableOutput.getFilePointer()
            + blocksTableOutput.getFilePointer()
            + blocksOutput.getFilePointer()
            + postingsOutput.getFilePointer();
      }
    }
  }
}
