package org.apache.lucene.sandbox.pim;

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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Collections;

/**
 * Extends {@link IndexWriter} to build the term indexes for each DPU after each commit.
 * The term indexes for DPUs are split by the Lucene internal docId, so that each DPU
 * receives the term index for an exclusive range of docIds, and for each index segment.
 */
public class PimIndexWriter extends IndexWriter {

  public static final String DPU_TERM_FIELD_INDEX_EXTENSION = "dpuf";
  public static final String DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION = "dpub";
  public static final String DPU_TERM_BLOCK_INDEX_EXTENSION = "dput";
  public static final String DPU_TERM_POSTINGS_INDEX_EXTENSION = "dpup";

  private final PimConfig pimConfig;
  private final Directory pimDirectory;

  public PimIndexWriter(Directory directory, Directory pimDirectory,
                        IndexWriterConfig indexWriterConfig, PimConfig pimConfig)
    throws IOException {
    super(directory, indexWriterConfig);
    this.pimConfig = pimConfig;
    this.pimDirectory = pimDirectory;
  }

  @Override
  protected void doAfterCommit() throws IOException {
    SegmentInfos segmentInfos = SegmentInfos.readCommit(getDirectory(),
                                                        SegmentInfos.getLastCommitSegmentsFileName(getDirectory()));
    try (IndexReader indexReader = DirectoryReader.open(getDirectory())) {
      List<LeafReaderContext> leaves = indexReader.leaves();
      // Iterate on segments.
      // There will be a different term index sub-part per segment and per DPU.
      for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
        LeafReaderContext leafReaderContext = leaves.get(leafIdx);
        LeafReader reader = leafReaderContext.reader();
        SegmentCommitInfo segmentCommitInfo = segmentInfos.info(leafIdx);
        System.out.println("segment=" + new BytesRef(segmentCommitInfo.getId())
                             + " " + segmentCommitInfo.info.name
                             + " leafReader ord=" + leafReaderContext.ord
                             + " maxDoc=" + segmentCommitInfo.info.maxDoc()
                             + " delCount=" + segmentCommitInfo.getDelCount());
        // Create a DpuTermIndexes that will build the term index for each DPU separately.
        try (DpuTermIndexes dpuTermIndexes = new DpuTermIndexes(segmentCommitInfo, reader.getFieldInfos().size())) {
          for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            // For each field in the term index.
            // There will be a different term index sub-part per segment, per field, and per DPU.
            //dpuTermIndexes.startField(fieldInfo);
            reader.getLiveDocs();//TODO: remove as useless. We are going to let Core Lucene handle the live docs.
            Terms terms = reader.terms(fieldInfo.name);
            if (terms != null) {
              int docCount = terms.getDocCount();
              System.out.println("  " + docCount + " docs");
              TermsEnum termsEnum = terms.iterator();
              // Send the term enum to DpuTermIndexes.
              // DpuTermIndexes separates the term docs according to the docId range split per DPU.
              dpuTermIndexes.writeTerms(fieldInfo, termsEnum);
            }
          }
        }
      }
    }
  }

  private class DpuTermIndexes implements Closeable {

    private final int numDocsPerDpu;
    private final DpuTermIndex[] termIndexes;
    private PostingsEnum postingsEnum;
    private final ByteBuffersDataOutput posBuffer;
    private FieldInfo fieldInfo;
    static final int blockSize = 8;

    DpuTermIndexes(SegmentCommitInfo segmentCommitInfo, int numFields) throws IOException {
      //TODO: The num docs per DPU could be
      // 1- per field
      // 2- adapted to the doc size, but it would require to keep the doc size info
      //    at indexing time, in a new file. Then here we could target (sumDocSizes / numDpus)
      //    per DPU.
      numDocsPerDpu = Math.max((segmentCommitInfo.info.maxDoc()) / pimConfig.getNumDpus(),
              1);
      termIndexes = new DpuTermIndex[pimConfig.getNumDpus()];

      System.out.println("Directory " + getDirectory() + " --------------");
      for (String fileNames : getDirectory().listAll()) {
        System.out.println(fileNames);
      }
      System.out.println("---------");

      Set<String> fileNames = Set.of(pimDirectory.listAll());
      for (int i = 0; i < termIndexes.length; i++) {
        termIndexes[i] = new DpuTermIndex(i,
                createIndexOutput(segmentCommitInfo.info.name, DPU_TERM_FIELD_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(segmentCommitInfo.info.name, DPU_TERM_BLOCK_TABLE_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(segmentCommitInfo.info.name, DPU_TERM_BLOCK_INDEX_EXTENSION, i, fileNames),
                createIndexOutput(segmentCommitInfo.info.name, DPU_TERM_POSTINGS_INDEX_EXTENSION, i, fileNames),
                numFields, blockSize);
      }
      posBuffer = ByteBuffersDataOutput.newResettableInstance();
    }

    void writeTerms(FieldInfo fieldInfo, TermsEnum termsEnum) throws IOException {

      this.fieldInfo = fieldInfo;
      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.resetForNextField();
      }
      System.out.println("  field " + fieldInfo.name);

      while (termsEnum.next() != null) {
        BytesRef term = termsEnum.term();
        System.out.println("   " + term.utf8ToString());
        for (DpuTermIndex termIndex : termIndexes) {
          termIndex.resetForNextTerm();
        }
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.POSITIONS);
        int doc;
        while ((doc = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          int dpuIndex = Math.min(doc / numDocsPerDpu, pimConfig.getNumDpus() - 1);
          DpuTermIndex termIndex = termIndexes[dpuIndex];
          termIndex.writeTermIfAbsent(term);
          termIndex.writeDoc(doc);
        }
      }

      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.endField();
      }
    }

    @Override
    public void close() throws IOException {
      //TODO: group all the DPU term index files for one segment in a single compound file.
      //TODO: for each DPU, at the beginning, write the mapping fieldName -> pointer
      //TODO: for each field of a DPU, write the 7 or 15 terms and their offset to jump fast
      // when searching alphabetically for a specific term.
      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.close();
      }
    }

    private static int numBytesToEncode(long value) {
      return ((63 - Long.numberOfLeadingZeros(value)) >> 3) + 1;
    }

    private IndexOutput createIndexOutput(String name, String ext, int dpuIndex, Set<String> fileNames) throws IOException {
      String indexName =
              IndexFileNames.segmentFileName(
                      name, Integer.toString(dpuIndex), ext);
      if (fileNames.contains(indexName)) {
        pimDirectory.deleteFile(indexName);
      }
      return pimDirectory.createOutput(indexName, IOContext.DEFAULT);
    }

    /**
     * @class DpuTermIndex
     * Write a DPU index where terms and their
     * posting lists are stored in blocks of fixed size. A block table
     * contains the first term of each block and the address to jump to
     * the block. Hence, for search it is sufficient to find the right block
     * from the block table and to scan the block to find the term.
     **/
    private class DpuTermIndex {

      final int dpuIndex;
      final ArrayList<PimTreeBasedTermTable.Block> fieldList;
      boolean fieldWritten;
      boolean termWritten;
      int doc;
      long numTerms;
      long lastTermPostingAddress;
      /** number of terms stored in one block of the index.
       *  each block is scanned linearly for searching a term.
       */
      int blockCapacity;
      int currBlockSz;
      ArrayList<PimTreeBasedTermTable.Block> blockList;
      IndexOutput fieldTableOutput;
      IndexOutput blocksTableOutput;
      IndexOutput blocksOutput;
      IndexOutput postingsOutput;

      DpuTermIndex(int dpuIndex,
                   IndexOutput fieldTableOutput, IndexOutput blockTablesOutput,
                   IndexOutput blocksOutput, IndexOutput postingsOutput,
                   int numFields, int blockCapacity) {
        this.dpuIndex = dpuIndex;
        this.fieldList = new ArrayList<>();
        this.fieldWritten = false;
        this.termWritten = false;
        this.doc = 0;
        this.numTerms = 0;
        this.lastTermPostingAddress = -1L;
        this.blockCapacity = blockCapacity;
        this.currBlockSz = 0;
        this.blockList = new ArrayList<>();
        this.fieldTableOutput = fieldTableOutput;
        this.blocksTableOutput = blockTablesOutput;
        this.blocksOutput = blocksOutput;
        this.postingsOutput = postingsOutput;
      }

      void resetForNextField() {

        fieldWritten = false;
        lastTermPostingAddress = -1L;
      }
      void resetForNextTerm() {
        termWritten = false;
        doc = 0;
      }

      void writeTermIfAbsent(BytesRef term) throws IOException {

        // Do not write the bytes of the first term of a block
        // since they are already written in the block table
        if(currBlockSz != 0) {
          // call parent method
          if(!termWritten) {

            // write byte size of the last term postings (if any)
            if(this.lastTermPostingAddress >= 0)
              blocksOutput.writeVLong(postingsOutput.getFilePointer() - this.lastTermPostingAddress);
            this.lastTermPostingAddress = postingsOutput.getFilePointer();

            // first check if the current block exceeds its capacity
            // if yes, start a new block
            if(currBlockSz == blockCapacity) {
              if(blockList.size() > 0) {
                blockList.get(blockList.size() - 1).byteSize += blocksOutput.getFilePointer();
              }
              blockList.add(new PimTreeBasedTermTable.Block(BytesRef.deepCopyOf(term),
                      blocksOutput.getFilePointer(), -blocksOutput.getFilePointer()));
              currBlockSz = 0;
            }
            else {

              //TODO: delta-prefix the term bytes (see UniformSplit).
              blocksOutput.writeVInt(term.length);
              blocksOutput.writeBytes(term.bytes, term.offset, term.length);
            }

            termWritten = true;
            numTerms++;

            // write pointer to the posting list for this term (in posting file)
            blocksOutput.writeVLong(postingsOutput.getFilePointer());

            currBlockSz++;
          }
        }
        else {
          if(blockList.size() == 0) {
            // this is the first term of the first block
            // save the address and term
            blockList.add(new PimTreeBasedTermTable.Block(BytesRef.deepCopyOf(term),
                    blocksOutput.getFilePointer(), -blocksOutput.getFilePointer()));
          }
          // Do not write the first term
          // but the parent method should act as if it was written
          termWritten = true;
          currBlockSz++;

          // write byte size of the postings of the previous term (if any)
          if(this.lastTermPostingAddress >= 0)
            blocksOutput.writeVLong(postingsOutput.getFilePointer() - this.lastTermPostingAddress);
          this.lastTermPostingAddress = postingsOutput.getFilePointer();

          // write pointer to the posting list for this term (in posting file)
          blocksOutput.writeVLong(postingsOutput.getFilePointer());
        }

        if (!fieldWritten) {
          if(fieldList.size() > 0) {
            fieldList.get(fieldList.size() - 1).byteSize += blocksTableOutput.getFilePointer();
          }
          System.out.println("Add field:" + fieldInfo.name + " to field table addr:" +  blocksTableOutput.getFilePointer());
          fieldList.add(new PimTreeBasedTermTable.Block(new BytesRef(fieldInfo.getName()),
                  blocksTableOutput.getFilePointer(), -blocksTableOutput.getFilePointer()));
          fieldWritten = true;
        }
      }

      void writeDoc(int doc) throws IOException {
        int deltaDoc = doc - this.doc;
        assert deltaDoc > 0 || doc == 0 && deltaDoc == 0;
        postingsOutput.writeVInt(deltaDoc);
        this.doc = doc;
        int freq = postingsEnum.freq();
        assert freq > 0;
        System.out.print("    doc=" + doc + " dpu=" + dpuIndex + " freq=" + freq);
        int previousPos = 0;
        for (int i = 0; i < freq; i++) {
          // TODO: If freq is large (>= 128) then it could be possible to better
          //  encode positions (see PForUtil).
          int pos = postingsEnum.nextPosition();
          int deltaPos = pos - previousPos;
          previousPos = pos;
          posBuffer.writeVInt(deltaPos);
          System.out.print(" pos=" + pos);
        }
        long numBytesPos = posBuffer.size();
        // The sign bit of freq defines how the offset to the next doc is encoded:
        // freq > 0 => offset encoded on 1 byte
        // freq < 0 => offset encoded on 2 bytes
        // freq = 0 => write real freq and offset encoded on variable length
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
        System.out.println();
      }

      void writeBlockTable() throws IOException {

        if (blockList.size() == 0)
          return;

        blockList.get(blockList.size() - 1).byteSize += blocksOutput.getFilePointer();

        PimTreeBasedTermTable table = new PimTreeBasedTermTable(blockList);
        table.write(blocksTableOutput);

        // minimal TESTING of search in block table
        // TODO remove
        blockList.forEach((block) -> { assert table.SearchForBlock(block.term).term == block.term; });

        // reset internal parameters
        currBlockSz = 0;
        blockList = new ArrayList<>();
      }

      void writeFieldTable() throws IOException {

        if(fieldList.size() == 0)
          return;

        fieldList.get(fieldList.size() - 1).byteSize += blocksTableOutput.getFilePointer();

        // NOTE: the fields are not read in sorted order in Lucene index (why ?)
        // sorting them here, otherwise the binary search in block table cannot work
        Collections.sort(fieldList, (b1, b2) -> b1.term.compareTo(b2.term));

        PimTreeBasedTermTable table = new PimTreeBasedTermTable(fieldList);
        table.write(fieldTableOutput);
      }

      void endField() {

        // at the end of a field,
        // write the block table
        try {
          // write byte size of the postings of the last term (if any)
          if(this.lastTermPostingAddress >= 0)
            blocksOutput.writeVLong(postingsOutput.getFilePointer() - this.lastTermPostingAddress);

          //System.out.println("Block table dpu:" + dpuIndex + " field:" + fieldInfo.name);
          writeBlockTable();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      void close() throws IOException {

        //System.out.println("Writing field table dpu:" + dpuIndex + " nb fields " + fieldList.size());
        writeFieldTable();
        fieldTableOutput.close();
        blocksTableOutput.close();
        blocksOutput.close();
        postingsOutput.close();
      }
    }
  }
}
