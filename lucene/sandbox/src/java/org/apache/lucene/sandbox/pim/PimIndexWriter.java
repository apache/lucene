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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;

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

  public PimIndexWriter(Directory directory, IndexWriterConfig indexWriterConfig, PimConfig pimConfig)
    throws IOException {
    super(directory, indexWriterConfig);
    this.pimConfig = pimConfig;
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
    static final int blockSize = 32;

    DpuTermIndexes(SegmentCommitInfo segmentCommitInfo, int numFields) throws IOException {
      //TODO: The num docs per DPU could be
      // 1- per field
      // 2- adapted to the doc size, but it would require to keep the doc size info
      //    at indexing time, in a new file. Then here we could target (sumDocSizes / numDpus)
      //    per DPU.
      numDocsPerDpu = Math.max((segmentCommitInfo.info.maxDoc() - segmentCommitInfo.getDelCount()) / pimConfig.getNumDpus(),
              1);
      termIndexes = new DpuTermIndex[pimConfig.getNumDpus()];

      System.out.println("Directory " + getDirectory() + " --------------");
      for (String fileNames : getDirectory().listAll()) {
        System.out.println(fileNames);
      }
      System.out.println("---------");

      Set<String> fileNames = Set.of(getDirectory().listAll());
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
        getDirectory().deleteFile(indexName);
      }
      return getDirectory().createOutput(indexName, IOContext.DEFAULT);
    }

    /**
     * @class TreeBasedTermTable
     * Write a term table as a tree structure, so that
     * binary search for a particular term can be performed.
     * Each term is associated to a pointer/offset (long).
     **/
    private class TreeBasedTermTable {

      TreeNode root;

      public static class Block {
        BytesRef term;
        Long address;
        Long byteSize;

        Block(BytesRef term, Long address, long byteSize) {
          this.term = term;
          this.address = address;
          this.byteSize = byteSize;
        }
      }

      /**
       * @param minTreeNodeSz the minimal size of a node in the tree
       * When this size is reached, binary search stops and terms must be scanned sequentially
       **/
      TreeBasedTermTable(ArrayList<Block> blockList, int minTreeNodeSz) {
        // build a BST for the terms
        root = buildBSTreeRecursive(blockList, minTreeNodeSz);
      }

      TreeBasedTermTable(ArrayList<Block> blockList) {
        this(blockList, 0);
      }

      void write(IndexOutput indexOutput) throws IOException {
        writeTreePreOrder(root, indexOutput);
      }

      private void writeTreePreOrder(TreeNode node, IndexOutput indexOutput) throws IOException {

        if(node == null) return;
        node.write(indexOutput);
        writeTreePreOrder(node.leftChild, indexOutput);
        writeTreePreOrder(node.rightChild, indexOutput);
      }

      private TreeNode buildBSTreeRecursive(List<Block> blockList, int minTreeNodeSz) {

        if(blockList.isEmpty()) return null;

        if((blockList.size() > 1) && (blockList.size() <= minTreeNodeSz)) {
          // stop here, create a CompoundTreeNode
          CompoundTreeNode node = new CompoundTreeNode(blockList.get(0));
          for(int i = 1; i < blockList.size(); ++i) {
            node.AddSibling(new TreeNode(blockList.get(i)));
          }
          return node;
        }

        // get median term and create a tree node for it
        int mid = blockList.size() / 2;
        TreeNode node = new TreeNode(blockList.get(mid));

        if(blockList.size() > 1) {
          node.leftChild = buildBSTreeRecursive(blockList.subList(0, mid), minTreeNodeSz);
          if(node.leftChild != null)
            node.totalByteSize += node.leftChild.totalByteSize;
        }

        if(blockList.size() > 2) {
          node.rightChild = buildBSTreeRecursive(blockList.subList(mid + 1, blockList.size()), minTreeNodeSz);
          if(node.rightChild != null)
            node.totalByteSize += node.rightChild.totalByteSize;
        }

        // update node's subtree byte size with the node size post-order
        // this ensures that the offset to the right child is already known and
        // correctly accounted for in node.getByteSize()
        node.totalByteSize += node.getByteSize();

        return node;
      }

      private class TreeNode {

        BytesRef term;
        Long blockAddress;
        Long byteSize;
        int totalByteSize;
        TreeNode leftChild;
        TreeNode rightChild;

        TreeNode(Block block) {
          this.term = block.term;
          this.blockAddress = block.address;
          this.byteSize = block.byteSize;
          this.totalByteSize = 0;
          this.leftChild = null;
          this.rightChild = null;
        }

        /**
         * Write this node to a DataOutput object
         **/
        void write(DataOutput output) throws IOException {

          // TODO implement an option to align on 4B
          // It will be faster for the DPU to search, at the expense of more memory space
          // TODO do we need to write the length before the term bytes ? Otherwise can we figure it out ?
          output.writeBytes(term.bytes, term.offset, term.length);
          output.writeVInt(getRightChildOffset());

          //write address of the block and its byte size
          output.writeVLong(blockAddress);
          output.writeVLong(byteSize);
          if(output instanceof IndexOutput)
            System.out.println("Tree term: " + term.utf8ToString() + " addr:" + blockAddress);
        }

        /**
         * @return the number of bytes used when writing this node
         **/
        int getByteSize() {
          ByteCountDataOutput out = new ByteCountDataOutput();
          try {
            write(out);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return (int) out.getByteCount().longValue();
        }

        private int getRightChildOffset() {

          // Note: use the least significant bit of right child offset to
          // encode whether the left child is null or not. If not null, the
          // left child is the consecutive node, so no need to write its address
          int rightChildAddress = 0;
          if (rightChild != null) {
            // the right child offset is the byte size of the left subtree
            assert (leftChild.totalByteSize & 0xC0000000) == 0;
            rightChildAddress = leftChild.totalByteSize << 2;
          }
          if (leftChild != null) rightChildAddress++;
          return rightChildAddress;
        }

        /**
         * A dummy DataOutput class that just counts how many bytes
         * have been written.
         **/
        private class ByteCountDataOutput extends DataOutput {
          private Long byteCount;

          ByteCountDataOutput() {
            this.byteCount = 0L;
          }
          @Override
          public void writeByte(byte b) throws IOException { byteCount++; }
          @Override
          public void writeBytes(byte[] b, int offset, int length) throws IOException { byteCount += length; }
          Long getByteCount() { return byteCount; }
        }
      }

      /**
       * @CompoundTreeNode a node for a list of terms that are written sequentially in the index file
       **/
      private class CompoundTreeNode extends TreeNode {

        private int nbNodes;
        TreeNode lastSibling;

        CompoundTreeNode(Block block) {
          super(block);
          nbNodes = 0;
          lastSibling = null;
        }

        void AddSibling(TreeNode node) {

          if (leftChild == null) {
            leftChild = node;
          } else {
            lastSibling.leftChild = node;
          }
          lastSibling = node;
          nbNodes++;
        }

        @Override
        void write(DataOutput output) throws IOException {

          output.writeBytes(term.bytes, term.offset, term.length);
          output.writeVInt((nbNodes << 2) + 2);

          //write address of the block and its byte size
          output.writeVLong(blockAddress);
          output.writeVLong(byteSize);
          if(output instanceof IndexOutput)
            System.out.println("Tree term (compound node): " + term.utf8ToString() + " addr:" + blockAddress);

          TreeNode node = leftChild;
          while(node != null) {
            output.writeBytes(term.bytes, term.offset, term.length);
            output.writeVLong(blockAddress);
            output.writeVLong(byteSize);
            if(output instanceof IndexOutput)
              System.out.println("Tree term(compound node): " + term.utf8ToString() + " addr:" + blockAddress);

            node = node.leftChild;
          }
        }
      }
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
      final ArrayList<TreeBasedTermTable.Block> fieldList;
      boolean fieldWritten;
      boolean termWritten;
      int doc;
      long numTerms;
      /** number of terms stored in one block of the index.
       *  each block is scanned linearly for searching a term.
       */
      int blockCapacity;
      int currBlockSz;
      ArrayList<TreeBasedTermTable.Block> blockList;
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
            // first check if the current block exceeds its capacity
            // if yes, start a new block
            if(currBlockSz == blockCapacity) {
              if(blockList.size() > 0) {
                blockList.get(blockList.size() - 1).byteSize += blocksOutput.getFilePointer();
              }
              blockList.add(new TreeBasedTermTable.Block(BytesRef.deepCopyOf(term),
                      blocksOutput.getFilePointer(), -blocksOutput.getFilePointer()));
              currBlockSz = 0;
            }

            //TODO: delta-prefix the term bytes (see UniformSplit).
            blocksOutput.writeBytes(term.bytes, term.offset, term.length);
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
            blockList.add(new TreeBasedTermTable.Block(BytesRef.deepCopyOf(term),
                    blocksOutput.getFilePointer(), -blocksOutput.getFilePointer()));
          }
          // Do not write the first term
          // but the parent method should act as if it was written
          termWritten = true;
          currBlockSz++;
          // write pointer to the posting list for this term (in posting file)
          blocksOutput.writeVLong(postingsOutput.getFilePointer());
        }

        if (!fieldWritten) {
          if(fieldList.size() > 0) {
            fieldList.get(fieldList.size() - 1).byteSize += blocksTableOutput.getFilePointer();
          }
          fieldList.add(new TreeBasedTermTable.Block(new BytesRef(fieldInfo.getName()),
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

        TreeBasedTermTable table = new TreeBasedTermTable(blockList);
        table.write(blocksTableOutput);

        // reset internal parameters
        currBlockSz = 0;
        blockList = new ArrayList<>();
      }

      void writeFieldTable() throws IOException {

        if(fieldList.size() == 0)
          return;

        fieldList.get(fieldList.size() - 1).byteSize += blocksTableOutput.getFilePointer();

        TreeBasedTermTable table = new TreeBasedTermTable(fieldList);
        table.write(fieldTableOutput);
      }

      void endField() {

        // at the end of a field,
        // write the block table
        try {
          System.out.println("Block table dpu:" + dpuIndex + " field:" + fieldInfo.name);
          writeBlockTable();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      void close() throws IOException {
        System.out.println("Writing field table dpu:" + dpuIndex + " nb fields " + fieldList.size());
        writeFieldTable();
        fieldTableOutput.close();
        blocksTableOutput.close();
        blocksOutput.close();
        postingsOutput.close();
      }
    }
  }
}
