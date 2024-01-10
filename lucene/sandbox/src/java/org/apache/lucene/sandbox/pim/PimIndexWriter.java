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

import static org.apache.lucene.index.FieldInfos.getMergedFieldInfos;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.SmallFloat;

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

  // default parameters for BM25 similarity scoring
  static private final float k1 = 1.2f;
  static private final float b = 0.75f;
  /** Cache of decoded bytes. TODO extracted from BM25Similarity class */
  private static final float[] LENGTH_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++) {
      LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
    }
  }

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

  /**
   * Generates the PIM index from an existing Lucene index
   * @throws IOException
   */
  public void generatePimIndex() throws IOException {
    doAfterCommit();
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

    // Create a DpuTermIndexes that will build the term index for each DPU separately.
    try (DpuTermIndexes dpuTermIndexes = new DpuTermIndexes(segmentInfos, nbDocPerDPUSegment)) {

      TreeMap<String, Integer> fieldNormInverseQuantFactor = new TreeMap<>();
      try (IndexReader indexReader = DirectoryReader.open(getDirectory())) {
        List<LeafReaderContext> leaves = indexReader.leaves();

        for (FieldInfo fieldInfo : getMergedFieldInfos(indexReader)) {
          // Iterate on segments.
          // There will be a different term index sub-part per segment and per DPU.
          TermsEnum[] termsEnums = new TermsEnum[leaves.size()];
          int docCount = 0;
          int sumTotalTermFreq = 0;
          for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
            LeafReaderContext leafReaderContext = leaves.get(leafIdx);
            LeafReader reader = leafReaderContext.reader();
            /*SegmentCommitInfo segmentCommitInfo = segmentInfos.info(leafIdx);
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
                      + segmentCommitInfo.getDelCount());*/
            // For each field in the term index.
            // There will be a different term index sub-part per segment, per field, and per DPU.
            reader.getLiveDocs(); // TODO: remove as useless. We are going to let Core Lucene handle
            // the live docs.
            Terms terms = reader.terms(fieldInfo.name);
            if (terms != null) {
              int leafDocCount = terms.getDocCount();
              docCount += leafDocCount;
              sumTotalTermFreq += terms.getSumTotalTermFreq();
              if (DEBUG_INDEX) System.out.println("  " + leafDocCount + " docs");
              TermsEnum termsEnum = terms.iterator();
              termsEnums[leafIdx] = termsEnum;
            }
          }

          // Send the term enum to DpuTermIndexes.
          // DpuTermIndexes separates the term docs according to the docId range split per DPU.
          dpuTermIndexes.writeTerms(fieldInfo, new CompositeTermsEnum(termsEnums, segmentInfos),
                  (float) (sumTotalTermFreq / (double) docCount));

          // write the norms to be stored in PIM index
          int startDoc = 0;
          for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
            LeafReaderContext leafReaderContext = leaves.get(leafIdx);
            LeafReader reader = leafReaderContext.reader();
            NumericDocValues norms = reader.getNormValues(fieldInfo.name);
            if(norms != null)
              dpuTermIndexes.writeDocNorms(norms, startDoc);
            SegmentCommitInfo segmentCommitInfo = segmentInfos.info(leafIdx);
            startDoc += segmentCommitInfo.info.maxDoc();
          }

          fieldNormInverseQuantFactor.put(fieldInfo.name, dpuTermIndexes.normInverseQuantFactor);
          dpuTermIndexes.endField();
        }
      }
      // successfully updated the PIM index, register it
      writePimIndexInfo(segmentInfos, pimConfig.getNumDpuSegments(), fieldNormInverseQuantFactor);
    }
    if (DEBUG_INDEX)
      System.out.printf(
          "\nPIM index creation took %.2f secs\n", (System.nanoTime() - start) * 1e-9);
  }

  private void writePimIndexInfo(SegmentInfos segmentInfos, int numDpuSegments,
                                 TreeMap<String, Integer> fieldNormInverseQuantFactor) throws IOException {

    pimIndexInfo = new PimIndexInfo(pimDirectory, pimConfig.nbDpus, numDpuSegments,
            segmentInfos, fieldNormInverseQuantFactor);
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

  /**
   * A class used to enumerate the term enums of several index leaves in parallel. It takes as input
   * an array of TermsEnum objects for each leaf. The next() method returns the next term in the
   * order defined by BytesRef.compareTo method. When two or more term enums have the same term, the
   * term is returned several times, one time for each enum in which it is present. The postings
   * method returns the postings for the current enum, and the enum order is the one provided in the
   * TermsEnum array in the constructor.
   */
  private class CompositeTermsEnum {

    private int startDoc[];
    private TreeSet<LeafTermsEnum> sortedEnums;

    public CompositeTermsEnum(TermsEnum[] termsEnums, SegmentInfos segmentInfos)
        throws IOException {
      assert termsEnums.length != 0 && termsEnums.length == segmentInfos.size();
      this.startDoc = new int[segmentInfos.size()];
      for (int i = 0; i < startDoc.length - 1; ++i) {
        SegmentCommitInfo segmentCommitInfo = segmentInfos.info(i);
        startDoc[i + 1] = startDoc[i] + segmentCommitInfo.info.maxDoc();
      }
      sortedEnums = new TreeSet<>();
      for (int i = 0; i < termsEnums.length; ++i) {
        if (termsEnums[i] != null && termsEnums[i].next() != null) {
            sortedEnums.add(new LeafTermsEnum(termsEnums[i], i));
        }
      }
    }

    public BytesRef term() throws IOException {

      if (sortedEnums.size() == 0) return null;
      return sortedEnums.first().termsEnum.term();
    }

    public BytesRef next() throws IOException {

      assert sortedEnums.size() > 0;
      LeafTermsEnum te = sortedEnums.pollFirst();
      if (te.termsEnum.next() != null) sortedEnums.add(te);
      return term();
    }

    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {

      assert sortedEnums.size() > 0;
      return sortedEnums.first().termsEnum.postings(reuse, flags);
    }

    public int getStartDoc() {

      assert sortedEnums.size() > 0;
      return startDoc[sortedEnums.first().leaf];
    }

    private static class LeafTermsEnum implements Comparable<LeafTermsEnum> {
      TermsEnum termsEnum;
      int leaf;

      LeafTermsEnum(TermsEnum termsEnum, int leaf) {
        this.termsEnum = termsEnum;
        this.leaf = leaf;
      }

      @Override
      public int compareTo(LeafTermsEnum o) {
        try {
          int cmp = termsEnum.term().compareTo(o.termsEnum.term());
          if (cmp != 0) return cmp;
          return leaf - o.leaf;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private class DpuTermIndexes implements Closeable {

    private HashMap<Integer, Integer> dpuForDoc;
    String commitName;
    private final DpuTermIndex[] termIndexes;
    private PostingsEnum postingsEnum;
    private final ByteBuffersDataOutput posBuffer;
    private FieldInfo fieldInfo;
    private final int nbDocPerDpuSegment;
    PriorityQueue<DpuIndexSize> dpuPrQ;
    final byte[] normInverseCache;
    int normInverseQuantFactor;
    static final int blockSize = 8;

    DpuTermIndexes(SegmentInfos segmentsInfo, int nbDocPerDpuSegment) throws IOException {
      // TODO: The num docs per DPU could be
      // 1- per field
      // 2- adapted to the doc size, but it would require to keep the doc size info
      //    at indexing time, in a new file. Then here we could target (sumDocSizes / numDpus)
      //    per DPU.
      dpuForDoc = new HashMap<>();
      termIndexes = new DpuTermIndex[pimConfig.getNumDpus()];
      this.nbDocPerDpuSegment = nbDocPerDpuSegment;

      if (DEBUG_INDEX) {
        System.out.println("Directory " + getDirectory() + " --------------");
        for (String fileNames : getDirectory().listAll()) {
          System.out.println(fileNames);
        }
        System.out.println("---------");
      }

      commitName = "dpu_" + segmentsInfo.getSegmentsFileName();

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

      this.normInverseCache = new byte[256];
      this.normInverseQuantFactor = 1;
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

    void writeTerms(FieldInfo fieldInfo, CompositeTermsEnum termsEnum,
                    float avgFieldLength) throws IOException {

      this.fieldInfo = fieldInfo;
      computeNormInverseCache(avgFieldLength);
      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.resetForNextField();
      }

      BytesRef prevTerm = null;
      while (termsEnum.term() != null) {
        BytesRef term = termsEnum.term();
        if (prevTerm == null || prevTerm.compareTo(term) != 0) {
          for (DpuTermIndex termIndex : termIndexes) {
            termIndex.resetForNextTerm();
          }
        }
        prevTerm = BytesRef.deepCopyOf(term);

        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.POSITIONS);
        int startDoc = termsEnum.getStartDoc();
        int doc;
        while ((doc = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
          int absDoc = startDoc + doc;
          int dpuIndex = getDpuForDoc(absDoc);
          DpuTermIndex termIndex = termIndexes[dpuIndex];
          termIndex.writeTermIfAbsent(term);
          termIndex.writeDoc(absDoc);
        }
        termsEnum.next();
      }
    }

    void endField() {

      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.endField();
      }
    }

    void writeDocNorms(NumericDocValues norms, int startDoc) throws IOException {
      int doc;
      while((doc = norms.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        doc += startDoc;
        int dpu = getDpuForDoc(doc);
        termIndexes[dpu].writeDocNorm(doc, norms.longValue());
      }
    }

    private void computeNormInverseCache(float avgFieldLength) {

      // set the norm inverse cache to be used for this field
      float[] cache = new float[256];
      float max = 0.0f;
      for (int i = 0; i < 256; i++) {
        cache[i] = 1f / (k1 * ((1 - b) + b * LENGTH_TABLE[i] / avgFieldLength));
        if(i == 0 || cache[i] > max)
          max = cache[i];
      }
      normInverseQuantFactor = (int) (256.0f / max);
      for (int i = 0; i < 256; i++) {
        normInverseCache[i] = (byte) ((int)(Math.ceil(cache[i] * normInverseQuantFactor)) & 0xFF);
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
      if (!dpuForDoc.containsKey(doc)) {
        int dpuIndex = getDpuWithSmallerIndex();
        dpuForDoc.put(doc, dpuIndex);
        addDpuIndexSize(dpuIndex, termIndexes[dpuIndex].getIndexSize());
      }
      return dpuForDoc.get(doc);
    }

    //TODO since we go over fields sequentially, the load balancing will be based
    // on the first field, which in case of wikipedia can be the title and not the contents
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

      // count number of bytes needed to write lucene segments info for DPU
      ByteCountDataOutput cntOutput = new ByteCountDataOutput();
      for (int j = 1; j < pimIndexInfo.getNumSegments(); ++j)
        cntOutput.writeVInt(pimIndexInfo.getStartDoc(j));
      cntOutput.writeVInt(Integer.MAX_VALUE);
      int numBytesLuceneSegmentInfo = Math.toIntExact(cntOutput.getByteCount());

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
                  + new ByteCountDataOutput(numBytesLuceneSegmentInfo).getByteCount()
                  + numBytesLuceneSegmentInfo
                  + 1 // byte specifying number of lucene segments
                  + 1; // byte specifiying the number of DPU segments
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

        assert compoundOutput.getFilePointer() == dpuIndexAddr[i] + offset;

        // write number of DPU segments (log2)
        compoundOutput.writeByte(
            (byte)
                (Integer.BYTES * 8
                    - Integer.numberOfLeadingZeros(pimConfig.getNumDpuSegments() - 1)));

        // write number of lucene segments and associated startDoc
        if (pimIndexInfo.getNumSegments() >= Byte.MAX_VALUE)
          throw new IOException("supporting only 127 lucene segments");
        compoundOutput.writeByte((byte) pimIndexInfo.getNumSegments());
        compoundOutput.writeVInt(numBytesLuceneSegmentInfo);
        for (int j = 1; j < pimIndexInfo.getNumSegments(); ++j)
          compoundOutput.writeVInt(pimIndexInfo.getStartDoc(j));
        compoundOutput.writeVInt(Integer.MAX_VALUE);

        // TODO write nbDocs and the hash table for doc norms

        // write offset to each section
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
      HashMap<Integer, Integer> docNormsMap;

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
        this.docNormsMap = new HashMap<>();
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

      private void writeNormsHashTable() throws IOException {

        int nbDocs = docNormsMap.size();

        // if this field has no norms, just write skipInfo which is zero
        if(nbDocs == 0) {
          blocksTableOutput.writeVInt(0);
          return;
        }

        // hashSize is the next power of 2 after nbDocs
        // we need hashSize to be a power of 2 so that modulo is easily computable in DPU
        int hashSize = 1 << (32 - Integer.numberOfLeadingZeros(nbDocs - 1));
        short[] hashTable = new short[hashSize];
        short[] hashCount = new short[hashSize];

        // first loop, count the number of elements per hash to identify conflicts
        for(HashMap.Entry<Integer, Integer> entry : docNormsMap.entrySet()) {
          int hash = entry.getKey() & (hashSize - 1);
          hashCount[hash]++;
        }

        // second loop, write norms in hash table or in conflict list
        for(HashMap.Entry<Integer, Integer> entry : docNormsMap.entrySet()) {
          //System.out.println("key=" + entry.getKey() + " hash=" + (entry.getKey() & (hashSize - 1))
          //        + " hashSize=" + hashSize
          //        + " nbDocs=" + nbDocs + " " + (Integer.numberOfLeadingZeros(0)));
          //System.out.flush();
          int hash = entry.getKey() & (hashSize - 1);
          // Note: we encode a conflict with a value of 0
          // There is also the case (is it possible ?) that the norm is effectively 0
          // This will be handled as if there was a conflict
          assert entry.getValue() < 256;
          if(entry.getValue() == 0 || hashCount[hash] > 1) {
            // conflict, store the norm value at the end
            hashTable[hash] = 0;
            posBuffer.writeVInt(entry.getKey());
            posBuffer.writeByte((byte) (entry.getValue() & 0xFF));
          }
          else {
              hashTable[hash] = entry.getValue().shortValue();
              //System.out.println("doc=" + entry.getKey() + " norm=" + entry.getValue().byteValue());
          }
        }

        // hashSize is a power of 2, the number of trailing zeros should be the log2
        int hashSizeLog2 = Integer.numberOfTrailingZeros(hashSize);
        assert hashSizeLog2 < 256;
        int skipInfo = Math.toIntExact(1 + 256 + hashSize + posBuffer.size());
        //System.out.println("skipInfo=" + skipInfo + " hashSize=" + hashSize + "posBuffer=" + posBuffer.size());
        blocksTableOutput.writeVInt(skipInfo);
        blocksTableOutput.writeByte((byte) (hashSizeLog2 & 0xFF));
        // write norm inverse cache
        writeNormInverseCache();
        for(int i = 0; i < hashSize; ++i)
          blocksTableOutput.writeByte((byte) (hashTable[i] & 0xFF));
        if(posBuffer.size() != 0)
          posBuffer.copyTo(blocksTableOutput);
        posBuffer.reset();
      }

      private void writeNormInverseCache() throws IOException {

        for (int i = 0; i < 256; i++) {
          blocksTableOutput.writeByte(normInverseCache[i]);
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
          writeNormsHashTable();
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

      public void writeDocNorm(int doc, long norm) {
        docNormsMap.put(doc, ((byte) norm) & 0xFF);
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
