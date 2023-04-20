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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extends {@link IndexWriter} to build the term indexes for each DPU after each commit.
 * The term indexes for DPUs are split by the Lucene internal docId, so that each DPU
 * receives the term index for an exclusive range of docIds, and for each index segment.
 */
public class PimIndexWriter extends IndexWriter {

  public static final String DPU_TERM_INDEX_EXTENSION = "dput";

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
            dpuTermIndexes.startField(fieldInfo);
            reader.getLiveDocs();//TODO: remove as useless. We are going to let Core Lucene handle the live docs.
            Terms terms = reader.terms(fieldInfo.name);
            if (terms != null) {
              int docCount = terms.getDocCount();
              System.out.println("  " + docCount + " docs");
              TermsEnum termsEnum = terms.iterator();
              while (termsEnum.next() != null) {
                // Send the term enum to DpuTermIndexes.
                // DpuTermIndexes separates the term docs according to the docId range split per DPU.
                dpuTermIndexes.writeTerm(termsEnum);
              }
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
        String indexName =
          IndexFileNames.segmentFileName(
            segmentCommitInfo.info.name, Integer.toString(i), DPU_TERM_INDEX_EXTENSION);
        if (fileNames.contains(indexName)) {
          getDirectory().deleteFile(indexName);
        }
        IndexOutput indexOutput = getDirectory().createOutput(indexName, IOContext.DEFAULT);
        termIndexes[i] = new DpuTermIndex(i, indexOutput, numFields);
      }
      posBuffer = ByteBuffersDataOutput.newResettableInstance();
    }

    void startField(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      for (DpuTermIndex termIndex : termIndexes) {
        termIndex.resetForNextField();
      }
      System.out.println("  field " + fieldInfo.name);
    }

    void writeTerm(TermsEnum termsEnum) throws IOException {
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

    private class DpuTermIndex {

      final int dpuIndex;
      final IndexOutput indexOutput;
      final Map<FieldInfo, Long> fieldPointer;
      boolean fieldWritten;
      boolean termWritten;
      int doc;
      long numTerms;

      DpuTermIndex(int dpuIndex, IndexOutput indexOutput, int numFields) {
        this.dpuIndex = dpuIndex;
        this.indexOutput = indexOutput;
        fieldPointer = new LinkedHashMap<>((int) (numFields / 0.75f) + 1);
      }

      void resetForNextField() {
        fieldWritten = false;
      }

      void resetForNextTerm() {
        termWritten = false;
        doc = 0;
      }

      void writeTermIfAbsent(BytesRef term) throws IOException {
        if (!termWritten) {
          if (!fieldWritten) {
            fieldPointer.put(fieldInfo, indexOutput.getFilePointer());
            fieldWritten = true;
          }
          //TODO: delta-prefix the term bytes (see UniformSplit).
          indexOutput.writeBytes(term.bytes, term.offset, term.length);
          termWritten = true;
          numTerms++;
        }
      }

      void close() throws IOException {
        indexOutput.close();
      }

      void writeDoc(int doc) throws IOException {
        int deltaDoc = doc - this.doc;
        assert deltaDoc > 0 || doc == 0 && deltaDoc == 0;
        indexOutput.writeVInt(deltaDoc);
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
            indexOutput.writeZInt(freq);
            indexOutput.writeByte((byte) numBytesPos);
          }
          case 2 -> {
            indexOutput.writeZInt(-freq);
            indexOutput.writeShort((short) numBytesPos);
          }
          default -> {
            indexOutput.writeZInt(0);
            indexOutput.writeVInt(freq);
            indexOutput.writeVLong(numBytesPos);
          }
        }
        posBuffer.copyTo(indexOutput);
        posBuffer.reset();
        System.out.println();
      }
    }
  }
}
