package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.hnsw.IntToIntFunction;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

public class MultiVectorOrdConfiguration {

  public static void writeStoredMeta(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput vectorData,
      int[] docOrdFreq,
      int[] docIds,
      int[] baseOrds,
      int[] nextBaseOrds)
      throws IOException {

    outputMeta.writeVInt(directMonotonicBlockShift); // block shift
    outputMeta.writeVInt(docOrdFreq.length); // number of values in docOrdFreq

    /* write docOrdFreq.
     * docOrdFreq[i] holds the cumulative count of vectors written before the
     * first vector of document at position i in a DISI over all documents with
     * vectors for the field. This is used to map document position to its base
     * ordinal for multivalued vector fields.
     */
    long docOrdFreqStart = vectorData.getFilePointer();
    outputMeta.writeLong(docOrdFreqStart);
    final DirectMonotonicWriter docOrdFreqWriter =
        DirectMonotonicWriter.getInstance(
            outputMeta, vectorData, docOrdFreq.length, directMonotonicBlockShift);
    for (int freq : docOrdFreq) {
      docOrdFreqWriter.add(freq);
    }
    docOrdFreqWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - docOrdFreqStart);

    final int numValues = docIds.length;
    outputMeta.writeVInt(numValues); // total number of vectors (ordinals)

    // write ordToDoc mapping
    long ordToDocStart = vectorData.getFilePointer();
    outputMeta.writeLong(ordToDocStart);
    final DirectMonotonicWriter ordToDocWriter =
        DirectMonotonicWriter.getInstance(
            outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      ordToDocWriter.add(docId);
    }
    ordToDocWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - ordToDocStart); // ordToDoc length

    // write ord to baseOrd mapping
    long baseOrdStart = vectorData.getFilePointer();
    outputMeta.writeLong(baseOrdStart);
    final DirectMonotonicWriter baseOrdWriter =
        DirectMonotonicWriter.getInstance(
            outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      baseOrdWriter.add(docId);
    }
    baseOrdWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - baseOrdStart); // baseOrd length

    // write nextBaseOrd i.e. the baseOrdinal for next docId with vectors.
    // this is used to compute the number of vectors per baseOrdinal
    long nextBaseOrdStart = vectorData.getFilePointer();
    outputMeta.writeLong(nextBaseOrdStart);
    final DirectMonotonicWriter nextBaseOrdWriter =
        DirectMonotonicWriter.getInstance(
            outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      nextBaseOrdWriter.add(docId);
    }
    nextBaseOrdWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - nextBaseOrdStart); // baseOrd length
  }

  public static MultiVectorOrdConfiguration fromStoredMeta(IndexInput inputMeta)
      throws IOException {
    final int blockShift = inputMeta.readVInt();

    final int docOrdFreqCount = inputMeta.readVInt();
    long docOrdFreqStart = inputMeta.readLong();
    DirectMonotonicReader.Meta docOrdFreqMeta =
        DirectMonotonicReader.loadMeta(inputMeta, docOrdFreqCount, blockShift);
    long docOrdFreqLength = inputMeta.readLong();

    final int numValues = inputMeta.readVInt();

    // ordToDoc mapping
    long ordToDocStart = inputMeta.readLong();
    DirectMonotonicReader.Meta ordToDocMeta =
        DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long ordToDocLength = inputMeta.readLong();

    long baseOrdStart = inputMeta.readLong();
    DirectMonotonicReader.Meta baseOrdMeta =
        DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long baseOrdLength = inputMeta.readLong();

    long nextBaseOrdStart = inputMeta.readLong();
    DirectMonotonicReader.Meta nextBaseOrdMeta =
        DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long nextBaseOrdLength = inputMeta.readLong();

    return new MultiVectorOrdConfiguration(
        docOrdFreqStart,
        docOrdFreqMeta,
        docOrdFreqLength,
        numValues,
        ordToDocStart,
        ordToDocMeta,
        ordToDocLength,
        baseOrdStart,
        baseOrdMeta,
        baseOrdLength,
        nextBaseOrdStart,
        nextBaseOrdMeta,
        nextBaseOrdLength);
  }

  private final long docOrdFreqStart, docOrdFreqLength;
  private final DirectMonotonicReader.Meta docOrdFreqMeta;
  private final int numValues;
  private final long ordToDocStart, ordToDocLength;
  private final DirectMonotonicReader.Meta ordToDocMeta;
  private final long baseOrdStart, baseOrdLength;
  private final DirectMonotonicReader.Meta baseOrdMeta;
  private final long nextBaseOrdStart, nextBaseOrdLength;
  private final DirectMonotonicReader.Meta nextBaseOrdMeta;

  public MultiVectorOrdConfiguration(
      long docOrdFreqStart,
      DirectMonotonicReader.Meta docOrdFreqMeta,
      long docOrdFreqLength,
      int numValues,
      long ordToDocStart,
      DirectMonotonicReader.Meta ordToDocMeta,
      long ordToDocLength,
      long baseOrdStart,
      DirectMonotonicReader.Meta baseOrdMeta,
      long baseOrdLength,
      long nextBaseOrdStart,
      DirectMonotonicReader.Meta nextBaseOrdMeta,
      long nextBaseOrdLength) {
    this.docOrdFreqStart = docOrdFreqStart;
    this.docOrdFreqMeta = docOrdFreqMeta;
    this.docOrdFreqLength = docOrdFreqLength;
    this.numValues = numValues;
    this.ordToDocStart = ordToDocStart;
    this.ordToDocMeta = ordToDocMeta;
    this.ordToDocLength = ordToDocLength;
    this.baseOrdStart = baseOrdStart;
    this.baseOrdMeta = baseOrdMeta;
    this.baseOrdLength = baseOrdLength;
    this.nextBaseOrdStart = nextBaseOrdStart;
    this.nextBaseOrdMeta = nextBaseOrdMeta;
    this.nextBaseOrdLength = nextBaseOrdLength;
  }

  public DirectMonotonicReader getDocIndexToBaseOrdReader(IndexInput dataIn) throws IOException {
    final RandomAccessInput slice = dataIn.randomAccessSlice(docOrdFreqStart, docOrdFreqLength);
    return DirectMonotonicReader.getInstance(docOrdFreqMeta, slice);
  }

  public DirectMonotonicReader getOrdToDocReader(IndexInput dataIn) throws IOException {
    final RandomAccessInput slice = dataIn.randomAccessSlice(ordToDocStart, ordToDocLength);
    return DirectMonotonicReader.getInstance(ordToDocMeta, slice);
  }

  public DirectMonotonicReader getBaseOrdReader(IndexInput dataIn) throws IOException {
    final RandomAccessInput slice = dataIn.randomAccessSlice(baseOrdStart, baseOrdLength);
    return DirectMonotonicReader.getInstance(baseOrdMeta, slice);
  }

  public DirectMonotonicReader getNextBaseOrdReader(IndexInput dataIn) throws IOException {
    final RandomAccessInput slice = dataIn.randomAccessSlice(nextBaseOrdStart, nextBaseOrdLength);
    return DirectMonotonicReader.getInstance(nextBaseOrdMeta, slice);
  }

  public int ordCount() {
    return numValues;
  }

  public static MultiVectorMaps createMultiVectorMaps(
      DocIdSetIterator disi, IntToIntFunction docIdToVectorCount, int ordCount, int docCount)
      throws IOException {
//    TODO: enable only after metadata format handles single-vector optimization
//    if (docCount == ordCount) {
//      // single valued vector field
//      return new MultiVectorMaps(docCount);
//    }
    int[] docOrdFreq = new int[docCount + 1];
    int[] ordToDocMap = new int[ordCount];
    int[] baseOrdMap = new int[ordCount];
    int[] nextBaseOrdMap = new int[ordCount];

    int ord = 0;
    int lastDocId = -1;
    int baseOrd = -1;
    int nextBaseOrd = -1;
    int idx = 0;
    for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
      int vectorCount = docIdToVectorCount.apply(doc);
      baseOrd = ord;
      nextBaseOrd = ord + vectorCount;
      docOrdFreq[idx++] = baseOrd;
      for (int i = 0; i < vectorCount; i++) {
        ordToDocMap[ord] = doc;
        baseOrdMap[ord] = baseOrd;
        nextBaseOrdMap[ord] = nextBaseOrd;
        ord++;
      }
    }
    assert ord == ordCount;
    assert idx == docCount;
    docOrdFreq[idx] = ordCount;
    return new MultiVectorMaps(
        true, docCount, ordCount, docOrdFreq, ordToDocMap, baseOrdMap, nextBaseOrdMap);
  }

  /**
   * MultiVectorMaps collect the metadata required to access ordinals and docIds with for
   * multivalued vector fields
   */
  public static record MultiVectorMaps(
      boolean isMultiVector,
      int docCount,
      int ordCount,
      int[] docOrdFreq,
      int[] ordToDocMap,
      int[] baseOrdMap,
      int[] nextBaseOrdMap) {
    public MultiVectorMaps(int docCount) {
      this(false, docCount, docCount, null, null, null, null);
    }
  }
  ;
}
