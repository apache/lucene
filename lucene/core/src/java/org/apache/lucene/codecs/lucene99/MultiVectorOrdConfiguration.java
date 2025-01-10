package org.apache.lucene.codecs.lucene99;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;

public class MultiVectorOrdConfiguration {

  public static void writeStoredMeta(
      int directMonotonicBlockShift,
      IndexOutput outputMeta,
      IndexOutput vectorData,
      int[] docIds,
      int[] baseOrds,
      int[] nextBaseOrds)
    throws IOException {

    outputMeta.writeVInt(directMonotonicBlockShift); // block shift
    final int numValues = docIds.length;
    outputMeta.writeVInt(numValues); // total number of vectors (ordinals)

    // write ordToDoc mapping
    long ordToDocStart = vectorData.getFilePointer();
    outputMeta.writeLong(ordToDocStart);
    final DirectMonotonicWriter ordToDocWriter = DirectMonotonicWriter.getInstance(outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      ordToDocWriter.add(docId);
    }
    ordToDocWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - ordToDocStart); // ordToDoc length

    // write ord to baseOrd mapping
    long baseOrdStart = vectorData.getFilePointer();
    outputMeta.writeLong(baseOrdStart);
    final DirectMonotonicWriter baseOrdWriter = DirectMonotonicWriter.getInstance(outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      baseOrdWriter.add(docId);
    }
    baseOrdWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - baseOrdStart); // baseOrd length

    // write nextBaseOrd i.e. the baseOrdinal for next docId with vectors.
    // this is used to compute the number of vectors per baseOrdinal
    long nextBaseOrdStart = vectorData.getFilePointer();
    outputMeta.writeLong(nextBaseOrdStart);
    final DirectMonotonicWriter nextBaseOrdWriter = DirectMonotonicWriter.getInstance(outputMeta, vectorData, numValues, directMonotonicBlockShift);
    for (int docId : docIds) {
      nextBaseOrdWriter.add(docId);
    }
    nextBaseOrdWriter.finish();
    outputMeta.writeLong(vectorData.getFilePointer() - nextBaseOrdStart); // baseOrd length
  }

  public static MultiVectorOrdConfiguration fromStoredMeta(IndexInput inputMeta) throws IOException {
    final int blockShift = inputMeta.readVInt();
    final int numValues = inputMeta.readVInt();

    // ordToDoc mapping
    long ordToDocStart = inputMeta.readLong();
    DirectMonotonicReader.Meta ordToDocMeta = DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long ordToDocLength = inputMeta.readLong();

    long baseOrdStart = inputMeta.readLong();
    DirectMonotonicReader.Meta baseOrdMeta = DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long baseOrdLength = inputMeta.readLong();

    long nextBaseOrdStart = inputMeta.readLong();
    DirectMonotonicReader.Meta nextBaseOrdMeta = DirectMonotonicReader.loadMeta(inputMeta, numValues, blockShift);
    long nextBaseOrdLength = inputMeta.readLong();

    return new MultiVectorOrdConfiguration(numValues,
        ordToDocStart, ordToDocLength, ordToDocMeta,
        baseOrdStart, baseOrdLength, baseOrdMeta,
        nextBaseOrdStart, nextBaseOrdLength, nextBaseOrdMeta);
  }

  final int numValues;
  final long ordToDocStart, ordToDocLength;
  final DirectMonotonicReader.Meta ordToDocMeta;
  final long baseOrdStart, baseOrdLength;
  final DirectMonotonicReader.Meta baseOrdMeta;
  final long nextBaseOrdStart, nextBaseOrdLength;
  final DirectMonotonicReader.Meta nextBaseOrdMeta;

  public MultiVectorOrdConfiguration(
      int numValues, long ordToDocStart, long ordToDocLength, DirectMonotonicReader.Meta ordToDocMeta,
      long baseOrdStart, long baseOrdLength, DirectMonotonicReader.Meta baseOrdMeta,
      long nextBaseOrdStart, long nextBaseOrdLength, DirectMonotonicReader.Meta nextBaseOrdMeta) {
    this.numValues = numValues;
    this.ordToDocStart = ordToDocStart;
    this.ordToDocLength = ordToDocLength;
    this.ordToDocMeta = ordToDocMeta;
    this.baseOrdStart = baseOrdStart;
    this.baseOrdLength = baseOrdLength;
    this.baseOrdMeta = baseOrdMeta;
    this.nextBaseOrdStart = nextBaseOrdStart;
    this.nextBaseOrdLength = nextBaseOrdLength;
    this.nextBaseOrdMeta = nextBaseOrdMeta;
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
}
