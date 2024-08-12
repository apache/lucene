package org.apache.lucene.codecs.lucene912;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

public class Lucene912BinaryQuantizedVectorsWriter extends FlatVectorsWriter {
  private static final long SHALLOW_RAM_BYTES_USED =
    shallowSizeOfInstance(Lucene912BinaryQuantizedVectorsWriter.class);

  private final SegmentWriteState segmentWriteState;
  private final List<FieldWriter> fields = new ArrayList<>();
  private final IndexOutput meta, binarizedVectorData;
  private final FlatVectorsWriter rawVectorDelegate;
  private boolean finished;

  /**
   * Sole constructor
   *
   * @param vectorsScorer the scorer to use for scoring vectors
   */
  protected Lucene912BinaryQuantizedVectorsWriter(
    BinaryFlatVectorsScorer vectorsScorer,
    FlatVectorsWriter rawVectorDelegate,
    SegmentWriteState state) throws IOException {
    super(vectorsScorer);
    this.segmentWriteState = state;
    String metaFileName =
      IndexFileNames.segmentFileName(
        state.segmentInfo.name,
        state.segmentSuffix,
        Lucene912BinaryQuantizedVectorsFormat.META_EXTENSION);

    String binarizedVectorDataFileName =
      IndexFileNames.segmentFileName(
        state.segmentInfo.name,
        state.segmentSuffix,
        Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION);
    this.rawVectorDelegate = rawVectorDelegate;
    boolean success = false;
    try {
      meta = state.directory.createOutput(metaFileName, state.context);
      binarizedVectorData =
        state.directory.createOutput(binarizedVectorDataFileName, state.context);

      CodecUtil.writeIndexHeader(
        meta,
        Lucene912BinaryQuantizedVectorsFormat.META_CODEC_NAME,
        Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
      CodecUtil.writeIndexHeader(
        binarizedVectorData,
        Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
        Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
        state.segmentInfo.getId(),
        state.segmentSuffix);
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
    if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
      @SuppressWarnings("unchecked")
      FieldWriter fieldWriter = new FieldWriter(fieldInfo, segmentWriteState.infoStream, (FlatFieldVectorsWriter<float[]>)rawVectorDelegate);
      fields.add(fieldWriter);
      return fieldWriter;
    }
    return rawVectorDelegate;
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {

  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;
    rawVectorDelegate.finish();
    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }
    if (binarizedVectorData != null) {
      CodecUtil.writeFooter(binarizedVectorData);
    }
  }

  @Override
  public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, binarizedVectorData, rawVectorDelegate);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : fields) {
      // the field tracks the delegate field usage
      total += field.ramBytesUsed();
    }
    return total;  }

  static class FieldWriter extends FlatFieldVectorsWriter<float[]> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private final InfoStream infoStream;
    private boolean finished;
    private final FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter;
    private final float[] dimensionSums;

    FieldWriter(FieldInfo fieldInfo, InfoStream infoStream, FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter) {
      this.fieldInfo = fieldInfo;
      this.infoStream = infoStream;
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dimensionSums = new float[fieldInfo.getVectorDimension()];
    }

    @Override
    public List<float[]> getVectors() {
      return flatFieldVectorsWriter.getVectors();
    }

    @Override
    public DocsWithFieldSet getDocsWithFieldSet() {
      return flatFieldVectorsWriter.getDocsWithFieldSet();
    }

    @Override
    public void finish() throws IOException {
      if (finished) {
        return;
      }
      assert flatFieldVectorsWriter.isFinished();
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && flatFieldVectorsWriter.isFinished();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      flatFieldVectorsWriter.addValue(docID, vectorValue);
      for (int i = 0; i < vectorValue.length; i++) {
        dimensionSums[i] += vectorValue[i];
      }
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
      long size = SHALLOW_SIZE;
      size += flatFieldVectorsWriter.ramBytesUsed();
      size += RamUsageEstimator.sizeOf(dimensionSums);
      return size;
    }
  }


  }
