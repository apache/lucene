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
package org.apache.lucene.codecs.lucene104;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.LongConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter.MergeScorerData;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.Mode;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.HnswGraphBuilder;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Tests writer-produced merge scorer data for asymmetric scalar encodings. The writer creates
 * query-side records while writing merged vectors, avoiding the reader's second quantization pass.
 *
 * <p>The tests pin that the read-back is gone, compare both paths' output, and verify that handoff
 * files do not outlive their merge. {@code TestHnswMergeAbort#testRollbackDuringQuantizedMerge}
 * covers aborts after the scorer supplier takes ownership.
 */
public class TestLucene104ScalarQuantizedMergeScorer extends LuceneTestCase {

  private static final int DIM = 64;

  private static final int DOCS_PER_SEGMENT = 50;

  /** Segment size that stays below the default HNSW graph threshold when two segments merge. */
  private static final int TINY_SEGMENT_DOCS = 200;

  private static final int MAX_CONN = 16;
  private static final int BEAM_WIDTH = 32;

  /** A threshold of 0 makes every merge build a graph, so the merge scorer is always requested. */
  private static final int ALWAYS_GRAPH = 0;

  private static final FlatVectorsFormat RAW_FORMAT =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
  private static final Lucene104ScalarQuantizedVectorScorer QUANTIZED_SCORER =
      new Lucene104ScalarQuantizedVectorScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  private static List<ScalarEncoding> asymmetricEncodings() {
    List<ScalarEncoding> encodings = new ArrayList<>();
    for (ScalarEncoding encoding : ScalarEncoding.values()) {
      if (encoding.isAsymmetric()) {
        encodings.add(encoding);
      }
    }
    assertFalse("no asymmetric encoding to test", encodings.isEmpty());
    return encodings;
  }

  /** The shipped format: the writer prepares the merge scorer's records. */
  private static KnnVectorsFormat writerPathFormat(ScalarEncoding encoding, int threshold) {
    return new Lucene104HnswScalarQuantizedVectorsFormat(
        encoding, MAX_CONN, BEAM_WIDTH, 1, null, threshold);
  }

  /**
   * Returns the same on-disk format with writer preparation disabled, forcing the {@code
   * QuantizedVectorsReader} fallback. Tests use it as a read-back control and to retain fallback
   * coverage.
   */
  private static KnnVectorsFormat readerPathFormat(ScalarEncoding encoding, int threshold) {
    return new HnswOverFlatFormat(new NoPrepareFlatFormat(encoding), threshold);
  }

  /**
   * Verifies that the writer path does not read the merged raw float vectors back; the graph build
   * still streams the merged quantized records. Opening the merged reader still reads the raw
   * vector header and checksum, so the limit is one vector rather than zero bytes.
   *
   * <p>The reader fallback is a positive control and must stream the entire merged raw vector file.
   */
  public void testMergeDoesNotReadMergedVectorsBack() throws IOException {
    for (ScalarEncoding encoding : asymmetricEncodings()) {
      for (VectorSimilarityFunction similarity :
          new VectorSimilarityFunction[] {
            VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.COSINE
          }) {
        MergeCounts counts =
            runMerge(writerPathFormat(encoding, ALWAYS_GRAPH), similarity, true, true);
        assertTrue(
            "the merge read the merged vectors back: "
                + counts.mergedRawBytesRead()
                + " bytes of "
                + encoding
                + "/"
                + similarity,
            counts.mergedRawBytesRead() < (long) DIM * Float.BYTES);
      }
      MergeCounts fallback =
          runMerge(
              readerPathFormat(encoding, ALWAYS_GRAPH),
              VectorSimilarityFunction.EUCLIDEAN,
              true,
              true);
      assertTrue(
          "the reader fallback did not stream the merged vectors back: "
              + fallback.mergedRawBytesRead()
              + " bytes for "
              + encoding,
          fallback.mergedRawBytesRead() >= (long) DIM * Float.BYTES * 2 * DOCS_PER_SEGMENT);
    }
  }

  /**
   * Verifies that a reader wrapping this format's reader still consumes the hand-off, as long as it
   * unwraps to it: the merge does not read the merged vectors back.
   */
  public void testHandOffServesAWrappingReader() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    MergeCounts counts =
        runMerge(
            new HnswOverFlatFormat(new WrappingFlatFormat(encoding), ALWAYS_GRAPH),
            VectorSimilarityFunction.EUCLIDEAN,
            true,
            true);
    assertTrue(
        "the merge read the merged vectors back through the wrapper: "
            + counts.mergedRawBytesRead()
            + " bytes for "
            + encoding,
        counts.mergedRawBytesRead() < (long) DIM * Float.BYTES);
  }

  /**
   * Verifies that writer and reader paths produce identical quantized vector data and HNSW graphs.
   * One {@code multiScalarQuantize} call must match separate index-side and query-side {@code
   * scalarQuantize} calls.
   *
   * <p>The sorted index and sparse second vector field exercise non-dense merged order. Non-unit
   * COSINE vectors exercise query-side normalization.
   */
  public void testGraphIsIdenticalToTheReaderFallback() throws IOException {
    for (ScalarEncoding encoding : asymmetricEncodings()) {
      for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
        assertBothPathsWriteTheSameFiles(encoding, similarity, List.of(), VECTOR_EXTENSIONS);
      }
    }
  }

  /**
   * Verifies that writer and reader paths produce identical vector records when deletions change
   * merged ordinals.
   *
   * <p>The graph is excluded because HNSW output is not reproducible across otherwise identical
   * merges that drop documents.
   */
  public void testMergedRecordsAreIdenticalWithDeletions() throws IOException {
    for (ScalarEncoding encoding : asymmetricEncodings()) {
      VectorSimilarityFunction similarity =
          RandomPicks.randomFrom(random(), VectorSimilarityFunction.values());
      List<String> deleted = new ArrayList<>();
      for (int i = 0; i < 2 * DOCS_PER_SEGMENT; i++) {
        if (random().nextInt(10) == 0) {
          deleted.add(Integer.toString(i));
        }
      }
      assertBothPathsWriteTheSameFiles(encoding, similarity, deleted, RECORD_EXTENSIONS);
    }
  }

  private void assertBothPathsWriteTheSameFiles(
      ScalarEncoding encoding,
      VectorSimilarityFunction similarity,
      List<String> deleted,
      Set<String> compared)
      throws IOException {
    long savedSeed = HnswGraphBuilder.randSeed;
    try {
      float[][] vectors = randomVectors(2 * DOCS_PER_SEGMENT, similarity);
      long seed = random().nextLong();
      HnswGraphBuilder.randSeed = seed;
      Map<String, byte[]> writerPath =
          mergedVectorFiles(writerPathFormat(encoding, ALWAYS_GRAPH), vectors, similarity, deleted);
      HnswGraphBuilder.randSeed = seed;
      Map<String, byte[]> readerPath =
          mergedVectorFiles(readerPathFormat(encoding, ALWAYS_GRAPH), vectors, similarity, deleted);
      assertEquals(
          "different files were written for " + encoding + "/" + similarity,
          writerPath.keySet(),
          readerPath.keySet());
      for (String extension : compared) {
        assertArrayEquals(
            "the body of the merged ."
                + extension
                + " file, header and footer aside, differs for "
                + encoding
                + "/"
                + similarity,
            writerPath.get(extension),
            readerPath.get(extension));
      }
    } finally {
      HnswGraphBuilder.randSeed = savedSeed;
    }
  }

  /**
   * Verifies that a merge with no graph never creates a handoff file. This covers an HNSW merge
   * rejected by the graph-size predicate and a flat merge that supplies no predicate to the
   * two-argument merge method.
   *
   * <p>The file must never be created, not merely cleaned up.
   */
  public void testNoHandOffWhenNoGraphIsBuilt() throws IOException {
    for (ScalarEncoding encoding : asymmetricEncodings()) {
      MergeCounts belowThreshold =
          runMerge(
              writerPathFormat(encoding, Lucene99HnswVectorsFormat.HNSW_GRAPH_THRESHOLD),
              VectorSimilarityFunction.EUCLIDEAN,
              false,
              true,
              TINY_SEGMENT_DOCS);
      assertEquals(
          "a hand-off file was created for a merge that builds no graph: "
              + belowThreshold.handOffs(),
          List.of(),
          belowThreshold.handOffs());

      MergeCounts flatOnly =
          runMerge(
              new Lucene104ScalarQuantizedVectorsFormat(encoding),
              VectorSimilarityFunction.EUCLIDEAN,
              false,
              false);
      assertEquals(
          "the flat format prepared merge scorer data nobody can ask for: " + flatOnly.handOffs(),
          List.of(),
          flatOnly.handOffs());
    }
  }

  /**
   * Verifies that {@link Mode#DATA_BLIND_WITH_FLOATS} prepares no data, leaving HNSW on the reader
   * fallback: only {@link Mode#CENTERED} prepares. Both paths create query-data files, but only the
   * writer path returns one as a handoff.
   *
   * <p>{@link Mode#DATA_BLIND_WITHOUT_FLOATS} rejects asymmetric encodings, so that case expects an
   * exception.
   */
  public void testDataBlindModesKeepTheReaderPath() throws IOException {
    for (ScalarEncoding encoding : asymmetricEncodings()) {
      List<MergeScorerData> handles = new ArrayList<>();
      MergeCounts counts =
          runMerge(
              new HnswOverFlatFormat(
                  new CapturingFlatFormat(encoding, Mode.DATA_BLIND_WITH_FLOATS, handles),
                  ALWAYS_GRAPH),
              VectorSimilarityFunction.EUCLIDEAN,
              true,
              true);
      assertEquals("a data-blind merge prepared a hand-off: " + handles, List.of(), handles);
      assertEquals(
          "the reader fallback did not write its own query file: " + counts.handOffs(),
          1,
          counts.handOffs().size());
      assertTrue(
          "the data-blind merge did not read the merged vectors back: "
              + counts.mergedRawBytesRead()
              + " bytes for "
              + encoding,
          counts.mergedRawBytesRead() >= (long) DIM * Float.BYTES * 2 * DOCS_PER_SEGMENT);

      expectThrows(
          IllegalArgumentException.class,
          () ->
              new Lucene104HnswScalarQuantizedVectorsFormat(
                  encoding,
                  Mode.DATA_BLIND_WITHOUT_FLOATS,
                  MAX_CONN,
                  BEAM_WIDTH,
                  1,
                  null,
                  ALWAYS_GRAPH));
    }
  }

  /**
   * Verifies that {@link IndexWriter#addIndexes(CodecReader...)} merges readers whose field numbers
   * differ from their underlying segments. The flat writer must resolve the field by name.
   */
  public void testMergeOfRenumberedCodecReaders() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(writerPathFormat(encoding, ALWAYS_GRAPH)))
            .setUseCompoundFile(false);
    config.getCodec().compoundFormat().setShouldUseCompoundFile(false);
    try (Directory source = newDirectory();
        Directory target = newDirectory()) {
      float[][] vectors = randomVectors(2 * DOCS_PER_SEGMENT, VectorSimilarityFunction.EUCLIDEAN);
      try (IndexWriter writer = new IndexWriter(source, config)) {
        for (int i = 0; i < vectors.length; i++) {
          Document doc = new Document();
          for (int f = 0; f < 5; f++) {
            doc.add(new StringField("s" + f, Integer.toString(i % 7), Field.Store.NO));
          }
          doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
          writer.addDocument(doc);
          if (i == DOCS_PER_SEGMENT - 1) {
            writer.commit();
          }
        }
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(source);
          IndexWriter writer =
              new IndexWriter(
                  target,
                  new IndexWriterConfig().setCodec(config.getCodec()).setUseCompoundFile(false))) {
        List<CodecReader> readers = new ArrayList<>();
        for (LeafReaderContext context : reader.leaves()) {
          readers.add(renumbered((CodecReader) context.reader()));
        }
        assertEquals(2, readers.size());
        writer.addIndexes(readers.toArray(new CodecReader[0]));
        writer.forceMerge(1);
      }
      assertNoTempFiles(target);
      try (DirectoryReader reader = DirectoryReader.open(target)) {
        assertEquals(vectors.length, reader.numDocs());
        KnnVectorsReader vectorsReader =
            ((CodecReader) getOnlyLeafReader(reader)).getVectorReader().unwrapReaderForField("v");
        HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph("v");
        assertNotNull("the merged segment has no graph", graph);
        assertEquals(vectors.length, graph.size());
      }
    }
  }

  /**
   * Verifies that each handoff supplies at most one scorer. Closing it after transfer is a no-op,
   * and later supplier requests fail.
   */
  public void testHandOffIsSingleUse() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    List<MergeScorerData> handles = new ArrayList<>();
    runMerge(
        new HnswOverFlatFormat(
            new CapturingFlatFormat(encoding, Mode.CENTERED, handles), ALWAYS_GRAPH),
        VectorSimilarityFunction.EUCLIDEAN,
        true,
        true);
    assertEquals("the merge prepared no hand-off to test", 1, handles.size());
    MergeScorerData handle = handles.get(0);
    expectThrows(IllegalStateException.class, () -> handle.scorerSupplier(null));
    handle.close();
    expectThrows(IllegalStateException.class, () -> handle.scorerSupplier(null));
  }

  /**
   * Verifies that a phase-one failure releases an earlier field's unconsumed handoff. The second
   * field fails before the first field's deferred graph work runs, so closing the handle must
   * remove its temporary file.
   */
  public void testPhaseOneFailureLeavesNoHandOff() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    try (Directory dir =
        new FilterDirectory(newDirectory()) {
          private final AtomicBoolean firstHandOff = new AtomicBoolean(true);

          @Override
          public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
              throws IOException {
            if ("queries".equals(suffix) && firstHandOff.compareAndSet(true, false) == false) {
              throw new IOException("simulated failure creating the second hand-off file");
            }
            return super.createTempOutput(prefix, suffix, context);
          }
        }) {
      assertMergeFailsWithoutLeftovers(dir, encoding, false);
    }
  }

  /**
   * Verifies that failure while reopening merged quantized vectors releases the handoff before its
   * scorer supplier consumes it.
   */
  public void testHandOffIsReleasedWhenItIsNeverConsumed() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    try (Directory dir =
        new FilterDirectory(newDirectory()) {
          private final AtomicBoolean handedOff = new AtomicBoolean();

          @Override
          public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
              throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            if ("queries".equals(suffix)) {
              handedOff.set(true);
            }
            return out;
          }

          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            // the merged quantized vectors are opened when the graph build reopens the segment,
            // which is before the handle is asked for anything
            if (handedOff.get()
                && name.endsWith(
                    "." + Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION)) {
              throw new IOException("simulated failure reopening the merged quantized vectors");
            }
            return super.openInput(name, context);
          }
        }) {
      assertMergeFailsWithoutLeftovers(dir, encoding, true);
    }
  }

  /** Verifies that the handoff is deleted when its scorer cannot open the temporary file. */
  public void testPhaseTwoFailureLeavesNoHandOff() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    try (Directory dir =
        new FilterDirectory(newDirectory()) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            if (name.contains("queries")) {
              throw new IOException("simulated failure opening the hand-off file");
            }
            return super.openInput(name, context);
          }
        }) {
      assertMergeFailsWithoutLeftovers(dir, encoding, true);
    }
  }

  /**
   * Verifies that a metadata-write failure deletes the handoff after its records are written but
   * before it is returned to {@link Lucene99HnswVectorsWriter}. Only the flat writer knows the
   * temporary file exists in this interval.
   */
  public void testMetaFailureLeavesNoHandOff() throws IOException {
    ScalarEncoding encoding = randomAsymmetricEncoding();
    try (Directory dir =
        new FilterDirectory(newDirectory()) {
          private final AtomicBoolean handedOff = new AtomicBoolean();

          @Override
          public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
              throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            if ("queries".equals(suffix)) {
              // Start failing metadata writes only after the handoff file has been created.
              handedOff.set(true);
            }
            return out;
          }

          @Override
          public IndexOutput createOutput(String name, IOContext context) throws IOException {
            IndexOutput out = super.createOutput(name, context);
            if (name.endsWith("." + Lucene104ScalarQuantizedVectorsFormat.META_EXTENSION)
                == false) {
              return out;
            }
            return new FilterIndexOutput("failing quantized meta", name, out) {
              @Override
              public void writeByte(byte b) throws IOException {
                failIfHandedOff();
                super.writeByte(b);
              }

              @Override
              public void writeBytes(byte[] b, int offset, int length) throws IOException {
                failIfHandedOff();
                super.writeBytes(b, offset, length);
              }

              private void failIfHandedOff() throws IOException {
                if (handedOff.get()) {
                  throw new IOException("simulated failure writing the quantized meta");
                }
              }
            };
          }
        }) {
      assertMergeFailsWithoutLeftovers(dir, encoding, true);
    }
  }

  /**
   * Runs a failing merge of one or two vector fields and verifies that the simulated failure is the
   * one raised and that no temporary files remain.
   */
  private void assertMergeFailsWithoutLeftovers(
      Directory dir, ScalarEncoding encoding, boolean singleField) throws IOException {
    IndexWriterConfig config =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(writerPathFormat(encoding, ALWAYS_GRAPH)))
            .setMergeScheduler(new SerialMergeScheduler())
            .setUseCompoundFile(false);
    config.getCodec().compoundFormat().setShouldUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, config);
    try {
      float[][] vectors = randomVectors(2 * DOCS_PER_SEGMENT, VectorSimilarityFunction.EUCLIDEAN);
      for (int i = 0; i < vectors.length; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.EUCLIDEAN));
        if (singleField == false) {
          doc.add(
              new KnnFloatVectorField(
                  "w", vectors[vectors.length - 1 - i], VectorSimilarityFunction.EUCLIDEAN));
        }
        writer.addDocument(doc);
        if (i == DOCS_PER_SEGMENT - 1) {
          writer.commit();
        }
      }
      writer.commit();
      Exception failure = expectThrows(Exception.class, () -> writer.forceMerge(1));
      boolean simulated = false;
      for (Throwable t = failure; t != null && simulated == false; t = t.getCause()) {
        simulated = String.valueOf(t.getMessage()).contains("simulated failure");
      }
      assertTrue("unexpected merge failure: " + failure, simulated);
      // Check before rollback. When no tragedy is recorded, rollback sweeps unreferenced temporary
      // files and could hide a merge cleanup failure.
      assertNoTempFiles(dir);
    } finally {
      // the writer may already be unusable after the failed merge, so this is best effort
      IOUtils.closeWhileHandlingException(writer::rollback);
    }
  }

  private ScalarEncoding randomAsymmetricEncoding() {
    List<ScalarEncoding> encodings = asymmetricEncodings();
    return encodings.get(random().nextInt(encodings.size()));
  }

  private static void assertNoTempFiles(Directory dir) throws IOException {
    for (String file : dir.listAll()) {
      assertFalse("a temporary file outlived the merge: " + file, file.endsWith(".tmp"));
    }
  }

  /** Returns the same leaf with every field assigned a number unused by its segment. */
  private static CodecReader renumbered(CodecReader reader) {
    List<FieldInfo> renumbered = new ArrayList<>();
    for (FieldInfo info : reader.getFieldInfos()) {
      renumbered.add(
          new FieldInfo(
              info.name,
              info.number + 100,
              info.hasTermVectors(),
              info.omitsNorms(),
              info.hasPayloads(),
              info.getIndexOptions(),
              info.getDocValuesType(),
              info.docValuesSkipIndexType(),
              info.getDocValuesGen(),
              info.attributes(),
              info.getPointDimensionCount(),
              info.getPointIndexDimensionCount(),
              info.getPointNumBytes(),
              info.getVectorDimension(),
              info.getVectorEncoding(),
              info.getVectorSimilarityFunction(),
              info.isSoftDeletesField(),
              info.isParentField()));
    }
    FieldInfos fieldInfos = new FieldInfos(renumbered.toArray(new FieldInfo[0]));
    return new FilterCodecReader(reader) {
      @Override
      public FieldInfos getFieldInfos() {
        return fieldInfos;
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  private record MergeCounts(long mergedRawBytesRead, List<String> handOffs) {}

  private MergeCounts runMerge(
      KnnVectorsFormat format,
      VectorSimilarityFunction similarity,
      boolean expectGraph,
      boolean hnsw)
      throws IOException {
    return runMerge(format, similarity, expectGraph, hnsw, DOCS_PER_SEGMENT);
  }

  /**
   * Indexes two segments, merges them, and reports what the merge read and what it created.
   *
   * @param expectGraph whether the merge is expected to build a graph
   * @param hnsw whether the format is an HNSW format at all (a flat format builds no graph and has
   *     no reader to ask about one)
   */
  private MergeCounts runMerge(
      KnnVectorsFormat format,
      VectorSimilarityFunction similarity,
      boolean expectGraph,
      boolean hnsw,
      int docsPerSegment)
      throws IOException {
    float[][] vectors = randomVectors(2 * docsPerSegment, similarity);
    try (ReadCountingDirectory dir = new ReadCountingDirectory(newDirectory())) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
              // keep the raw vector file a file of its own rather than a region of a .cfs
              .setUseCompoundFile(false);
      config.getCodec().compoundFormat().setShouldUseCompoundFile(false);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < vectors.length; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("v", vectors[i], similarity));
          writer.addDocument(doc);
          if (i == docsPerSegment - 1) {
            writer.commit();
          }
        }
        writer.commit();
        SegmentInfos beforeMerge = SegmentInfos.readLatestCommit(dir);
        assertEquals("expected two segments to merge", 2, beforeMerge.size());
        long sourceRawBytes = 0;
        for (SegmentCommitInfo info : beforeMerge) {
          for (String file : info.files()) {
            if (file.endsWith(".vec")) {
              sourceRawBytes += dir.fileLength(file);
            }
          }
        }
        dir.resetCounts();

        writer.forceMerge(1);
        writer.commit();

        SegmentInfos afterMerge = SegmentInfos.readLatestCommit(dir);
        assertEquals("expected one segment after the merge", 1, afterMerge.size());
        SegmentCommitInfo merged = afterMerge.info(0);
        String mergedRaw = null;
        for (String file : merged.files()) {
          assertFalse("a temporary file became a segment file: " + file, file.endsWith(".tmp"));
          if (file.endsWith(".vec")) {
            mergedRaw = file;
          }
        }
        assertNotNull("the merged segment has no raw vector file", mergedRaw);
        assertNoTempFiles(dir);

        long sourceRead = 0;
        for (SegmentCommitInfo info : beforeMerge) {
          for (String file : info.files()) {
            if (file.endsWith(".vec")) {
              sourceRead += dir.bytesRead(file);
            }
          }
        }
        // positive control for the counter: the merge did read the source vectors, in full
        assertTrue(
            "the byte counter saw no read of the source vectors: " + sourceRead,
            sourceRead >= sourceRawBytes);

        if (hnsw) {
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            KnnVectorsReader vectorsReader =
                ((CodecReader) getOnlyLeafReader(reader))
                    .getVectorReader()
                    .unwrapReaderForField("v");
            HnswGraph graph = ((HnswGraphProvider) vectorsReader).getGraph("v");
            assertEquals("graph presence", expectGraph, graph != null && graph.size() > 0);
          }
        }
        return new MergeCounts(dir.bytesRead(mergedRaw), dir.handOffsCreated());
      }
    }
  }

  /**
   * Merges two segments and returns merged vector-file bodies keyed by extension. Headers and
   * footers are omitted because random segment IDs and their checksums differ between equivalent
   * indexes.
   *
   * <p>The index is sorted, a second vector field is sparse, and documents named by {@code deleted}
   * are removed before the merge.
   */
  private Map<String, byte[]> mergedVectorFiles(
      KnnVectorsFormat format,
      float[][] vectors,
      VectorSimilarityFunction similarity,
      List<String> deleted)
      throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
              .setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)))
              // the two arms have to meet the same segments, so nothing may merge in the background
              .setMergeScheduler(new SerialMergeScheduler())
              .setUseCompoundFile(false);
      config.getCodec().compoundFormat().setShouldUseCompoundFile(false);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < vectors.length; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new NumericDocValuesField("sort", (i * 7919L) % 1000));
          doc.add(new KnnFloatVectorField("v", vectors[i], similarity));
          if (i % 3 == 0) {
            doc.add(new KnnFloatVectorField("w", vectors[vectors.length - 1 - i], similarity));
          }
          writer.addDocument(doc);
          if (i == vectors.length / 2 - 1) {
            writer.commit();
          }
        }
        writer.commit();
        for (String id : deleted) {
          writer.deleteDocuments(new Term("id", id));
        }
        writer.commit();
        writer.forceMerge(1);
        writer.commit();
      }
      SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, infos.size());
      Map<String, byte[]> files = new HashMap<>();
      for (String file : infos.info(0).files()) {
        String extension = file.substring(file.lastIndexOf('.') + 1);
        if (VECTOR_EXTENSIONS.contains(extension)) {
          files.put(extension, fileBody(dir, file));
        }
      }
      assertTrue("no raw vectors were written", files.containsKey("vec"));
      assertTrue("no quantized vectors were written", files.containsKey("veq"));
      assertTrue("no graph was written, so this compares nothing", files.get("vex").length > 0);
      return files;
    }
  }

  /** Vector record files, excluding the HNSW graph. */
  private static final Set<String> RECORD_EXTENSIONS = Set.of("vec", "veq", "vemq");

  private static final Set<String> VECTOR_EXTENSIONS = Set.of("vec", "veq", "vemq", "vex");

  /** The bytes of a codec file between its index header and its footer. */
  private static byte[] fileBody(Directory dir, String file) throws IOException {
    try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
      in.readInt(); // magic
      in.readString(); // codec name
      in.readInt(); // version
      in.skipBytes(StringHelper.ID_LENGTH);
      in.skipBytes(in.readByte() & 0xFF); // segment suffix
      long start = in.getFilePointer();
      long length = in.length() - CodecUtil.footerLength() - start;
      byte[] body = new byte[Math.toIntExact(length)];
      in.readBytes(body, 0, body.length);
      return body;
    }
  }

  private float[][] randomVectors(int count, VectorSimilarityFunction similarity) {
    float[][] vectors = new float[count][];
    for (int i = 0; i < count; i++) {
      float[] vector = new float[DIM];
      for (int j = 0; j < DIM; j++) {
        vector[j] = random().nextFloat() * 2 - 1;
      }
      if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
        // DOT_PRODUCT requires unit vectors; leave COSINE non-unit to exercise normalization.
        VectorUtil.l2normalize(vector);
      } else if (similarity == VectorSimilarityFunction.COSINE) {
        for (int j = 0; j < DIM; j++) {
          vector[j] *= 5f;
        }
      }
      vectors[i] = vector;
    }
    return vectors;
  }

  /** Counts every byte read from every file, through clones and slices, and names files created. */
  private static class ReadCountingDirectory extends FilterDirectory {
    private final Map<String, Long> bytesRead = new ConcurrentHashMap<>();
    private final Set<String> created = ConcurrentHashMap.newKeySet();

    ReadCountingDirectory(Directory in) {
      super(in);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      return new CountingIndexInput(
          super.openInput(name, context), read -> bytesRead.merge(name, read, Long::sum));
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
      created.add(name);
      return super.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
        throws IOException {
      IndexOutput output = super.createTempOutput(prefix, suffix, context);
      created.add(output.getName());
      return output;
    }

    void resetCounts() {
      bytesRead.clear();
      created.clear();
    }

    long bytesRead(String name) {
      return bytesRead.getOrDefault(name, 0L);
    }

    /** Returns query-data files created by either merge-scorer path. */
    List<String> handOffsCreated() {
      return created.stream().filter(name -> name.contains("queries")).sorted().toList();
    }
  }

  /** Reports every byte read through it, and through everything cloned or sliced off it. */
  private static class CountingIndexInput extends FilterIndexInput {
    private final LongConsumer bytesRead;

    CountingIndexInput(IndexInput in, LongConsumer bytesRead) {
      super(in.toString(), in);
      this.bytesRead = bytesRead;
    }

    @Override
    public IndexInput clone() {
      return new CountingIndexInput(in.clone(), bytesRead);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingIndexInput(in.slice(sliceDescription, offset, length), bytesRead);
    }

    @Override
    public byte readByte() throws IOException {
      bytesRead.accept(1);
      return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      bytesRead.accept(len);
      in.readBytes(b, offset, len);
    }
  }

  /**
   * An HNSW format backed by a caller-supplied flat format. It writes Lucene104 bytes and reuses
   * the registered name so test segments reopen through SPI with the shipped format.
   */
  private static final class HnswOverFlatFormat extends KnnVectorsFormat {
    private static final KnnVectorsFormat READ_FORMAT =
        new Lucene104HnswScalarQuantizedVectorsFormat();

    private final FlatVectorsFormat flatVectorsFormat;
    private final int tinySegmentsThreshold;

    HnswOverFlatFormat(FlatVectorsFormat flatVectorsFormat, int tinySegmentsThreshold) {
      super(Lucene104HnswScalarQuantizedVectorsFormat.NAME);
      this.flatVectorsFormat = flatVectorsFormat;
      this.tinySegmentsThreshold = tinySegmentsThreshold;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return new Lucene99HnswVectorsWriter(
          state,
          MAX_CONN,
          BEAM_WIDTH,
          flatVectorsFormat,
          flatVectorsFormat.fieldsWriter(state),
          1,
          null,
          tinySegmentsThreshold);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
      return READ_FORMAT.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
      return 1024;
    }
  }

  /** Lucene104 flat format whose writer uses the default no-preparation merge path. */
  private static final class NoPrepareFlatFormat extends Lucene104ScalarQuantizedVectorsFormat {
    private final ScalarEncoding encoding;

    NoPrepareFlatFormat(ScalarEncoding encoding) {
      super(encoding, Mode.CENTERED);
      this.encoding = encoding;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return new Lucene104ScalarQuantizedVectorsWriter(
          state, encoding, Mode.CENTERED, RAW_FORMAT.fieldsWriter(state), QUANTIZED_SCORER) {
        @Override
        public MergeScorerData mergeOneFlatVectorFieldForMergeScorer(
            FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
            throws IOException {
          mergeOneFlatVectorField(fieldInfo, mergeState);
          return null;
        }
      };
    }
  }

  /** The shipped flat format, keeping every hand-off it prepares for the test to inspect. */
  private static final class CapturingFlatFormat extends Lucene104ScalarQuantizedVectorsFormat {
    private final ScalarEncoding encoding;
    private final Mode mode;
    private final List<MergeScorerData> handles;

    CapturingFlatFormat(ScalarEncoding encoding, Mode mode, List<MergeScorerData> handles) {
      super(encoding, mode);
      this.encoding = encoding;
      this.mode = mode;
      this.handles = handles;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return new Lucene104ScalarQuantizedVectorsWriter(
          state, encoding, mode, RAW_FORMAT.fieldsWriter(state), QUANTIZED_SCORER) {
        @Override
        public MergeScorerData mergeOneFlatVectorFieldForMergeScorer(
            FieldInfo fieldInfo, MergeState mergeState, IntPredicate needsMergeScorer)
            throws IOException {
          MergeScorerData handle =
              super.mergeOneFlatVectorFieldForMergeScorer(fieldInfo, mergeState, needsMergeScorer);
          if (handle != null) {
            handles.add(handle);
          }
          return handle;
        }
      };
    }
  }

  /** The shipped flat format behind a reader wrapper that unwraps to the shipped reader. */
  private static final class WrappingFlatFormat extends FlatVectorsFormat {
    private final FlatVectorsFormat delegate;

    WrappingFlatFormat(ScalarEncoding encoding) {
      super("WrappingFlatFormat");
      this.delegate = new Lucene104ScalarQuantizedVectorsFormat(encoding);
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
      return delegate.fieldsWriter(state);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
      return new WrappingFlatReader(delegate.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
      return delegate.getMaxDimensions(fieldName);
    }
  }

  /** Forwards everything, and unwraps to the reader it wraps, which the hand-off relies on. */
  private static final class WrappingFlatReader extends FlatVectorsReader {
    private final FlatVectorsReader delegate;

    WrappingFlatReader(FlatVectorsReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public KnnVectorsReader unwrapReaderForField(String field) {
      return delegate.unwrapReaderForField(field);
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
      return delegate.getFlatVectorScorer(field);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target)
        throws IOException {
      return delegate.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target)
        throws IOException {
      return delegate.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, short[] target)
        throws IOException {
      return delegate.getRandomVectorScorer(field, target);
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
      delegate.checkIntegrity(merge);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      return delegate.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      return delegate.getByteVectorValues(field);
    }

    @Override
    public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
      return delegate.getFloat16VectorValues(field);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public long ramBytesUsed() {
      return delegate.ramBytesUsed();
    }
  }
}
