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

import static java.lang.String.format;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SameThreadExecutorService;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.junit.Before;

public class TestLucene104HnswScalarQuantizedVectorsFormat extends BaseKnnVectorsFormatTestCase {

  private KnnVectorsFormat format;
  private ScalarEncoding encoding;

  @Before
  @Override
  public void setUp() throws Exception {
    var encodingValues = ScalarEncoding.values();
    encoding = encodingValues[random().nextInt(encodingValues.length)];
    format =
        new Lucene104HnswScalarQuantizedVectorsFormat(
            encoding,
            Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
            Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
            1,
            null);
    super.setUp();
  }

  @Override
  protected Codec getCodec() {
    return TestUtil.alwaysKnnVectorsFormat(format);
  }

  public void testToString() {
    FilterCodec customCodec =
        new FilterCodec("foo", Codec.getDefault()) {
          @Override
          public KnnVectorsFormat knnVectorsFormat() {
            return new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 10, 20, 1, null);
          }
        };
    String expectedPattern =
        "Lucene104HnswScalarQuantizedVectorsFormat(name=Lucene104HnswScalarQuantizedVectorsFormat,"
            + " maxConn=10, beamWidth=20, tinySegmentsThreshold=100,"
            + " flatVectorFormat=Lucene104ScalarQuantizedVectorsFormat(name=Lucene104ScalarQuantizedVectorsFormat,"
            + " encoding=UNSIGNED_BYTE,"
            + " flatVectorScorer=Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate=%s()),"
            + " rawVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%s())))";

    var defaultScorer =
        format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer", "DefaultFlatVectorScorer");
    var memSegScorer =
        format(
            Locale.ROOT,
            expectedPattern,
            "Lucene99MemorySegmentFlatVectorsScorer",
            "Lucene99MemorySegmentFlatVectorsScorer");
    assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
  }

  public void testSingleVectorCase() throws Exception {
    float[] vector = randomVector(random().nextInt(12, 500));
    for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        float[] docVector =
            similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                ? VectorUtil.l2normalize(ArrayUtil.copyArray(vector))
                : vector;
        doc.add(new KnnFloatVectorField("f", docVector, similarityFunction));
        w.addDocument(doc);
        w.commit();
        try (IndexReader reader = DirectoryReader.open(w)) {
          LeafReader r = getOnlyLeafReader(reader);
          FloatVectorValues vectorValues = r.getFloatVectorValues("f");
          KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();
          assert (vectorValues.size() == 1);
          while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
            assertArrayEquals(
                docVector, vectorValues.vectorValue(docIndexIterator.index()), 0.00001f);
          }
          float[] randomVector =
              similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
                  ? randomNormalizedVector(vector.length)
                  : randomVector(vector.length);
          float trueScore = similarityFunction.compare(docVector, randomVector);
          TopDocs td =
              r.searchNearestVectors(
                  "f",
                  randomVector,
                  1,
                  AcceptDocs.fromLiveDocs(null, r.maxDoc()),
                  Integer.MAX_VALUE);
          assertEquals(1, td.totalHits.value());
          assertTrue(td.scoreDocs[0].score >= 0);
          // When it's the only vector in a segment, the score should be very close to the true
          // score
          assertEquals(trueScore, td.scoreDocs[0].score, 0.01f);
        }
      }
    }
  }

  public void testLimits() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(-1, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(0, 20));
    expectThrows(
        IllegalArgumentException.class, () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, -1));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(512 + 1, 20));
    expectThrows(
        IllegalArgumentException.class,
        () -> new Lucene104HnswScalarQuantizedVectorsFormat(20, 3201));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new Lucene104HnswScalarQuantizedVectorsFormat(
                ScalarEncoding.UNSIGNED_BYTE, 20, 100, 1, new SameThreadExecutorService()));
  }

  // Ensures that all expected vector similarity functions are translatable in the format.
  public void testVectorSimilarityFuncs() {
    // This does not necessarily have to be all similarity functions, but
    // differences should be considered carefully.
    var expectedValues = Arrays.stream(VectorSimilarityFunction.values()).toList();
    assertEquals(Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS, expectedValues);
  }

  public void testSimpleOffHeapSize() throws IOException {
    float[] vector = randomVector(random().nextInt(12, 500));
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        LeafReader r = getOnlyLeafReader(reader);
        if (r instanceof CodecReader codecReader) {
          KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
          knnVectorsReader = knnVectorsReader.unwrapReaderForField("f");
          var fieldInfo = r.getFieldInfos().fieldInfo("f");
          var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);
          assertEquals(vector.length * Float.BYTES, (long) offHeap.get("vec"));
          assertNotNull(offHeap.get("vex"));
          long corrections = Float.BYTES + Float.BYTES + Float.BYTES + Integer.BYTES;
          long expected = encoding.getDocPackedLength(fieldInfo.getVectorDimension()) + corrections;
          assertEquals(expected, (long) offHeap.get("veq"));
          assertEquals(3, offHeap.size());
        }
      }
    }
  }

  public void testMergeInstancePropagatesToRawVectorsReader() throws IOException {
    RecordingDirectory dir = new RecordingDirectory(newDirectory());
    // a deterministic merge policy (MockRandomMergePolicy may wrap the merge in a reordering or
    // slow reader, which does not propagate merge instances at all) and deterministic flushing, so
    // that exactly the two committed segments reach forceMerge below
    IndexWriterConfig config =
        newIndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
            .setMergePolicy(new LogDocMergePolicy())
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB);
    // compound files would hide the raw vector file opens: disable them for flushed segments
    // (IndexWriterConfig) and for merged segments (the codec's CompoundFormat, restored below)
    config.setUseCompoundFile(false);
    CompoundFormat compoundFormat = config.getCodec().compoundFormat();
    boolean restoreCompoundFile = compoundFormat.getShouldUseCompoundFile();
    compoundFormat.setShouldUseCompoundFile(false);
    try (dir;
        IndexWriter w = new IndexWriter(dir, config)) {
      int dims = random().nextInt(12, 100);
      int numDocs = random().nextInt(200, 400);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(
            new KnnFloatVectorField("f", randomVector(dims), VectorSimilarityFunction.EUCLIDEAN));
        w.addDocument(doc);
        if (i == numDocs / 2) {
          w.commit(); // ensure at least two segments exist, so forceMerge does real work
        }
      }
      w.commit();
      // keep a reader open across the merge so the merge reuses the pooled search readers, as on
      // an index that is serving searches. Readers the merge opens itself use a merge context that
      // ignores advice hints, so the sequential flip would not be visible through them.
      try (IndexReader beforeMerge = DirectoryReader.open(w)) {
        assertEquals(numDocs, beforeMerge.numDocs());
        assertEquals(2, beforeMerge.leaves().size());
        w.forceMerge(1);
      }
      try (IndexReader reader = DirectoryReader.open(w)) {
        SegmentReader leaf = (SegmentReader) getOnlyLeafReader(reader);
        String mergedSegmentPrefix = leaf.getSegmentName() + "_";
        // the merge itself propagated to the source segments: their raw vector inputs received
        // the sequential hint while they were merged away (matched by full file name, so the
        // merged segment's own file cannot satisfy this), and finishMerge reverted it before
        // those inputs were closed
        assertTrue(
            dir.contextUpdates.entrySet().stream()
                .anyMatch(
                    e ->
                        e.getKey().startsWith(mergedSegmentPrefix) == false
                            && hasSequentialUpdate(e.getValue())));
        for (Map.Entry<String, List<IOContext>> e : dir.contextUpdates.entrySet()) {
          List<IOContext> updates = e.getValue();
          if (hasSequentialUpdate(updates)) {
            assertFalse(
                "read advice on [" + e.getKey() + "] was not reverted after the merge",
                updates.get(updates.size() - 1).hints().contains(DataAccessHint.SEQUENTIAL));
          }
        }

        // the merged segment's own raw vector file; nothing has touched its advice yet
        List<String> mergedVecs =
            dir.vecFiles.stream().filter(n -> n.startsWith(mergedSegmentPrefix)).toList();
        assertEquals(mergedVecs.toString(), 1, mergedVecs.size());
        String mergedVec = mergedVecs.get(0);
        assertTrue(dir.updatesFor(mergedVec).isEmpty());

        KnnVectorsReader knnReader = leaf.getVectorReader().unwrapReaderForField("f");
        KnnVectorsReader mergeInstance = knnReader.getMergeInstance();
        // asking for a merge instance must flip the merged segment's raw vector input to
        // sequential advice; without propagation through Lucene104ScalarQuantizedVectorsReader
        // nothing is recorded
        assertTrue(hasSequentialUpdate(dir.updatesFor(mergedVec)));

        // the merge view reads the same vectors in the same order as the live reader
        FloatVectorValues expected = knnReader.getFloatVectorValues("f");
        FloatVectorValues actual = mergeInstance.getFloatVectorValues("f");
        assertEquals(expected.size(), actual.size());
        for (int ord = 0; ord < expected.size(); ord++) {
          assertArrayEquals(expected.vectorValue(ord), actual.vectorValue(ord), 0f);
        }

        // finishMerge reaches the raw reader, which reverts the advice
        mergeInstance.finishMerge();
        List<IOContext> updates = dir.updatesFor(mergedVec);
        assertFalse(updates.get(updates.size() - 1).hints().contains(DataAccessHint.SEQUENTIAL));

        // merge instances are views over the live reader's resources and are never closed; the
        // live reader must keep working after the view is abandoned
        assertNotNull(knnReader.getFloatVectorValues("f"));
      }
    } finally {
      compoundFormat.setShouldUseCompoundFile(restoreCompoundFile);
    }
  }

  private static boolean hasSequentialUpdate(List<IOContext> updates) {
    return updates.stream().anyMatch(ctx -> ctx.hints().contains(DataAccessHint.SEQUENTIAL));
  }

  /** Records {@link IndexInput#updateIOContext} calls on raw vector files, per full file name. */
  private static final class RecordingDirectory extends FilterDirectory {
    final Map<String, List<IOContext>> contextUpdates = new ConcurrentHashMap<>();
    final Set<String> vecFiles = ConcurrentHashMap.newKeySet();

    RecordingDirectory(Directory in) {
      super(in);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput input = super.openInput(name, context);
      if (name.endsWith(".vec") == false) {
        return input;
      }
      vecFiles.add(name);
      return new RecordingIndexInput(input, name, contextUpdates);
    }

    List<IOContext> updatesFor(String name) {
      return contextUpdates.getOrDefault(name, List.of());
    }
  }

  /**
   * Records {@link IndexInput#updateIOContext} calls and forwards them, which {@link
   * FilterIndexInput} does not do on its own (the {@link IndexInput} default is a no-op). Clones
   * and slices come straight from the wrapped input: readers only update the context on their
   * top-level input.
   */
  private static final class RecordingIndexInput extends FilterIndexInput {
    private final String name;
    private final Map<String, List<IOContext>> contextUpdates;

    RecordingIndexInput(IndexInput in, String name, Map<String, List<IOContext>> contextUpdates) {
      super("RecordingIndexInput(" + name + ")", in);
      this.name = name;
      this.contextUpdates = contextUpdates;
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
      contextUpdates
          .computeIfAbsent(name, _ -> Collections.synchronizedList(new ArrayList<>()))
          .add(context);
      in.updateIOContext(context);
    }

    @Override
    public IndexInput clone() {
      return in.clone();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return in.slice(sliceDescription, offset, length);
    }
  }

  @Override
  protected boolean supportsFloatVectorFallback() {
    return false;
  }
}
