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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * A merge reads the raw vectors front to back while a graph search reads them at random, and read
 * advice applies to a whole mapping, so a merge opens the data file for itself.
 */
public class TestVectorsMergeReadAdvice extends LuceneTestCase {

  private static final int DIM = 8;

  public void testMergeOpensItsOwnVectors() throws Exception {
    assertMergeOpensItsOwnVectors(new Lucene99HnswVectorsFormat(), false);
  }

  public void testQuantizedMergeOpensItsOwnVectors() throws Exception {
    assertMergeOpensItsOwnVectors(new Lucene104HnswScalarQuantizedVectorsFormat(), true);
  }

  /**
   * @param noReuse whether the format opens its raw vectors with {@link NoReuseHint}
   */
  private static void assertMergeOpensItsOwnVectors(KnnVectorsFormat format, boolean noReuse)
      throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      iwc.setUseCompoundFile(false); // so the directory sees the data file by name
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < 2; segment++) {
          for (int i = 0; i < 64; i++) {
            Document doc = new Document();
            doc.add(
                new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }

        // searches open the segments first, so the merge finds readers that are already open
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(2, reader.leaves().size());
          opens.clear();
          w.forceMerge(1);

          // the merge is over, so what it opened is gone even though the segments are still open
          List<Open> sequential = sequentialOpens(opens);
          assertFalse(
              "the merge never opened the vectors for itself: " + opens, sequential.isEmpty());
          for (Open open : sequential) {
            assertTrue("the merge did not read the vectors it opened: " + open, open.read());
            assertTrue("the merge kept its vectors open: " + open, open.closed());
            assertEquals(
                "the merge mapping keeps what the format said: " + open,
                noReuse,
                open.hints().contains(NoReuseHint.INSTANCE));
          }
        }
      }

      assertEquals(
          "the merge re-advised the vectors searches are reading: " + opens,
          List.of(),
          opens.advised());
    }
  }

  /**
   * In a compound segment, a merge reads the vectors through a mapping of the compound file of its
   * own, opened without advice and closed with the segment's reader.
   */
  public void testACompoundMergeOpensItsOwnMapping() throws Exception {
    KnnVectorsFormat format =
        random().nextBoolean()
            ? new Lucene99HnswVectorsFormat()
            : new Lucene104HnswScalarQuantizedVectorsFormat();
    Opens opens = new Opens();
    List<Open> mergeOpens;
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      iwc.setUseCompoundFile(true);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < 2; segment++) {
          for (int i = 0; i < 64; i++) {
            Document doc = new Document();
            doc.add(
                new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          List<String> compoundFiles = new ArrayList<>();
          for (var leaf : reader.leaves()) {
            SegmentReader segmentReader = (SegmentReader) leaf.reader();
            assertTrue(segmentReader.getSegmentInfo().info.getUseCompoundFile());
            compoundFiles.add(segmentReader.getSegmentName() + ".cfs");
          }
          compoundFiles.sort(null);
          opens.clear();
          w.forceMerge(1);

          // the segments were open before the merge, so opening their compound files again is the
          // merge
          mergeOpens = opens.all().stream().filter(o -> compoundFiles.contains(o.name())).toList();
          assertEquals(
              "the merge mapped each compound file once: " + opens,
              compoundFiles,
              mergeOpens.stream().map(Open::name).sorted().toList());
          for (Open open : mergeOpens) {
            assertTrue("the merge did not read its mapping: " + open, open.read());
            assertEquals("opened without advice: " + open, Set.of(), open.hints());
          }
        }
      }
    }
    for (Open open : mergeOpens) {
      assertTrue("closed with the segment's reader: " + open, open.closed());
    }
  }

  /** A reader a merge opened already reads the vectors the way a merge does. */
  public void testAMergeReadsThroughTheReaderItOpened() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < 2; segment++) {
          for (int i = 0; i < 16; i++) {
            Document doc = new Document();
            doc.add(
                new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }
        // no reader is open, so the merge opens the segments itself, with a merge context
        opens.clear();
        w.forceMerge(1);
      }
      assertEquals(
          "the merge mapped vectors its own readers already map: " + opens,
          List.of(),
          sequentialOpens(opens));
      assertEquals("the merge re-advised the vectors: " + opens, List.of(), opens.advised());
    }
  }

  /** Searches that do not read the vectors at random leave nothing for a merge to undo. */
  public void testNoMappingWhenSearchesDoNotReadAtRandom() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) getOnlyLeafReader(reader);
        try (FlatVectorsReader flat = flatReader(dir, segment, IOContext.DEFAULT)) {
          opens.clear();
          assertSame(flat, flat.getMergeInstance());
          flat.finishMerge();
          assertEquals("nothing to map again: " + opens, List.of(), opens.all());
        }
      }
    }
  }

  /** The merge mapping changes how the file is read and keeps everything else the caller said. */
  public void testTheMergeMappingKeepsWhatTheCallerSaid() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SegmentReader segment = (SegmentReader) getOnlyLeafReader(reader);
        IOContext searchContext =
            IOContext.DEFAULT.withHints(
                DataAccessHint.RANDOM, NoReuseHint.INSTANCE, CallerHint.INSTANCE);
        try (FlatVectorsReader flat = flatReader(dir, segment, searchContext)) {
          opens.clear();
          FlatVectorsReader mergeInstance = flat.getMergeInstance();
          List<Open> mapped = sequentialOpens(opens);
          assertEquals("one mapping for the merge: " + opens, 1, mapped.size());
          Set<IOContext.FileOpenHint> hints = mapped.get(0).hints();
          assertTrue("the caller's hint is kept: " + hints, hints.contains(CallerHint.INSTANCE));
          assertTrue(hints.contains(NoReuseHint.INSTANCE));
          assertTrue(hints.contains(FileTypeHint.DATA));
          assertTrue(hints.contains(FileDataHint.KNN_VECTORS));
          assertFalse(
              "searches read at random, not the merge: " + hints,
              hints.contains(DataAccessHint.RANDOM));
          mergeInstance.finishMerge();
        }
      }
    }
  }

  /** A hint only the caller knows about. */
  private enum CallerHint implements IOContext.FileOpenHint {
    INSTANCE
  }

  /**
   * A flat vectors reader over the only vectors file of {@code segment}, opened with {@code
   * context}.
   */
  private static FlatVectorsReader flatReader(
      Directory dir, SegmentReader segment, IOContext context) throws IOException {
    String vectorData = null;
    for (String file : segment.getSegmentInfo().files()) {
      if (file.endsWith("." + Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION)) {
        vectorData = file;
      }
    }
    assertNotNull(vectorData);
    String segmentName = segment.getSegmentInfo().info.name;
    // the per-field suffix, between the segment name and the extension
    String suffix = vectorData.substring(segmentName.length() + 1, vectorData.lastIndexOf('.'));
    SegmentReadState state =
        new SegmentReadState(
            dir, segment.getSegmentInfo().info, segment.getFieldInfos(), context, suffix);
    return new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
        .fieldsReader(state);
  }

  public void testTheMappingIsSharedThenReleasedByTheLastMergeInstance() throws Exception {
    assertMappingIsSharedThenReleasedByTheLastMergeInstance(new Lucene99HnswVectorsFormat());
  }

  public void testQuantizedMappingIsSharedThenReleasedByTheLastMergeInstance() throws Exception {
    assertMappingIsSharedThenReleasedByTheLastMergeInstance(
        new Lucene104HnswScalarQuantizedVectorsFormat());
  }

  private static void assertMappingIsSharedThenReleasedByTheLastMergeInstance(
      KnnVectorsFormat format) throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader))
                .getVectorReader()
                .unwrapReaderForField("field");
        opens.clear();

        KnnVectorsReader first = vectors.getMergeInstance();
        List<Open> mapped = sequentialOpens(opens);
        assertEquals("one mapping for the first merge instance: " + opens, 1, mapped.size());

        KnnVectorsReader second = vectors.getMergeInstance();
        assertEquals(
            "a second merge instance shares it: " + opens, 1, sequentialOpens(opens).size());

        first.finishMerge();
        assertFalse("still held by the second instance: " + opens, mapped.get(0).closed());

        second.finishMerge();
        assertTrue("released by the last instance: " + opens, mapped.get(0).closed());

        KnnVectorsReader third = vectors.getMergeInstance();
        assertEquals("a later merge maps it again: " + opens, 2, sequentialOpens(opens).size());
        third.finishMerge();
      }
    }
  }

  /**
   * Fields sharing a reader each ask it for a merge instance, so one segment has several merge
   * instances at once. They share one mapping, each through a clone of its own.
   */
  public void testFieldsSharingAReaderShareOneMapping() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      int segments = 3;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < segments; segment++) {
          for (int i = 0; i < 16; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("a", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            doc.add(new KnnFloatVectorField("b", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            doc.add(new KnnFloatVectorField("c", vector(), VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
          }
          w.commit();
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(segments, reader.leaves().size());
          opens.clear();
          w.forceMerge(1);

          List<Open> sequential = sequentialOpens(opens);
          assertEquals("one mapping per merged segment: " + opens, segments, sequential.size());
          for (Open open : sequential) {
            assertTrue("released once every field was done: " + open, open.closed());
          }
        }
      }
    }
  }

  /**
   * A merge instance gives the mapping back once. Finishing it again, or finishing the reader it
   * came from, must not release the mapping another merge instance still reads.
   */
  public void testOnlyAMergeInstanceReleasesTheMappingAndOnlyOnce() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      List<Open> mapped;
      KnnVectorsReader third;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader))
                .getVectorReader()
                .unwrapReaderForField("field");
        opens.clear();

        KnnVectorsReader first = vectors.getMergeInstance();
        KnnVectorsReader second = vectors.getMergeInstance();
        mapped = sequentialOpens(opens);
        assertEquals("one mapping for both merge instances: " + opens, 1, mapped.size());

        first.finishMerge();
        first.finishMerge();
        assertFalse("finishing twice released another's hold: " + opens, mapped.get(0).closed());

        vectors.finishMerge();
        assertFalse("the reader itself holds nothing to release: " + opens, mapped.get(0).closed());

        // the second instance still reads through the mapping
        FloatVectorValues values = second.getFloatVectorValues("field");
        assertEquals(DIM, values.vectorValue(0).length);

        second.finishMerge();
        assertTrue("released by the last instance: " + opens, mapped.get(0).closed());

        // a merge instance left unfinished when the reader closes
        third = vectors.getMergeInstance();
        mapped = sequentialOpens(opens);
        assertEquals(2, mapped.size());
      }
      assertTrue("closed with the reader: " + opens, mapped.get(1).closed());
      // and finishing it afterwards does not close it a second time
      third.finishMerge();
    }
  }

  /** The mapping goes with the reader, even if no merge ever says it is finished. */
  public void testTheMappingIsClosedWithTheReader() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      List<Open> mapped;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader))
                .getVectorReader()
                .unwrapReaderForField("field");
        opens.clear();

        // a merge instance nobody finishes, as an abandoned merge leaves behind
        assertNotNull(vectors.getMergeInstance());
        mapped = sequentialOpens(opens);
        assertEquals("one mapping for the merge instance: " + opens, 1, mapped.size());
        assertFalse("still held by the merge instance: " + opens, mapped.get(0).closed());
      }
      assertTrue("closed with the reader: " + mapped.get(0), mapped.get(0).closed());
    }
  }

  /** A merge open that fails leaves nothing behind, so a later merge still releases the mapping. */
  public void testAFailedMergeOpenIsNotCountedAsAMergeInstance() throws Exception {
    Opens opens = new Opens();
    AtomicBoolean failMergeOpens = new AtomicBoolean();
    try (Directory dir =
        new RecordingDirectory(newDirectory(), opens) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            if (failMergeOpens.get()
                && name.endsWith(".vec")
                && context.context() == IOContext.Context.MERGE) {
              throw new IOException("injected");
            }
            return super.openInput(name, context);
          }
        }) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
      iwc.setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 16; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader))
                .getVectorReader()
                .unwrapReaderForField("field");
        opens.clear();

        failMergeOpens.set(true);
        expectThrows(IOException.class, vectors::getMergeInstance);
        assertEquals("a failed open maps nothing: " + opens, List.of(), sequentialOpens(opens));

        failMergeOpens.set(false);
        KnnVectorsReader merging = vectors.getMergeInstance();
        List<Open> mapped = sequentialOpens(opens);
        assertEquals("one mapping for the one merge instance: " + opens, 1, mapped.size());

        merging.finishMerge();
        assertTrue("released by the only merge instance: " + opens, mapped.get(0).closed());
      }
    }
  }

  private static float[] vector() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = random().nextFloat() + 0.01f;
    }
    return v;
  }

  /** The opens a merge made for itself: the vector data, read front to back. */
  private static List<Open> sequentialOpens(Opens opens) {
    List<Open> sequential = new ArrayList<>();
    for (Open open : opens.all()) {
      // integrity checks read a file once, front to back, and say so with READONCE
      if (open.name().endsWith(".vec")
          && open.hint() == DataAccessHint.SEQUENTIAL
          && open.hints().contains(ReadOnceHint.INSTANCE) == false) {
        assertSame(
            "the open says a merge is reading, so a directory can route it: " + open,
            IOContext.Context.MERGE,
            open.context());
        sequential.add(open);
      }
    }
    return sequential;
  }

  /** One {@link Directory#openInput} call. */
  private static final class Open {
    private final String name;
    private final IOContext context;
    private volatile boolean closed;
    private volatile boolean read;

    Open(String name, IOContext context) {
      this.name = name;
      this.context = context;
    }

    String name() {
      return name;
    }

    Set<IOContext.FileOpenHint> hints() {
      return context.hints();
    }

    DataAccessHint hint() {
      return context.hints(DataAccessHint.class).findFirst().orElse(null);
    }

    IOContext.Context context() {
      return context.context();
    }

    boolean closed() {
      return closed;
    }

    /** Whether the input was sliced, which is how the vectors are read. */
    boolean read() {
      return read;
    }

    @Override
    public String toString() {
      return name + " [hint=" + hint() + " closed=" + closed + " read=" + read + "]";
    }
  }

  private static final class Opens {
    private final List<Open> opens = new ArrayList<>();
    private final List<String> advised = new ArrayList<>();

    synchronized Open record(String name, IOContext context) {
      Open open = new Open(name, context);
      opens.add(open);
      return open;
    }

    /** Files whose advice was changed after they were opened. */
    synchronized void recordAdvice(String name) {
      advised.add(name);
    }

    synchronized List<Open> all() {
      return List.copyOf(opens);
    }

    synchronized List<String> advised() {
      return List.copyOf(advised);
    }

    synchronized void clear() {
      opens.clear();
      advised.clear();
    }

    @Override
    public synchronized String toString() {
      return "opens: " + opens + ", advised: " + advised;
    }
  }

  private static class RecordingDirectory extends FilterDirectory {
    private final Opens opens;

    RecordingDirectory(Directory in, Opens opens) {
      super(in);
      this.opens = opens;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = opens.record(name, context);
      return new RecordingIndexInput(super.openInput(name, context), name, open, opens);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final String name;
    private final Open open;
    private final Opens opens;

    RecordingIndexInput(IndexInput in, String name, Open open, Opens opens) {
      super("Recording(" + name + ")", in);
      this.name = name;
      this.open = open;
      this.opens = opens;
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
      opens.recordAdvice(name);
      in.updateIOContext(context);
    }

    @Override
    public void close() throws IOException {
      open.closed = true;
      in.close();
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(in.clone(), name, open, opens);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      open.read = true;
      return new RecordingIndexInput(in.slice(sliceDescription, offset, length), name, open, opens);
    }
  }
}
