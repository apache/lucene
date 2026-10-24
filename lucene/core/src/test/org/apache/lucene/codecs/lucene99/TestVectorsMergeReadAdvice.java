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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Raw vectors read at random get a second input for sequential reads, shared by merges and
 * sequential values and closed with the reader.
 */
public class TestVectorsMergeReadAdvice extends LuceneTestCase {

  private static final int DIM = 8;

  private static List<KnnVectorsFormat> formats() {
    return List.of(
        new Lucene99HnswVectorsFormat(), new Lucene104HnswScalarQuantizedVectorsFormat());
  }

  public void testMergeOpensTheVectorsAgain() throws Exception {
    for (KnnVectorsFormat format : formats()) {
      List<Open> opens = new CopyOnWriteArrayList<>();
      List<Open> sequential;
      try (Directory dir = new RecordingDirectory(newDirectory(), opens);
          IndexWriter w = writer(dir, format, false)) {
        addSegments(w, 2);
        // open the segments first, so the merge reuses readers that read at random
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(2, reader.leaves().size());
          opens.clear();
          w.forceMerge(1);
          sequential = sequentialOpens(opens);
          assertEquals(opens.toString(), 2, sequential.size());
          for (Open open : sequential) {
            assertTrue(open.toString(), open.read);
            assertEquals(
                "keeps the format's hints: " + open,
                format instanceof Lucene104HnswScalarQuantizedVectorsFormat,
                open.context.hints().contains(NoReuseHint.INSTANCE));
          }
        }
      }
      for (Open open : sequential) {
        assertTrue("closed with the reader: " + open, open.closed);
      }
    }
  }

  /** In a compound segment, a merge opens the compound file again, without advice. */
  public void testCompoundMergeOpensTheCompoundFileAgain() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
    List<Open> cfsOpens;
    try (Directory dir = new RecordingDirectory(newDirectory(), opens);
        IndexWriter w = writer(dir, new Lucene99HnswVectorsFormat(), true)) {
      addSegments(w, 2);
      try (DirectoryReader reader = DirectoryReader.open(w)) {
        assertEquals(2, reader.leaves().size());
        opens.clear();
        w.forceMerge(1);
        cfsOpens = opens.stream().filter(o -> o.name.endsWith(".cfs")).toList();
        assertEquals(opens.toString(), 2, cfsOpens.size());
        for (Open open : cfsOpens) {
          assertTrue(open.toString(), open.read);
          assertEquals(open.toString(), Set.of(), open.context.hints());
        }
      }
    }
    for (Open open : cfsOpens) {
      assertTrue("closed with the reader: " + open, open.closed);
    }
  }

  /** A reader opened by a merge already reads sequentially. */
  public void testMergeReadsThroughTheReaderItOpened() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      try (IndexWriter w = writer(dir, new Lucene99HnswVectorsFormat(), false)) {
        addSegments(w, 2);
        opens.clear();
        w.forceMerge(1);
      }
      assertEquals(opens.toString(), List.of(), sequentialOpens(opens));
    }
  }

  /** Nothing is opened again when the input already serves sequential reads. */
  public void testNoSecondInputWhenNotNeeded() throws Exception {
    assertNoSecondInput(true, IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL));
    assertNoSecondInput(
        false, IOContext.DEFAULT.withHints(DataAccessHint.RANDOM, NoReuseHint.INSTANCE));
  }

  private static void assertNoSecondInput(boolean advises, IOContext context) throws Exception {
    RecordingDirectory dir =
        new RecordingDirectory(newDirectory(), new CopyOnWriteArrayList<>(), advises);
    try (dir;
        DirectoryReader reader = openReader(dir, new Lucene99HnswVectorsFormat());
        FlatVectorsReader flat = flatReader(dir, reader, context)) {
      dir.opens.clear();
      assertSame(flat, flat.getMergeInstance());
      assertEquals(dir.opens.toString(), List.of(), dir.opens);
    }
  }

  /** The sequential input keeps the caller's hints and only changes the access. */
  public void testSequentialInputKeepsTheCallerHints() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
    IOContext context =
        IOContext.DEFAULT.withHints(
            DataAccessHint.RANDOM, NoReuseHint.INSTANCE, CallerHint.INSTANCE);
    try (Directory dir = new RecordingDirectory(newDirectory(), opens);
        DirectoryReader reader = openReader(dir, new Lucene99HnswVectorsFormat());
        FlatVectorsReader flat = flatReader(dir, reader, context)) {
      opens.clear();
      assertNotSame(flat, flat.getMergeInstance());
      List<Open> sequential = sequentialOpens(opens);
      assertEquals(opens.toString(), 1, sequential.size());
      Set<IOContext.FileOpenHint> hints = sequential.get(0).context.hints();
      assertTrue(hints.toString(), hints.contains(CallerHint.INSTANCE));
      assertTrue(hints.toString(), hints.contains(NoReuseHint.INSTANCE));
      assertFalse(hints.toString(), hints.contains(DataAccessHint.RANDOM));
    }
  }

  private enum CallerHint implements IOContext.FileOpenHint {
    INSTANCE
  }

  /** Merges and sequential values share one input, kept until the reader closes. */
  public void testOneSequentialInputUntilTheReaderCloses() throws Exception {
    for (KnnVectorsFormat format : formats()) {
      List<Open> opens = new CopyOnWriteArrayList<>();
      List<Open> sequential;
      try (Directory dir = new RecordingDirectory(newDirectory(), opens);
          DirectoryReader reader = openReader(dir, format)) {
        KnnVectorsReader vectors = ((CodecReader) getOnlyLeafReader(reader)).getVectorReader();
        opens.clear();

        FloatVectorValues random = vectors.getFloatVectorValues("field", DataAccessHint.RANDOM);
        assertEquals(opens.toString(), List.of(), sequentialOpens(opens));

        FloatVectorValues values = vectors.getFloatVectorValues("field", DataAccessHint.SEQUENTIAL);
        sequential = sequentialOpens(opens);
        assertEquals(opens.toString(), 1, sequential.size());
        assertEquals(random.size(), values.size());
        for (int ord = 0; ord < random.size(); ord++) {
          assertArrayEquals(random.vectorValue(ord), values.vectorValue(ord), 0f);
        }

        KnnVectorsReader field = vectors.unwrapReaderForField("field");
        field.getMergeInstance().finishMerge();
        assertEquals(DIM, field.getMergeInstance().getFloatVectorValues("field").dimension());
        assertEquals(opens.toString(), 1, sequentialOpens(opens).size());
        assertFalse("kept while the reader is open", sequential.get(0).closed);
      }
      assertTrue("closed with the reader", sequential.get(0).closed);
    }
  }

  public void testNoSecondInputOnceClosed() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens);
        DirectoryReader reader = openReader(dir, new Lucene99HnswVectorsFormat())) {
      FlatVectorsReader flat =
          flatReader(
              dir,
              reader,
              IOContext.DEFAULT.withHints(DataAccessHint.RANDOM, NoReuseHint.INSTANCE));
      flat.close();
      opens.clear();
      expectThrows(AlreadyClosedException.class, flat::getMergeInstance);
      assertEquals(opens.toString(), List.of(), sequentialOpens(opens));
    }
  }

  public void testFailedOpenIsRetried() throws Exception {
    List<Open> opens = new CopyOnWriteArrayList<>();
    AtomicBoolean fail = new AtomicBoolean();
    Directory dir =
        new RecordingDirectory(newDirectory(), opens) {
          @Override
          public IndexInput openInput(String name, IOContext context) throws IOException {
            if (fail.get()
                && name.endsWith(".vec")
                && context.hints().contains(DataAccessHint.SEQUENTIAL)) {
              throw new IOException("injected");
            }
            return super.openInput(name, context);
          }
        };
    try (dir;
        DirectoryReader reader = openReader(dir, new Lucene99HnswVectorsFormat())) {
      KnnVectorsReader vectors =
          ((CodecReader) getOnlyLeafReader(reader)).getVectorReader().unwrapReaderForField("field");
      opens.clear();
      fail.set(true);
      expectThrows(IOException.class, vectors::getMergeInstance);
      assertEquals(opens.toString(), List.of(), sequentialOpens(opens));
      fail.set(false);
      vectors.getMergeInstance();
      assertEquals(opens.toString(), 1, sequentialOpens(opens).size());
    }
    assertTrue("closed with the reader", opens.getLast().closed);
  }

  private static IndexWriter writer(Directory dir, KnnVectorsFormat format, boolean compound)
      throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
    iwc.setUseCompoundFile(compound);
    return new IndexWriter(dir, iwc);
  }

  private static void addSegments(IndexWriter w, int segments) throws IOException {
    for (int segment = 0; segment < segments; segment++) {
      for (int i = 0; i < 16; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("field", vector(), VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(doc);
      }
      w.commit();
    }
  }

  /** Opens a reader over a single segment of vectors. */
  private static DirectoryReader openReader(Directory dir, KnnVectorsFormat format)
      throws IOException {
    try (IndexWriter w = writer(dir, format, false)) {
      addSegments(w, 1);
    }
    return DirectoryReader.open(dir);
  }

  /** A flat vectors reader over the vectors of the only segment, opened with {@code context}. */
  private static FlatVectorsReader flatReader(
      Directory dir, DirectoryReader reader, IOContext context) throws IOException {
    SegmentReader segment = (SegmentReader) getOnlyLeafReader(reader);
    String segmentName = segment.getSegmentInfo().info.name;
    String vectorData =
        segment.getSegmentInfo().files().stream()
            .filter(f -> f.endsWith("." + Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION))
            .findFirst()
            .orElseThrow();
    // the per-field suffix, between the segment name and the extension
    String suffix = vectorData.substring(segmentName.length() + 1, vectorData.lastIndexOf('.'));
    SegmentReadState state =
        new SegmentReadState(
            dir, segment.getSegmentInfo().info, segment.getFieldInfos(), context, suffix);
    return new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
        .fieldsReader(state);
  }

  private static float[] vector() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = random().nextFloat() + 0.01f;
    }
    return v;
  }

  /** The opens of the vector data for sequential reads, other than integrity checks. */
  private static List<Open> sequentialOpens(List<Open> opens) {
    return opens.stream()
        .filter(o -> o.name.endsWith(".vec"))
        .filter(o -> o.context.hints().contains(DataAccessHint.SEQUENTIAL))
        .filter(o -> o.context.hints().contains(ReadOnceHint.INSTANCE) == false)
        .toList();
  }

  /** One {@link Directory#openInput} call. */
  private static final class Open {
    final String name;
    final IOContext context;
    volatile boolean closed;
    // whether the input was sliced, which is how vectors are read
    volatile boolean read;

    Open(String name, IOContext context) {
      this.name = name;
      this.context = context;
    }

    @Override
    public String toString() {
      return name + context.hints() + (closed ? " closed" : "") + (read ? " read" : "");
    }
  }

  /**
   * Records opens. Its inputs are efficient for a context like memory-mapped inputs using {@link
   * MMapDirectory#ADVISE_BY_CONTEXT}, or for every context.
   */
  private static class RecordingDirectory extends FilterDirectory {
    final List<Open> opens;
    private final boolean advises;

    RecordingDirectory(Directory in, List<Open> opens) {
      this(in, opens, true);
    }

    RecordingDirectory(Directory in, List<Open> opens, boolean advises) {
      super(in);
      this.opens = opens;
      this.advises = advises;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      Open open = new Open(name, context);
      opens.add(open);
      return new RecordingIndexInput(super.openInput(name, context), open, context, advises);
    }
  }

  private static final class RecordingIndexInput extends FilterIndexInput {
    private final Open open;
    // the context the pages of this input were advised with
    private final IOContext context;
    private final boolean advises;

    RecordingIndexInput(IndexInput in, Open open, IOContext context, boolean advises) {
      super("Recording(" + open.name + ")", in);
      this.open = open;
      this.context = context;
      this.advises = advises;
    }

    @Override
    public boolean isEfficientFor(IOContext other) {
      if (advises == false) {
        return true;
      }
      ReadAdvice advice = MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, context).orElseThrow();
      ReadAdvice otherAdvice =
          MMapDirectory.ADVISE_BY_CONTEXT.apply(open.name, other).orElseThrow();
      return advice == ReadAdvice.NORMAL
          || otherAdvice == ReadAdvice.NORMAL
          || advice == otherAdvice;
    }

    @Override
    public void close() throws IOException {
      open.closed = true;
      in.close();
    }

    @Override
    public IndexInput clone() {
      return new RecordingIndexInput(in.clone(), open, context, advises);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      open.read = true;
      return new RecordingIndexInput(
          in.slice(sliceDescription, offset, length), open, context, advises);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context)
        throws IOException {
      open.read = true;
      return new RecordingIndexInput(
          in.slice(sliceDescription, offset, length, context), open, context, advises);
    }
  }
}
