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
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadOnceHint;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * Who says how vector files are read: the format that walks them, rather than the one that holds
 * them. Directories decide what to make of it.
 */
public class TestVectorAccessHints extends LuceneTestCase {

  private static final int DIM = 8;
  private static final String FIELD = "field";

  /** The vectors the graph build scores against, reopened from the segment the merge just wrote. */
  public void testMergedVectorsStateTheirAccessPattern() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      // threshold 0: always build the graph, whatever the segment size
      try (IndexWriter w =
          new IndexWriter(
              dir,
              writerConfig(
                  new Lucene99HnswVectorsFormat(
                      Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                      Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                      0)))) {
        for (int segment = 0; segment < 2; segment++) {
          addDocuments(w, 128);
          w.commit();
        }
        opens.clear();
        w.forceMerge(1);
      }
      assertVectorHints(opens.endingWith(Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION), opens);
    }
  }

  /** The quantized vectors a merge writes to a temporary file and scores against. */
  public void testMergeTemporaryFileStatesItsAccessPattern() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      // an asymmetric encoding quantizes the query side differently, which is what makes the merge
      // write these query vectors to a temporary file
      try (IndexWriter w =
          new IndexWriter(
              dir,
              writerConfig(
                  new Lucene104HnswScalarQuantizedVectorsFormat(
                      ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE,
                      Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN,
                      Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH,
                      1,
                      null,
                      0)))) {
        for (int segment = 0; segment < 2; segment++) {
          addDocuments(w, 128);
          w.commit();
        }
        opens.clear();
        w.forceMerge(1);
      }

      List<Open> temps = new ArrayList<>();
      for (Open open : opens.endingWith("tmp")) {
        if (open.name().contains("_queries_")) {
          temps.add(open);
        }
      }
      assertFalse("no temporary file was read back: " + opens, temps.isEmpty());
      assertVectorHints(temps, opens);
    }
  }

  /**
   * A flat format on its own says nothing about how its vectors are read. Only a format that walks
   * them, like HNSW, knows that, so only it asks for random access.
   */
  public void testFlatFormatClaimsNoAccessPattern() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      try (IndexWriter w = new IndexWriter(dir, writerConfig(new Lucene99HnswVectorsFormat()))) {
        addDocuments(w, 16);
        w.commit();
      }

      opens.clear();
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
          .fieldsReader(flatReadState(dir, IOContext.DEFAULT))
          .close();

      for (Open open : vectorFileOpens(opens)) {
        assertNull("a flat format assumed how its vectors are read: " + open, open.hint());
      }
    }
  }

  /** A caller that says how it reads the vectors is followed, whatever the format would say. */
  public void testReadStateAccessHintWins() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      try (IndexWriter w = new IndexWriter(dir, writerConfig(new Lucene99HnswVectorsFormat()))) {
        addDocuments(w, 16);
        w.commit();
      }

      opens.clear();
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
          .fieldsReader(
              flatReadState(
                  dir,
                  IOContext.DEFAULT.withHints(
                      FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.SEQUENTIAL)))
          .close();

      for (Open open : vectorFileOpens(opens)) {
        assertEquals(
            "the format ignored what the caller asked for: " + open,
            DataAccessHint.SEQUENTIAL,
            open.hint());
      }
    }
  }

  /**
   * A graph scores the quantized vectors as it walks and reads the raw ones back to rescore. Both
   * are read at random, and both take the access pattern asked of the format holding them.
   */
  public void testQuantizedAndRawVectorsAreReadAtRandom() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      try (IndexWriter w =
          new IndexWriter(dir, writerConfig(new Lucene104HnswScalarQuantizedVectorsFormat()))) {
        opens.clear();
        addDocuments(w, 16);
        w.commit();
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(1, reader.leaves().size());
        }
      }

      List<Open> quantized = opens.endingWith("veq");
      assertFalse("no quantized vectors were opened: " + opens, quantized.isEmpty());
      for (Open open : quantized) {
        if (open.context().hints().contains(ReadOnceHint.INSTANCE) == false) {
          assertEquals(
              "the quantized vectors are scored in graph order: " + open,
              DataAccessHint.RANDOM,
              open.hint());
        }
      }

      for (Open open : vectorFileOpens(opens)) {
        assertEquals(
            "the raw vectors are rescored at random: " + open, DataAccessHint.RANDOM, open.hint());
      }
    }
  }

  /** A read state for the flat format holding the vectors of the single segment in {@code dir}. */
  private static SegmentReadState flatReadState(Directory dir, IOContext context)
      throws IOException {
    SegmentInfo info = SegmentInfos.readLatestCommit(dir).info(0).info;
    FieldInfos fieldInfos =
        info.getCodec().fieldInfosFormat().read(dir, info, "", IOContext.DEFAULT);
    return new SegmentReadState(dir, info, fieldInfos, context, "Lucene99HnswVectorsFormat_0");
  }

  /** Opens of the vectors data file, skipping the ones integrity checks make. */
  private static List<Open> vectorFileOpens(Opens opens) {
    List<Open> vectorOpens =
        opens.endingWith(Lucene99FlatVectorsFormat.VECTOR_DATA_EXTENSION).stream()
            .filter(open -> open.context().hints().contains(ReadOnceHint.INSTANCE) == false)
            .toList();
    assertFalse("no vectors were opened: " + opens, vectorOpens.isEmpty());
    return vectorOpens;
  }

  private static void assertVectorHints(List<Open> allOpens, Opens opens) {
    List<Open> vectorOpens = new ArrayList<>();
    for (Open open : allOpens) {
      // integrity checks read the file once, front to back, and say so with READONCE
      if (open.context().hints().contains(ReadOnceHint.INSTANCE) == false) {
        vectorOpens.add(open);
      }
    }
    assertFalse("no vectors were opened: " + opens, vectorOpens.isEmpty());
    for (Open open : vectorOpens) {
      assertTrue(
          "opened without saying it holds vectors: " + open,
          open.context().hints().contains(FileDataHint.KNN_VECTORS));
      assertTrue(
          "opened without saying how it is read: " + open,
          open.context().hints().contains(DataAccessHint.RANDOM));
    }
  }

  private static IndexWriterConfig writerConfig(KnnVectorsFormat format) {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
    // keep the vector files out of a compound file so the directory sees them by name
    iwc.setUseCompoundFile(false);
    return iwc;
  }

  private static void addDocuments(IndexWriter w, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      Document doc = new Document();
      doc.add(
          new KnnFloatVectorField(
              FIELD,
              BaseKnnVectorsFormatTestCase.randomNormalizedVector(DIM),
              VectorSimilarityFunction.DOT_PRODUCT));
      w.addDocument(doc);
    }
  }

  /** One {@link Directory#openInput} call. */
  private record Open(String name, IOContext context) {
    DataAccessHint hint() {
      return context.hints(DataAccessHint.class).findFirst().orElse(null);
    }

    @Override
    public String toString() {
      return name + " " + context.hints();
    }
  }

  private static final class Opens {
    private final List<Open> opens = new ArrayList<>();

    synchronized void record(String name, IOContext context) {
      opens.add(new Open(name, context));
    }

    synchronized void clear() {
      opens.clear();
    }

    synchronized List<Open> endingWith(String extension) {
      List<Open> matching = new ArrayList<>();
      for (Open open : opens) {
        if (open.name().endsWith("." + extension)) {
          matching.add(open);
        }
      }
      return matching;
    }

    @Override
    public synchronized String toString() {
      return opens.toString();
    }
  }

  private static final class RecordingDirectory extends FilterDirectory {
    private final Opens opens;

    RecordingDirectory(Directory in, Opens opens) {
      super(in);
      this.opens = opens;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      opens.record(name, context);
      return super.openInput(name, context);
    }
  }
}
