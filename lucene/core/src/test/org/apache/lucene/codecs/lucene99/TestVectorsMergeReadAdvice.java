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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
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
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()));
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
            assertTrue("the merge kept its vectors open: " + open, open.closed());
          }
        }
      }

      assertEquals(
          "the merge re-advised the vectors searches are reading: " + opens,
          List.of(),
          opens.advised());
    }
  }

  public void testTheMappingIsSharedThenReleasedByTheLastMergeInstance() throws Exception {
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

  private static float[] vector() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = random().nextFloat() + 0.01f;
    }
    return v;
  }

  /** The opens a merge made for itself: the vector data, read front to back and once. */
  private static List<Open> sequentialOpens(Opens opens) {
    List<Open> sequential = new ArrayList<>();
    for (Open open : opens.all()) {
      // integrity checks read a file once, front to back, and say so with READONCE
      if (open.name().endsWith(".vec")
          && open.hint() == DataAccessHint.SEQUENTIAL
          && open.hints().contains(ReadOnceHint.INSTANCE) == false) {
        assertTrue(
            "a merge reads the vectors once and does not come back: " + open,
            open.hints().contains(NoReuseHint.INSTANCE));
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

    @Override
    public String toString() {
      return name + " [hint=" + hint() + " closed=" + closed + "]";
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

  private static final class RecordingDirectory extends FilterDirectory {
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
      return new RecordingIndexInput(in.slice(sliceDescription, offset, length), name, open, opens);
    }
  }
}
