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
package org.apache.lucene.sandbox.codecs.dedup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Searches read the de-duplicated vectors at random, and read advice applies to a whole mapping, so
 * a merge opens the data files for itself without that advice.
 */
public class TestDedupVectorsMergeReadAdvice extends LuceneTestCase {

  private static final int DIM = 8;

  public void testFlatMergeOpensItsOwnVectors() throws Exception {
    assertMergeOpensItsOwnVectors(new DedupHnswVectorsFormat(), List.of("vdd"));
  }

  public void testQuantizedMergeOpensItsOwnVectors() throws Exception {
    assertMergeOpensItsOwnVectors(
        new DedupHnswScalarQuantizedVectorsFormat(), List.of("vdd", "vdqd"));
  }

  private void assertMergeOpensItsOwnVectors(KnnVectorsFormat format, List<String> extensions)
      throws Exception {
    Opens opens = new Opens();
    // vectors repeat within and across segments, so the merge compares and revisits them
    float[][] distinct = new float[8][];
    for (int i = 0; i < distinct.length; i++) {
      distinct[i] = vector();
    }
    List<float[]> indexed = new ArrayList<>();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
      iwc.setUseCompoundFile(false); // so the directory sees the data files by name
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int segment = 0; segment < 3; segment++) {
          for (int i = 0; i < 32; i++) {
            float[] v = distinct[random().nextInt(distinct.length)];
            Document doc = new Document();
            doc.add(new StoredField("id", indexed.size()));
            doc.add(new KnnFloatVectorField("field", v, VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
            indexed.add(v);
          }
          w.commit();
        }

        // searches open the segments first, so the merge finds readers that are already open
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          assertEquals(3, reader.leaves().size());
          opens.clear();
          w.forceMerge(1);

          for (String extension : extensions) {
            List<Open> merged = mergeOpens(opens, extension);
            assertFalse(
                "the merge never opened ." + extension + " for itself: " + opens, merged.isEmpty());
            for (Open open : merged) {
              assertTrue("the merge kept ." + extension + " open: " + open, open.closed());
            }
          }
        }
        w.commit();
      }
      assertEquals(
          "the merge re-advised the vectors searches are reading: " + opens,
          List.of(),
          opens.advised());

      // and the merge read the right vectors
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        FloatVectorValues values = leaf.getFloatVectorValues("field");
        KnnVectorValues.DocIndexIterator it = values.iterator();
        int count = 0;
        for (int doc = it.nextDoc();
            doc != KnnVectorValues.DocIndexIterator.NO_MORE_DOCS;
            doc = it.nextDoc()) {
          int id = leaf.storedFields().document(doc).getField("id").numericValue().intValue();
          assertTrue(Arrays.equals(indexed.get(id), values.vectorValue(it.index())));
          count++;
        }
        assertEquals(indexed.size(), count);
      }
    }
  }

  public void testTheMappingIsSharedThenReleasedByTheLastMergeInstance() throws Exception {
    Opens opens = new Opens();
    try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setCodec(
          TestUtil.alwaysKnnVectorsFormat(
              random().nextBoolean()
                  ? new DedupHnswVectorsFormat()
                  : new DedupHnswScalarQuantizedVectorsFormat()));
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
      KnnVectorsReader abandoned;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        KnnVectorsReader vectors =
            ((CodecReader) getOnlyLeafReader(reader))
                .getVectorReader()
                .unwrapReaderForField("field");
        opens.clear();

        KnnVectorsReader first = vectors.getMergeInstance();
        mapped = mergeOpens(opens, "vdd");
        assertEquals("one mapping for the first merge instance: " + opens, 1, mapped.size());

        KnnVectorsReader second = vectors.getMergeInstance();
        assertEquals(
            "a second merge instance shares it: " + opens, 1, mergeOpens(opens, "vdd").size());

        first.finishMerge();
        assertFalse("still held by the second instance: " + opens, mapped.get(0).closed());
        first.finishMerge();
        assertFalse("finishing twice released another's hold: " + opens, mapped.get(0).closed());
        vectors.finishMerge();
        assertFalse("the reader itself holds nothing to release: " + opens, mapped.get(0).closed());

        second.finishMerge();
        assertTrue("released by the last instance: " + opens, mapped.get(0).closed());

        // a merge instance nobody finishes, as an abandoned merge leaves behind
        abandoned = vectors.getMergeInstance();
        mapped = mergeOpens(opens, "vdd");
        assertEquals("a later merge maps it again: " + opens, 2, mapped.size());
        assertFalse(mapped.get(1).closed());
      }
      assertTrue("closed with the reader: " + mapped.get(1), mapped.get(1).closed());
      // and finishing it afterwards does not close it a second time
      abandoned.finishMerge();
    }
  }

  private static float[] vector() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = random().nextFloat() + 0.01f;
    }
    return v;
  }

  /** The opens a merge made for itself of the data file with this extension. */
  private static List<Open> mergeOpens(Opens opens, String extension) {
    List<Open> merge = new ArrayList<>();
    for (Open open : opens.all()) {
      // integrity checks read the whole file once and say so with an access hint
      if (open.name().endsWith("." + extension)
          && open.context().context() == IOContext.Context.MERGE
          && open.context().hints(DataAccessHint.class).findAny().isEmpty()) {
        merge.add(open);
      }
    }
    return merge;
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

    IOContext context() {
      return context;
    }

    boolean closed() {
      return closed;
    }

    @Override
    public String toString() {
      return name + " [" + context.context() + " " + context.hints() + " closed=" + closed + "]";
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
      return new RecordingIndexInput(in.slice(sliceDescription, offset, length), name, open, opens);
    }
  }
}
