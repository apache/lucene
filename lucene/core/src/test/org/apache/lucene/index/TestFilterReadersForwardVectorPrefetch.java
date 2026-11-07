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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RescoreTopNQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Filter readers that wrap {@link KnnVectorValues} have to forward {@link
 * KnnVectorValues#prefetch(int, int)} along with everything else. A wrapper that forwards {@code
 * vectorValue()} but not {@code prefetch()} inherits the no-op default, so prefetching silently
 * stops reaching the store and reads go back to one at a time. Nothing else changes, which is why
 * this needs asserting rather than eyeballing.
 */
public class TestFilterReadersForwardVectorPrefetch extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final String SORT_FIELD = "sort";
  private static final int DIMS = 4;

  public void testExitableDirectoryReaderForwardsPrefetch() throws Exception {
    try (PrefetchCountingDirectory dir = newIndex()) {
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // A timeout that never fires: the wrapping, not the timeout, is what is under test.
        DirectoryReader exitable = new ExitableDirectoryReader(reader, () -> false);
        LeafReader leaf = getOnlyLeafReader(exitable);
        assertPrefetchReachesStore("ExitableDirectoryReader", dir, leaf);
      }
    }
  }

  public void testSortingCodecReaderForwardsPrefetch() throws Exception {
    try (PrefetchCountingDirectory dir = newIndex()) {
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Sort sort = new Sort(new SortField(SORT_FIELD, SortField.Type.LONG, true));
        CodecReader sorting =
            SortingCodecReader.wrap(SlowCodecReaderWrapper.wrap(getOnlyLeafReader(reader)), sort);
        assertPrefetchReachesStore("SortingCodecReader", dir, sorting);
      }
    }
  }

  public void testFullPrecisionRescoringWorksThroughExitableReader() throws Exception {
    // Prefetching runs over a second copy() of the vector values, so a wrapper whose copy() throws
    // breaks rescoring outright rather than just losing the prefetch. A search carrying a timeout
    // goes through ExitableDirectoryReader, so this is the common case, not an exotic one.
    try (PrefetchCountingDirectory dir = newIndex()) {
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        DirectoryReader exitable = new ExitableDirectoryReader(reader, () -> false);
        IndexSearcher searcher = new IndexSearcher(exitable);
        float[] target = new float[DIMS];
        target[0] = 1f;
        Query knn = new KnnFloatVectorQuery(FIELD, target, 8);
        TopDocs topDocs =
            searcher.search(
                RescoreTopNQuery.createFullPrecisionRescorerQuery(knn, target, FIELD, 4), 4);
        assertEquals(4, topDocs.scoreDocs.length);
      }
    }
  }

  public void testUnwrappedReaderPrefetchesForComparison() throws Exception {
    // Guards the test itself: if the plain reader stopped prefetching, the assertions above would
    // pass or fail for reasons that have nothing to do with the wrappers.
    try (PrefetchCountingDirectory dir = newIndex()) {
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertPrefetchReachesStore("unwrapped reader", dir, getOnlyLeafReader(reader));
      }
    }
  }

  private static void assertPrefetchReachesStore(
      String what, PrefetchCountingDirectory dir, LeafReader leaf) throws IOException {
    FloatVectorValues values = leaf.getFloatVectorValues(FIELD);
    assertNotNull(what + " has no vector values", values);
    int before = dir.prefetchCount(".vec");
    values.prefetch(0, 2);
    assertTrue(
        what
            + " dropped prefetch: no read was started on the raw vector file. Counted prefetches "
            + "per extension: "
            + dir.counts,
        dir.prefetchCount(".vec") > before);
  }

  private PrefetchCountingDirectory newIndex() throws IOException {
    PrefetchCountingDirectory dir = new PrefetchCountingDirectory(new ByteBuffersDirectory());
    IndexWriterConfig config = new IndexWriterConfig();
    config.setCodec(
        TestUtil.alwaysKnnVectorsFormat(new Lucene104HnswScalarQuantizedVectorsFormat()));
    // Attribute prefetches to the vector file itself rather than to an enclosing .cfs, and keep
    // everything in one segment so getOnlyLeafReader() applies.
    config.setUseCompoundFile(false);
    config.setMaxBufferedDocs(64);
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 16; i++) {
        Document doc = new Document();
        float[] vector = new float[DIMS];
        vector[i % DIMS] = 1f;
        doc.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.DOT_PRODUCT));
        doc.add(new NumericDocValuesField(SORT_FIELD, i));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
    return dir;
  }

  /** Counts {@link IndexInput#prefetch} calls per file extension, including through slices. */
  private static class PrefetchCountingDirectory extends FilterDirectory {
    final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

    PrefetchCountingDirectory(Directory in) {
      super(in);
    }

    int prefetchCount(String extension) {
      AtomicInteger counter = counts.get(extension);
      return counter == null ? 0 : counter.get();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput in = super.openInput(name, context);
      int dot = name.lastIndexOf('.');
      String extension = dot < 0 ? name : name.substring(dot);
      AtomicInteger counter = counts.computeIfAbsent(extension, _ -> new AtomicInteger());
      return new CountingIndexInput(in, counter);
    }
  }

  private static class CountingIndexInput extends FilterIndexInput {
    private final AtomicInteger counter;

    CountingIndexInput(IndexInput in, AtomicInteger counter) {
      super("CountingIndexInput(" + in + ")", in);
      this.counter = counter;
    }

    @Override
    public boolean prefetch(long offset, long length) throws IOException {
      counter.incrementAndGet();
      // The count, not the return value, is what the assertions look at: ByteBuffersDirectory does
      // not implement prefetch, so the delegate reports false even though the call arrived.
      return in.prefetch(offset, length);
    }

    @Override
    public IndexInput clone() {
      return new CountingIndexInput(in.clone(), counter);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingIndexInput(in.slice(sliceDescription, offset, length), counter);
    }
  }
}
