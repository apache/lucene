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

package org.apache.lucene.sandbox.codecs.segmentivf;

import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.DEFAULT_OVERSAMPLE;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.PAGE_BYTES;
import static org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SECTION_FINE;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.FineTier;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.SearchStrategy;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.VectorUtil;

@ThreadLeakFilters(defaultFilters = true, filters = SegmentIVFBuildThreadsFilter.class)
public class TestSegmentIVFVectorsFormat extends LuceneTestCase {
  private static final int DIM = 24;

  /** The similarities SegmentIVF indexes: those that ignore a vector's length or require unit. */
  private static final VectorSimilarityFunction[] SIMILARITIES = {
    VectorSimilarityFunction.COSINE, VectorSimilarityFunction.DOT_PRODUCT
  };

  private static IndexWriterConfig config(
      FineTier fine, CoarseTier coarse, int nprobe, boolean sorted) {
    return config(DIM, fine, coarse, nprobe, sorted);
  }

  private static IndexWriterConfig config(
      int dim, FineTier fine, CoarseTier coarse, int nprobe, boolean sorted) {
    SegmentIVFVectorsFormat format = SegmentIVFTestUtil.format(8, dim, nprobe, 2, fine, coarse);
    IndexWriterConfig config =
        newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
    if (sorted) {
      config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    }
    return config;
  }

  private static float[] randomVector() {
    return randomVector(DIM);
  }

  private static float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int d = 0; d < dim; d++) {
      vector[d] = random().nextFloat() - 0.5f;
    }
    return vector;
  }

  /**
   * A field SegmentIVF cannot index rejects the documents that carry it, not the writer: thrown
   * from addField, the exception would be tragic and close the IndexWriter.
   */
  public void testUnsupportedFieldsRejectDocumentsNotTheWriter() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      List<Document> rejected = new ArrayList<>();
      for (VectorSimilarityFunction similarity :
          new VectorSimilarityFunction[] {
            VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
          }) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("float-" + similarity, randomVector(), similarity));
        rejected.add(doc);
      }
      Document bytes = new Document();
      byte[] byteVector = new byte[DIM];
      random().nextBytes(byteVector);
      bytes.add(new KnnByteVectorField("bytes", byteVector, VectorSimilarityFunction.DOT_PRODUCT));
      rejected.add(bytes);
      for (int round = 0; round < 2; round++) {
        for (Document doc : rejected) {
          IllegalArgumentException e =
              expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
          assertTrue(e.getMessage(), e.getMessage().startsWith("SegmentIVF supports only"));
        }
        for (int i = 0; i < 150; i++) {
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField(
                  "v", VectorUtil.l2normalize(randomVector()), VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.commit();
        assertRejectedFieldsAreEmpty(writer);
      }
      assertTrue(writer.isOpen());
      writer.forceMerge(1);
      assertRejectedFieldsAreEmpty(writer);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertEquals(300, reader.numDocs());
        TopDocs top =
            newSearcher(reader)
                .search(
                    new SegmentIVFKnnQuery("v", randomVector(), 10, null, new SearchStrategy(8)),
                    10);
        assertEquals(10, top.scoreDocs.length);
      }
      writer.commit();
      TestUtil.checkIndex(dir);
    }
  }

  /**
   * Fields SegmentIVF rejects keep vectors in their FieldInfo, so core code that finds them there
   * (FieldExistsQuery) must get empty values, before and after a merge.
   */
  private static void assertRejectedFieldsAreEmpty(IndexWriter writer) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(writer)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      for (String field :
          List.of(
              "float-" + VectorSimilarityFunction.EUCLIDEAN,
              "float-" + VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
              "bytes")) {
        assertEquals(field, 0, searcher.count(new FieldExistsQuery(field)));
      }
      for (var leaf : reader.leaves()) {
        // A segment no rejected document went to has no such field, and no values for it.
        if (leaf.reader().getFieldInfos().fieldInfo("bytes") != null) {
          assertEquals(0, leaf.reader().getByteVectorValues("bytes").size());
        }
        String euclidean = "float-" + VectorSimilarityFunction.EUCLIDEAN;
        if (leaf.reader().getFieldInfos().fieldInfo(euclidean) != null) {
          assertEquals(0, leaf.reader().getFloatVectorValues(euclidean).size());
        }
      }
    }
  }

  /**
   * A merge into SegmentIVF of fields it rejects, from segments of another format (addIndexes, or a
   * change of per-field format), writes them empty: their vectors are neither re-encoded under the
   * wrong similarity nor, for bytes, asked for as floats.
   */
  public void testMergeFromAnotherFormatDropsUnsupportedFields() throws Exception {
    try (Directory hnswDir = newDirectory();
        Directory dir = newDirectory()) {
      try (IndexWriter hnsw =
          new IndexWriter(hnswDir, newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.EUCLIDEAN));
          byte[] bytes = new byte[DIM];
          random().nextBytes(bytes);
          doc.add(new KnnByteVectorField("b", bytes, VectorSimilarityFunction.DOT_PRODUCT));
          doc.add(
              new KnnFloatVectorField(
                  "ok", VectorUtil.l2normalize(randomVector()), VectorSimilarityFunction.COSINE));
          hnsw.addDocument(doc);
        }
      }
      try (IndexWriter writer =
              new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false));
          DirectoryReader source = DirectoryReader.open(hnswDir)) {
        CodecReader[] leaves = new CodecReader[source.leaves().size()];
        for (int i = 0; i < leaves.length; i++) {
          leaves[i] = (CodecReader) source.leaves().get(i).reader();
        }
        writer.addIndexes(leaves);
        writer.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          assertEquals(0, leaf.getFloatVectorValues("v").size());
          assertEquals(0, leaf.getByteVectorValues("b").size());
          assertEquals(50, leaf.getFloatVectorValues("ok").size());
          IndexSearcher searcher = new IndexSearcher(reader);
          assertEquals(0, searcher.count(new FieldExistsQuery("v")));
          assertEquals(50, searcher.count(new FieldExistsQuery("ok")));
        }
        writer.commit();
        TestUtil.checkIndex(dir);
      }
    }
  }

  /** A query whose dimension differs from the field's is rejected, not truncated or overrun. */
  public void testQueryOfTheWrongDimensionIsRejected() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 200; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        for (int dim : new int[] {DIM - 1, DIM + 1}) {
          TopKnnCollector collector = new TopKnnCollector(10, Integer.MAX_VALUE);
          expectThrows(
              IllegalArgumentException.class,
              () -> leaf.getFloatVectorValues("v").scorer(randomVector(dim)));
          expectThrows(
              IllegalArgumentException.class,
              () ->
                  sivfReader(leaf)
                      .search(
                          "v",
                          randomVector(dim),
                          collector,
                          AcceptDocs.fromLiveDocs(null, leaf.maxDoc())));
        }
      }
    }
  }

  /**
   * A filter whose matches all lie far from the query, too many to search exactly: probing the
   * cells near the query finds none of them, so the search falls back to scoring every match by
   * coarse code. Widened, it probes until it reaches them.
   */
  public void testSelectiveConjunctionFarFromTheQueryFindsEveryMatch() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 1, false))) {
        float[] target = VectorUtil.l2normalize(randomVector());
        Set<Integer> far = new HashSet<>();
        for (int i = 0; i < 3000; i++) {
          // 500 matches, more than the exact search's limit, so the cells are probed.
          boolean both = i % 6 == 0;
          float[] vector = randomVector();
          if (both) {
            for (int d = 0; d < DIM; d++) {
              vector[d] = -target[d] + 0.05f * vector[d];
            }
            far.add(i);
          }
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField(
                  "v", VectorUtil.l2normalize(vector), VectorSimilarityFunction.COSINE));
          doc.add(new StoredField("id", i));
          if (both || i % 2 == 0) {
            doc.add(new StringField("a", "y", Field.Store.NO));
          }
          if (both || i % 2 == 1) {
            doc.add(new StringField("b", "y", Field.Store.NO));
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          Query filter =
              new BooleanQuery.Builder()
                  .add(new TermQuery(new Term("a", "y")), BooleanClause.Occur.FILTER)
                  .add(new TermQuery(new Term("b", "y")), BooleanClause.Occur.FILTER)
                  .build();
          StoredFields stored = reader.storedFields();
          for (boolean widen : new boolean[] {false, true}) {
            SearchStrategy strategy =
                new SearchStrategy(1, FilterPlan.Mode.PUSHDOWN, widen, DEFAULT_OVERSAMPLE);
            ScoreDoc[] hits =
                searcher.search(new SegmentIVFKnnQuery("v", target, 10, filter, strategy), 10)
                    .scoreDocs;
            assertEquals(coarse + " widen=" + widen, 10, hits.length);
            for (ScoreDoc hit : hits) {
              int id = stored.document(hit.doc).getField("id").numericValue().intValue();
              assertTrue(coarse + " widen=" + widen + " id=" + id, far.contains(id));
            }
          }
        }
      }
    }
  }

  private static SegmentIVFVectorsReader sivfReader(LeafReader leaf) {
    return SegmentIVFVectorsReader.of(leaf, "v");
  }

  public void testNlistIsTheSquareRootOfTheMaximumVectorCount() {
    assertEquals(1000, new SegmentIVFVectorsFormat().nlist());
    for (CoarseTier coarse : CoarseTier.values()) {
      SegmentIVFVectorsFormat format =
          new SegmentIVFVectorsFormat(250_000, 8, 1, FineTier.INT8, coarse);
      assertEquals(coarse.toString(), 500, format.nlist());
    }
    assertEquals(1, SegmentIVFVectorsFormat.configuredNlist(1));
    assertEquals(3, SegmentIVFVectorsFormat.configuredNlist(10));
    // An int count stays below the graph's 2-byte cell ordinals.
    assertEquals(46341, SegmentIVFVectorsFormat.configuredNlist(Integer.MAX_VALUE));
    assertTrue(46341 <= SegmentIVFVectorsFormat.MAX_NLIST);
    for (int maxVectors : new int[] {0, -1, Integer.MIN_VALUE}) {
      expectThrows(
          IllegalArgumentException.class, () -> new SegmentIVFVectorsFormat(maxVectors, 8));
    }
  }

  public void testRoundTripDeletesSortSparseFieldsAndMerge() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      for (FineTier fine : FineTier.values()) {
        for (VectorSimilarityFunction similarity : SIMILARITIES) {
          try (Directory dir = newDirectory();
              IndexWriter writer = new IndexWriter(dir, config(fine, coarse, 8, true))) {
            for (int i = 0; i < 240; i++) {
              Document doc = new Document();
              doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
              doc.add(new NumericDocValuesField("sort", 240 - i));
              float[] vector = VectorUtil.l2normalize(randomVector());
              if (i % 7 != 0) {
                doc.add(new KnnFloatVectorField("v", vector, similarity));
              }
              if (i % 4 == 0) {
                doc.add(new KnnFloatVectorField("other", vector, similarity));
              }
              writer.addDocument(doc);
              if (i % 60 == 59) {
                writer.commit();
              }
            }
            for (int i = 0; i < 240; i += 13) {
              writer.deleteDocuments(new Term("id", Integer.toString(i)));
            }
            // Search the flushed segments, then the merged one.
            for (boolean merged : new boolean[] {false, true}) {
              if (merged) {
                writer.forceMerge(1);
              }
              try (DirectoryReader reader = DirectoryReader.open(writer)) {
                for (var leaf : reader.leaves()) {
                  for (String field : List.of("v", "other")) {
                    assertSearch(leaf.reader(), field, similarity, null);
                  }
                }
              }
            }
            writer.commit();
            TestUtil.checkIndex(dir);
          }
        }
      }
    }
  }

  public void testFilteredScanAndExactSmallFilter() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      for (FineTier fine : FineTier.values()) {
        try (Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, config(fine, coarse, 2, false))) {
          for (int i = 0; i < 3000; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            // Half the documents: far above the exact limit, so the cells are scanned.
            FixedBitSet half = new FixedBitSet(leaf.maxDoc());
            for (int doc = 0; doc < leaf.maxDoc(); doc += 2) {
              half.set(doc);
            }
            assertSearch(leaf, "v", VectorSimilarityFunction.COSINE, half);
            // A handful: every match is reranked, so the result is exact to the tier's precision.
            FixedBitSet few = new FixedBitSet(leaf.maxDoc());
            for (int doc = 5; doc < leaf.maxDoc(); doc += 97) {
              few.set(doc);
            }
            assertSearch(leaf, "v", VectorSimilarityFunction.COSINE, few);
            TopKnnCollector none = new TopKnnCollector(10, Integer.MAX_VALUE);
            leaf.searchNearestVectors("v", randomVector(), none, accepting(new FixedBitSet(3000)));
            assertEquals(0, none.topDocs().scoreDocs.length);
          }
        }
      }
    }
  }

  /** The INT8 scorer scores codes in place; it must agree with scoring the decoded vectors. */
  public void testInt8ScorerMatchesDecodedVectors() throws Exception {
    int dim = 1024;
    for (VectorSimilarityFunction similarity : SIMILARITIES) {
      try (Directory dir = newDirectory();
          IndexWriter writer =
              new IndexWriter(dir, config(dim, FineTier.INT8, CoarseTier.S1, 8, false))) {
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          float[] vector = VectorUtil.l2normalize(randomVector(dim));
          doc.add(new KnnFloatVectorField("v", vector, similarity));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          float[] target = randomVector(dim);
          if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
            VectorUtil.l2normalize(target);
          }
          VectorScorer scorer = leaf.getFloatVectorValues("v").scorer(target);
          FloatVectorValues decoded = leaf.getFloatVectorValues("v");
          var decodedIterator = decoded.iterator();
          DocIdSetIterator it = scorer.iterator();
          int scored = 0;
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            assertEquals(doc, decodedIterator.advance(doc));
            float expected =
                similarity.compare(target, decoded.vectorValue(decodedIterator.index()));
            float tolerance = 1e-3f * Math.max(1, expected);
            assertEquals(similarity + " doc " + doc, expected, scorer.score(), tolerance);
            scored++;
          }
          assertEquals(50, scored);
        }
      }
    }
  }

  /** Mapped and io_uring reads of the fine codes must return identical hits. */
  public void testEveryReadPathReturnsTheSameHits() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      try (Directory dir = newFSDirectory(createTempDir())) {
        // One flushed, non-compound segment, so the data file can be opened directly.
        IndexWriterConfig config =
            config(FineTier.INT8, coarse, 8, false)
                .setUseCompoundFile(false)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setMaxBufferedDocs(10_000)
                .setRAMBufferSizeMB(256);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < 2000; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
        }
        float[] target = randomVector();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          SegmentIVFVectorsReader sivf = sivfReader(leaf);
          IVFFieldReader field = sivf.field("v");
          assertTrue("test needs spill copies", field.cellStart[field.nlist] > field.count);
          FineCodeIO.forceUring = false;
          ScoreDoc[] mapped = searchLeaf(leaf, target);
          FineCodeIO.forceUring = true;
          ScoreDoc[] batched = sivf.readsFineDirectly() ? searchLeaf(leaf, target) : mapped;
          assertSameHits(mapped, batched);
          // The selected coarse tier is the only one: in the scan, the graph and every code.
          assertEquals(coarse, field.coarseTier);
          assertEquals(CoarseCodec.of(coarse, DIM).getClass(), field.coarseCodec.getClass());
          if (field.graph != null) {
            assertEquals(field.coarseCodec.getClass(), field.graph.codec().getClass());
          }
        } finally {
          FineCodeIO.forceUring = null;
        }
      }
    }
  }

  /** 4096-dimension INT8 codes are whole pages, so io_uring reads them uncached (O_DIRECT). */
  public void testUncachedFineReadsMatchTheMappedSection() throws Exception {
    int dim = SegmentIVFVectorsFormat.MAX_DIMENSIONS;
    int docs = 400;
    try (Directory dir = newFSDirectory(createTempDir())) {
      IndexWriterConfig config =
          config(dim, FineTier.INT8, CoarseTier.S1, 8, false)
              .setUseCompoundFile(false)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(docs + 1)
              .setRAMBufferSizeMB(256);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docs; i++) {
          Document doc = new Document();
          float[] vector = VectorUtil.l2normalize(randomVector(dim));
          doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.DOT_PRODUCT));
          writer.addDocument(doc);
        }
      }
      float[] target = VectorUtil.l2normalize(randomVector(dim));
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        SegmentIVFVectorsReader sivf = sivfReader(leaf);
        assertEquals(PAGE_BYTES, sivf.field("v").fine.recordBytes);
        FineCodeIO.forceUring = false;
        ScoreDoc[] mapped = searchLeaf(leaf, target);
        FineCodeIO.forceUring = true;
        ScoreDoc[] uncached = sivf.readsFineDirectly() ? searchLeaf(leaf, target) : mapped;
        assertSameHits(mapped, uncached);
      } finally {
        FineCodeIO.forceUring = null;
      }
    }
  }

  /**
   * 1024-dimension INT8 codes are a quarter of a page, so an uncached rerank reads each code's
   * page, and codes sharing a page read it once; the default, unforced path is the same off-disk
   * read.
   */
  public void testQuarterPageFineReadsMatchTheMappedSection() throws Exception {
    int dim = 1024;
    int docs = 400;
    try (Directory dir = newFSDirectory(createTempDir())) {
      IndexWriterConfig config =
          config(dim, FineTier.INT8, CoarseTier.S1, 8, false)
              .setUseCompoundFile(false)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(docs + 1)
              .setRAMBufferSizeMB(256);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docs; i++) {
          Document doc = new Document();
          float[] vector = VectorUtil.l2normalize(randomVector(dim));
          doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.DOT_PRODUCT));
          writer.addDocument(doc);
        }
      }
      float[] target = VectorUtil.l2normalize(randomVector(dim));
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        SegmentIVFVectorsReader sivf = sivfReader(leaf);
        assertEquals(PAGE_BYTES / 4, sivf.field("v").fine.recordBytes);
        FineCodeIO.forceUring = false;
        ScoreDoc[] mapped = searchLeaf(leaf, target);
        FineCodeIO.forceUring = null;
        ScoreDoc[] byDefault = searchLeaf(leaf, target);
        assertSameHits(mapped, byDefault);
      } finally {
        FineCodeIO.forceUring = null;
      }
    }
  }

  /**
   * A search nested in another on one thread, as a filter's own vector query runs, gets its own
   * scratch; once released, the thread's scratch is handed out again.
   */
  public void testNestedSearchGetsItsOwnScratch() {
    SearchScratch outer = SearchScratch.acquire();
    SearchScratch inner = SearchScratch.acquire();
    assertNotSame(outer, inner);
    inner.release();
    outer.release();
    SearchScratch again = SearchScratch.acquire();
    assertSame(outer, again);
    again.release();
  }

  /**
   * A batch that fails must not poison the next one: rings are shared by every segment, and a
   * failed batch used to leave completions behind that the next batch reaped as its own. io_uring
   * is Linux-only, so elsewhere this is skipped.
   */
  public void testFailedRingBatchDoesNotPoisonTheNextRead() throws Exception {
    Path file = createTempDir().resolve("data");
    byte[] bytes = new byte[3 * PAGE_BYTES];
    random().nextBytes(bytes);
    Files.write(file, bytes);
    IoUring ioUring;
    try {
      ioUring = IoUring.open(file, false);
    } catch (IOException e) {
      assumeTrue("io_uring is unavailable here: " + e.getMessage(), false);
      return;
    }
    try {
      // The third read starts past the end of the file, so the batch comes back short.
      long[] failing = {0, PAGE_BYTES, 1L << 40};
      MemorySegment buffer = IoUring.buffer(MemorySegment.NULL, 3 * PAGE_BYTES);
      expectThrows(IOException.class, () -> ioUring.readBatch(failing, PAGE_BYTES, buffer));
      for (int round = 0; round < 3; round++) {
        MemorySegment read =
            ioUring.readBatch(
                new long[] {2 * PAGE_BYTES, PAGE_BYTES},
                PAGE_BYTES,
                IoUring.buffer(MemorySegment.NULL, 2 * PAGE_BYTES));
        assertArrayEquals(
            ArrayUtil.copyOfSubArray(bytes, 2 * PAGE_BYTES, 3 * PAGE_BYTES),
            read.asSlice(0, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE));
        assertArrayEquals(
            ArrayUtil.copyOfSubArray(bytes, PAGE_BYTES, 2 * PAGE_BYTES),
            read.asSlice(PAGE_BYTES, PAGE_BYTES).toArray(ValueLayout.JAVA_BYTE));
      }
    } finally {
      ioUring.close();
    }
  }

  /**
   * Rings are pooled, not per thread: threads that read once and exit leave at most {@link
   * IoUring#MAX_IDLE} idle rings, and closing the last instance frees them.
   */
  public void testIoUringRingsArePooledAndFreed() throws Exception {
    Path file = createTempDir().resolve("data");
    byte[] bytes = new byte[2 * PAGE_BYTES];
    random().nextBytes(bytes);
    Files.write(file, bytes);
    IoUring ioUring;
    try {
      ioUring = IoUring.open(file, false);
    } catch (IOException e) {
      assumeTrue("io_uring is unavailable here: " + e.getMessage(), false);
      return;
    }
    try {
      Thread[] threads = new Thread[2 * IoUring.MAX_IDLE + 4];
      for (int t = 0; t < threads.length; t++) {
        threads[t] =
            new Thread(() -> {
              try {
                MemorySegment read =
                    ioUring.readBatch(
                        new long[] {PAGE_BYTES},
                        PAGE_BYTES,
                        IoUring.buffer(MemorySegment.NULL, PAGE_BYTES));
                assertEquals(bytes[PAGE_BYTES], read.get(ValueLayout.JAVA_BYTE, 0));
              } catch (IOException e) {
                throw new AssertionError(e);
              }
            });
        threads[t].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      assertTrue(IoUring.idleRings() + " idle", IoUring.idleRings() <= IoUring.MAX_IDLE);
    } finally {
      ioUring.close();
    }
    assertEquals(0, IoUring.idleRings());
  }

  /**
   * Fine codes are bare codes on a page boundary, one per vector, with no per-vector metadata;
   * vectors decode to their originals before and after a merge, which rebuilds staged rows from
   * them. Up to the format's 4096 dimensions, where an INT8 code is exactly one page.
   */
  public void testFineCodesArePageAlignedAndFaithful() throws Exception {
    for (int dim : new int[] {1024, SegmentIVFVectorsFormat.MAX_DIMENSIONS}) {
      for (FineTier fine : FineTier.values()) {
        assertPageAlignedAndFaithfulThroughMerge(dim, dim == 1024 ? 300 : 90, fine);
      }
    }
  }

  private static void assertPageAlignedAndFaithfulThroughMerge(int dim, int docs, FineTier fine)
      throws Exception {
    try (Directory dir = newFSDirectory(createTempDir())) {
      SegmentIVFVectorsFormat format = SegmentIVFTestUtil.format(8, dim, 4, 1, fine, CoarseTier.S1);
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
              .setUseCompoundFile(false)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(docs / 3 + 1);
      float[][] vectors = new float[docs][];
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < docs; i++) {
          vectors[i] = randomVector(dim);
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.DOT_PRODUCT));
          doc.add(new StoredField("id", i));
          writer.addDocument(doc);
        }
        writer.commit();
        assertPageAlignedAndFaithful(dir, vectors);
        // NoMergePolicy kept the flushed segments apart; a real one lets the merge run.
        writer.getConfig().setMergePolicy(new LogDocMergePolicy());
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("the merge ran", 1, reader.leaves().size());
      }
      assertPageAlignedAndFaithful(dir, vectors);
      if (dim == SegmentIVFVectorsFormat.MAX_DIMENSIONS) {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          IVFFieldReader field = sivfReader(reader.leaves().get(0).reader()).field("v");
          assertEquals(
              "a 4096-dimension INT8 code is one page", PAGE_BYTES, field.fine.recordBytes);
        }
      }
    }
  }

  private static void assertPageAlignedAndFaithful(Directory dir, float[][] vectors)
      throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext context : reader.leaves()) {
        IVFFieldReader field = sivfReader(context.reader()).field("v");
        int codeBytes = field.fine.codeBytes;
        assertEquals("codes start on a page", 0, field.sectionStart(SECTION_FINE) % PAGE_BYTES);
        assertEquals("a record is the bare code", codeBytes, field.fine.recordBytes);
        assertEquals((long) field.count * codeBytes, field.sectionLength(SECTION_FINE));
        FloatVectorValues values = context.reader().getFloatVectorValues("v");
        var it = values.iterator();
        StoredFields stored = context.reader().storedFields();
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          int id = stored.document(doc).getField("id").numericValue().intValue();
          float[] expected = VectorUtil.l2normalize(ArrayUtil.copyArray(vectors[id]));
          float cosine = VectorUtil.dotProduct(expected, values.vectorValue(it.index()));
          assertTrue("doc " + id + " decodes to cosine " + cosine, cosine > 0.99f);
        }
      }
    }
  }

  /**
   * A shortlist larger than the probed cells hold still fills: the search probes past {@code
   * numProbes} until its cells hold the pool. With spill off, one of 8 cells holds about 375 of
   * 3000 documents, far fewer than the 900 asked for.
   */
  public void testShortlistLargerThanTheProbedCellsFills() throws Exception {
    int k = 900;
    try (Directory dir = newFSDirectory(createTempDir())) {
      SegmentIVFVectorsFormat format =
          SegmentIVFTestUtil.format(8, DIM, 1, 0, FineTier.INT8, CoarseTier.S1);
      IndexWriterConfig config =
          newIndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(10_000)
              .setRAMBufferSizeMB(256);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < 3000; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        // Oversample 1: a 900-document shortlist pools 900 of the 3000 slots, so it probes.
        SearchStrategy strategy = new SearchStrategy(1, FilterPlan.Mode.PUSHDOWN, false, 1);
        TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
        leaf.searchNearestVectors("v", randomVector(), collector, null);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        assertEquals(k, hits.length);
        assertEquals(k, Arrays.stream(hits).mapToInt(hit -> hit.doc).distinct().count());
      }
    }
  }

  /**
   * Deleted documents never take the shortlist's places: with the documents nearest the query
   * deleted, an unfiltered search still returns {@code k} live hits. A shortlist cut before deletes
   * were dropped would have been filled mostly by deleted documents.
   */
  public void testDeletesNearTheQueryDoNotShortenTheResults() throws Exception {
    int k = 100;
    int count = 3000;
    try (Directory dir = newFSDirectory(createTempDir())) {
      SegmentIVFVectorsFormat format =
          SegmentIVFTestUtil.format(8, DIM, 8, 0, FineTier.INT8, CoarseTier.S1);
      IndexWriterConfig config =
          newIndexWriterConfig()
              .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(10_000)
              .setRAMBufferSizeMB(256);
      float[] target = VectorUtil.l2normalize(randomVector());
      float[][] vectors = new float[count][];
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int i = 0; i < count; i++) {
          vectors[i] = randomVector();
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new KnnFloatVectorField("v", vectors[i], VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.commit();
        // Delete the 2k documents nearest the query by exact cosine.
        Integer[] byCosine = new Integer[count];
        for (int i = 0; i < count; i++) {
          byCosine[i] = i;
        }
        Arrays.sort(
            byCosine,
            Comparator.comparingDouble((Integer i) -> -VectorUtil.cosine(target, vectors[i])));
        for (int i = 0; i < 2 * k; i++) {
          writer.deleteDocuments(new Term("id", Integer.toString(byCosine[i])));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        Bits liveDocs = leaf.getLiveDocs();
        assertNotNull(liveDocs);
        // Oversample 1: the pool holds exactly the shortlist, so no spare places absorb deletes.
        SearchStrategy strategy = new SearchStrategy(8, FilterPlan.Mode.PUSHDOWN, false, 1);
        TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE, strategy);
        leaf.searchNearestVectors(
            "v", target, collector, AcceptDocs.fromLiveDocs(liveDocs, leaf.maxDoc()));
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        assertEquals(k, hits.length);
        for (ScoreDoc hit : hits) {
          assertTrue("doc " + hit.doc + " is deleted", liveDocs.get(hit.doc));
        }
      }
    }
  }

  private static ScoreDoc[] searchLeaf(LeafReader leaf, float[] target) throws Exception {
    TopKnnCollector collector = new TopKnnCollector(10, Integer.MAX_VALUE, new SearchStrategy(8));
    leaf.searchNearestVectors("v", target, collector, null);
    return collector.topDocs().scoreDocs;
  }

  private static void assertSameHits(ScoreDoc[] expected, ScoreDoc[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i].doc, actual[i].doc);
      assertEquals(expected[i].score, actual[i].score, 0f);
    }
  }

  public void testGlobalRerankAcrossSegmentsIsExactWhenEverythingIsReranked() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 8, false))) {
        for (int i = 0; i < 600; i++) {
          Document doc = new Document();
          doc.add(
              new KnnFloatVectorField(
                  "v", VectorUtil.l2normalize(randomVector()), VectorSimilarityFunction.COSINE));
          doc.add(new StringField("keep", i % 3 == 0 ? "y" : "n", Field.Store.NO));
          writer.addDocument(doc);
          if (i % 200 == 199) {
            writer.commit();
          }
        }
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          float[] target = VectorUtil.l2normalize(randomVector());
          // Top-100 at an oversample of 7 reranks 700 candidates, more than the 600 documents, so
          // the result must be the brute-force top 100 by the exact scorer, which scores the fine
          // codes as the rerank does; the filtered scores also show no unfiltered document leaked
          // in.
          for (FilterPlan.Mode mode : FilterPlan.Mode.values()) {
            SearchStrategy strategy = new SearchStrategy(8, mode, false, 7);
            for (boolean filtered : new boolean[] {false, true}) {
              Query filter = filtered ? new TermQuery(new Term("keep", "y")) : null;
              SegmentIVFKnnQuery query = new SegmentIVFKnnQuery("v", target, 100, filter, strategy);
              TopDocs hits = newSearcher(reader).search(query, 100);
              List<Float> expected = exactScores(reader, target, filter);
              assertEquals(100, hits.scoreDocs.length);
              for (int i = 0; i < 100; i++) {
                assertEquals(mode + " rank " + i, expected.get(i), hits.scoreDocs[i].score, 1e-4f);
              }
            }
          }
        }
      }
    }
  }

  /**
   * A filtered search must return only live matches, and a filter small enough to be reranked whole
   * must return exactly its brute-force top hits. Recall is not asserted otherwise: at this low
   * dimension coarse distances tie often, so which true neighbours survive the shortlist cut
   * depends on the seed. Filtered recall is measured by benchmarks.
   */
  public void testFilteredSearchReturnsOnlyLiveMatches() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      for (boolean deletes : new boolean[] {false, true}) {
        try (Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 2, false))) {
          for (int i = 0; i < 3000; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
            if (i % 2 == 0) {
              doc.add(new StringField("tag", "half", Field.Store.NO));
            }
            if (i % 97 == 5) {
              doc.add(new StringField("tag", "few", Field.Store.NO));
            }
            doc.add(new LongPoint("n", i));
            doc.add(new SortedNumericDocValuesField("n", i));
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
          if (deletes) {
            for (int i = 0; i < 3000; i += 11) {
              writer.deleteDocuments(new Term("id", Integer.toString(i)));
            }
          }
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            LeafReaderContext context = reader.leaves().get(0);
            Bits live = context.reader().getLiveDocs();
            Query range =
                new IndexOrDocValuesQuery(
                    LongPoint.newRangeQuery("n", 100, 2100),
                    SortedNumericDocValuesField.newSlowRangeQuery("n", 100, 2100));
            Map<String, Query> filters = new LinkedHashMap<>();
            filters.put("half", new TermQuery(new Term("tag", "half")));
            filters.put("few", new TermQuery(new Term("tag", "few")));
            filters.put("range", range);
            filters.put(
                "half+range",
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("tag", "half")), BooleanClause.Occur.FILTER)
                    .add(range, BooleanClause.Occur.FILTER)
                    .build());
            for (Map.Entry<String, Query> filter : filters.entrySet()) {
              FixedBitSet accepted = matches(searcher, filter.getValue(), context, live);
              for (int probes : new int[] {1, 2, 8}) {
                float[] target = VectorUtil.l2normalize(randomVector());
                String where =
                    coarse
                        + " deletes="
                        + deletes
                        + " filter="
                        + filter.getKey()
                        + " probes="
                        + probes;
                // Collect 100 so 300 candidates are reranked; see assertSearch.
                ScoreDoc[] all = search(searcher, target, filter.getValue(), probes, 100);
                assertTrue(where, all.length > 0);
                for (ScoreDoc hit : all) {
                  assertTrue(
                      where + " hit must pass the filter and be live", accepted.get(hit.doc));
                }
                if (filter.getKey().equals("few")) {
                  // Small enough to be searched exactly.
                  List<Float> expected = exactScores(reader, target, filter.getValue());
                  assertEquals(where, expected.size(), all.length);
                  for (int i = 0; i < all.length; i++) {
                    assertEquals(where + " rank " + i, expected.get(i), all[i].score, 1e-5f);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private static ScoreDoc[] search(
      IndexSearcher searcher, float[] target, Query filter, int probes, int k) throws IOException {
    SearchStrategy strategy =
        new SearchStrategy(probes, FilterPlan.Mode.PUSHDOWN, false, DEFAULT_OVERSAMPLE);
    return searcher.search(new SegmentIVFKnnQuery("v", target, k, filter, strategy), k).scoreDocs;
  }

  /**
   * Every live document's score for {@code target} by the exact scorer, which scores the fine codes
   * as the rerank does, best first; only documents {@code filter} matches when it is not null.
   */
  private static List<Float> exactScores(DirectoryReader reader, float[] target, Query filter)
      throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    List<Float> scores = new ArrayList<>();
    for (LeafReaderContext context : reader.leaves()) {
      Bits live = context.reader().getLiveDocs();
      FixedBitSet accepted = filter == null ? null : matches(searcher, filter, context, live);
      VectorScorer scorer = context.reader().getFloatVectorValues("v").scorer(target);
      DocIdSetIterator it = scorer.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        if ((live == null || live.get(doc)) && (accepted == null || accepted.get(doc))) {
          scores.add(scorer.score());
        }
      }
    }
    scores.sort(Comparator.reverseOrder());
    return scores;
  }

  /** The live documents of {@code context} that {@code filter} matches. */
  private static FixedBitSet matches(
      IndexSearcher searcher, Query filter, LeafReaderContext context, Bits live)
      throws IOException {
    Weight weight =
        searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
    FixedBitSet accepted = new FixedBitSet(context.reader().maxDoc());
    DocIdSetIterator all = weight.scorer(context).iterator();
    for (int doc = all.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = all.nextDoc()) {
      if (live == null || live.get(doc)) {
        accepted.set(doc);
      }
    }
    return accepted;
  }

  /**
   * Coarse codes read without a mapping score exactly as mapped ones: whole regions for a probed
   * scan, and for a filter small enough to search exactly, which visits slots across cells, only
   * the block holding each slot.
   */
  public void testUnmappedCoarseCodesMatchMapped() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      try (Directory unmapped = new ByteBuffersDirectory();
          Directory mapped = new MMapDirectory(createTempDir())) {
        try (IndexWriter writer =
            new IndexWriter(unmapped, config(FineTier.INT8, coarse, 2, false))) {
          for (int i = 0; i < 3000; i++) {
            Document doc = new Document();
            if (i % 2 == 0) {
              doc.add(new StringField("tag", "half", Field.Store.NO));
            }
            if (i % 97 == 5) {
              doc.add(new StringField("tag", "few", Field.Store.NO));
            }
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
        }
        for (String file : unmapped.listAll()) {
          mapped.copyFrom(unmapped, file, file, IOContext.DEFAULT);
        }
        try (DirectoryReader fromHeap = DirectoryReader.open(unmapped);
            DirectoryReader fromMap = DirectoryReader.open(mapped)) {
          for (String tag : new String[] {"half", "few"}) {
            Query filter = new TermQuery(new Term("tag", tag));
            for (int probes : new int[] {1, 8}) {
              float[] target = VectorUtil.l2normalize(randomVector());
              assertSameHits(
                  search(new IndexSearcher(fromMap), target, filter, probes, 10),
                  search(new IndexSearcher(fromHeap), target, filter, probes, 10));
            }
          }
        }
      }
    }
  }

  /** Widening keeps probing until a rare filter fills the shortlist. */
  public void testWideningFillsTheShortlistForARareFilter() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 1, false))) {
      for (int i = 0; i < 3000; i++) {
        Document doc = new Document();
        doc.add(new StringField("tag", i % 7 == 0 ? "rare" : "common", Field.Store.NO));
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = newSearcher(reader);
        Query rare = new TermQuery(new Term("tag", "rare"));
        for (FilterPlan.Mode mode : FilterPlan.Mode.values()) {
          SearchStrategy widen = new SearchStrategy(1, mode, true, DEFAULT_OVERSAMPLE);
          assertNotEquals(widen, new SearchStrategy(1, mode, false, DEFAULT_OVERSAMPLE));
          TopDocs hits =
              searcher.search(new SegmentIVFKnnQuery("v", randomVector(), 50, rare, widen), 50);
          assertEquals(mode.toString(), 50, hits.scoreDocs.length);
        }
      }
    }
  }

  /** The global rerank must see through wrapped leaves, as searchers commonly hand out. */
  public void testGlobalRerankThroughWrappedLeaves() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 600; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
        if (i % 200 == 199) {
          writer.commit();
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(writer);
          DirectoryReader wrapped = wrapCodecReaders(reader)) {
        for (LeafReaderContext context : wrapped.leaves()) {
          assertTrue(context.reader() instanceof FilterCodecReader);
          assertNotNull(SegmentIVFVectorsReader.of(context.reader(), "v"));
        }
        float[] target = randomVector();
        SegmentIVFKnnQuery query = new SegmentIVFKnnQuery("v", target, 10, null, 8);
        assertSameHits(
            new IndexSearcher(reader).search(query, 10).scoreDocs,
            new IndexSearcher(wrapped).search(query, 10).scoreDocs);
      }
    }
  }

  /**
   * A subclass's mergeLeafResults runs once, on the top k merged across every segment, and the hits
   * it returns are the query's.
   */
  public void testSubclassPostProcessesTheMergedHits() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 600; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
        if (i % 200 == 199) {
          writer.commit();
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertTrue(reader.leaves().size() > 1);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] target = randomVector();
        ScoreDoc[] merged =
            searcher.search(new SegmentIVFKnnQuery("v", target, 20, null, 8), 20).scoreDocs;
        assertEquals(20, merged.length);
        float threshold = merged[9].score;
        List<TopDocs> seen = new ArrayList<>();
        SegmentIVFKnnQuery thresholded =
            new SegmentIVFKnnQuery("v", target, 20, null, 8) {
              @Override
              protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
                TopDocs topK = super.mergeLeafResults(perLeafResults);
                seen.add(topK);
                ScoreDoc[] kept =
                    Arrays.stream(topK.scoreDocs)
                        .filter(hit -> hit.score >= threshold)
                        .toArray(ScoreDoc[]::new);
                return new TopDocs(topK.totalHits, kept);
              }
            };
        ScoreDoc[] hits = searcher.search(thresholded, 20).scoreDocs;
        assertEquals(1, seen.size());
        assertSameHits(merged, seen.get(0).scoreDocs);
        assertSameHits(
            Arrays.stream(merged).filter(hit -> hit.score >= threshold).toArray(ScoreDoc[]::new),
            hits);
      }
    }
  }

  /**
   * With a segment of another format the query searches segment by segment, and a subclass's
   * mergeLeafResults still runs once, on the merged top k.
   */
  public void testSubclassPostProcessesMixedFormatHits() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig hnsw =
          newIndexWriterConfig()
              .setCodec(TestUtil.getDefaultCodec())
              .setMergePolicy(NoMergePolicy.INSTANCE);
      IndexWriterConfig sivf =
          config(FineTier.INT8, CoarseTier.S1, 8, false).setMergePolicy(NoMergePolicy.INSTANCE);
      for (IndexWriterConfig config : new IndexWriterConfig[] {hnsw, sivf}) {
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < 200; i++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertTrue(reader.leaves().size() >= 2);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] target = randomVector();
        List<TopDocs> seen = new ArrayList<>();
        SegmentIVFKnnQuery firstTen =
            new SegmentIVFKnnQuery("v", target, 20, null, 8) {
              @Override
              protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
                TopDocs topK = super.mergeLeafResults(perLeafResults);
                seen.add(topK);
                return new TopDocs(topK.totalHits, ArrayUtil.copyOfSubArray(topK.scoreDocs, 0, 10));
              }
            };
        ScoreDoc[] hits = searcher.search(firstTen, 20).scoreDocs;
        assertEquals(1, seen.size());
        assertEquals(20, seen.get(0).scoreDocs.length);
        assertEquals(10, hits.length);
      }
    }
  }

  /** A filter that rewrites to match everything is searched as no filter, as core does. */
  public void testMatchAllFilterSearchesUnfiltered() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 500; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] target = randomVector();
        Query all = MatchAllDocsQuery.INSTANCE;
        SegmentIVFKnnQuery filtered = new SegmentIVFKnnQuery("v", target, 10, all, 8);
        assertSame(all, filtered.getFilter());
        assertSameHits(
            searcher.search(new SegmentIVFKnnQuery("v", target, 10, null, 8), 10).scoreDocs,
            searcher.search(filtered, 10).scoreDocs);
      }
    }
  }

  /** An expired timeout skips the segments not yet searched and marks the hits incomplete. */
  public void testExpiredTimeoutSkipsSegments() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 300; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setTimeout(() -> true);
        assertEquals(
            0,
            searcher.search(new SegmentIVFKnnQuery("v", randomVector(), 10, null, 8), 10)
                .scoreDocs
                .length);
        searcher.setTimeout(() -> false);
        assertEquals(
            10,
            searcher.search(new SegmentIVFKnnQuery("v", randomVector(), 10, null, 8), 10)
                .scoreDocs
                .length);
      }
    }
  }

  /**
   * Deleted documents' slots count toward the cells a search selects but are not admitted, so with
   * most documents deleted the probed cells hold too few live ones: the search scans further cells
   * until the shortlist is full.
   */
  public void testHeavyDeletionsStillFindK() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 1, false))) {
        for (int i = 0; i < 3000; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        for (int i = 0; i < 3000; i++) {
          if (i % 20 != 0) {
            writer.deleteDocuments(new Term("id", Integer.toString(i)));
          }
        }
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(1, reader.leaves().size());
          assertEquals(150, reader.numDocs());
          SearchStrategy strategy = new SearchStrategy(1, FilterPlan.Mode.PUSHDOWN, false, 1f);
          ScoreDoc[] hits =
              new IndexSearcher(reader)
                  .search(new SegmentIVFKnnQuery("v", randomVector(), 50, null, strategy), 50)
                  .scoreDocs;
          assertEquals(coarse.toString(), 50, hits.length);
        }
      }
    }
  }

  /**
   * A zero query on a COSINE field scores 0.5 on the exact path for every fine tier, as the rerank
   * scores it.
   */
  public void testZeroQueryScoresAlikeOnEveryTier() throws Exception {
    for (FineTier fine : FineTier.values()) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(fine, CoarseTier.S1, 8, false))) {
        for (int i = 0; i < 200; i++) {
          Document doc = new Document();
          float[] vector = VectorUtil.l2normalize(randomVector());
          doc.add(new KnnFloatVectorField("dot", vector, VectorSimilarityFunction.DOT_PRODUCT));
          doc.add(new KnnFloatVectorField("cos", vector, VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          VectorScorer zero = leaf.getFloatVectorValues("cos").scorer(new float[DIM]);
          DocIdSetIterator it = zero.iterator();
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            assertEquals(fine.toString(), 0.5f, zero.score(), 0f);
          }
        }
      }
    }
  }

  /** The rotation is orthonormal and inverted exactly, for dimensions of every shape. */
  public void testRotationRoundTripsForEveryDimension() {
    for (int dim : new int[] {1, 2, 3, 24, 100, 300, 512, 1000, 1025, 4096}) {
      SegmentIVFVectorsWriter.HadamardRotation rotation =
          SegmentIVFVectorsWriter.HadamardRotation.forDimension(dim);
      float[] vector = randomVector(dim);
      float[] rotated = new float[dim];
      float[] back = new float[dim];
      rotation.rotate(vector, rotated);
      rotation.inverseRotate(rotated, back);
      assertEquals(
          "dim=" + dim,
          VectorUtil.dotProduct(vector, vector),
          VectorUtil.dotProduct(rotated, rotated),
          1e-4f * VectorUtil.dotProduct(vector, vector));
      for (int d = 0; d < dim; d++) {
        assertEquals("dim=" + dim + " d=" + d, vector[d], back[d], 1e-5f);
      }
    }
  }

  /**
   * The rotation spreads a vector's energy over every component whatever the dimension, so the
   * frozen INT8 scale fits it: a vector concentrated in any one raw dimension keeps nearly all of
   * its length through an INT8 code. Rotating each power-of-two block of a dimension on its own
   * left such a vector in a small block, clipped: at 300 dimensions it kept 0.46 of its length.
   */
  public void testOneHotVectorsSurviveInt8ForAnyDimension() {
    for (int dim : new int[] {100, 300, 1000, 1025}) {
      SegmentIVFVectorsWriter.HadamardRotation rotation =
          SegmentIVFVectorsWriter.HadamardRotation.forDimension(dim);
      FineCodec fine = FineCodec.of(FineTier.INT8, dim);
      byte[] code = new byte[fine.recordBytes];
      float[] decoded = new float[dim];
      float[] rotated = new float[dim];
      float worst = Float.MAX_VALUE;
      for (int j = 0; j < dim; j++) {
        float[] oneHot = new float[dim];
        oneHot[j] = 1;
        rotation.rotate(oneHot, rotated);
        fine.encode(rotated, code, 0);
        fine.decode(code, 0, decoded);
        worst = Math.min(worst, VectorUtil.dotProduct(rotated, decoded));
      }
      assertTrue("dim=" + dim + " worst self-dot " + worst, worst > 0.98f);
    }
  }

  private static DirectoryReader wrapCodecReaders(DirectoryReader reader) throws IOException {
    return new FilterDirectoryReader(reader, new CodecReaderWrapper()) {
      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return wrapCodecReaders(in);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  private static final class CodecReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
    @Override
    public LeafReader wrap(LeafReader reader) {
      return new FilterCodecReader((CodecReader) reader) {
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
  }

  public void testSparseIteratorAdvanceMatchesNextDoc() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 4, false))) {
      for (int i = 0; i < 700; i++) {
        Document doc = new Document();
        if (random().nextInt(3) != 0) {
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        List<Integer> docs = new ArrayList<>();
        var all = leaf.getFloatVectorValues("v").iterator();
        for (int doc = all.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = all.nextDoc()) {
          docs.add(doc);
        }
        assertTrue("the field must be sparse", docs.size() < leaf.maxDoc());
        FloatVectorValues values = leaf.getFloatVectorValues("v");
        var it = values.iterator();
        int target = 0;
        while (true) {
          target += 1 + random().nextInt(40);
          int found = it.advance(target);
          int expected = DocIdSetIterator.NO_MORE_DOCS;
          for (int doc : docs) {
            if (doc >= target) {
              expected = doc;
              break;
            }
          }
          assertEquals("advance(" + target + ")", expected, found);
          if (found == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          assertEquals(docs.indexOf(found), it.index());
          assertEquals(found, values.ordToDoc(it.index()));
          target = found;
        }
      }
    }
  }

  public void testSearchStrategyRejectsInvalidArguments() {
    expectThrows(IllegalArgumentException.class, () -> new SearchStrategy(0));
    expectThrows(
        IllegalArgumentException.class,
        () -> new SearchStrategy(8, null, false, DEFAULT_OVERSAMPLE));
    for (float oversample : new float[] {Float.NaN, 0.5f, Float.POSITIVE_INFINITY}) {
      expectThrows(
          IllegalArgumentException.class,
          () -> new SearchStrategy(8, FilterPlan.Mode.PUSHDOWN, false, oversample));
    }
  }

  /** The oversample sets how many coarse candidates a search fine-reranks. */
  public void testOversampleSetsTheRerankCount() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, CoarseTier.S1, 8, false))) {
      for (int i = 0; i < 3000; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        // k = 10: the default reranks max(100, 7 * 10) = 100, an oversample of 20 reranks 200.
        for (float oversample : new float[] {DEFAULT_OVERSAMPLE, 20}) {
          SearchStrategy strategy =
              new SearchStrategy(8, FilterPlan.Mode.PUSHDOWN, false, oversample);
          TopKnnCollector collector = new TopKnnCollector(10, Integer.MAX_VALUE, strategy);
          leaf.searchNearestVectors("v", randomVector(), collector, null);
          assertEquals(strategy.rerankCount(10), collector.visitedCount());
        }
      }
    }
  }

  /**
   * A search probes every requested cell even when the nearest centroid points away from the query;
   * an early cutoff relative to the nearest similarity once kept a single cell then.
   */
  public void testOpposedQueryProbesEveryRequestedCell() throws Exception {
    // Enough slots that cells are selected rather than all scanned, and small cells.
    SegmentIVFVectorsFormat format =
        SegmentIVFTestUtil.format(128, DIM, 128, 0, FineTier.INT8, CoarseTier.S1);
    IndexWriterConfig config =
        newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 2000; i++) {
        float[] vector = new float[DIM];
        for (int d = 0; d < DIM; d++) {
          vector[d] = random().nextFloat();
        }
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.COSINE));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        LeafReader leaf = reader.leaves().get(0).reader();
        float[] target = new float[DIM];
        Arrays.fill(target, -1f);
        // The nearest cell alone holds too few documents to fill the results; 16 cells do.
        TopKnnCollector collector =
            new TopKnnCollector(50, Integer.MAX_VALUE, new SearchStrategy(16));
        leaf.searchNearestVectors(
            "v", target, collector, AcceptDocs.fromLiveDocs(null, leaf.maxDoc()));
        int found = collector.topDocs().scoreDocs.length;
        assertTrue(found + " hits from 16 cells", found > 16);
      }
    }
  }

  /**
   * The approximate rerank and the exact scorer score a query that is not unit length alike, so
   * hits from the two paths, which Lucene merges across segments, compare.
   */
  public void testApproximateAndExactScoresAgreeForLongQueries() throws Exception {
    for (FineTier fine : FineTier.values()) {
      for (VectorSimilarityFunction similarity : SIMILARITIES) {
        try (Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, config(fine, CoarseTier.S1, 8, false))) {
          for (int i = 0; i < 200; i++) {
            Document doc = new Document();
            float[] vector = VectorUtil.l2normalize(randomVector());
            doc.add(new KnnFloatVectorField("v", vector, similarity));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            float[] target = VectorUtil.l2normalize(randomVector());
            for (int d = 0; d < DIM; d++) {
              target[d] *= 10;
            }
            TopKnnCollector collector =
                new TopKnnCollector(10, Integer.MAX_VALUE, new SearchStrategy(8));
            leaf.searchNearestVectors(
                "v", target, collector, AcceptDocs.fromLiveDocs(null, leaf.maxDoc()));
            ScoreDoc[] hits = collector.topDocs().scoreDocs;
            assertEquals(10, hits.length);
            for (ScoreDoc hit : hits) {
              VectorScorer exact = leaf.getFloatVectorValues("v").scorer(target);
              assertEquals(hit.doc, exact.iterator().advance(hit.doc));
              String message = fine + " " + similarity + " doc=" + hit.doc;
              assertEquals(message, exact.score(), hit.score, 1e-3f * Math.abs(exact.score()));
            }
          }
        }
      }
    }
  }

  /**
   * With a segment of another vector format, the query searches each segment as a plain kNN query
   * would: a filter matching fewer documents than k finds every match in each segment.
   */
  public void testMixedFormatsFindEveryFilterMatch() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig hnsw =
          newIndexWriterConfig()
              .setCodec(TestUtil.getDefaultCodec())
              .setMergePolicy(NoMergePolicy.INSTANCE);
      IndexWriterConfig sivf =
          config(FineTier.INT8, CoarseTier.S1, 8, false).setMergePolicy(NoMergePolicy.INSTANCE);
      int id = 0;
      for (IndexWriterConfig config : new IndexWriterConfig[] {hnsw, sivf}) {
        config.setMaxBufferedDocs(1000).setRAMBufferSizeMB(64).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < 300; i++, id++) {
            Document doc = new Document();
            doc.add(new StringField("tag", id % 50 == 0 ? "rare" : "common", Field.Store.NO));
            doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
            writer.addDocument(doc);
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(2, reader.leaves().size());
        IndexSearcher searcher = newSearcher(reader);
        Query filter = new TermQuery(new Term("tag", "rare"));
        SegmentIVFKnnQuery query =
            new SegmentIVFKnnQuery("v", randomVector(), 20, filter, new SearchStrategy(8));
        // Six rare documents in each segment, fewer than k: every one is found.
        assertEquals(12, searcher.search(query, 20).scoreDocs.length);
      }
    }
  }

  /** DOT_PRODUCT is the one supported similarity Lucene lets index and query zero vectors. */
  public void testZeroVectorsAreIndexedMergedAndSearched() throws Exception {
    for (CoarseTier coarse : CoarseTier.values()) {
      for (VectorSimilarityFunction similarity :
          new VectorSimilarityFunction[] {VectorSimilarityFunction.DOT_PRODUCT}) {
        try (Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 8, false))) {
          for (int i = 0; i < 60; i++) {
            Document doc = new Document();
            float[] vector = i % 10 == 0 ? new float[DIM] : randomVector();
            doc.add(new KnnFloatVectorField("v", vector, similarity));
            writer.addDocument(doc);
            if (i == 29) {
              writer.commit();
            }
          }
          writer.forceMerge(1);
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            for (float[] target : new float[][] {randomVector(), new float[DIM]}) {
              TopKnnCollector collector =
                  new TopKnnCollector(10, Integer.MAX_VALUE, new SearchStrategy(8));
              leaf.searchNearestVectors(
                  "v", target, collector, AcceptDocs.fromLiveDocs(null, leaf.maxDoc()));
              ScoreDoc[] hits = collector.topDocs().scoreDocs;
              assertEquals(coarse + " " + similarity, 10, hits.length);
              for (ScoreDoc hit : hits) {
                assertTrue(Float.isFinite(hit.score));
              }
            }
          }
        }
      }
    }
  }

  /**
   * Too few vectors per cell to cluster: the segment is a single cell. The merge case also covers
   * the plan mismatch: both sources are clustered (and offer eight-cell seeds), but only three of
   * their vectors survive, so the merge must start cold rather than take an eight-cell seed.
   */
  public void testFewVectorsPerCellWriteOneCell() throws Exception {
    int perSegment = SegmentIVFVectorsFormat.MIN_VECTORS_PER_CELL * 8 + 72;
    for (CoarseTier coarse : CoarseTier.values()) {
      // Only the two commits flush and only forceMerge merges, so each source holds perSegment.
      IndexWriterConfig sources =
          config(FineTier.INT8, coarse, 8, false)
              .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
              .setRAMBufferSizeMB(256)
              .setMergePolicy(new LogDocMergePolicy());
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, sources)) {
        int total = 2 * perSegment;
        for (int i = 0; i < total; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
          if (i == perSegment - 1) {
            writer.commit();
          }
        }
        writer.commit();
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          for (LeafReaderContext ctx : reader.leaves()) {
            assertEquals(8, sivfReader(ctx.reader()).field("v").nlist);
          }
        }
        for (int i = 0; i < total; i++) {
          if (i % perSegment != 0 && i != 1) {
            writer.deleteDocuments(new Term("id", Integer.toString(i)));
          }
        }
        writer.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          LeafReader leaf = reader.leaves().get(0).reader();
          assertEquals(3, leaf.numDocs());
          assertEquals(1, sivfReader(leaf).field("v").nlist);
          assertSearch(leaf, "v", VectorSimilarityFunction.COSINE, null);
        }
      }
      // A small flush is a single cell from the start.
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(FineTier.INT8, coarse, 8, false))) {
        for (int i = 0; i < 40; i++) {
          Document doc = new Document();
          doc.add(new KnnFloatVectorField("v", randomVector(), VectorSimilarityFunction.COSINE));
          writer.addDocument(doc);
        }
        writer.commit();
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          LeafReader leaf = reader.leaves().get(0).reader();
          assertEquals(1, sivfReader(leaf).field("v").nlist);
          assertSearch(leaf, "v", VectorSimilarityFunction.COSINE, null);
        }
      }
    }
  }

  public void testCellCountFloor() {
    SegmentIVFVectorsFormat format = SegmentIVFTestUtil.format(8, 16, 4);
    int floor = SegmentIVFVectorsFormat.MIN_VECTORS_PER_CELL * 8;
    assertEquals(0, format.nlist(0));
    assertEquals(1, format.nlist(1));
    assertEquals(1, format.nlist(floor - 1));
    assertEquals(8, format.nlist(floor));
  }

  public void testBuildThreads() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            new SegmentIVFVectorsFormat(
                SegmentIVFVectorsFormat.DEFAULT_MAX_VECTORS_PER_SEGMENT,
                32,
                1,
                FineTier.INT8,
                CoarseTier.S1,
                0));
    assertEquals(
        Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
        SegmentIVFVectorsFormat.DEFAULT_BUILD_THREADS);
    assertEquals(1, new Clustering.Parallel(1).workers);
    assertTrue(new Clustering.Parallel(Integer.MAX_VALUE).workers >= 1);
  }

  /**
   * Checks a top-10 search of {@code field} against brute force over the decoded vectors. Every
   * cell is probed, so the search loses only what eight bits lose.
   */
  private static void assertSearch(
      LeafReader reader, String field, VectorSimilarityFunction similarity, FixedBitSet filter)
      throws Exception {
    float[] target = VectorUtil.l2normalize(randomVector());
    FloatVectorValues values = reader.getFloatVectorValues(field);
    if (values == null) {
      // A small random segment may hold no vectors for this field.
      return;
    }
    Bits live = reader.getLiveDocs();
    List<float[]> expected = new ArrayList<>(); // {doc, score}
    var iterator = values.iterator();
    for (int doc = iterator.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = iterator.nextDoc()) {
      float[] vector = values.vectorValue(iterator.index());
      assertEquals(1f, VectorUtil.dotProduct(vector, vector), 1e-3f);
      if ((live == null || live.get(doc)) && (filter == null || filter.get(doc))) {
        expected.add(new float[] {doc, similarity.compare(target, vector)});
      }
    }
    expected.sort(Comparator.comparingDouble(hit -> -hit[1]));
    // Collect 100 at an oversample of 7 so 700 candidates are reranked: at DIM 24 the coarse codes
    // alone are too coarse to rank a short shortlist, and this checks correctness, not low-dim
    // recall. Every cell is probed, so a filtered search finds every match without widening.
    TopKnnCollector collector =
        new TopKnnCollector(
            100, Integer.MAX_VALUE, new SearchStrategy(8, FilterPlan.Mode.PUSHDOWN, false, 7));
    AcceptDocs accept =
        filter == null ? AcceptDocs.fromLiveDocs(live, reader.maxDoc()) : accepting(filter);
    reader.searchNearestVectors(field, target, collector, accept);
    ScoreDoc[] all = collector.topDocs().scoreDocs;
    ScoreDoc[] hits = ArrayUtil.copyOfSubArray(all, 0, Math.min(10, all.length));
    assertEquals(Math.min(10, expected.size()), hits.length);
    Set<Integer> truth = new HashSet<>();
    for (int i = 0; i < hits.length; i++) {
      truth.add((int) expected.get(i)[0]);
    }
    int found = 0;
    for (ScoreDoc hit : hits) {
      assertTrue(live == null || live.get(hit.doc));
      assertTrue(filter == null || filter.get(hit.doc));
      if (truth.contains(hit.doc)) {
        found++;
      }
    }
    int floor = (int) (0.7 * hits.length);
    assertTrue(found + " of " + hits.length, found >= floor);
  }

  private static AcceptDocs accepting(FixedBitSet accepted) {
    return new AcceptDocs() {
      @Override
      public Bits bits() {
        return accepted;
      }

      @Override
      public int cost() {
        return accepted.cardinality();
      }

      @Override
      public DocIdSetIterator iterator() {
        return new BitSetIterator(accepted, accepted.cardinality());
      }
    };
  }
}
