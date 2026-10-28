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

import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.QUANTIZED_VECTOR_COMPONENT;
import static org.apache.lucene.codecs.lucene104.MergedCentroidCollector.FALLBACK_MESSAGE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloat16VectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/** The merged centroid must be computed from the live vectors only. */
public class TestCentroidMergeWithDeletes extends LuceneTestCase {

  private static final String FIELD = "vec";
  private static final int DIMS = 8;
  private static final int PER_SEGMENT = 200;
  private static final float OFFSET = 10f;

  private static final VectorSimilarityFunction[] NON_COSINE = {
    VectorSimilarityFunction.EUCLIDEAN,
    VectorSimilarityFunction.DOT_PRODUCT,
    VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
  };

  /** Counts how many times the centroid fallback ran. */
  private static final class FallbackCounter extends InfoStream {
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public void message(String component, String message) {
      if (FALLBACK_MESSAGE.equals(message)) {
        count.incrementAndGet();
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return QUANTIZED_VECTOR_COMPONENT.equals(component);
    }

    @Override
    public void close() {}
  }

  /** A quarter of segment 0 deleted. */
  public void testFewDeletes() throws Exception {
    for (VectorSimilarityFunction sim : NON_COSINE) {
      check(sim, i -> i < PER_SEGMENT && i % 4 == 0, false, false);
    }
  }

  /** Three quarters of segment 0 deleted. */
  public void testManyDeletes() throws Exception {
    for (VectorSimilarityFunction sim : NON_COSINE) {
      check(sim, i -> i < PER_SEGMENT && i % 4 != 0, false, false);
    }
  }

  /** COSINE sums the live unit vectors of every segment when any segment has deletes. */
  public void testCosine() throws Exception {
    check(VectorSimilarityFunction.COSINE, i -> i % 4 == 0, false, false);
    check(VectorSimilarityFunction.COSINE, i -> i % 4 != 0, false, false);
    // Deletes in segment 0 only: segment 1 must not contribute its normalized centroid.
    check(VectorSimilarityFunction.COSINE, i -> i < PER_SEGMENT && i % 4 == 0, false, false);
  }

  /** fp16 vectors take the same paths, read through their fp32 view. */
  public void testFloat16() throws Exception {
    for (VectorSimilarityFunction sim : VectorSimilarityFunction.values()) {
      check(sim, i -> i < PER_SEGMENT && i % 4 == 0, true, false);
      check(sim, i -> i < PER_SEGMENT && i % 4 != 0, true, false);
    }
  }

  /** Segment 0 comes from a format without a stored centroid, so its vectors are summed. */
  public void testSegmentWithoutCentroid() throws Exception {
    // No deletes in the centroid-less segment, a quarter deleted in the other one.
    check(VectorSimilarityFunction.EUCLIDEAN, i -> i >= PER_SEGMENT && i % 4 == 0, false, true);
    // Deletes in the centroid-less segment.
    check(VectorSimilarityFunction.EUCLIDEAN, i -> i < PER_SEGMENT && i % 4 == 0, false, true);
  }

  /**
   * HNSW merges go through mergeOneFlatVectorFieldForMergeScorer, and an asymmetric encoding takes
   * its branch that also writes query-side vectors. A graph threshold of 0 always builds a graph,
   * so that branch runs.
   */
  public void testHnsw() throws Exception {
    for (ScalarEncoding encoding :
        new ScalarEncoding[] {
          ScalarEncoding.UNSIGNED_BYTE, ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE
        }) {
      KnnVectorsFormat hnsw =
          new Lucene104HnswScalarQuantizedVectorsFormat(encoding, 16, 100, 1, null, 0);
      check(
          hnsw,
          VectorSimilarityFunction.EUCLIDEAN,
          i -> i < PER_SEGMENT && i % 4 == 0,
          false,
          false);
    }
  }

  /** Ways a raw vector merge might read the wrapped vectors. */
  private enum ReadPattern {
    /** Each live vector once, in ordinal order. */
    IN_ORDER,
    /** Nothing read. */
    NONE,
    /** Live vectors in descending ordinal order. */
    REVERSE,
    /** Live vectors read twice, through two separate vector values. */
    TWICE,
    /** Live vectors read through {@code copy()}, which is not wrapped. */
    VIA_COPY,
    /** Deleted vectors read as well. */
    WITH_DELETED,
    /** In ordinal order, one deleted vector read instead of a live one, so the count matches. */
    SWAPPED
  }

  /**
   * Drives the collector with each read pattern. IN_ORDER, REVERSE and TWICE add exactly the live
   * vectors, so the collected sums are used and the fallback does not run; the other patterns must
   * fall back. Both fp32 and fp16 fields, so the fp16 fallback runs as well.
   */
  public void testFallbackOnUnexpectedReads() throws Exception {
    for (VectorSimilarityFunction sim :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN, VectorSimilarityFunction.COSINE
        }) {
      for (ReadPattern pattern : ReadPattern.values()) {
        checkCollector(sim, pattern, false);
        checkCollector(sim, pattern, true);
      }
    }
  }

  /** Index sort interleaves the source segments during the merge. */
  public void testIndexSort() throws Exception {
    checkSortedMerge(VectorSimilarityFunction.EUCLIDEAN, false);
    checkSortedMerge(VectorSimilarityFunction.COSINE, false);
  }

  /** A SortingCodecReader source returns its vectors in a permuted order. */
  public void testSortingCodecReader() throws Exception {
    checkSortedMerge(VectorSimilarityFunction.EUCLIDEAN, true);
    checkSortedMerge(VectorSimilarityFunction.COSINE, true);
  }

  private void checkCollector(VectorSimilarityFunction sim, ReadPattern pattern, boolean fp16)
      throws Exception {
    float[][] vectors = shiftedVectors(2 * PER_SEGMENT, sim, fp16);
    boolean[] deleted = new boolean[vectors.length];
    for (int i = 0; i < deleted.length; i++) {
      deleted[i] = i % 4 == 0;
    }
    FallbackCounter fallbacks = new FallbackCounter();
    try (Directory dir = newDirectory()) {
      writeTwoSegments(dir, vectors, deleted, sim, fp16, null);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        int n = reader.leaves().size();
        assertEquals(2, n);
        KnnVectorsReader[] readers = new KnnVectorsReader[n];
        FieldInfos[] fieldInfos = new FieldInfos[n];
        Bits[] liveDocs = new Bits[n];
        int[] maxDocs = new int[n];
        MergeState.DocMap[] docMaps = new MergeState.DocMap[n];
        int nextDoc = 0;
        for (int i = 0; i < n; i++) {
          CodecReader leaf = (CodecReader) reader.leaves().get(i).reader();
          readers[i] = leaf.getVectorReader().getMergeInstance();
          fieldInfos[i] = leaf.getFieldInfos();
          liveDocs[i] = leaf.getLiveDocs();
          maxDocs[i] = leaf.maxDoc();
          int[] map = new int[leaf.maxDoc()];
          for (int d = 0; d < map.length; d++) {
            map[d] = liveDocs[i] == null || liveDocs[i].get(d) ? nextDoc++ : -1;
          }
          docMaps[i] = d -> map[d];
        }
        MergeState state =
            new MergeState(
                docMaps,
                null,
                null,
                null,
                null,
                null,
                null,
                fieldInfos,
                liveDocs,
                null,
                null,
                readers,
                maxDocs,
                fallbacks,
                null,
                false,
                null);
        MergedCentroidCollector collector =
            new MergedCentroidCollector(fieldInfos[0].fieldInfo(FIELD), n);
        MergeState wrapped = collector.wrap(state);
        float[] centroid = new float[DIMS];
        try {
          for (int i = 0; i < n; i++) {
            read(wrapped.knnVectorsReaders[i], liveDocs[i], pattern, fp16);
          }
          assertEquals(nextDoc, collector.verifyAndComputeCentroid(state, centroid));
        } finally {
          for (KnnVectorsReader r : readers) {
            r.finishMerge();
          }
        }
        boolean collectsLiveVectors =
            pattern == ReadPattern.IN_ORDER
                || pattern == ReadPattern.REVERSE
                || pattern == ReadPattern.TWICE;
        String message = sim + " " + pattern + " fp16=" + fp16;
        assertEquals(message, collectsLiveVectors ? 0 : 1, fallbacks.count.get());
        assertArrayEquals(
            message,
            liveMean(vectors, deleted, sim == VectorSimilarityFunction.COSINE),
            centroid,
            1e-3f);
      }
    }
  }

  private static void read(
      KnnVectorsReader reader, Bits liveDocs, ReadPattern pattern, boolean fp16)
      throws IOException {
    switch (pattern) {
      case IN_ORDER -> readLive(values(reader, fp16, false), liveDocs, false, false);
      case NONE -> {}
      case REVERSE -> readLive(values(reader, fp16, false), liveDocs, true, false);
      case TWICE -> {
        readLive(values(reader, fp16, false), liveDocs, false, false);
        readLive(values(reader, fp16, false), liveDocs, false, false);
      }
      case VIA_COPY -> readLive(values(reader, fp16, true), liveDocs, false, false);
      case WITH_DELETED -> readLive(values(reader, fp16, false), liveDocs, false, true);
      case SWAPPED -> readSwapped(values(reader, fp16, false), liveDocs);
    }
  }

  /**
   * The vectors of {@link #FIELD} as fp32, read through the wrapped reader. An fp16 field is read
   * through its fp16 values, converted the way the merge does. With {@code copy}, the vector values
   * are copied first, which the collector does not wrap.
   */
  private static FloatVectorValues values(KnnVectorsReader reader, boolean fp16, boolean copy)
      throws IOException {
    if (fp16) {
      Float16VectorValues values = reader.getFloat16VectorValues(FIELD);
      return new Lucene104ScalarQuantizedVectorsWriter.Float16AsFloatVectorValues(
          copy ? values.copy() : values);
    }
    FloatVectorValues values = reader.getFloatVectorValues(FIELD);
    return copy ? values.copy() : values;
  }

  private static void readSwapped(FloatVectorValues values, Bits liveDocs) throws IOException {
    List<Integer> live = new ArrayList<>();
    int deletedOrd = -1;
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      if (liveDocs.get(doc)) {
        live.add(it.index());
      } else if (deletedOrd == -1) {
        deletedOrd = it.index();
      }
    }
    live.set(0, deletedOrd);
    Collections.sort(live);
    for (int ord : live) {
      values.vectorValue(ord);
    }
  }

  private static void readLive(
      FloatVectorValues values, Bits liveDocs, boolean reverse, boolean withDeleted)
      throws IOException {
    List<Integer> ords = new ArrayList<>();
    KnnVectorValues.DocIndexIterator it = values.iterator();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      if (withDeleted || liveDocs == null || liveDocs.get(doc)) {
        ords.add(it.index());
      }
    }
    if (reverse) {
      Collections.reverse(ords);
    }
    for (int ord : ords) {
      values.vectorValue(ord);
    }
  }

  /**
   * Merges two segments with deletes through a real IndexWriter, either with an index sort or by
   * adding them back through SortingCodecReader with a reversed sort.
   */
  private void checkSortedMerge(VectorSimilarityFunction sim, boolean sortingCodecReader)
      throws Exception {
    float[][] vectors = shiftedVectors(2 * PER_SEGMENT, sim, false);
    boolean[] deleted = new boolean[vectors.length];
    for (int i = 0; i < deleted.length; i++) {
      deleted[i] = i % 4 == 0;
    }
    Sort sort = new Sort(new SortField("sort", SortField.Type.INT));
    Codec sqCodec = TestUtil.alwaysKnnVectorsFormat(new Lucene104ScalarQuantizedVectorsFormat());
    FallbackCounter fallbacks = new FallbackCounter();
    try (Directory dir = newDirectory();
        Directory target = newDirectory()) {
      writeTwoSegments(dir, vectors, deleted, sim, false, sortingCodecReader ? null : sort);
      Directory mergedDir;
      if (sortingCodecReader) {
        Sort reversed = new Sort(new SortField("sort", SortField.Type.INT, true));
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(sqCodec).setInfoStream(fallbacks);
        try (DirectoryReader reader = DirectoryReader.open(dir);
            IndexWriter w = new IndexWriter(target, iwc)) {
          CodecReader[] sorted = new CodecReader[reader.leaves().size()];
          for (int i = 0; i < sorted.length; i++) {
            sorted[i] =
                SortingCodecReader.wrap((CodecReader) reader.leaves().get(i).reader(), reversed);
          }
          w.addIndexes(sorted);
          w.forceMerge(1);
          w.commit();
        }
        mergedDir = target;
      } else {
        IndexWriterConfig iwc =
            new IndexWriterConfig().setCodec(sqCodec).setIndexSort(sort).setInfoStream(fallbacks);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
          w.forceMerge(1);
          w.commit();
        }
        mergedDir = dir;
      }
      try (DirectoryReader reader = DirectoryReader.open(mergedDir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        FloatVectorValues values = leaf.getFloatVectorValues(FIELD);
        float[] merged =
            ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues) values)
                .getQuantizedVectorValues()
                .getCentroid();
        float[] expected = liveMean(vectors, deleted, sim == VectorSimilarityFunction.COSINE);
        assertEquals(leaf.numDocs(), values.size());
        assertEquals(0, fallbacks.count.get());
        assertArrayEquals(
            sim + " sortingCodecReader=" + sortingCodecReader, expected, merged, 1e-3f);
      }
    }
  }

  /**
   * Random vectors shifted away from the origin, with an extra offset on dim 0 for every fourth
   * one. A centroid of zero, or one that includes the wrong vectors, then fails the comparison.
   * With {@code fp16}, the values are rounded to fp16, which is what the index stores.
   */
  private static float[][] shiftedVectors(int count, VectorSimilarityFunction sim, boolean fp16) {
    float[][] vectors = new float[count][DIMS];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < DIMS; d++) {
        vectors[i][d] = random().nextFloat() - 0.5f + 1f;
      }
      if (i % 4 == 0) {
        vectors[i][0] += OFFSET;
      }
      if (sim == VectorSimilarityFunction.DOT_PRODUCT) {
        VectorUtil.l2normalize(vectors[i]);
      }
      if (fp16) {
        for (int d = 0; d < DIMS; d++) {
          vectors[i][d] = Float.float16ToFloat(Float.floatToFloat16(vectors[i][d]));
        }
      }
    }
    return vectors;
  }

  /** Writes two segments of PER_SEGMENT docs each, then deletes the marked docs. */
  private static void writeTwoSegments(
      Directory dir,
      float[][] vectors,
      boolean[] deleted,
      VectorSimilarityFunction sim,
      boolean fp16,
      Sort indexSort)
      throws IOException {
    IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene104ScalarQuantizedVectorsFormat()))
            .setMergePolicy(NoMergePolicy.INSTANCE);
    if (indexSort != null) {
      iwc.setIndexSort(indexSort);
    }
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < vectors.length; i++) {
        Document doc = doc(i, vectors[i], sim, fp16);
        doc.add(new NumericDocValuesField("sort", random().nextInt(1000)));
        w.addDocument(doc);
        if (i == PER_SEGMENT - 1) {
          w.commit();
        }
      }
      w.commit();
      for (int i = 0; i < vectors.length; i++) {
        if (deleted[i]) {
          w.deleteDocuments(new Term("id", Integer.toString(i)));
        }
      }
      w.commit();
    }
  }

  private void check(
      VectorSimilarityFunction sim,
      IntPredicate isDeleted,
      boolean fp16,
      boolean firstSegmentWithoutCentroid)
      throws Exception {
    check(
        new Lucene104ScalarQuantizedVectorsFormat(),
        sim,
        isDeleted,
        fp16,
        firstSegmentWithoutCentroid);
  }

  private void check(
      KnnVectorsFormat format,
      VectorSimilarityFunction sim,
      IntPredicate isDeleted,
      boolean fp16,
      boolean firstSegmentWithoutCentroid)
      throws Exception {
    // Two segments of PER_SEGMENT docs. Deleted vectors get a large offset in dim 0, so a
    // centroid that still includes them is easy to tell apart from a live-only one.
    int total = 2 * PER_SEGMENT;
    float[][] vectors = new float[total][];
    boolean[] deleted = new boolean[total];
    int liveCount = 0;
    for (int i = 0; i < total; i++) {
      deleted[i] = isDeleted.test(i);
      vectors[i] = new float[DIMS];
      for (int d = 0; d < DIMS; d++) {
        vectors[i][d] = random().nextFloat() - 0.5f;
      }
      if (deleted[i]) {
        vectors[i][0] += OFFSET;
      } else {
        liveCount++;
      }
      if (sim == VectorSimilarityFunction.DOT_PRODUCT) {
        VectorUtil.l2normalize(vectors[i]);
      }
      if (fp16) {
        // The index stores fp16, so the expected centroid is computed from the rounded values.
        for (int d = 0; d < DIMS; d++) {
          vectors[i][d] = Float.float16ToFloat(Float.floatToFloat16(vectors[i][d]));
        }
      }
    }

    Codec sqCodec = TestUtil.alwaysKnnVectorsFormat(format);
    FallbackCounter fallbacks = new FallbackCounter();
    try (Directory dir = newDirectory()) {
      int next = 0;
      if (firstSegmentWithoutCentroid) {
        IndexWriterConfig iwc =
            new IndexWriterConfig()
                .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()))
                .setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
          for (; next < PER_SEGMENT; next++) {
            w.addDocument(doc(next, vectors[next], sim, fp16));
          }
          w.commit();
        }
      }
      // No merges until forceMerge, so the centroid comes from exactly one merge of both segments.
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(sqCodec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setInfoStream(fallbacks);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (; next < total; next++) {
          w.addDocument(doc(next, vectors[next], sim, fp16));
          if (next == PER_SEGMENT - 1) {
            w.commit();
          }
        }
        w.commit();
        for (int i = 0; i < total; i++) {
          if (deleted[i]) {
            w.deleteDocuments(new Term("id", Integer.toString(i)));
          }
        }
        w.commit();
        w.getConfig().setMergePolicy(new TieredMergePolicy());
        w.forceMerge(1);

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          KnnVectorValues values;
          float[] merged;
          if (fp16) {
            values = leaf.getFloat16VectorValues(FIELD);
            merged =
                ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedFloat16VectorValues) values)
                    .getQuantizedVectorValues()
                    .getCentroid();
          } else {
            values = leaf.getFloatVectorValues(FIELD);
            merged =
                ((Lucene104ScalarQuantizedVectorsReader.ScalarQuantizedVectorValues) values)
                    .getQuantizedVectorValues()
                    .getCentroid();
          }
          assertEquals(liveCount, values.size());
          assertEquals(0, fallbacks.count.get());
          float[] expected = liveMean(vectors, deleted, sim == VectorSimilarityFunction.COSINE);
          assertArrayEquals(sim + " fp16=" + fp16, expected, merged, 1e-3f);
        }
      }
    }
  }

  private static Document doc(int id, float[] vector, VectorSimilarityFunction sim, boolean fp16) {
    Document doc = new Document();
    doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
    if (fp16) {
      short[] half = new short[vector.length];
      for (int d = 0; d < vector.length; d++) {
        half[d] = Float.floatToFloat16(vector[d]);
      }
      doc.add(new KnnFloat16VectorField(FIELD, half, sim));
    } else {
      doc.add(new KnnFloatVectorField(FIELD, vector, sim));
    }
    return doc;
  }

  /** Mean of the live vectors; for COSINE, the normalized mean of the normalized vectors. */
  private static float[] liveMean(float[][] vectors, boolean[] deleted, boolean cosine) {
    double[] sum = new double[DIMS];
    int n = 0;
    for (int i = 0; i < vectors.length; i++) {
      if (deleted[i]) {
        continue;
      }
      n++;
      double norm = cosine ? Math.sqrt(VectorUtil.dotProduct(vectors[i], vectors[i])) : 1;
      for (int d = 0; d < DIMS; d++) {
        sum[d] += vectors[i][d] / norm;
      }
    }
    float[] mean = new float[DIMS];
    for (int d = 0; d < DIMS; d++) {
      mean[d] = (float) (sum[d] / n);
    }
    if (cosine) {
      VectorUtil.l2normalize(mean);
    }
    return mean;
  }
}
