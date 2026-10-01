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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Verifies that full-precision rescoring actually reaches {@link IndexInput#prefetch} on the raw
 * vector file. The prefetch has to travel from {@link RescoreTopNQuery} through {@link
 * DoubleValues} and the quantized {@code FloatVectorValues} wrapper down to the store; a missing
 * override anywhere on that path silently degrades rescoring to one outstanding read at a time,
 * which is invisible in correctness tests.
 */
public class TestRescoreTopNQueryPrefetch extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final int DIMS = 8;
  private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.DOT_PRODUCT;

  public void testRescoringPrefetchesRawVectors() throws Exception {
    Random random = random();
    Map<Integer, float[]> vectors = new HashMap<>();
    PrefetchCountingDirectory dir = new PrefetchCountingDirectory(new ByteBuffersDirectory());
    try {
      IndexWriterConfig config = new IndexWriterConfig();
      config.setCodec(
          TestUtil.alwaysKnnVectorsFormat(new Lucene104HnswScalarQuantizedVectorsFormat()));
      // Keep files uncompounded so prefetches can be attributed to the raw vector file itself
      // rather than to the enclosing .cfs.
      config.setUseCompoundFile(false);
      int perSegment = 60;
      int segments = 3;
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int s = 0; s < segments; s++) {
          for (int i = 0; i < perSegment; i++) {
            int id = s * perSegment + i;
            float[] vector = randomVector(random);
            Document doc = new Document();
            doc.add(new IntField("id", id, Field.Store.YES));
            doc.add(new KnnFloatVectorField(FIELD, vector, SIMILARITY));
            writer.addDocument(doc);
            vectors.put(id, vector);
          }
          writer.flush();
        }
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] target = randomVector(random);
        int k = 10;
        int shortlist = k * 5;

        // Baseline: the first-phase query alone must not touch the raw vectors at all.
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery(FIELD, target, shortlist);
        searcher.search(knnQuery, shortlist);
        int afterFirstPhase = dir.prefetchCount(".vec");

        Query rescored =
            RescoreTopNQuery.createFullPrecisionRescorerQuery(knnQuery, target, FIELD, k);
        TopDocs topDocs = searcher.search(rescored, k);

        int prefetched = dir.prefetchCount(".vec") - afterFirstPhase;
        assertTrue(
            "rescoring issued no prefetch on the raw vector file; counted prefetches per extension: "
                + dir.counts
                + ". A missing prefetch override on the rescore path leaves the store at one "
                + "outstanding read per candidate.",
            prefetched > 0);

        // Rescoring reads one raw vector per candidate, so prefetches should scale with the
        // shortlist rather than being a token few.
        assertTrue(
            "expected prefetches to scale with the rescored candidate count, got " + prefetched,
            prefetched >= topDocs.scoreDocs.length);

        // Deferring the scoring behind the prefetch window must not change the scores.
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          Document doc = searcher.storedFields().document(scoreDoc.doc);
          int id = doc.getField("id").numericValue().intValue();
          float[] docVector = vectors.get(id);
          assertNotNull("vector for id " + id + " not found", docVector);
          assertEquals(
              "score does not match full-precision similarity for id " + id,
              SIMILARITY.compare(target, docVector),
              scoreDoc.score,
              1e-5);
        }
      }
    } finally {
      dir.close();
    }
  }

  private static float[] randomVector(Random random) {
    float[] v = new float[DIMS];
    for (int i = 0; i < v.length; i++) {
      v[i] = random.nextFloat() - 0.5f;
    }
    VectorUtilSupport.normalize(v);
    return v;
  }

  /** Normalizes so DOT_PRODUCT stays in range. */
  private static class VectorUtilSupport {
    static void normalize(float[] v) {
      double sum = 0;
      for (float f : v) {
        sum += (double) f * f;
      }
      float norm = (float) Math.sqrt(sum);
      if (norm == 0) {
        v[0] = 1f;
        return;
      }
      for (int i = 0; i < v.length; i++) {
        v[i] /= norm;
      }
    }
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
      if (dot < 0) {
        return in;
      }
      AtomicInteger counter = counts.computeIfAbsent(name.substring(dot), _ -> new AtomicInteger());
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
      return in.prefetch(offset, length);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingIndexInput(in.slice(sliceDescription, offset, length), counter);
    }

    @Override
    public IndexInput clone() {
      return new CountingIndexInput(in.clone(), counter);
    }
  }
}
