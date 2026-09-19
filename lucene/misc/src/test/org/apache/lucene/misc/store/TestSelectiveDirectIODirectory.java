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
package org.apache.lucene.misc.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RescoreTopNQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.VectorBatch;
import org.apache.lucene.store.VectorBatchCapable;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.BeforeClass;

public class TestSelectiveDirectIODirectory extends LuceneTestCase {

  @BeforeClass
  public static void checkSupported() throws IOException {
    assumeTrue(
        "test requires a JDK with ExtendedOpenOption.DIRECT",
        DirectIODirectory.ExtendedOpenOption_DIRECT != null);
    Path path = createTempDir("selectiveDirectIOProbe");
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path));
        IndexOutput out = dir.createOutput("probe.vec", IOContext.DEFAULT)) {
      out.writeString("test");
    } catch (IOException e) {
      assumeNoException("test requires a filesystem that supports Direct IO", e);
    }
  }

  public void testParallelBatchReads() throws Exception {
    doTest(atLeast(200), 1 + random().nextInt(16), true, 4);
  }

  public void testSerialBatchReads() throws Exception {
    doTest(atLeast(50), 8, false, 0);
  }

  private void doTest(int count, int dim, boolean shuffle, int threads) throws Exception {
    ExecutorService es =
        threads > 0
            ? Executors.newFixedThreadPool(threads, new NamedThreadFactory("test-rerank"))
            : null;
    Path path = createTempDir("selectiveDirectIO");
    float[][] expected = new float[count][dim];
    long[] offsets = new long[count];
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path), es)) {
      try (IndexOutput out = dir.createOutput("data.vec", IOContext.DEFAULT)) {
        for (int i = 0; i < count; i++) {
          offsets[i] = out.getFilePointer();
          ByteBuffer bb = ByteBuffer.allocate(dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
          for (int d = 0; d < dim; d++) {
            float v = random().nextFloat();
            expected[i][d] = v;
            bb.putFloat(v);
          }
          out.writeBytes(bb.array(), 0, bb.capacity());
        }
      }

      if (shuffle) {
        for (int i = count - 1; i > 0; i--) {
          int j = random().nextInt(i + 1);
          long to = offsets[i];
          offsets[i] = offsets[j];
          offsets[j] = to;
          float[] te = expected[i];
          expected[i] = expected[j];
          expected[j] = te;
        }
      }

      try (IndexInput in = dir.openInput("data.vec", IOContext.DEFAULT)) {
        assertTrue(
            "the .vec input must expose the parallel batch read capability",
            in instanceof VectorBatchCapable);
        float[] actual = new float[count * dim];
        VectorBatch batch = ((VectorBatchCapable) in).newBatch();
        assertNotNull("the .vec input must supply a batch", batch);
        assertTrue(batch.add(in, offsets, dim, count, actual));
        batch.execute();
        for (int i = 0; i < count; i++) {
          float[] got = ArrayUtil.copyOfSubArray(actual, i * dim, i * dim + dim);
          assertArrayEquals("vector " + i + " mismatch", expected[i], got, 0f);
        }
      }
    } finally {
      if (es != null) {
        es.shutdown();
      }
    }
  }

  /**
   * One batch drawing uneven numbers of vectors from several inputs. execute() partitions along the
   * per-input runs and caps each task, so this covers runs shorter than a task, runs split across
   * several tasks, and the boundaries between them.
   */
  public void testBatchSpansInputsWithUnevenCounts() throws Exception {
    int dim = 8;
    int[] counts = {1, 37, 4, 64, 13}; // deliberately not multiples of the task target
    ExecutorService es =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("test-rerank-uneven"));
    Path path = createTempDir("selectiveDirectIOUneven");
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path), es, 8)) {
      float[][][] expected = new float[counts.length][][];
      List<IndexInput> inputs = new ArrayList<>();
      try {
        for (int f = 0; f < counts.length; f++) {
          String name = "seg" + f + ".vec";
          expected[f] = new float[counts[f]][dim];
          try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            for (int i = 0; i < counts[f]; i++) {
              ByteBuffer bb = ByteBuffer.allocate(dim * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
              for (int d = 0; d < dim; d++) {
                expected[f][i][d] = random().nextFloat();
                bb.putFloat(expected[f][i][d]);
              }
              out.writeBytes(bb.array(), 0, bb.capacity());
            }
          }
          inputs.add(dir.openInput(name, IOContext.DEFAULT));
        }
        VectorBatch batch = ((VectorBatchCapable) inputs.get(0)).newBatch();
        assertNotNull(batch);
        float[][] actual = new float[counts.length][];
        // Queue in reverse order, so the runs are not in the order the inputs were opened.
        for (int f = counts.length - 1; f >= 0; f--) {
          long[] positions = new long[counts[f]];
          for (int i = 0; i < counts[f]; i++) {
            positions[i] = (long) i * dim * Float.BYTES;
          }
          actual[f] = new float[counts[f] * dim];
          assertTrue(batch.add(inputs.get(f), positions, dim, counts[f], actual[f]));
        }
        batch.execute();
        for (int f = 0; f < counts.length; f++) {
          for (int i = 0; i < counts[f]; i++) {
            assertArrayEquals(
                "file " + f + " vector " + i,
                expected[f][i],
                ArrayUtil.copyOfSubArray(actual[f], i * dim, i * dim + dim),
                0f);
          }
        }
      } finally {
        IOUtils.close(inputs);
      }
    } finally {
      es.shutdown();
    }
  }

  /**
   * The batch is opened from the first segment that has hits, and that segment may be unable to
   * supply one — a field added partway through indexing leaves earlier segments without it. The
   * rest of the segments must still be batched together, so the read executor must see exactly one
   * round of reads; opening the batch from segment 0 alone would silently drop the whole query onto
   * the unbatched path, which shows up here as zero tasks reaching the executor.
   */
  public void testBatchOpensOnFirstSegmentThatCanSupplyOne() throws Exception {
    int dim = 8;
    int docsPerSegment = 30;
    ExecutorService es =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("test-rerank-missing-field"));
    AtomicInteger readTasks = new AtomicInteger();
    Executor counting =
        r -> {
          readTasks.incrementAndGet();
          es.execute(r);
        };
    Path path = createTempDir("selectiveDirectIOMissingField");
    Map<Integer, float[]> vectors = new HashMap<>();
    try (Directory dir = new SelectiveDirectIODirectory(FSDirectory.open(path), counting)) {
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              // Keep the segments separate, so the first one genuinely lacks the field.
              .setMergePolicy(NoMergePolicy.INSTANCE)
              // Pin the format: batching goes through the flat raw-vector reader, and the
              // randomized
              // default codec need not expose one.
              .setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat()))
              // No compound files: inside a .cfs the .vec data is a slice of the compound input, so
              // it is not the directory's own batch-capable input.
              .setUseCompoundFile(false);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        // Segment 0 matches the query and has vectors, but under a different field name, so it has
        // a vector reader yet no entry for the field being reranked. (A segment with no vector
        // field at all has no vector reader and is rejected earlier, before the field lookup.)
        for (int i = 0; i < docsPerSegment; i++) {
          float[] other = new float[dim];
          for (int d = 0; d < dim; d++) {
            other[d] = random().nextFloat();
          }
          Document doc = new Document();
          doc.add(new IntField("id", -1, Field.Store.YES));
          doc.add(new KnnFloatVectorField("other", other, VectorSimilarityFunction.DOT_PRODUCT));
          w.addDocument(doc);
        }
        w.commit();
        // Later segments carry the vector field.
        for (int seg = 1; seg <= 2; seg++) {
          for (int i = 0; i < docsPerSegment; i++) {
            int id = seg * docsPerSegment + i;
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
              v[d] = random().nextFloat();
            }
            Document doc = new Document();
            doc.add(new IntField("id", id, Field.Store.YES));
            doc.add(new KnnFloatVectorField("vector", v, VectorSimilarityFunction.DOT_PRODUCT));
            w.addDocument(doc);
            vectors.put(id, v);
          }
          w.commit();
        }
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertTrue("need several segments for this test", reader.leaves().size() >= 3);
        IndexSearcher searcher = new IndexSearcher(reader);
        float[] target = new float[dim];
        for (int d = 0; d < dim; d++) {
          target[d] = random().nextFloat();
        }
        int k = 5;
        // MatchAllDocs so the shortlist reaches the vectorless first segment too.
        Query q =
            RescoreTopNQuery.createFullPrecisionRescorerQuery(
                new MatchAllDocsQuery(), target, "vector", k);
        readTasks.set(0);
        TopDocs topDocs = searcher.search(q, k);
        assertEquals(k, topDocs.scoreDocs.length);
        assertTrue(
            "the segments that can batch must still be served by the batch, even though the first"
                + " segment with hits could not supply one",
            readTasks.get() > 0);
        StoredFields storedFields = searcher.storedFields();
        for (ScoreDoc sd : topDocs.scoreDocs) {
          int id = storedFields.document(sd.doc).getField("id").numericValue().intValue();
          float[] v = vectors.get(id);
          assertNotNull("a vectorless doc must not outrank a scored one: id=" + id, v);
          assertEquals(
              "score must match the full-precision similarity for id=" + id,
              VectorSimilarityFunction.DOT_PRODUCT.compare(target, v),
              sd.score,
              1e-5);
        }
      }
    } finally {
      es.shutdown();
    }
  }
}
