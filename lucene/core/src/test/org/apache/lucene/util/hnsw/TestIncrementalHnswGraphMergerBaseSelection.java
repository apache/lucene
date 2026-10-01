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
package org.apache.lucene.util.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Tests which source graph {@link IncrementalHnswGraphMerger} picks as the base graph when some
 * candidates carry deletions.
 */
public class TestIncrementalHnswGraphMergerBaseSelection extends LuceneTestCase {

  private static final int DIM = 16;
  private static final int M = 16;
  private static final int BEAM_WIDTH = 100;
  private static final String VECTOR_FIELD = "v";
  private static final String ID_FIELD = "id";
  private static final int DELETE_PCT_THRESHOLD = IncrementalHnswGraphMerger.DELETE_PCT_THRESHOLD;

  // A deletes half of what the threshold allows, so it stays eligible as a base
  private static final int A_DOCS = 1000;
  private static final int A_DELETES = A_DOCS * DELETE_PCT_THRESHOLD / 200;
  // B has no deletions and falls strictly between A's live count and A's total node count
  private static final int B_DOCS = A_DOCS - A_DELETES / 2;

  /**
   * Segment A carries deletions under the threshold; segment B has none. B holds more live vectors
   * than A but fewer nodes, so it must be chosen as the base regardless of the order in which the
   * readers are added.
   */
  public void testBaseGraphChosenByLiveCount() throws IOException {
    assertTrue(A_DELETES > 0);
    assertTrue(A_DOCS - A_DELETES < B_DOCS && B_DOCS < A_DOCS);
    try (Directory dir = newDirectory()) {
      buildIndex(dir, new int[] {A_DOCS, B_DOCS}, new int[] {A_DELETES, 0});
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<CodecReader> segments = segments(reader);
        assertEquals(2, segments.size());
        CodecReader a = segments.get(0);
        CodecReader b = segments.get(1);
        assertEquals(A_DOCS, a.maxDoc());
        assertEquals(A_DOCS - A_DELETES, a.numDocs());
        assertEquals(B_DOCS, b.numDocs());

        for (boolean aFirst : new boolean[] {true, false}) {
          for (boolean concurrent : new boolean[] {false, true}) {
            IncrementalHnswGraphMerger merger =
                addReaders(aFirst ? List.of(a, b) : List.of(b, a), concurrent);
            assertNotNull(merger.largestGraphReader);
            assertSame(
                "aFirst=" + aFirst + " concurrent=" + concurrent,
                b.getVectorReader(),
                merger.largestGraphReader.reader());
          }
        }
      }
    }
  }

  /**
   * Segments of random sizes and deletion counts, added in random order. The base must be a graph
   * within the deletion threshold that has the most live vectors, or none if no graph is within the
   * threshold.
   */
  public void testBaseGraphHasMostLiveVectorsAmongEligible() throws IOException {
    int numSegments = TestUtil.nextInt(random(), 2, 6);
    int[] docs = new int[numSegments];
    int[] deletes = new int[numSegments];
    for (int s = 0; s < numSegments; s++) {
      docs[s] = TestUtil.nextInt(random(), 10, 200);
      // half of the segments stay within the threshold; at least one doc stays live, otherwise the
      // segment is dropped
      int maxDeletes = random().nextBoolean() ? docs[s] * DELETE_PCT_THRESHOLD / 100 : docs[s] - 1;
      deletes[s] = TestUtil.nextInt(random(), 0, maxDeletes);
    }
    try (Directory dir = newDirectory()) {
      buildIndex(dir, docs, deletes);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        List<CodecReader> segments = new ArrayList<>(segments(reader));
        assertEquals(numSegments, segments.size());
        int expectedLive = -1;
        for (CodecReader segment : segments) {
          int total = segment.maxDoc();
          int live = segment.numDocs();
          if ((total - live) * 100 / total <= DELETE_PCT_THRESHOLD) {
            expectedLive = Math.max(expectedLive, live);
          }
        }

        for (int iter = 0; iter < 5; iter++) {
          Collections.shuffle(segments, random());
          for (boolean concurrent : new boolean[] {false, true}) {
            IncrementalHnswGraphMerger merger = addReaders(segments, concurrent);
            if (expectedLive == -1) {
              assertNull(merger.largestGraphReader);
            } else {
              assertNotNull(merger.largestGraphReader);
              CodecReader base = owner(segments, merger.largestGraphReader.reader());
              assertEquals("concurrent=" + concurrent, expectedLive, base.numDocs());
            }
          }
        }
      }
    }
  }

  private static IncrementalHnswGraphMerger addReaders(
      List<CodecReader> segments, boolean concurrent) throws IOException {
    FieldInfo fieldInfo = segments.get(0).getFieldInfos().fieldInfo(VECTOR_FIELD);
    IncrementalHnswGraphMerger merger =
        concurrent
            ? new ConcurrentHnswMerger(fieldInfo, null, M, BEAM_WIDTH, null, 2)
            : new IncrementalHnswGraphMerger(fieldInfo, null, M, BEAM_WIDTH);
    for (CodecReader segment : segments) {
      merger.addReader(segment.getVectorReader(), doc -> doc, segment.getLiveDocs());
    }
    return merger;
  }

  private static CodecReader owner(List<CodecReader> segments, KnnVectorsReader vectorReader) {
    for (CodecReader segment : segments) {
      if (segment.getVectorReader() == vectorReader) {
        return segment;
      }
    }
    throw new AssertionError("base graph reader does not belong to any segment");
  }

  private static List<CodecReader> segments(DirectoryReader reader) {
    List<CodecReader> segments = new ArrayList<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      segments.add((CodecReader) ctx.reader());
    }
    return segments;
  }

  /** Flushes one segment per entry of {@code docs}, then deletes the first docs of each segment. */
  private void buildIndex(Directory dir, int[] docs, int[] deletes) throws IOException {
    IndexWriterConfig cfg = new IndexWriterConfig();
    cfg.setCodec(TestUtil.alwaysKnnVectorsFormat(new Lucene99HnswVectorsFormat(M, BEAM_WIDTH, 0)));
    cfg.setMergePolicy(NoMergePolicy.INSTANCE);
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (int s = 0; s < docs.length; s++) {
        for (int i = 0; i < docs[s]; i++) {
          Document doc = new Document();
          doc.add(new StringField(ID_FIELD, s + "_" + i, Field.Store.NO));
          float[] v = new float[DIM];
          for (int j = 0; j < DIM; j++) {
            v[j] = random().nextFloat();
          }
          doc.add(new KnnFloatVectorField(VECTOR_FIELD, v));
          w.addDocument(doc);
        }
        w.flush();
      }
      for (int s = 0; s < docs.length; s++) {
        for (int i = 0; i < deletes[s]; i++) {
          w.deleteDocuments(new Term(ID_FIELD, s + "_" + i));
        }
      }
      w.commit();
    }
  }
}
