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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.util.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.tests.util.*;
import org.apache.lucene.util.*;

public class TestQuantizedTiers extends LuceneTestCase {
  public void testHammingThresholdSelection() {
    for (int trial = 0; trial < 30; trial++) {
      int count = random().nextInt(5000), limit = 1 + random().nextInt(800);
      int maxDistance = 1 + random().nextInt(2048);
      var candidates = new IVFasterEvoVectorsReader.HammingCandidates(limit, maxDistance);
      int[] distances = new int[count];
      Integer[] order = new Integer[count];
      for (int doc = 0; doc < count; doc++) {
        order[doc] = doc;
        distances[doc] = trial % 3 == 0 ? maxDistance : random().nextInt(maxDistance + 1);
      }
      List<Integer> shuffled = Arrays.asList(order.clone());
      Collections.shuffle(shuffled, random());
      for (int doc : shuffled) candidates.add(doc, count - doc, distances[doc]);
      Arrays.sort(
          order,
          Comparator.<Integer>comparingInt(doc -> distances[doc]).thenComparingInt(doc -> doc));
      long[] expected = new long[Math.min(limit, count)];
      for (int i = 0; i < expected.length; i++)
        expected[i] = ((long) order[i] << 32) | (count - order[i]);
      long[] actual = candidates.finish();
      Arrays.sort(expected);
      Arrays.sort(actual);
      assertArrayEquals(expected, actual);
    }
  }

  public void testBulkReadsMatchScalarAndRespectAdmission() throws Exception {
    for (int dim : new int[] {7, 65, 1024}) {
      var coarse = new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim);
      var fine = new TierCodec(IVFasterEvoVectorsFormat.Tier.U8, dim);
      int rows = 513;
      byte[] packed = new byte[rows * coarse.bytes];
      random().nextBytes(packed);
      float[] query = new float[dim];
      for (int d = 0; d < dim; d++) query[d] = random().nextFloat();
      var scorer = coarse.scorer(query, VectorSimilarityFunction.DOT_PRODUCT);
      boolean[] admitted = new boolean[256];
      int[] scores = new int[256];
      byte[] block = new byte[256 * coarse.bytes];
      try (Directory dir = new ByteBuffersDirectory()) {
        try (var out = dir.createOutput("codes", IOContext.DEFAULT)) {
          out.writeBytes(packed, packed.length);
        }
        int[] reads = {0};
        try (var input =
            new FilterIndexInput("counted", dir.openInput("codes", IOContext.DEFAULT)) {
              @Override
              public void readBytes(byte[] b, int off, int len) throws java.io.IOException {
                reads[0]++;
                super.readBytes(b, off, len);
              }
            }) {
          int[] docs = new int[rows];
          Arrays.setAll(docs, i -> i);
          var values =
              new TieredVectors(
                  coarse,
                  fine,
                  VectorSimilarityFunction.DOT_PRODUCT,
                  docs,
                  0,
                  rows,
                  docs,
                  input,
                  input);
          for (int start = 0; start < rows; start += 256) {
            int count = Math.min(256, rows - start);
            for (int row = 0; row < count; row++) admitted[row] = (start + row) % 3 == 0;
            Arrays.fill(scores, -1);
            values.hammingBulk(start, count, scorer.query, admitted, block, scores);
            for (int row = 0; row < count; row++) {
              int from = (start + row) * coarse.bytes;
              byte[] record = ArrayUtil.copyOfSubArray(packed, from, from + coarse.bytes);
              int expected = admitted[row] ? new VectorKernels().hamming(scorer.query, record) : -1;
              assertEquals(expected, scores[row]);
            }
            int[] mapped = new int[256];
            Arrays.fill(mapped, -1);
            VectorKernels.INSTANCE.hammingBulk(
                scorer.query,
                java.lang.foreign.MemorySegment.ofArray(packed)
                    .asSlice((long) start * coarse.bytes, (long) count * coarse.bytes),
                coarse.bytes,
                count,
                admitted,
                mapped);
            assertArrayEquals(scores, mapped);
            try (var arena = java.lang.foreign.Arena.ofConfined()) {
              var nativeRecords = arena.allocate((long) count * coarse.bytes);
              nativeRecords.copyFrom(
                  java.lang.foreign.MemorySegment.ofArray(packed)
                      .asSlice((long) start * coarse.bytes, (long) count * coarse.bytes));
              Arrays.fill(mapped, -1);
              VectorKernels.INSTANCE.hammingBulk(
                  scorer.query, nativeRecords, coarse.bytes, count, admitted, mapped);
              assertArrayEquals(scores, mapped);
              // The scalar fallback must agree on mapped memory too.
              Arrays.fill(mapped, -1);
              new VectorKernels()
                  .hammingBulk(scorer.query, nativeRecords, coarse.bytes, count, admitted, mapped);
              assertArrayEquals(scores, mapped);
            }
          }
          assertEquals(3, reads[0]);
        }
      }
    }
  }

  public void testPrefetchHintsExactRecordRanges() throws Exception {
    int dim = 64, slots = 100;
    var coarse = new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim);
    var fine = new TierCodec(IVFasterEvoVectorsFormat.Tier.U8, dim);
    long offset = 17;
    try (Directory dir = new ByteBuffersDirectory()) {
      try (var out = dir.createOutput("records", IOContext.DEFAULT)) {
        out.writeBytes(new byte[(int) offset + slots * (coarse.bytes + fine.bytes)], 0, 1);
      }
      List<long[]> hints = new ArrayList<>();
      try (var input =
          new FilterIndexInput("hinted", dir.openInput("records", IOContext.DEFAULT)) {
            @Override
            public boolean prefetch(long at, long length) {
              hints.add(new long[] {at, length});
              return true;
            }
          }) {
        int[] docs = new int[slots];
        Arrays.setAll(docs, i -> i);
        var values =
            new TieredVectors(
                coarse,
                fine,
                VectorSimilarityFunction.DOT_PRODUCT,
                docs,
                offset,
                slots,
                docs,
                input,
                input);
        values.prefetch(7, 30, true); // a cell: one contiguous coarse run
        values.prefetch(42, 1, false); // a shortlisted fine record, after every coarse record
        assertArrayEquals(
            new long[] {offset + 7L * coarse.bytes, 30L * coarse.bytes}, hints.get(0));
        assertArrayEquals(
            new long[] {offset + (long) slots * coarse.bytes + 42L * fine.bytes, fine.bytes},
            hints.get(1));
      }
    }
  }

  public void testKernelsAndQuantizers() throws Exception {
    VectorKernels scalar = new VectorKernels();
    for (int dim : new int[] {1, 7, 16, 31, 64, 65, 128, 1024}) {
      float[] a = new float[dim], b = new float[dim];
      for (int i = 0; i < dim; i++) {
        a[i] = random().nextFloat() - 0.5f;
        b[i] = random().nextFloat() - 0.5f;
      }
      assertEquals(scalar.distance(a, b), VectorKernels.INSTANCE.distance(a, b), 1e-12);
      byte[] x = new byte[2 * ((dim + 7) / 8)], y = new byte[x.length];
      random().nextBytes(x);
      random().nextBytes(y);
      assertEquals(scalar.hamming(x, y), VectorKernels.INSTANCE.hamming(x, y));
      var coarse = new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim);
      assertEquals(
          0d, coarse.scorer(a, VectorSimilarityFunction.DOT_PRODUCT).score(coarse.encode(a)), 0d);
      var fine = new TierCodec(IVFasterEvoVectorsFormat.Tier.U8, dim);
      byte[] qa = fine.encode(a), qb = fine.encode(b);
      float[] da = new float[dim], db = new float[dim];
      fine.decode(qa, da);
      fine.decode(qb, db);
      for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
        assertEquals(similarity.compare(da, db), fine.scorer(a, similarity).score(qb), 2e-5);
      }
      for (int i = 0; i < dim; i++) assertEquals(a[i], da[i], 0.03);
    }
    if (ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent()) {
      assertEquals("Panama", VectorKernels.INSTANCE.getClass().getSimpleName());
    }
  }

  public void testCopyMergeAndNoRawCopy() throws Exception {
    for (var fine :
        new IVFasterEvoVectorsFormat.Tier[] {
          IVFasterEvoVectorsFormat.Tier.FP32, IVFasterEvoVectorsFormat.Tier.U8
        }) {
      try (Directory dir = newDirectory()) {
        var format = new IVFasterEvoVectorsFormat(8, 8, 1, 1.05, fine);
        var config =
            newIndexWriterConfig()
                .setCodec(TestUtil.alwaysKnnVectorsFormat(format))
                .setUseCompoundFile(false)
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
        Map<String, byte[]> records = new HashMap<>();
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < 180; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, Field.Store.YES));
            doc.add(new NumericDocValuesField("sort", 180 - i));
            float[] vector = new float[65];
            for (int d = 0; d < vector.length; d++) vector[d] = random().nextFloat() - 0.5f;
            VectorUtil.l2normalize(vector);
            if (i % 7 != 0)
              doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.DOT_PRODUCT));
            writer.addDocument(doc);
            if (i % 60 == 59) writer.commit();
          }
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            capture(reader, records, false);
          }
          writer.deleteDocuments(new Term("id", "5"));
          records.remove("5");
          writer.getConfig().setMergePolicy(new LogDocMergePolicy());
          writer.forceMerge(1);
          writer.commit();
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          assertEquals(1, reader.leaves().size());
          capture(reader, records, true);
        }
        for (String file : dir.listAll()) {
          assertFalse(
              file, file.endsWith(".vec") || file.endsWith(".vemf") || file.endsWith(".tmp"));
        }
        TestUtil.checkIndex(dir);
      }
    }
  }

  private void capture(DirectoryReader reader, Map<String, byte[]> records, boolean check)
      throws Exception {
    int count = 0;
    for (var leaf : reader.leaves()) {
      CodecReader segment = (CodecReader) leaf.reader();
      var evo = (IVFasterEvoVectorsReader) segment.getVectorReader().unwrapReaderForField("v");
      var values = evo.getFloatVectorValues("v");
      // Separate slot-aligned sections, including spill copies, with no hidden FP32 payload.
      assertEquals(
          values.offset
              + (long) values.slotCount * (values.coarse.bytes + values.fine.bytes)
              + org.apache.lucene.codecs.CodecUtil.footerLength(),
          values.input.length());
      assertTrue(values.slotCount >= values.size());
      for (int ord = 0; ord < values.size(); ord++) {
        byte[] c = new byte[values.coarse.bytes], f = new byte[values.fine.bytes];
        values.read(ord, true, c);
        values.read(ord, false, f);
        byte[] combined = new byte[c.length + f.length];
        System.arraycopy(c, 0, combined, 0, c.length);
        System.arraycopy(f, 0, combined, c.length, f.length);
        String id = segment.storedFields().document(values.ordToDoc(ord)).get("id");
        if (check) assertArrayEquals(records.get(id), combined);
        else records.put(id, combined);
        count++;
      }
    }
    if (check) assertEquals(records.size(), count);
  }

  public void testTwoStageFilteredShortlist() throws Exception {
    for (var fine :
        new IVFasterEvoVectorsFormat.Tier[] {
          IVFasterEvoVectorsFormat.Tier.FP32, IVFasterEvoVectorsFormat.Tier.U8
        }) {
      try (Directory dir = newDirectory()) {
        var config =
            newIndexWriterConfig()
                .setCodec(
                    TestUtil.alwaysKnnVectorsFormat(
                        new IVFasterEvoVectorsFormat(8, 8, 1, 1.05, fine)));
        try (IndexWriter writer = new IndexWriter(dir, config)) {
          for (int i = 0; i < 1800; i++) {
            Document doc = new Document();
            float[] v = new float[32];
            for (int d = 0; d < v.length; d++) v[d] = random().nextFloat() - 0.5f;
            VectorUtil.l2normalize(v);
            doc.add(new KnnFloatVectorField("v", v, VectorSimilarityFunction.DOT_PRODUCT));
            writer.addDocument(doc);
          }
          writer.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          var leaf = (CodecReader) reader.leaves().get(0).reader();
          var evo = (IVFasterEvoVectorsReader) leaf.getVectorReader().unwrapReaderForField("v");
          var values = evo.getFloatVectorValues("v");
          float[] query = values.vectorValue(1799).clone();
          var coarseScore = values.coarse.scorer(query, VectorSimilarityFunction.DOT_PRODUCT);
          var fineScore = values.fine.scorer(query, VectorSimilarityFunction.DOT_PRODUCT);
          List<ScoreDoc> admitted = new ArrayList<>();
          FixedBitSet allowed = new FixedBitSet(1800);
          for (int ord = 0; ord < values.size(); ord++) {
            if (ord % 2 != 0) continue;
            allowed.set(ord);
            byte[] code = new byte[values.coarse.bytes];
            values.read(ord, true, code);
            admitted.add(new ScoreDoc(ord, (float) coarseScore.score(code)));
          }
          Comparator<ScoreDoc> order =
              Comparator.<ScoreDoc>comparingDouble(hit -> -hit.score)
                  .thenComparingInt(hit -> hit.doc);
          admitted.sort(order);
          admitted = new ArrayList<>(admitted.subList(0, 700));
          for (ScoreDoc hit : admitted) {
            byte[] code = new byte[values.fine.bytes];
            values.read(hit.doc, false, code);
            hit.score = (float) fineScore.score(code);
          }
          admitted.sort(order);
          var collector =
              new TopKnnCollector(
                  10, Integer.MAX_VALUE, new IVFasterEvoVectorsFormat.SearchStrategy(8));
          evo.search("v", query, collector, TestIVFasterEvoVectorsFormat.accepting(allowed));
          var actual = collector.topDocs();
          // A filtered scan deduplicates spill copies at admission: visits are documents.
          assertEquals(900, actual.totalHits.value());
          for (int i = 0; i < 10; i++) {
            assertEquals(admitted.get(i).doc, actual.scoreDocs[i].doc);
            assertEquals(admitted.get(i).score, actual.scoreDocs[i].score, 0f);
          }
          for (int budget : new int[] {1, 17, 257}) {
            var limited =
                new TopKnnCollector(10, budget, new IVFasterEvoVectorsFormat.SearchStrategy(8));
            evo.search("v", query, limited, TestIVFasterEvoVectorsFormat.accepting(allowed));
            assertEquals(budget, limited.visitedCount());
            for (ScoreDoc hit : limited.topDocs().scoreDocs) assertTrue(allowed.get(hit.doc));
          }
        }
      }
    }
  }
}
