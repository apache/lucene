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
package org.apache.lucene.codecs.fuse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.lucene.codecs.bloom.FuzzySet;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

/** Comprehensive validation and empirical benchmark comparison for Binary Fuse filters. */
public class TestBinaryFuseFilter extends LuceneTestCase {

  public void testZeroFalseNegativesRoundTrip() throws IOException {
    int[] sizes = {100, 1000, 10000, 50000};
    Random rnd = random();

    for (int size : sizes) {
      long[] keys = new long[size];
      for (int i = 0; i < size; i++) {
        keys[i] = rnd.nextLong();
      }
      BinaryFuse8 filter8 = BinaryFuse8.construct(keys);

      for (long key : keys) {
        assertTrue("FN before serialization in BinaryFuse8", filter8.contains(key));
      }

      byte[] bytes = new byte[size * 2 + 1024];
      ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
      filter8.serialize(out);

      ByteArrayDataInput in = new ByteArrayDataInput(bytes, 0, out.getPosition());
      BinaryFuse8 restored8 = BinaryFuse8.deserialize(in);

      for (long key : keys) {
        assertTrue("FN after serialization in BinaryFuse8", restored8.contains(key));
      }
    }
  }

  public void testBenchmarkAndEmpiricalFpr() throws IOException {
    int[] keyCounts = {1_000, 10_000, 100_000, 500_000, 1_000_000};
    int absentCount = 1_000_000;
    Random rnd = new Random(42);

    // Pre-generate 1,000,000 distinct absent queries (fast bit-packing, ~60MB RAM)
    BytesRef[] absentTerms = new BytesRef[absentCount];
    long[] absentHashes = new long[absentCount];
    for (int i = 0; i < absentCount; i++) {
      byte[] b = new byte[16];
      long v1 = 0x616273656e745f00L | (i & 0xFF); // "absent_\0"
      long v2 = rnd.nextLong();
      for (int j = 0; j < 8; j++) {
        b[j] = (byte) (v1 >>> (j * 8));
        b[j + 8] = (byte) (v2 >>> (j * 8));
      }
      BytesRef term = new BytesRef(b);
      long[] h = StringHelper.murmurhash3_x64_128(term);
      absentTerms[i] = term;
      absentHashes[i] = h[0] ^ Long.rotateLeft(h[1], 32);
    }

    List<BenchmarkRow> rows = new ArrayList<>();

    for (int size : keyCounts) {
      // 1. Generate Present Keys
      long[] presentHashes = new long[size];
      BytesRef[] presentTerms = new BytesRef[size];

      for (int i = 0; i < size; i++) {
        byte[] b = new byte[16];
        long v1 = 0x706b5f0000000000L | (i & 0xFFFFFFL); // "pk_\0"
        long v2 = rnd.nextLong();
        for (int j = 0; j < 8; j++) {
          b[j] = (byte) (v1 >>> (j * 8));
          b[j + 8] = (byte) (v2 >>> (j * 8));
        }
        BytesRef term = new BytesRef(b);
        long[] h = StringHelper.murmurhash3_x64_128(term);
        presentHashes[i] = h[0] ^ Long.rotateLeft(h[1], 32);
        presentTerms[i] = term;
      }

      // --- Benchmark A: Lucene FuzzySet Bloom (1% target) ---
      long start = System.nanoTime();
      FuzzySet bloom = FuzzySet.createOptimalSet(size, 0.01f);
      for (BytesRef term : presentTerms) {
        bloom.addValue(term);
      }
      long bloomBuildMs = (System.nanoTime() - start) / 1_000_000;

      ByteArrayDataOutput bloomOut = new ByteArrayDataOutput(new byte[size * 5 + 4096]);
      bloom.serialize(bloomOut);
      int bloomBytes = bloomOut.getPosition();

      int bloomFn = 0;
      start = System.nanoTime();
      for (BytesRef term : presentTerms) {
        if (bloom.contains(term) == FuzzySet.ContainsResult.NO) bloomFn++;
      }
      double bloomPresentQps = (size * 1_000_000_000.0) / (System.nanoTime() - start);

      int bloomFp = 0;
      start = System.nanoTime();
      for (BytesRef term : absentTerms) {
        if (bloom.contains(term) == FuzzySet.ContainsResult.MAYBE) bloomFp++;
      }
      double bloomAbsentQps = (absentCount * 1_000_000_000.0) / (System.nanoTime() - start);

      rows.add(
          new BenchmarkRow(
              size,
              "Bloom (1% target)",
              bloomBytes,
              (8.0 * bloomBytes) / size,
              bloomBuildMs,
              bloomFp,
              absentCount,
              (double) bloomFp / absentCount,
              bloomPresentQps,
              bloomAbsentQps,
              bloomFn));

      // --- Benchmark B: BinaryFuse8 ---
      start = System.nanoTime();
      BinaryFuse8 fuse8 = BinaryFuse8.construct(presentHashes);
      long fuse8BuildMs = (System.nanoTime() - start) / 1_000_000;

      ByteArrayDataOutput fuse8Out = new ByteArrayDataOutput(new byte[size * 3 + 4096]);
      fuse8.serialize(fuse8Out);
      int fuse8Bytes = fuse8Out.getPosition();

      int fuse8Fn = 0;
      start = System.nanoTime();
      for (long h : presentHashes) {
        if (fuse8.contains(h) == false) fuse8Fn++;
      }
      double fuse8PresentQps = (size * 1_000_000_000.0) / (System.nanoTime() - start);

      int fuse8Fp = 0;
      start = System.nanoTime();
      for (long h : absentHashes) {
        if (fuse8.contains(h)) fuse8Fp++;
      }
      double fuse8AbsentQps = (absentCount * 1_000_000_000.0) / (System.nanoTime() - start);

      rows.add(
          new BenchmarkRow(
              size,
              "BinaryFuse 8-bit",
              fuse8Bytes,
              (8.0 * fuse8Bytes) / size,
              fuse8BuildMs,
              fuse8Fp,
              absentCount,
              (double) fuse8Fp / absentCount,
              fuse8PresentQps,
              fuse8AbsentQps,
              fuse8Fn));

      // --- Benchmark C: BinaryFuse16 ---
      start = System.nanoTime();
      BinaryFuse16 fuse16 = BinaryFuse16.construct(presentHashes);
      long fuse16BuildMs = (System.nanoTime() - start) / 1_000_000;

      ByteArrayDataOutput fuse16Out = new ByteArrayDataOutput(new byte[size * 4 + 4096]);
      fuse16.serialize(fuse16Out);
      int fuse16Bytes = fuse16Out.getPosition();

      int fuse16Fn = 0;
      start = System.nanoTime();
      for (long h : presentHashes) {
        if (fuse16.contains(h) == false) fuse16Fn++;
      }
      double fuse16PresentQps = (size * 1_000_000_000.0) / (System.nanoTime() - start);

      int fuse16Fp = 0;
      start = System.nanoTime();
      for (long h : absentHashes) {
        if (fuse16.contains(h)) fuse16Fp++;
      }
      double fuse16AbsentQps = (absentCount * 1_000_000_000.0) / (System.nanoTime() - start);

      rows.add(
          new BenchmarkRow(
              size,
              "BinaryFuse 16-bit",
              fuse16Bytes,
              (8.0 * fuse16Bytes) / size,
              fuse16BuildMs,
              fuse16Fp,
              absentCount,
              (double) fuse16Fp / absentCount,
              fuse16PresentQps,
              fuse16AbsentQps,
              fuse16Fn));
    }

    // 1. Compact Console Output (under 80 chars - never wraps in terminal!)
    System.out.println("\n--- COMPACT RESULTS (CONSOLE FRIENDLY) ---");
    System.out.printf(
        Locale.ROOT,
        "%-9s | %-18s | %-9s | %-6s | %-5s | %-7s | %-8s%n",
        "Keys",
        "Filter",
        "Size",
        "Bits/K",
        "Build",
        "FPR",
        "AbsentQPS");
    System.out.println("-------------------------------------------------------------------------");

    for (BenchmarkRow row : rows) {
      System.out.printf(
          Locale.ROOT,
          "%-9s | %-18s | %-9s | %-6.2f | %-4dms | %-6.2f%% | %-7.1fM%n",
          formatCount(row.keys),
          row.filter,
          formatBytes(row.bytes),
          row.bitsPerKey,
          row.buildMs,
          row.empiricalFpr * 100.0,
          row.absentQps / 1_000_000.0);
      assertEquals("Must have 0 false negatives", 0, row.falseNegatives);
    }

    // 2. Full GitHub Markdown Table (ready to copy-paste into GitHub)
    System.out.println("\n--- GITHUB MARKDOWN TABLE ---");
    System.out.println(
        "| Keys | Filter | Size | Bits/Key | Build | False Positives | Empirical FPR | Present QPS | Absent QPS | FN |");
    System.out.println("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |");
    for (BenchmarkRow row : rows) {
      System.out.printf(
          Locale.ROOT,
          "| %s | %s | %s | %.2f | %d ms | %,d / %,d | %.4f%% | %.1f M/s | %.1f M/s | %d |%n",
          formatCount(row.keys),
          row.filter,
          formatBytes(row.bytes),
          row.bitsPerKey,
          row.buildMs,
          row.falsePositives,
          row.absentQueries,
          row.empiricalFpr * 100.0,
          row.presentQps / 1_000_000.0,
          row.absentQps / 1_000_000.0,
          row.falseNegatives);
    }
    System.out.println(
        "-------------------------------------------------------------------------\n");
  }

  private static String formatCount(int n) {
    if (n >= 1_000_000) return (n / 1_000_000) + ",000,000";
    if (n >= 1_000) return (n / 1_000) + ",000";
    return String.valueOf(n);
  }

  private static String formatBytes(int bytes) {
    if (bytes >= 1024 * 1024) return String.format(Locale.ROOT, "%.2f MB", bytes / 1048576.0);
    if (bytes >= 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    return bytes + " B";
  }

  private record BenchmarkRow(
      int keys,
      String filter,
      int bytes,
      double bitsPerKey,
      long buildMs,
      int falsePositives,
      int absentQueries,
      double empiricalFpr,
      double presentQps,
      double absentQps,
      int falseNegatives) {}
}
