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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Partitioning a merge on <em>key</em> boundaries rather than on document counts.
 *
 * <p>The point of a partitioned merge is that afterwards a key lives in one output rather than
 * being spread across all of them. That only holds if the cuts are placed on real key values: cuts
 * derived from each input's document counts land in different places in each input, so a key
 * straddles two outputs and nothing is gained.
 *
 * <p>{@link #testCutsOnDocCountsDoNotLocalizeKeys()} shows the naive form failing that property,
 * and {@link #testKeysAreLocalizedToOneSegment()} shows the key-boundary form holding it.
 */
public class TestKeyRangePartitioning extends LuceneTestCase {

  private static final String KEY = "key";
  private static final int OUTPUTS = 4;

  /** Cuts every {@code totalDocs/outputs} documents, on a key boundary. */
  static int[][] keyBoundaryPartitions(List<CodecReader> readers, String field, int outputs)
      throws IOException {
    final int[][] starts = new int[readers.size()][];
    final SortedDocValues[] dvs = new SortedDocValues[readers.size()];
    long totalDocs = 0;
    for (int i = 0; i < readers.size(); i++) {
      starts[i] = firstDocPerOrd(readers.get(i), field);
      dvs[i] = readers.get(i).getSortedDocValues(field);
      totalDocs += readers.get(i).numDocs();
    }

    // Streaming k-way merge over the input dictionaries: memory is proportional
    // to the number of inputs, not to the number of distinct keys.
    final long per = Math.max(1, totalDocs / outputs);
    final int[] cursor = new int[readers.size()];
    final List<BytesRef> cuts = new ArrayList<>();
    long acc = 0;
    while (cuts.size() < outputs - 1) {
      BytesRef min = null;
      for (int i = 0; i < readers.size(); i++) {
        if (dvs[i] == null || cursor[i] >= dvs[i].getValueCount()) {
          continue;
        }
        BytesRef cand = dvs[i].lookupOrd(cursor[i]);
        if (min == null || cand.compareTo(min) < 0) {
          BytesRefBuilder copy = new BytesRefBuilder();
          copy.copyBytes(cand);
          min = copy.toBytesRef();
        }
      }
      if (min == null) {
        break; // dictionaries exhausted
      }
      if (acc >= per * (cuts.size() + 1)) {
        cuts.add(min);
      }
      for (int i = 0; i < readers.size(); i++) {
        if (dvs[i] == null || cursor[i] >= dvs[i].getValueCount()) {
          continue;
        }
        if (dvs[i].lookupOrd(cursor[i]).compareTo(min) == 0) {
          acc += starts[i][cursor[i] + 1] - starts[i][cursor[i]];
          cursor[i]++;
        }
      }
    }

    final int actual = cuts.size() + 1;
    final int[][] partitions = new int[readers.size()][actual + 1];
    for (int i = 0; i < readers.size(); i++) {
      partitions[i][0] = 0;
      for (int c = 0; c < cuts.size(); c++) {
        partitions[i][c + 1] = dvs[i] == null ? 0 : docOffsetOf(dvs[i], starts[i], cuts.get(c));
      }
      partitions[i][actual] = readers.get(i).maxDoc();
      // A key absent from this reader can make two cuts land on the same
      // offset; an empty output range is legal, a decreasing one is not.
      for (int o = 1; o <= actual; o++) {
        if (partitions[i][o] < partitions[i][o - 1]) {
          partitions[i][o] = partitions[i][o - 1];
        }
      }
    }
    return partitions;
  }

  private static int[] firstDocPerOrd(CodecReader r, String field) throws IOException {
    SortedDocValues dv = r.getSortedDocValues(field);
    int k = dv == null ? 0 : dv.getValueCount();
    int[] starts = new int[k + 1];
    Arrays.fill(starts, -1);
    if (dv != null) {
      for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
        int ord = dv.ordValue();
        if (starts[ord] == -1) {
          starts[ord] = doc;
        }
      }
    }
    starts[k] = r.maxDoc();
    for (int i = k - 1; i >= 0; i--) {
      if (starts[i] == -1) {
        starts[i] = starts[i + 1];
      }
    }
    return starts;
  }

  private static int docOffsetOf(SortedDocValues dv, int[] starts, BytesRef key)
      throws IOException {
    int ord = dv.lookupTerm(key);
    if (ord < 0) {
      ord = -ord - 1; // insertion point: first key >= the cut
    }
    if (ord >= starts.length - 1) {
      return starts[starts.length - 1];
    }
    return starts[ord];
  }

  /** Cuts each input into equal document counts, ignoring where keys begin and end. */
  static int[][] docCountPartitions(List<CodecReader> readers, int outputs) {
    int[][] partitions = new int[readers.size()][outputs + 1];
    for (int i = 0; i < readers.size(); i++) {
      int maxDoc = readers.get(i).maxDoc();
      for (int o = 0; o <= outputs; o++) {
        partitions[i][o] = (int) ((long) o * maxDoc / outputs);
      }
    }
    return partitions;
  }

  public void testKeysAreLocalizedToOneSegment() throws Exception {
    assertEquals(
        "every key must end up in exactly one segment", Set.of(1), spreadPerKey(true).keySet());
  }

  public void testCutsOnDocCountsDoNotLocalizeKeys() throws Exception {
    Map<Integer, Integer> spread = spreadPerKey(false);
    assertTrue(
        "cutting on document counts is expected to spread some key across segments, got " + spread,
        spread.keySet().stream().anyMatch(s -> s > 1));
  }

  /**
   * A wide split: real callers partition into tens of ranges, not two or three. Exercises the
   * partition spec, the per-output doc maps and the empty-output path at that width.
   */
  public void testManyOutputs() throws Exception {
    final int outputs = 64;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(null);
      iwc.setIndexSort(new Sort(new SortField(KEY, SortField.Type.STRING)));
      iwc.setMergePolicy(new Partitioner(readers -> keyBoundaryPartitions(readers, KEY, outputs)));
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int s = 0; s < 4; s++) {
          for (int k = 0; k < 500; k++) {
            String key = String.format(java.util.Locale.ROOT, "k%05d", k);
            Document doc = new Document();
            doc.add(new SortedDocValuesField(KEY, new BytesRef(key)));
            doc.add(new StoredField("id", s + ":" + k));
            w.addDocument(doc);
            expected.add(s + ":" + k);
          }
          w.flush();
        }
        w.commit();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("expected a wide split, got " + r.leaves().size(), r.leaves().size() > 8);
        Set<String> seen = new HashSet<>();
        for (LeafReaderContext ctx : r.leaves()) {
          StoredFields sf = ctx.reader().storedFields();
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++)
            seen.add(sf.document(doc).get("id"));
        }
        assertEquals("a wide split must not lose or duplicate documents", expected, seen);
      }
    }
  }

  /**
   * An output that owns no document in a given input is legal: a key missing from that input makes
   * two cuts land on the same offset. The merge must still succeed and produce the other outputs.
   */
  public void testEmptyRangeInSomeInput() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(null);
      iwc.setIndexSort(new Sort(new SortField(KEY, SortField.Type.STRING)));
      // Every cut collapses to maxDoc for each input, so all outputs but the
      // first own nothing at all -- including a range that starts at maxDoc.
      iwc.setMergePolicy(
          new Partitioner(
              readers -> {
                int[][] parts = new int[readers.size()][OUTPUTS + 1];
                for (int i = 0; i < readers.size(); i++) {
                  int maxDoc = readers.get(i).maxDoc();
                  for (int o = 1; o <= OUTPUTS; o++) {
                    parts[i][o] = maxDoc;
                  }
                }
                return parts;
              }));
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int s = 0; s < 3; s++) {
          for (int d = 0; d < 25; d++) {
            String key = String.format(java.util.Locale.ROOT, "k%03d", d);
            Document doc = new Document();
            doc.add(new SortedDocValuesField(KEY, new BytesRef(key)));
            doc.add(new StoredField("id", s + ":" + d));
            w.addDocument(doc);
            expected.add(s + ":" + d);
          }
          w.flush();
        }
        w.commit();
        w.maybeMerge();
      }
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        Set<String> seen = new HashSet<>();
        for (LeafReaderContext ctx : r.leaves()) {
          StoredFields sf = ctx.reader().storedFields();
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            seen.add(sf.document(doc).get("id"));
          }
        }
        assertEquals("empty outputs must not lose documents", expected, seen);
      }
    }
  }

  /**
   * Indexes several segments whose keys interleave, runs one partitioned merge, and returns a
   * histogram of how many segments each key ended up in.
   */
  private Map<Integer, Integer> spreadPerKey(boolean onKeyBoundaries) throws Exception {
    final int segments = 4;
    final int keys = 40;
    Map<Integer, Integer> histogram = new HashMap<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(null);
      iwc.setIndexSort(new Sort(new SortField(KEY, SortField.Type.STRING)));
      iwc.setMergePolicy(new Partitioner(onKeyBoundaries));
      Set<String> expected = new HashSet<>();
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int s = 0; s < segments; s++) {
          for (int k = 0; k < keys; k++) {
            // Each segment weights the keys differently. With a uniform
            // distribution, equal-document-count cuts happen to land on the
            // same key boundaries in every segment, which would make the
            // doc-count partitioner look correct by accident.
            int docsHere = 1 + ((k * (s + 1) + s) % 9);
            for (int d = 0; d < docsHere; d++) {
              String key = String.format(java.util.Locale.ROOT, "k%03d", k);
              Document doc = new Document();
              doc.add(new SortedDocValuesField(KEY, new BytesRef(key)));
              doc.add(new StoredField(KEY, key));
              doc.add(new StoredField("id", s + ":" + k + ":" + d));
              w.addDocument(doc);
              expected.add(s + ":" + k + ":" + d);
            }
          }
          w.flush();
        }
        w.commit();
        w.maybeMerge();
      }

      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertTrue("the merge must have produced several outputs", r.leaves().size() > 1);
        Map<String, Set<Integer>> segmentsPerKey = new HashMap<>();
        Set<String> seen = new HashSet<>();
        for (int leaf = 0; leaf < r.leaves().size(); leaf++) {
          LeafReader lr = r.leaves().get(leaf).reader();
          StoredFields sf = lr.storedFields();
          for (int doc = 0; doc < lr.maxDoc(); doc++) {
            segmentsPerKey
                .computeIfAbsent(sf.document(doc).get(KEY), _ -> new HashSet<>())
                .add(leaf);
            seen.add(sf.document(doc).get("id"));
          }
        }
        assertEquals("partitioning must not lose or duplicate documents", expected, seen);
        assertEquals(keys, segmentsPerKey.size());
        for (Set<Integer> in : segmentsPerKey.values()) {
          histogram.merge(in.size(), 1, Integer::sum);
        }
      }
    }
    return histogram;
  }

  /** Computes the partition offsets for a set of merge readers. */
  private interface Partitions {
    int[][] apply(List<CodecReader> readers) throws IOException;
  }

  /** Runs exactly one partitioned merge over everything that has been flushed. */
  private static class Partitioner extends MergePolicy {
    private final Partitions partitions;
    private boolean done;

    Partitioner(boolean onKeyBoundaries) {
      this(
          readers ->
              onKeyBoundaries
                  ? keyBoundaryPartitions(readers, KEY, OUTPUTS)
                  : docCountPartitions(readers, OUTPUTS));
    }

    Partitioner(Partitions partitions) {
      this.partitions = partitions;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger t, SegmentInfos infos, MergeContext ctx) {
      // Only on maybeMerge(), so every segment has been flushed. Firing on a
      // flush trigger would partition an early subset and leave the segments
      // flushed afterwards holding the whole key range.
      if (t != MergeTrigger.EXPLICIT || done || infos.size() < 2) {
        return null;
      }
      List<SegmentCommitInfo> segs = new ArrayList<>();
      for (SegmentCommitInfo si : infos) {
        if (ctx.getMergingSegments().contains(si)) {
          return null;
        }
        segs.add(si);
      }
      done = true;
      MergeSpecification spec = new MergeSpecification();
      spec.add(
          new OneMerge(segs) {
            @Override
            public boolean isPartitioned() {
              return true;
            }

            @Override
            public int[][] getDocRangePartitions(List<CodecReader> readers) throws IOException {
              return partitions.apply(readers);
            }
          });
      return spec;
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos i, int m, Map<SegmentCommitInfo, Boolean> s, MergeContext c) {
      return null;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos i, MergeContext c) {
      return null;
    }
  }
}
