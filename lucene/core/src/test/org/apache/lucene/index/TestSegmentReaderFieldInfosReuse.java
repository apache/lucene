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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

public class TestSegmentReaderFieldInfosReuse extends LuceneTestCase {

  private static final int DOCS = 10;
  private static final int[] MAX_DOC_VALUES_OVERLAYS = {
    0, IndexWriterConfig.DEFAULT_MAX_DOC_VALUES_OVERLAYS
  };

  public void testUpdatedGenerationSharesUnchangedCoreFieldInfo() throws IOException {
    for (int maxOverlays : MAX_DOC_VALUES_OVERLAYS) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, newConfig(maxOverlays))) {
        indexSegments(writer);
        writer.updateNumericDocValue(new Term("id", "3"), "ndv", 42L);
        writer.updateNumericDocValue(new Term("id", "4"), "fresh", 7L);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          SegmentReader updated = updatedSegment(reader);
          FieldInfos core = updated.core.coreFieldInfos;
          FieldInfos current = updated.getFieldInfos();
          assertNotSame(core, current);
          for (String field : new String[] {"id", "body", "sdv", "late", "soft_deletes"}) {
            if (core.fieldInfo(field) != null) {
              assertSame(field, core.fieldInfo(field), current.fieldInfo(field));
            }
          }
          assertNotSame(core.fieldInfo("ndv"), current.fieldInfo("ndv"));
          assertEquals(-1L, core.fieldInfo("ndv").getDocValuesGen());
          assertNotEquals(-1L, current.fieldInfo("ndv").getDocValuesGen());
          assertNull(core.fieldInfo("fresh"));
          assertNotNull(current.fieldInfo("fresh"));
          assertValues(reader, Map.of(3, 42L), Map.of(), Map.of(4, 7L));
        }
      }
    }
  }

  public void testSoftDeleteSharesAllOtherCoreFieldInfos() throws IOException {
    for (int maxOverlays : MAX_DOC_VALUES_OVERLAYS) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, newConfig(maxOverlays))) {
        indexSegments(writer);
        Document replacement = new Document();
        replacement.add(new StringField("id", "3", Field.Store.NO));
        writer.softUpdateDocument(
            new Term("id", "3"), replacement, new NumericDocValuesField("soft_deletes", 1));
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          SegmentReader updated = updatedSegment(reader);
          FieldInfos core = updated.core.coreFieldInfos;
          FieldInfos current = updated.getFieldInfos();
          assertNotSame(core, current);
          for (FieldInfo coreFi : core) {
            assertSame(coreFi.name, coreFi, current.fieldInfo(coreFi.name));
          }
          assertNull(core.fieldInfo("soft_deletes"));
          assertNotNull(current.fieldInfo("soft_deletes"));
          assertEquals(DOCS, updated.maxDoc());
          assertEquals(1, updated.getSegmentInfo().getSoftDelCount());
        }
      }
    }
  }

  public void testReopenAcrossAnotherUpdateKeepsOldReaderValues() throws IOException {
    for (int maxOverlays : MAX_DOC_VALUES_OVERLAYS) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, newConfig(maxOverlays))) {
        indexSegments(writer);
        writer.updateNumericDocValue(new Term("id", "3"), "ndv", 42L);
        writer.commit();

        try (DirectoryReader first = DirectoryReader.open(dir)) {
          writer.updateNumericDocValue(new Term("id", "5"), "ndv", 55L);
          writer.updateNumericDocValue(new Term("id", "6"), "fresh", 66L);
          writer.updateNumericDocValue(new Term("id", "7"), "late", 700L);
          writer.commit();
          try (DirectoryReader second = DirectoryReader.openIfChanged(first)) {
            assertNotNull(second);
            SegmentReader oldSegment = updatedSegment(first);
            SegmentReader newSegment = updatedSegment(second);
            assertSame(oldSegment.core, newSegment.core);
            FieldInfos core = newSegment.core.coreFieldInfos;
            for (String field : new String[] {"id", "body", "sdv"}) {
              assertSame(field, core.fieldInfo(field), oldSegment.getFieldInfos().fieldInfo(field));
              assertSame(field, core.fieldInfo(field), newSegment.getFieldInfos().fieldInfo(field));
            }
            assertNotSame(
                oldSegment.getFieldInfos().fieldInfo("ndv"),
                newSegment.getFieldInfos().fieldInfo("ndv"));
            assertSame(core.fieldInfo("late"), oldSegment.getFieldInfos().fieldInfo("late"));
            assertNotSame(core.fieldInfo("late"), newSegment.getFieldInfos().fieldInfo("late"));
            assertEquals(-1L, core.fieldInfo("late").getDocValuesGen());
            assertEquals(-1L, oldSegment.getFieldInfos().fieldInfo("late").getDocValuesGen());
            assertNotEquals(-1L, newSegment.getFieldInfos().fieldInfo("late").getDocValuesGen());
            assertNull(oldSegment.getFieldInfos().fieldInfo("fresh"));
            assertValues(first, Map.of(3, 42L), Map.of(), Map.of());
            assertValues(second, Map.of(3, 42L, 5, 55L), Map.of(7, 700L), Map.of(6, 66L));
          }
        }
      }
    }
  }

  public void testMergeAndAddIndexesLeaveSharedCoreFieldInfoUntouched() throws IOException {
    for (int maxOverlays : MAX_DOC_VALUES_OVERLAYS) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, newConfig(maxOverlays))) {
        indexSegments(writer);
        writer.updateNumericDocValue(new Term("id", "3"), "ndv", 42L);
        writer.updateNumericDocValue(new Term("id", "4"), "fresh", 7L);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          SegmentReader updated = updatedSegment(reader);
          FieldInfos core = updated.core.coreFieldInfos;
          Map<String, Map<String, String>> attributesBefore = attributesByField(core);
          Map<String, Long> dvGensBefore = dvGensByField(core);

          try (Directory target = newDirectory();
              IndexWriter targetWriter = new IndexWriter(target, newConfig(maxOverlays))) {
            CodecReader[] leaves =
                reader.leaves().stream()
                    .map(ctx -> (CodecReader) ctx.reader())
                    .toArray(CodecReader[]::new);
            targetWriter.addIndexes(leaves);
            targetWriter.updateNumericDocValue(new Term("id", "8"), "ndv", 88L);
            targetWriter.commit();
            try (DirectoryReader copy = DirectoryReader.open(target)) {
              assertValues(copy, Map.of(3, 42L, 8, 88L), Map.of(), Map.of(4, 7L));
            }
          }

          writer.getConfig().setMergePolicy(newLogMergePolicy());
          writer.forceMerge(1);
          writer.commit();
          try (DirectoryReader merged = DirectoryReader.open(dir)) {
            assertEquals(1, merged.leaves().size());
            assertValues(merged, Map.of(3, 42L), Map.of(), Map.of(4, 7L));
          }

          assertEquals(attributesBefore, attributesByField(core));
          assertEquals(dvGensBefore, dvGensByField(core));
          assertValues(reader, Map.of(3, 42L), Map.of(), Map.of(4, 7L));
        }
      }
    }
  }

  public void testHasSameState() {
    FieldInfo base = new FieldInfoSpec().build();
    assertTrue(base.hasSameState(new FieldInfoSpec().build()));
    FieldInfoSpec equalAttributes = new FieldInfoSpec();
    equalAttributes.attributes = new HashMap<>(equalAttributes.attributes);
    assertTrue(base.hasSameState(equalAttributes.build()));

    List<Consumer<FieldInfoSpec>> changes =
        List.of(
            spec -> spec.name = "g",
            spec -> spec.number = 2,
            spec -> spec.storeTermVector = true,
            spec -> spec.omitNorms = true,
            spec -> spec.storePayloads = true,
            spec -> spec.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
            spec -> spec.docValuesType = DocValuesType.SORTED_NUMERIC,
            spec -> spec.docValuesSkipIndex = DocValuesSkipIndexType.RANGE,
            spec -> spec.dvGen = 3L,
            spec -> spec.attributes = Map.of("k", "w"),
            spec -> spec.pointDimensionCount = 3,
            spec -> spec.pointIndexDimensionCount = 2,
            spec -> spec.pointNumBytes = 8,
            spec -> spec.vectorDimension = 8,
            spec -> spec.vectorEncoding = VectorEncoding.BYTE,
            spec -> spec.vectorSimilarityFunction = VectorSimilarityFunction.DOT_PRODUCT,
            spec -> spec.softDeletesField = true,
            spec -> spec.isParentField = true);
    for (int i = 0; i < changes.size(); i++) {
      FieldInfoSpec changed = new FieldInfoSpec();
      changes.get(i).accept(changed);
      assertFalse("change " + i, base.hasSameState(changed.build()));
    }
  }

  private IndexWriterConfig newConfig(int maxDocValuesOverlays) {
    return newIndexWriterConfig(new MockAnalyzer(random()))
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setMaxBufferedDocs(DOCS + 1)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setSoftDeletesField("soft_deletes")
        .setMaxDocValuesOverlays(maxDocValuesOverlays);
  }

  private static void indexSegments(IndexWriter writer) throws IOException {
    for (int i = 0; i < DOCS; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      doc.add(new TextField("body", "common term" + i, Field.Store.NO));
      doc.add(new NumericDocValuesField("ndv", i));
      doc.add(new NumericDocValuesField("late", 10L * i));
      doc.add(new SortedDocValuesField("sdv", new BytesRef("s" + i)));
      writer.addDocument(doc);
    }
    writer.commit();
    Document other = new Document();
    other.add(new StringField("id", "other", Field.Store.NO));
    other.add(new NumericDocValuesField("fresh", -1L));
    writer.addDocument(other);
    writer.commit();
  }

  private static SegmentReader updatedSegment(DirectoryReader reader) {
    for (LeafReaderContext ctx : reader.leaves()) {
      SegmentReader segmentReader = (SegmentReader) ctx.reader();
      if (segmentReader.getSegmentInfo().hasFieldUpdates()
          && segmentReader.core.coreFieldInfos.fieldInfo("body") != null) {
        return segmentReader;
      }
    }
    throw new AssertionError("no segment with field updates");
  }

  private static void assertValues(
      DirectoryReader reader,
      Map<Integer, Long> ndvOverrides,
      Map<Integer, Long> lateOverrides,
      Map<Integer, Long> freshValues)
      throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < DOCS; i++) {
      String id = Integer.toString(i);
      assertEquals(1, searcher.count(new TermQuery(new Term("id", id))));
      assertEquals(1, searcher.count(new TermQuery(new Term("body", "term" + i))));
      boolean found = false;
      for (LeafReaderContext ctx : reader.leaves()) {
        LeafReader leaf = ctx.reader();
        PostingsEnum postings = leaf.postings(new Term("id", id));
        if (postings == null) {
          continue;
        }
        int doc = postings.nextDoc();
        if (doc == PostingsEnum.NO_MORE_DOCS) {
          continue;
        }
        found = true;
        NumericDocValues ndv = leaf.getNumericDocValues("ndv");
        assertTrue(ndv.advanceExact(doc));
        assertEquals(id, ndvOverrides.getOrDefault(i, (long) i).longValue(), ndv.longValue());
        NumericDocValues late = leaf.getNumericDocValues("late");
        assertTrue(late.advanceExact(doc));
        assertEquals(id, lateOverrides.getOrDefault(i, 10L * i).longValue(), late.longValue());
        SortedDocValues sdv = leaf.getSortedDocValues("sdv");
        assertTrue(sdv.advanceExact(doc));
        assertEquals(new BytesRef("s" + i), sdv.lookupOrd(sdv.ordValue()));
        NumericDocValues fresh = leaf.getNumericDocValues("fresh");
        if (freshValues.containsKey(i)) {
          assertNotNull(id, fresh);
          assertTrue(id, fresh.advanceExact(doc));
          assertEquals(id, freshValues.get(i).longValue(), fresh.longValue());
        } else if (fresh != null) {
          assertFalse(id, fresh.advanceExact(doc));
        }
      }
      assertTrue(id, found);
    }
  }

  private static Map<String, Map<String, String>> attributesByField(FieldInfos infos) {
    Map<String, Map<String, String>> attributes = new HashMap<>();
    for (FieldInfo fi : infos) {
      attributes.put(fi.name, new HashMap<>(fi.attributes()));
    }
    return attributes;
  }

  private static Map<String, Long> dvGensByField(FieldInfos infos) {
    Map<String, Long> dvGens = new HashMap<>();
    for (FieldInfo fi : infos) {
      dvGens.put(fi.name, fi.getDocValuesGen());
    }
    return dvGens;
  }

  private static class FieldInfoSpec {
    String name = "f";
    int number = 1;
    boolean storeTermVector;
    boolean omitNorms;
    boolean storePayloads;
    IndexOptions indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    DocValuesType docValuesType = DocValuesType.NUMERIC;
    DocValuesSkipIndexType docValuesSkipIndex = DocValuesSkipIndexType.NONE;
    long dvGen = -1L;
    Map<String, String> attributes = Map.of("k", "v");
    int pointDimensionCount = 2;
    int pointIndexDimensionCount = 1;
    int pointNumBytes = 4;
    int vectorDimension = 4;
    VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
    VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    boolean softDeletesField;
    boolean isParentField;

    FieldInfo build() {
      return new FieldInfo(
          name,
          number,
          storeTermVector,
          omitNorms,
          storePayloads,
          indexOptions,
          docValuesType,
          docValuesSkipIndex,
          dvGen,
          attributes,
          pointDimensionCount,
          pointIndexDimensionCount,
          pointNumBytes,
          vectorDimension,
          vectorEncoding,
          vectorSimilarityFunction,
          softDeletesField,
          isParentField);
    }
  }
}
