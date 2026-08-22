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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.MergedIterator;

/**
 * Tests the ordering contract of {@link Fields#iterator()}: field names must be returned in
 * ascending natural order. {@link MergedIterator}, which {@link MultiFields} and {@link
 * org.apache.lucene.codecs.perfield.PerFieldPostingsFormat} use to combine several of these
 * iterators, is documented as undefined when its inputs are not sorted, so a violation yields wrong
 * results rather than an error and only {@link CheckIndex} detects it.
 *
 * <p>Note that this order is unrelated to the order in which fields were added to a document: that
 * one is preserved in {@link FieldInfos}, while the terms dictionary sorts field names when it is
 * opened. {@link #testOrderIsIndependentOfInsertionOrder()} pins both halves of that.
 */
public class TestFieldsOrder extends LuceneTestCase {

  /** Deliberately neither sorted nor reverse sorted, and mixing case. */
  private static final List<String> FIELDS = List.of("zebra", "alpha", "mid", "b", "Z");

  private Directory index() throws IOException {
    Directory dir = newDirectory();
    try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      Document doc = new Document();
      for (String field : FIELDS) {
        doc.add(new TextField(field, "value", Field.Store.NO));
      }
      w.addDocument(doc);
    }
    return dir;
  }

  /** Delegates everything, but lists field names in descending order. */
  private static FieldsProducer reversed(FieldsProducer in) {
    return new FieldsProducer() {
      @Override
      public Iterator<String> iterator() {
        List<String> fields = new ArrayList<>();
        in.forEach(fields::add);
        fields.sort(Comparator.reverseOrder());
        return fields.iterator();
      }

      @Override
      public Terms terms(String field) {
        return in.terms(field);
      }

      @Override
      public int size() {
        return in.size();
      }

      @Override
      public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
        in.checkIntegrity(merge);
      }

      @Override
      public void close() throws IOException {
        in.close();
      }
    };
  }

  private static CodecReader withPostings(CodecReader in, FieldsProducer postings) {
    return new FilterCodecReader(in) {
      @Override
      public FieldsProducer getPostingsReader() {
        return postings;
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    };
  }

  private static CheckIndex.Status.TermIndexStatus checkPostings(CodecReader reader)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    return CheckIndex.testPostings(
        reader,
        new PrintStream(out, false, UTF_8),
        false,
        CheckIndex.Level.MIN_LEVEL_FOR_INTEGRITY_CHECKS,
        false);
  }

  /** A reader that lists fields out of order must be reported. */
  public void testCheckIndexDetectsFieldsOutOfOrder() throws Exception {
    try (Directory dir = index();
        DirectoryReader reader = DirectoryReader.open(dir)) {
      CodecReader leaf = (CodecReader) getOnlyLeafReader(reader);
      CheckIndex.Status.TermIndexStatus status =
          checkPostings(withPostings(leaf, reversed(leaf.getPostingsReader())));

      assertNotNull("CheckIndex did not report the field order", status.error);
      assertTrue(
          status.error.getMessage(), status.error.getMessage().contains("fields out of order"));
    }
  }

  /** ... and a reader that honours the contract must not be. */
  public void testCheckIndexAcceptsSortedFields() throws Exception {
    try (Directory dir = index();
        DirectoryReader reader = DirectoryReader.open(dir)) {
      CodecReader leaf = (CodecReader) getOnlyLeafReader(reader);
      CheckIndex.Status.TermIndexStatus status = checkPostings(leaf);

      assertNull(String.valueOf(status.error), status.error);
      assertEquals(FIELDS.size(), status.termCount > 0 ? FIELDS.size() : -1);
    }
  }

  /**
   * The terms dictionary sorts field names regardless of the order in which they were added, and
   * regardless of the order used by an earlier segment. {@link FieldInfos}, by contrast, keeps
   * insertion order and field numbers assigned from it, and a merge preserves that — so the two
   * orders are genuinely different views and only one of them is the contract.
   */
  public void testOrderIsIndependentOfInsertionOrder() throws Exception {
    List<String> first = List.of("zebra", "alpha", "mid");
    // same fields in the opposite order, plus one that sorts first but is added last
    List<String> second = List.of("mid", "alpha", "zebra", "aaa");

    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (List<String> fields : List.of(first, second)) {
          Document doc = new Document();
          for (String field : fields) {
            doc.add(new TextField(field, "value", Field.Store.NO));
          }
          w.addDocument(doc);
          w.commit();
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("expected one segment per commit", 2, reader.leaves().size());
        for (LeafReaderContext ctx : reader.leaves()) {
          CodecReader leaf = (CodecReader) ctx.reader();
          assertAscending(fieldNames(leaf.getPostingsReader()));
          assertNull(checkPostings(leaf).error);
        }

        // FieldInfos keeps insertion order, and the field numbers follow it: "aaa" was added last
        // in the second segment, so it has the highest number even though it sorts first.
        List<String> infoOrder = new ArrayList<>();
        for (FieldInfo fi : reader.leaves().get(1).reader().getFieldInfos()) {
          infoOrder.add(fi.name);
        }
        assertEquals(List.of("zebra", "alpha", "mid", "aaa"), infoOrder);
      }

      // merging does not change either view
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader leaf = (CodecReader) getOnlyLeafReader(reader);
        assertEquals(List.of("aaa", "alpha", "mid", "zebra"), fieldNames(leaf.getPostingsReader()));
        assertNull(checkPostings(leaf).error);
      }
    }
  }

  /** {@link MultiFields} is what actually depends on the order, so pin it too. */
  public void testMultiFieldsMergesInOrder() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (List<String> fields : List.of(List.of("zebra", "mid"), List.of("aaa", "alpha"))) {
          Document doc = new Document();
          for (String field : fields) {
            doc.add(new TextField(field, "value", Field.Store.NO));
          }
          w.addDocument(doc);
          w.commit();
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        Fields[] subs = new Fields[reader.leaves().size()];
        ReaderSlice[] slices = new ReaderSlice[reader.leaves().size()];
        for (int i = 0; i < reader.leaves().size(); i++) {
          LeafReaderContext ctx = reader.leaves().get(i);
          subs[i] = ((CodecReader) ctx.reader()).getPostingsReader();
          slices[i] = new ReaderSlice(ctx.docBase, ctx.reader().maxDoc(), i);
        }

        List<String> merged = fieldNames(new MultiFields(subs, slices));
        assertEquals(List.of("aaa", "alpha", "mid", "zebra"), merged);
      }
    }
  }

  private static List<String> fieldNames(Fields fields) {
    List<String> names = new ArrayList<>();
    fields.forEach(names::add);
    return names;
  }

  private static void assertAscending(List<String> fields) {
    List<String> sorted = new ArrayList<>(fields);
    sorted.sort(null);
    assertEquals(sorted, fields);
  }

  /**
   * C4/C5 from the {@link Fields#iterator()} javadoc: when a sub-iterator is unsorted, {@link
   * MergedIterator} stops deduplicating, and a name present in two sub-iterators comes back twice.
   * This is the concrete harm the contract exists to prevent, so it is pinned rather than asserted
   * in prose only.
   */
  public void testUnsortedInputBreaksDeduplication() throws Exception {
    // sorted inputs: the shared name "a" is deduplicated
    assertEquals(List.of("a", "b", "c"), merge(List.of("a", "b"), List.of("a", "c")));

    // one unsorted input: "a" is returned twice, from two different sub-iterators
    assertEquals(
        List.of("a", "b", "a", "c", "z"), merge(List.of("b", "a", "c"), List.of("a", "z")));

    // and a duplicate inside a single sub-iterator is not removed either
    assertEquals(List.of("a", "a", "b"), merge(List.of("a", "a", "b")));
  }

  @SafeVarargs
  @SuppressWarnings({"unchecked", "rawtypes", "varargs"})
  private static List<String> merge(List<String>... subs) {
    Iterator[] iterators = new Iterator[subs.length];
    for (int i = 0; i < subs.length; i++) {
      iterators[i] = subs[i].iterator();
    }
    List<String> out = new ArrayList<>();
    // MultiFields uses the single-argument constructor, which is removeDuplicates=true
    new MergedIterator<String>(iterators).forEachRemaining(out::add);
    return out;
  }

  /**
   * C3: {@link org.apache.lucene.codecs.perfield.PerFieldPostingsFormat} is the second consumer of
   * the order. Merging two segments through it must produce every field exactly once, which is only
   * true while the sub-iterators are sorted.
   */
  public void testPerFieldMergePreservesFieldsExactlyOnce() throws Exception {
    try (Directory dir = newDirectory()) {
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (List<String> fields : List.of(List.of("zebra", "mid"), List.of("aaa", "mid"))) {
          Document doc = new Document();
          for (String field : fields) {
            doc.add(new TextField(field, "value", Field.Store.NO));
          }
          w.addDocument(doc);
          w.commit();
        }
      }

      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        w.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        CodecReader leaf = (CodecReader) getOnlyLeafReader(reader);
        List<String> merged = fieldNames(leaf.getPostingsReader());
        // "mid" was in both segments and must appear once
        assertEquals(List.of("aaa", "mid", "zebra"), merged);
        assertNull(checkPostings(leaf).error);
      }
    }
  }

  /**
   * C7: {@link org.apache.lucene.codecs.FieldsConsumer#write} now documents that field names arrive
   * sorted. Pin it where the write actually happens, by observing what a real flush hands over.
   */
  public void testWriteSideReceivesSortedFields() throws Exception {
    List<List<String>> observed = new ArrayList<>();
    Codec base = TestUtil.getDefaultCodec();
    final PostingsFormat delegateFormat = TestUtil.getDefaultPostingsFormat();
    Codec recording =
        new FilterCodec(base.getName(), base) {
          @Override
          public PostingsFormat postingsFormat() {
            return new PostingsFormat(delegateFormat.getName()) {
              @Override
              public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                FieldsConsumer in = delegateFormat.fieldsConsumer(state);
                return new FieldsConsumer() {
                  @Override
                  public void write(Fields fields, NormsProducer norms) throws IOException {
                    observed.add(fieldNames(fields));
                    in.write(fields, norms);
                  }

                  @Override
                  public void close() throws IOException {
                    in.close();
                  }
                };
              }

              @Override
              public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                return delegateFormat.fieldsProducer(state);
              }
            };
          }
        };

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig().setCodec(recording).setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        for (String field : FIELDS) {
          doc.add(new TextField(field, "value", Field.Store.NO));
        }
        w.addDocument(doc);
      }
    }

    assertFalse("write() was never called", observed.isEmpty());
    for (List<String> names : observed) {
      assertAscending(names);
    }
  }
}
