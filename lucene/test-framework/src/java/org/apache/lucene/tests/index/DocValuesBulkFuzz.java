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
package org.apache.lucene.tests.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.tests.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.tests.util.LuceneTestCase.assertEquals;
import static org.apache.lucene.tests.util.LuceneTestCase.assertTrue;
import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.newIndexWriterConfig;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.RandomLongValues.Distribution;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * Writes random doc values with the codec under test, and checks the bulk methods of the doc values
 * that are read back against an in-memory model of the values: {@code intoBitSet}, {@code
 * intoArray} and {@code docIDRunEnd} of every kind, and the methods that are specific to a kind,
 * such as {@code longValues} and {@code rangeIntoBitSet}. The iteration of the doc values and the
 * skip index are checked by other tests.
 *
 * <p>The values of each kind of doc values are generated in up to three consecutive regions of
 * documents, so that the encoding of each part of a segment may differ. The index is written in
 * several segments, optionally with updates and deletes, checked, merged and checked again. The
 * subclasses add the checks that are specific to the kind of doc values.
 */
abstract class DocValuesBulkFuzz {

  /** The kinds of doc values that are fuzzed. */
  enum Kind {
    NUMERIC,
    SORTED_NUMERIC,
    BINARY,
    SORTED,
    SORTED_SET
  }

  /** Wraps the readers that are used for checks, for example to read merge instances. */
  interface ReaderWrapper {
    DirectoryReader wrap(DirectoryReader reader) throws IOException;
  }

  /** A range of docs, and the offset that is subtracted from the docs to get bits of a bit set. */
  record DocRange(int fromDoc, int toDoc, int offset) {
    FixedBitSet newBitSet() {
      return new FixedBitSet(toDoc - offset);
    }
  }

  /** Some docs in ascending order, and where to read them and write their values in arrays. */
  record Batch(int size, int[] docs, int docsOffset, int valuesOffset) {}

  static final String FIELD = "dv";
  static final String ID_FIELD = "id";

  /**
   * Fuzzes every shape of data for a kind of doc values, on small indexes unless this is a nightly
   * run, in which case the indexes are bigger and every shape is repeated.
   */
  static void runAll(Kind kind, ReaderWrapper readerWrapper) throws IOException {
    int iterations = TEST_NIGHTLY ? 5 : 1;
    for (int i = 0; i < iterations; i++) {
      switch (kind) {
        case NUMERIC, SORTED_NUMERIC -> {
          for (Distribution distribution : Distribution.values()) {
            new NumericDocValuesBulkFuzz(kind, distribution, readerWrapper).run();
          }
        }
        case BINARY, SORTED, SORTED_SET -> {
          for (BytesDocValuesBulkFuzz.Shape shape : BytesDocValuesBulkFuzz.Shape.values()) {
            new BytesDocValuesBulkFuzz(kind, shape, readerWrapper).run();
          }
        }
      }
    }
  }

  final Kind kind;
  final int numDocs;
  final boolean skipIndex;
  private final ReaderWrapper readerWrapper;

  DocValuesBulkFuzz(Kind kind, ReaderWrapper readerWrapper) {
    Random random = random();
    this.kind = kind;
    this.readerWrapper = readerWrapper;
    this.numDocs =
        TEST_NIGHTLY
            ? TestUtil.nextInt(random, 20_000, 70_000)
            : TestUtil.nextInt(random, 100, 5_000);
    this.skipIndex = random.nextBoolean();
  }

  /** Whether the doc with this id has a value in the model. */
  abstract boolean hasValue(int id);

  /** Adds the doc values of the doc with this id to a document. */
  abstract void addFields(Document doc, int id);

  /** Updates some values of the index, and the model. */
  void applyUpdates(RandomIndexWriter writer) throws IOException {}

  abstract SegmentCheck newSegmentCheck(LeafReader reader) throws IOException;

  // the docs that are expected to match when there is no condition on the values
  static boolean everyDoc(int doc) {
    return true;
  }

  static Term idTerm(int id) {
    return new Term(ID_FIELD, Integer.toString(id));
  }

  final void run() throws IOException {
    Random random = random();
    try (Directory dir = newDirectory()) {
      RandomIndexWriter writer = new RandomIndexWriter(random, dir, newIndexWriterConfig());
      for (int id = 0; id < numDocs; id++) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, Integer.toString(id), Field.Store.YES));
        addFields(doc, id);
        writer.addDocument(doc);
        if (random.nextInt(numDocs / 3 + 1) == 0) {
          writer.commit();
        }
      }

      applyUpdates(writer);
      if (random.nextBoolean()) {
        int numDeletes = random.nextInt(numDocs / 5 + 1);
        for (int i = 0; i < numDeletes; i++) {
          writer.deleteDocuments(idTerm(random.nextInt(numDocs)));
        }
      }

      writer.commit();
      verify(dir);

      switch (random.nextInt(3)) {
        case 0 -> writer.forceMerge(1);
        case 1 -> writer.forceMerge(random.nextInt(2, 4));
        default -> {
          // keep the segments that are there
        }
      }
      writer.close();
      verify(dir);
    }
  }

  private void verify(Directory dir) throws IOException {
    try (DirectoryReader reader = readerWrapper.wrap(DirectoryReader.open(dir))) {
      TestUtil.checkReader(reader);
      for (LeafReaderContext context : reader.leaves()) {
        newSegmentCheck(context.reader()).run();
      }
    }
  }

  /** The checks of the doc values of one segment. */
  abstract class SegmentCheck {
    final LeafReader reader;
    final int maxDoc;
    final int[] ids;
    final Bits liveDocs;
    // the docs of the segment that have a value, which may include deleted docs
    final int[] docsWithValues;

    SegmentCheck(LeafReader reader) throws IOException {
      this.reader = reader;
      this.maxDoc = reader.maxDoc();
      this.liveDocs = reader.getLiveDocs();
      this.ids = new int[maxDoc];
      StoredFields storedFields = reader.storedFields();
      for (int doc = 0; doc < maxDoc; doc++) {
        ids[doc] = Integer.parseInt(storedFields.document(doc, Set.of(ID_FIELD)).get(ID_FIELD));
      }
      this.docsWithValues = IntStream.range(0, maxDoc).filter(doc -> hasValue(ids[doc])).toArray();
    }

    /** Returns a new iterator over the docs that have a value, or null if there are none. */
    abstract DocIdSetIterator newIterator() throws IOException;

    /** Adds the checks that are specific to the kind of doc values. */
    void checkExtras() throws IOException {}

    final void run() throws IOException {
      checkIntoBitSet();
      checkIntoArray();
      checkDocIDRunEnd();
      checkExtras();
    }

    final boolean isLive(int doc) {
      return liveDocs == null || liveDocs.get(doc);
    }

    final int firstDocWithValueFrom(int target) {
      int index = Arrays.binarySearch(docsWithValues, target);
      index = index >= 0 ? index : -index - 1;
      return index < docsWithValues.length ? docsWithValues[index] : NO_MORE_DOCS;
    }

    final int randomJump() {
      Random random = random();
      return switch (random.nextInt(4)) {
        case 0 -> 0;
        case 1 -> random.nextInt(4);
        case 2 -> random.nextInt(100);
        default -> random.nextInt(maxDoc + 1);
      };
    }

    /** Returns a range of docs that starts at or after a doc. */
    final DocRange randomDocRange(int minFromDoc) {
      Random random = random();
      int fromDoc = random.nextInt(minFromDoc, maxDoc);
      int toDoc = Math.min(maxDoc, fromDoc + 1 + randomJump());
      return new DocRange(fromDoc, toDoc, fromDoc - random.nextInt(130));
    }

    // some of the bits before the range are set, to check that they are not cleared
    final FixedBitSet newPresetBitSet(DocRange range) {
      FixedBitSet bitSet = range.newBitSet();
      for (int bit = 0; bit < range.fromDoc() - range.offset(); bit++) {
        if (random().nextInt(4) == 0) {
          bitSet.set(bit);
        }
      }
      return bitSet;
    }

    // compares the live docs of a range with the docs that are expected to match, as the values of
    // deleted docs may not be up to date
    final void assertMatches(
        String message,
        DocRange range,
        FixedBitSet preset,
        FixedBitSet actual,
        IntPredicate expectedMatch) {
      for (int bit = 0; bit < range.fromDoc() - range.offset(); bit++) {
        assertEquals(message + " bit " + bit, preset.get(bit), actual.get(bit));
      }
      for (int doc = range.fromDoc(); doc < range.toDoc(); doc++) {
        if (isLive(doc)) {
          boolean expected = hasValue(ids[doc]) && expectedMatch.test(doc);
          assertEquals(message + " doc " + doc, expected, actual.get(doc - range.offset()));
        }
      }
    }

    /** Returns some docs in ascending order. */
    final Batch randomBatch() {
      Random random = random();
      TreeSet<Integer> chosen = new TreeSet<>();
      for (int i = random.nextInt(1, Math.min(maxDoc, 64) + 1); i > 0; i--) {
        chosen.add(random.nextInt(maxDoc));
      }
      int docsOffset = random.nextInt(3);
      int[] docs = new int[docsOffset + chosen.size()];
      int index = docsOffset;
      for (int doc : chosen) {
        docs[index++] = doc;
      }
      return new Batch(chosen.size(), docs, docsOffset, random.nextInt(3));
    }

    // returns an iterator that is positioned on a doc with a value, or null if there is none
    private DocIdSetIterator positionedIterator() throws IOException {
      DocIdSetIterator iterator = newIterator();
      if (iterator == null) {
        return null;
      }
      return iterator.advance(random().nextInt(maxDoc)) == NO_MORE_DOCS ? null : iterator;
    }

    private void checkIntoBitSet() throws IOException {
      for (int round = 0; round < 5; round++) {
        DocIdSetIterator iterator = positionedIterator();
        if (iterator == null) {
          return;
        }
        int doc = iterator.docID();
        int upTo = Math.min(maxDoc, doc + 1 + randomJump());
        DocRange range = new DocRange(doc, upTo, doc - random().nextInt(130));
        FixedBitSet preset = newPresetBitSet(range);
        FixedBitSet actual = preset.clone();
        iterator.intoBitSet(upTo, actual, range.offset());
        assertMatches("intoBitSet", range, preset, actual, DocValuesBulkFuzz::everyDoc);
        assertEquals("position after intoBitSet", firstDocWithValueFrom(upTo), iterator.docID());
      }
    }

    private void checkIntoArray() throws IOException {
      for (int round = 0; round < 5; round++) {
        DocIdSetIterator iterator = positionedIterator();
        if (iterator == null) {
          return;
        }
        int upTo = Math.min(maxDoc, iterator.docID() + 1 + randomJump());
        int index = Arrays.binarySearch(docsWithValues, iterator.docID());
        assertTrue("doc " + iterator.docID() + " has a value", index >= 0);
        while (true) {
          int[] docs = new int[random().nextInt(1, 40)];
          int below = 0;
          while (index + below < docsWithValues.length && docsWithValues[index + below] < upTo) {
            below++;
          }
          int size = iterator.intoArray(upTo, docs);
          assertEquals("number of docs copied by intoArray", Math.min(docs.length, below), size);
          for (int i = 0; i < size; i++) {
            assertEquals(docsWithValues[index + i], docs[i]);
          }
          index += size;
          int expectedPosition =
              index < docsWithValues.length ? docsWithValues[index] : NO_MORE_DOCS;
          assertEquals("position after intoArray", expectedPosition, iterator.docID());
          if (size == 0) {
            break;
          }
        }
      }
    }

    private void checkDocIDRunEnd() throws IOException {
      for (int round = 0; round < 5; round++) {
        DocIdSetIterator iterator = positionedIterator();
        if (iterator == null) {
          return;
        }
        for (int i = 0; i < 5 && iterator.docID() != NO_MORE_DOCS; i++) {
          int doc = iterator.docID();
          int runEnd = iterator.docIDRunEnd();
          assertTrue("run end " + runEnd + " is after " + doc, runEnd > doc);
          assertEquals("position after docIDRunEnd", doc, iterator.docID());
          for (int inRun = doc; inRun < Math.min(runEnd, maxDoc); inRun++) {
            assertTrue("doc " + inRun + " is in the run of " + doc, hasValue(ids[inRun]));
          }
          iterator.advance(Math.max(doc + 1, runEnd) + randomJump());
        }
      }
    }
  }
}
