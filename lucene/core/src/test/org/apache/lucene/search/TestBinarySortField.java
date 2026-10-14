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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/** Tests for {@link BinarySortField} used as an index sort (and as a search sort). */
public class TestBinarySortField extends LuceneTestCase {

  @SuppressWarnings({"unlikely-arg-type", "SelfAssertion"})
  public void testEquals() {
    SortField sf = new BinarySortField("a", false);
    assertFalse(sf.equals(null));
    assertEquals(sf, sf);

    SortField sf2 = new BinarySortField("a", false);
    assertEquals(sf, sf2);
    assertEquals(sf.hashCode(), sf2.hashCode());

    assertFalse(sf.equals(new BinarySortField("a", true)));
    assertFalse(sf.equals(new BinarySortField("b", false)));
    assertFalse(sf.equals(new BinarySortField("a", false, SortField.STRING_LAST)));
    assertFalse(sf.equals("foo"));
  }

  public void testToString() {
    assertEquals("<binary: \"a\">", new BinarySortField("a", false).toString());
    assertEquals("<binary: \"a\">!", new BinarySortField("a", true).toString());
    assertTrue(
        new BinarySortField("a", false, SortField.STRING_LAST).toString().contains("missingValue"));
  }

  public void testIllegalMissingValue() {
    expectThrows(
        IllegalArgumentException.class, () -> new BinarySortField("a", false, SortField.FIELD_DOC));
  }

  /** Round-trips through the registered {@link SortFieldProvider} (verifies SPI registration). */
  public void testSerialization() throws Exception {
    assertSerializes(new BinarySortField("field", false));
    assertSerializes(new BinarySortField("field", true));
    assertSerializes(new BinarySortField("field", false, SortField.STRING_FIRST));
    assertSerializes(new BinarySortField("field", true, SortField.STRING_LAST));
  }

  private static void assertSerializes(BinarySortField sf) throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    SortFieldProvider.write(sf, out);
    ByteBuffersDataInput in = out.toDataInput();
    SortField read = SortFieldProvider.forName(BinarySortField.Provider.NAME).readSortField(in);
    assertEquals(sf, read);
  }

  public void testEmptyIndex() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", random().nextBoolean())));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        w.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(0, reader.maxDoc());
      }
    }
  }

  public void testSingleDocument() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", random().nextBoolean())));
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("dv", new BytesRef("only")));
        w.addDocument(doc);
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.maxDoc());
        BinaryDocValues dv = reader.leaves().get(0).reader().getBinaryDocValues("dv");
        assertTrue(dv.advanceExact(0));
        assertEquals(new BytesRef("only"), dv.binaryValue());
      }
    }
  }

  /** Index sorting on a field that no document has: sort must be a no-op, index stays valid. */
  public void testAllMissing() throws Exception {
    final boolean reverse = random().nextBoolean();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(
          new Sort(
              new BinarySortField(
                  "dv", reverse, random().nextBoolean() ? SortField.STRING_FIRST : null)));
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 10, 30));
      int numDocs = atLeast(50);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StoredField("ord", i));
          w.addDocument(doc);
        }
        if (random().nextBoolean()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(numDocs, reader.maxDoc());
      }
    }
  }

  /**
   * The index sort is immutable: a binary sort can only be set on a new index, and cannot be added
   * to, or changed on, an existing one.
   */
  public void testIndexSortOnlyOnNewIndex() throws Exception {
    try (Directory dir = newDirectory()) {
      // a plain (unsorted) index
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("dv", new BytesRef("a")));
        w.addDocument(doc);
        w.commit();
      }
      // a binary index sort cannot be retrofitted onto the existing unsorted index
      IndexWriterConfig sortedConfig =
          newIndexWriterConfig().setIndexSort(new Sort(new BinarySortField("dv", false)));
      expectThrows(IllegalArgumentException.class, () -> new IndexWriter(dir, sortedConfig));
    }

    try (Directory dir = newDirectory()) {
      // a new index created with the binary sort
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setIndexSort(new Sort(new BinarySortField("dv", false))))) {
        Document doc = new Document();
        doc.add(new BinaryDocValuesField("dv", new BytesRef("a")));
        w.addDocument(doc);
        w.commit();
      }
      // reopening with the same sort is fine
      try (IndexWriter w =
          new IndexWriter(
              dir,
              newIndexWriterConfig().setIndexSort(new Sort(new BinarySortField("dv", false))))) {
        assertEquals(1, w.getDocStats().maxDoc);
      }
      // reopening with a different binary sort is rejected
      IndexWriterConfig changed =
          newIndexWriterConfig().setIndexSort(new Sort(new BinarySortField("dv", true)));
      expectThrows(IllegalArgumentException.class, () -> new IndexWriter(dir, changed));
    }
  }

  public void testSingleSegment() throws Exception {
    assertIndexSorted(false, false, false);
  }

  public void testReverse() throws Exception {
    assertIndexSorted(false, true, false);
  }

  public void testMultiSegmentMerge() throws Exception {
    assertIndexSorted(true, false, false);
    assertIndexSorted(true, true, false);
  }

  public void testWithMissingValues() throws Exception {
    assertIndexSorted(false, false, true);
    assertIndexSorted(true, false, true);
    assertIndexSorted(true, true, true);
  }

  /** Mixes empty (zero-length) values with non-empty ones and missing values. */
  public void testEmptyByteValues() throws Exception {
    final boolean reverse = random().nextBoolean();
    final Object missingValue =
        random().nextBoolean() ? SortField.STRING_FIRST : SortField.STRING_LAST;
    final boolean missingLast = missingValue == SortField.STRING_LAST;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", reverse, missingValue)));
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 10, 30));
      int numDocs = atLeast(100);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          int r = random().nextInt(3);
          if (r == 0) {
            // missing
          } else if (r == 1) {
            doc.add(new BinaryDocValuesField("dv", new BytesRef(BytesRef.EMPTY_BYTES)));
          } else {
            byte[] b = new byte[TestUtil.nextInt(random(), 1, 5)];
            random().nextBytes(b);
            doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertLeavesSorted(reader, reverse, missingLast);
      }
    }
  }

  /**
   * Index sorting with document blocks (nested documents): the sort value lives on the parent
   * document, and blocks must stay intact and ordered by their parent's value.
   */
  public void testNestedBlocks() throws Exception {
    final boolean reverse = random().nextBoolean();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("sort", reverse)));
      iwc.setParentField("parent");
      iwc.setMergePolicy(newLogMergePolicy());
      final int numBlocks = atLeast(80);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int b = 0; b < numBlocks; b++) {
          List<Document> block = new ArrayList<>();
          int numChildren = random().nextInt(3);
          for (int c = 0; c < numChildren; c++) {
            Document child = new Document();
            child.add(new StoredField("block", b));
            child.add(new StoredField("role", "child"));
            block.add(child);
          }
          Document parent = new Document();
          byte[] val = new byte[TestUtil.nextInt(random(), 1, 10)];
          random().nextBytes(val);
          parent.add(new BinaryDocValuesField("sort", new BytesRef(val)));
          parent.add(new StoredField("block", b));
          parent.add(new StoredField("role", "parent"));
          block.add(parent);
          w.addDocuments(block);
          if (rarely()) {
            w.commit();
          }
        }
        if (random().nextBoolean()) {
          w.forceMerge(1);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leaf = ctx.reader();
          NumericDocValues parents = leaf.getNumericDocValues("parent");
          BinaryDocValues sort = leaf.getBinaryDocValues("sort");
          StoredFields storedFields = leaf.storedFields();
          BytesRef previous = null;
          int previousDoc = -1;
          int doc;
          while ((doc = parents.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            assertTrue("parent must have the sort value", sort.advanceExact(doc));
            BytesRef current = BytesRef.deepCopyOf(sort.binaryValue());
            assertEquals("parent", storedFields.document(doc).get("role"));
            int block = Integer.parseInt(storedFields.document(doc).get("block"));
            // every doc preceding this parent (back to the previous parent) is one of its children
            // and belongs to the same block: the block stayed intact through sort + merge.
            for (int d = previousDoc + 1; d < doc; d++) {
              assertEquals("child", storedFields.document(d).get("role"));
              assertEquals(block, Integer.parseInt(storedFields.document(d).get("block")));
            }
            if (previous != null) {
              int cmp = previous.compareTo(current);
              if (reverse) {
                cmp = -cmp;
              }
              assertTrue(
                  "parent sort values out of order: " + previous + " vs " + current, cmp <= 0);
            }
            previous = current;
            previousDoc = doc;
          }
        }
      }
    }
  }

  /** Deleting documents and then merging must keep the surviving documents sorted. */
  public void testDeletes() throws Exception {
    final boolean reverse = random().nextBoolean();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", reverse)));
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 50));
      int numDocs = atLeast(200);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          byte[] b = new byte[TestUtil.nextInt(random(), 1, 8)];
          random().nextBytes(b);
          doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
          w.addDocument(doc);
        }
        for (int i = 0; i < numDocs; i++) {
          if (random().nextInt(3) == 0) {
            w.deleteDocuments(new Term("id", Integer.toString(i)));
          }
        }
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertLeavesSorted(reader, reverse, false);
      }
    }
  }

  /** Binary sort combined with a numeric sort, as primary and as secondary. */
  public void testMultipleSortFields() throws Exception {
    for (boolean binaryFirst : new boolean[] {true, false}) {
      try (Directory dir = newDirectory()) {
        SortField binary = new BinarySortField("bin", false);
        SortField numeric = new SortField("num", SortField.Type.LONG, false);
        Sort sort = binaryFirst ? new Sort(binary, numeric) : new Sort(numeric, binary);
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(sort);
        iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 50));
        int numDocs = atLeast(200);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
          for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            // low cardinality on both so ties on the primary are common, exercising the secondary
            byte[] b = {(byte) random().nextInt(4)};
            doc.add(new BinaryDocValuesField("bin", new BytesRef(b)));
            doc.add(new NumericDocValuesField("num", random().nextInt(4)));
            w.addDocument(doc);
          }
          w.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReader leaf = getOnlyLeafReader(reader);
          BinaryDocValues bin = leaf.getBinaryDocValues("bin");
          NumericDocValues num = leaf.getNumericDocValues("num");
          BytesRef prevBin = null;
          long prevNum = 0;
          for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            assertTrue(bin.advanceExact(doc));
            assertTrue(num.advanceExact(doc));
            BytesRef curBin = BytesRef.deepCopyOf(bin.binaryValue());
            long curNum = num.longValue();
            if (doc > 0) {
              if (binaryFirst) {
                int c = prevBin.compareTo(curBin);
                assertTrue(c <= 0);
                if (c == 0) {
                  assertTrue(prevNum <= curNum);
                }
              } else {
                assertTrue(prevNum <= curNum);
                if (prevNum == curNum) {
                  assertTrue(prevBin.compareTo(curBin) <= 0);
                }
              }
            }
            prevBin = curBin;
            prevNum = curNum;
          }
        }
      }
    }
  }

  /** Two binary doc-values fields in the same segment, used together as a two-level index sort. */
  public void testMultipleBinaryFields() throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("a", false), new BinarySortField("b", false)));
      iwc.setMaxBufferedDocs(25);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE); // keep the flush-sorted segments separate
      int numDocs = 300;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          byte[] a = {(byte) random().nextInt(3)}; // low cardinality so ties on 'a' exercise 'b'
          byte[] b = new byte[TestUtil.nextInt(random(), 1, 6)];
          random().nextBytes(b);
          doc.add(new BinaryDocValuesField("a", new BytesRef(a)));
          doc.add(new BinaryDocValuesField("b", new BytesRef(b)));
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertTrue("expected multiple flush-sorted segments", reader.leaves().size() > 1);
        for (LeafReaderContext ctx : reader.leaves()) {
          BinaryDocValues da = ctx.reader().getBinaryDocValues("a");
          BinaryDocValues db = ctx.reader().getBinaryDocValues("b");
          BytesRef prevA = null;
          BytesRef prevB = null;
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            assertTrue(da.advanceExact(doc));
            assertTrue(db.advanceExact(doc));
            BytesRef a = BytesRef.deepCopyOf(da.binaryValue());
            BytesRef b = BytesRef.deepCopyOf(db.binaryValue());
            if (prevA != null) {
              int c = prevA.compareTo(a);
              assertTrue("primary binary field out of order", c <= 0);
              if (c == 0) {
                assertTrue("secondary binary field out of order", prevB.compareTo(b) <= 0);
              }
            }
            prevA = a;
            prevB = b;
          }
        }
      }
    }
  }

  /**
   * Mixes a custom {@link BinarySortField} subclass ({@link FirstValueSortField}, which decodes its
   * value) with a default binary sort field and a numeric sort field in a single index sort. Also
   * exercises two binary doc-values fields in the same segment, through flush, merge and reopen.
   */
  public void testMixCustomAndDefaultSortFields() throws Exception {
    try (Directory dir = newDirectory()) {
      Sort sort =
          new Sort(
              new FirstValueSortField("a", false), // custom binary (decoding comparator)
              new BinarySortField("b", false), // default binary
              new SortField("n", SortField.Type.LONG, false)); // numeric
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(sort);
      iwc.setMaxBufferedDocs(25);
      iwc.setMergePolicy(random().nextBoolean() ? NoMergePolicy.INSTANCE : newLogMergePolicy());
      int numDocs = atLeast(300);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          List<byte[]> aValues = new ArrayList<>();
          aValues.add(new byte[] {(byte) random().nextInt(3)}); // low-card first value -> ties
          if (random().nextBoolean()) {
            byte[] extra = new byte[TestUtil.nextInt(random(), 1, 4)];
            random().nextBytes(extra);
            aValues.add(extra);
          }
          doc.add(new BinaryDocValuesField("a", encodeValues(aValues)));
          doc.add(
              new BinaryDocValuesField("b", new BytesRef(new byte[] {(byte) random().nextInt(3)})));
          doc.add(new NumericDocValuesField("n", random().nextInt(5)));
          w.addDocument(doc);
        }
        if (random().nextBoolean()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // the persisted sort deserializes through three different providers
        for (LeafReaderContext ctx : reader.leaves()) {
          assertEquals(sort, ctx.reader().getMetaData().sort());
          BinaryDocValues da = ctx.reader().getBinaryDocValues("a");
          BinaryDocValues db = ctx.reader().getBinaryDocValues("b");
          NumericDocValues dn = ctx.reader().getNumericDocValues("n");
          BytesRef prevA = null;
          BytesRef prevB = null;
          long prevN = 0;
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            assertTrue(da.advanceExact(doc));
            assertTrue(db.advanceExact(doc));
            assertTrue(dn.advanceExact(doc));
            BytesRef a = BytesRef.deepCopyOf(da.binaryValue());
            BytesRef b = BytesRef.deepCopyOf(db.binaryValue());
            long n = dn.longValue();
            if (prevA != null) {
              int ca =
                  FirstValueSortField.firstValue(prevA)
                      .compareTo(FirstValueSortField.firstValue(a)); // decodes the first value
              assertTrue("custom primary out of order", ca <= 0);
              if (ca == 0) {
                int cb = prevB.compareTo(b);
                assertTrue("default binary secondary out of order", cb <= 0);
                if (cb == 0) {
                  assertTrue("numeric tertiary out of order", prevN <= n);
                }
              }
            }
            prevA = a;
            prevB = b;
            prevN = n;
          }
        }
      }
    }
  }

  /** addIndexes(CodecReader...) merges already-sorted segments into a sorted index. */
  public void testAddIndexes() throws Exception {
    final boolean reverse = random().nextBoolean();
    Sort sort = new Sort(new BinarySortField("dv", reverse));
    try (Directory src1 = newDirectory();
        Directory src2 = newDirectory();
        Directory dest = newDirectory()) {
      indexRandom(src1, sort, atLeast(100));
      indexRandom(src2, sort, atLeast(100));

      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(sort);
      try (IndexWriter w = new IndexWriter(dest, iwc);
          DirectoryReader r1 = DirectoryReader.open(src1);
          DirectoryReader r2 = DirectoryReader.open(src2)) {
        List<CodecReader> readers = new ArrayList<>();
        for (LeafReaderContext ctx : r1.leaves()) {
          readers.add(SlowCodecReaderWrapper.wrap(ctx.reader()));
        }
        for (LeafReaderContext ctx : r2.leaves()) {
          readers.add(SlowCodecReaderWrapper.wrap(ctx.reader()));
        }
        w.addIndexes(readers.toArray(new CodecReader[0]));
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dest)) {
        assertLeavesSorted(reader, reverse, false);
      }
    }
  }

  /** Verifies parity of the produced order with sorting the same values as SortedDocValues. */
  public void testParityWithSortedDocValues() throws Exception {
    final int numDocs = atLeast(300);
    final boolean reverse = random().nextBoolean();
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      // low cardinality so SortedDocValues is a fair comparison
      byte[] b = new byte[TestUtil.nextInt(random(), 1, 6)];
      random().nextBytes(b);
      values.add(b);
    }

    int[] binaryOrder = indexAndReadStoredOrder(values, new BinarySortField("dv", reverse), true);
    int[] sortedOrder =
        indexAndReadStoredOrder(values, new SortField("dv", SortField.Type.STRING, reverse), false);

    // Ties between equal byte[] values may be broken differently; compare the sequence of values
    // rather than the original ords.
    for (int i = 0; i < numDocs; i++) {
      assertArrayEquals(
          "mismatch at position " + i, values.get(binaryOrder[i]), values.get(sortedOrder[i]));
    }
  }

  /** Uses {@link BinarySortField} to sort search results (the {@code getComparator} path). */
  public void testSearchSort() throws Exception {
    final boolean reverse = random().nextBoolean();
    final Object missingValue =
        random().nextBoolean() ? SortField.STRING_FIRST : SortField.STRING_LAST;
    final boolean missingLast = missingValue == SortField.STRING_LAST;
    try (Directory dir = newDirectory()) {
      int numDocs = atLeast(100);
      try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          if (random().nextInt(5) != 0) {
            byte[] b = new byte[TestUtil.nextInt(random(), 0, 8)];
            random().nextBytes(b);
            doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
          }
          w.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = newSearcher(reader);
        TopFieldDocs td =
            searcher.search(
                new MatchAllDocsQuery(),
                reader.maxDoc(),
                new Sort(new BinarySortField("dv", reverse, missingValue)));
        BytesRef previous = null;
        boolean previousMissing = false;
        boolean first = true;
        for (ScoreDoc sd : td.scoreDocs) {
          BytesRef current = (BytesRef) ((FieldDoc) sd).fields[0];
          boolean missing = current == null;
          if (first == false) {
            int cmp = compareForSort(previous, previousMissing, current, missing, missingLast);
            if (reverse) {
              cmp = -cmp;
            }
            assertTrue("search results out of order", cmp <= 0);
          }
          previous = current;
          previousMissing = missing;
          first = false;
        }
      }
    }
  }

  /**
   * End-to-end test of the value-source extension point with a key that must be <em>decoded</em>
   * from the binary value (the raw bytes are not sortable as-is). {@link FirstValueSortField} sorts
   * on the first of several length-prefixed values, mirroring how a multi-valued field is encoded.
   * The sort is persisted, reopened (which deserializes it through the custom {@link
   * SortFieldProvider} and runs {@code CheckIndex}) and merged, then the stored order is verified
   * against the decoded key — proving the derived key survives serialization and is used on merge.
   */
  public void testFirstValueSortKeyIndexSort() throws Exception {
    final boolean reverse = random().nextBoolean();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new FirstValueSortField("dv", reverse)));
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 50));
      iwc.setMergePolicy(newLogMergePolicy());
      int numDocs = atLeast(200);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new BinaryDocValuesField("dv", encodeValues(randomValues())));
          w.addDocument(doc);
          if (random().nextInt(30) == 0) {
            w.commit();
          }
        }
        if (random().nextBoolean()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext ctx : reader.leaves()) {
          // the persisted sort was deserialized through FirstValueSortField.Provider
          assertEquals(
              new Sort(new FirstValueSortField("dv", reverse)), ctx.reader().getMetaData().sort());
          BinaryDocValues dv = ctx.reader().getBinaryDocValues("dv");
          BytesRef previous = null;
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            assertTrue(dv.advanceExact(doc));
            BytesRef current = BytesRef.deepCopyOf(dv.binaryValue());
            if (previous != null) {
              int cmp =
                  FirstValueSortField.firstValue(previous)
                      .compareTo(FirstValueSortField.firstValue(current));
              if (reverse) {
                cmp = -cmp;
              }
              assertTrue("custom order violated", cmp <= 0);
            }
            previous = current;
          }
        }
      }
    }
  }

  private List<byte[]> randomValues() {
    int count = TestUtil.nextInt(random(), 1, 3);
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] b = new byte[TestUtil.nextInt(random(), 1, 8)];
      random().nextBytes(b);
      values.add(b);
    }
    return values;
  }

  /** Encodes values as {@code [vInt length][bytes]} pairs, like a multi-valued binary field. */
  private static BytesRef encodeValues(List<byte[]> values) {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    try {
      for (byte[] v : values) {
        out.writeVInt(v.length);
        out.writeBytes(v, 0, v.length);
      }
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return new BytesRef(out.toArrayCopy());
  }

  /**
   * Selecting the MIN value of a multi-valued, self-describing binary field ({@link
   * MinValueSortField}). Forces many small flushed segments (no force-merge) so each is sorted at
   * flush using the value source, and also covers the merge case; the order is verified against the
   * per-document minimum value.
   */
  public void testMinValueSortKey() throws Exception {
    final boolean reverse = random().nextBoolean();
    final boolean merge = random().nextBoolean();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new MinValueSortField("dv", reverse)));
      iwc.setMaxBufferedDocs(25); // force frequent flushes
      iwc.setMergePolicy(merge ? newLogMergePolicy() : NoMergePolicy.INSTANCE);
      int numDocs = 400;
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          int count = TestUtil.nextInt(random(), 1, 4);
          List<byte[]> values = new ArrayList<>();
          for (int v = 0; v < count; v++) {
            byte[] b = new byte[TestUtil.nextInt(random(), 1, 6)];
            random().nextBytes(b);
            values.add(b);
          }
          doc.add(new BinaryDocValuesField("dv", encodeValues(values)));
          w.addDocument(doc);
        }
        if (merge && random().nextBoolean()) {
          w.forceMerge(1);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        if (merge == false) {
          assertTrue("expected multiple flush-sorted segments", reader.leaves().size() > 1);
        }
        for (LeafReaderContext ctx : reader.leaves()) {
          assertEquals(
              new Sort(new MinValueSortField("dv", reverse)), ctx.reader().getMetaData().sort());
          BinaryDocValues dv = ctx.reader().getBinaryDocValues("dv");
          BytesRef previous = null;
          for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
            assertTrue(dv.advanceExact(doc));
            BytesRef key = BytesRef.deepCopyOf(MinValueSortField.min(dv.binaryValue()));
            if (previous != null) {
              int cmp = previous.compareTo(key);
              if (reverse) {
                cmp = -cmp;
              }
              assertTrue("min-value order violated", cmp <= 0);
            }
            previous = key;
          }
        }
      }
    }
  }

  /**
   * Exercises a non-natural {@link Comparator} through both the flush ({@link
   * IndexSorter#getDocComparator}) and merge ({@link IndexSorter#getComparableValues}) comparison
   * paths.
   */
  public void testCustomComparator() throws Exception {
    // order by length first, then by unsigned bytes
    Comparator<BytesRef> cmp =
        Comparator.comparingInt((BytesRef b) -> b.length).thenComparing(Comparator.naturalOrder());
    IndexSorter.BinarySorter sorter =
        new IndexSorter.BinarySorter("test", false, cmp, r -> DocValues.getBinary(r, "dv"));

    try (Directory dir = newDirectory()) {
      int numDocs = atLeast(100);
      // keep segments separate so the merge-path check below sees more than one leaf
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          byte[] b = new byte[TestUtil.nextInt(random(), 1, 8)];
          random().nextBytes(b);
          doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
          w.addDocument(doc);
          if (i == numDocs / 2) {
            w.commit();
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // flush path: random pairs within a leaf must follow the comparator
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leaf = ctx.reader();
          IndexSorter.DocComparator dc = sorter.getDocComparator(leaf, leaf.maxDoc());
          List<BytesRef> docValues = readAll(leaf, "dv");
          for (int iter = 0; iter < 200; iter++) {
            int a = random().nextInt(leaf.maxDoc());
            int b = random().nextInt(leaf.maxDoc());
            assertEquals(
                Integer.signum(cmp.compare(docValues.get(a), docValues.get(b))),
                Integer.signum(dc.compare(a, b)));
          }
        }

        // merge path: advancing every segment in lock-step, cross-segment compares must follow the
        // comparator
        if (reader.leaves().size() >= 2) {
          List<LeafReader> leaves = new ArrayList<>();
          for (LeafReaderContext ctx : reader.leaves()) {
            leaves.add(ctx.reader());
          }
          IndexSorter.ComparableValues cv = sorter.getComparableValues(leaves);
          List<List<BytesRef>> values = new ArrayList<>();
          for (LeafReader leaf : leaves) {
            values.add(readAll(leaf, "dv"));
          }
          int n = leaves.size();
          int maxCommon = leaves.get(0).maxDoc();
          for (LeafReader leaf : leaves) {
            maxCommon = Math.min(maxCommon, leaf.maxDoc());
          }
          for (int doc = 0; doc < maxCommon; doc++) {
            for (int r = 0; r < n; r++) {
              cv.setTopValue(r, doc);
            }
            for (int a = 0; a < n; a++) {
              for (int b = 0; b < n; b++) {
                assertEquals(
                    Integer.signum(cmp.compare(values.get(a).get(doc), values.get(b).get(doc))),
                    Integer.signum(cv.compare(a, b)));
              }
            }
          }
        }
      }
    }
  }

  /** Randomized stress test over values, missing, duplicates, commits, deletes and merges. */
  public void testRandom() throws Exception {
    final boolean reverse = random().nextBoolean();
    final Object missingValue =
        random().nextBoolean()
            ? null
            : (random().nextBoolean() ? SortField.STRING_FIRST : SortField.STRING_LAST);
    final boolean missingLast = missingValue == SortField.STRING_LAST;
    final int cardinality = TestUtil.nextInt(random(), 1, 1000);
    byte[][] pool = new byte[cardinality][];
    for (int i = 0; i < cardinality; i++) {
      byte[] b = new byte[TestUtil.nextInt(random(), 0, 12)];
      random().nextBytes(b);
      pool[i] = b;
    }
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", reverse, missingValue)));
      if (random().nextBoolean()) {
        iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 80));
      }
      iwc.setMergePolicy(newLogMergePolicy());
      int numDocs = atLeast(500);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          if (random().nextInt(8) != 0) {
            doc.add(
                new BinaryDocValuesField("dv", new BytesRef(pool[random().nextInt(cardinality)])));
          }
          w.addDocument(doc);
          if (random().nextInt(100) == 0) {
            w.commit();
          }
          if (random().nextInt(100) == 0) {
            w.deleteDocuments(new Term("id", Integer.toString(random().nextInt(i + 1))));
          }
        }
        if (random().nextBoolean()) {
          w.forceMerge(TestUtil.nextInt(random(), 1, 3));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertLeavesSorted(reader, reverse, missingLast);
      }
    }
  }

  // ---- helpers ----

  private void indexRandom(Directory dir, Sort sort, int numDocs) throws IOException {
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(sort);
    iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 50));
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        byte[] b = new byte[TestUtil.nextInt(random(), 1, 8)];
        random().nextBytes(b);
        doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
        w.addDocument(doc);
      }
    }
  }

  private static List<BytesRef> readAll(LeafReader leaf, String field) throws IOException {
    List<BytesRef> values = new ArrayList<>();
    BinaryDocValues dv = leaf.getBinaryDocValues(field);
    for (int doc = 0; doc < leaf.maxDoc(); doc++) {
      assertTrue("test assumes a value on every document", dv.advanceExact(doc));
      values.add(BytesRef.deepCopyOf(dv.binaryValue()));
    }
    return values;
  }

  /** Asserts every leaf is sorted by "dv" according to the given reverse / missing policy. */
  private static void assertLeavesSorted(
      DirectoryReader reader, boolean reverse, boolean missingLast) throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      BinaryDocValues dv = ctx.reader().getBinaryDocValues("dv");
      BytesRef previous = null;
      boolean previousMissing = false;
      boolean first = true;
      for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
        boolean hasValue = dv != null && dv.advanceExact(doc);
        BytesRef current = hasValue ? BytesRef.deepCopyOf(dv.binaryValue()) : null;
        if (first == false) {
          int cmp = compareForSort(previous, previousMissing, current, !hasValue, missingLast);
          if (reverse) {
            cmp = -cmp;
          }
          assertTrue(
              "values out of order at doc " + doc + ": prev=" + previous + " cur=" + current,
              cmp <= 0);
        }
        previous = current;
        previousMissing = !hasValue;
        first = false;
      }
    }
  }

  /** Indexes random byte[] values, sorts the index, and verifies the order within each leaf. */
  private void assertIndexSorted(boolean multiSegment, boolean reverse, boolean missing)
      throws Exception {
    final int numDocs = atLeast(200);
    Object missingValue =
        missing ? (random().nextBoolean() ? SortField.STRING_FIRST : SortField.STRING_LAST) : null;
    final boolean missingLast = missingValue == SortField.STRING_LAST;

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(new BinarySortField("dv", reverse, missingValue)));
      if (multiSegment) {
        iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 50));
        iwc.setMergePolicy(newLogMergePolicy());
      }
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          if (missing == false || random().nextInt(5) != 0) {
            byte[] b = new byte[TestUtil.nextInt(random(), 0, 12)];
            random().nextBytes(b);
            doc.add(new BinaryDocValuesField("dv", new BytesRef(b)));
          }
          doc.add(new StoredField("ord", i));
          w.addDocument(doc);
          if (multiSegment && random().nextInt(50) == 0) {
            w.commit();
          }
        }
        if (multiSegment && random().nextBoolean()) {
          w.forceMerge(1);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        // Each segment is independently sorted; segments are not globally ordered relative to one
        // another unless merged, so we verify the order within each leaf.
        assertLeavesSorted(reader, reverse, missingLast);
        for (LeafReaderContext ctx : reader.leaves()) {
          assertNotNull("index sort not persisted", ctx.reader().getMetaData().sort());
        }
      }
    }
  }

  /**
   * Compares two (possibly missing) values the way the index sort would, before applying reverse.
   */
  private static int compareForSort(
      BytesRef a, boolean aMissing, BytesRef b, boolean bMissing, boolean missingLast) {
    if (aMissing || bMissing) {
      if (aMissing == bMissing) {
        return 0;
      }
      int c = aMissing ? -1 : 1;
      return missingLast ? -c : c;
    }
    return a.compareTo(b);
  }

  private int[] indexAndReadStoredOrder(List<byte[]> values, SortField sortField, boolean binary)
      throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setIndexSort(new Sort(sortField));
      iwc.setMaxBufferedDocs(TestUtil.nextInt(random(), 20, 60));
      iwc.setMergePolicy(newLogMergePolicy());
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < values.size(); i++) {
          Document doc = new Document();
          BytesRef v = new BytesRef(values.get(i));
          if (binary) {
            doc.add(new BinaryDocValuesField("dv", v));
          } else {
            doc.add(new SortedDocValuesField("dv", v));
          }
          doc.add(new StoredField("ord", i));
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }
      List<Integer> order = new ArrayList<>();
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        StoredFields storedFields = leaf.storedFields();
        for (int doc = 0; doc < leaf.maxDoc(); doc++) {
          order.add(Integer.parseInt(storedFields.document(doc).get("ord")));
        }
      }
      return order.stream().mapToInt(Integer::intValue).toArray();
    }
  }

  /**
   * Example value-source extension: the binary value holds one or more {@code [vInt length][bytes]}
   * values (so the raw bytes are not sortable as-is); the sort key is the <em>first</em> value,
   * extracted once per document in {@link #getSortKeyDocValues}. The {@link Provider} reconstructs
   * the field when the index sort is read back, so the same key is produced on merge. It is
   * registered via {@code META-INF/services/org.apache.lucene.index.SortFieldProvider} under the
   * test resources.
   */
  public static final class FirstValueSortField extends BinarySortField {
    static final String NAME = "FirstValueSortField";

    public FirstValueSortField(String field, boolean reverse) {
      super(field, reverse, null, NAME);
    }

    @Override
    protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
      BinaryDocValues binary = DocValues.getBinary(reader, getField());
      return new BinaryDocValues() {
        @Override
        public BytesRef binaryValue() throws IOException {
          return firstValue(binary.binaryValue());
        }

        @Override
        public int docID() {
          return binary.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return binary.nextDoc();
        }

        @Override
        public int advance(int t) throws IOException {
          return binary.advance(t);
        }

        @Override
        public boolean advanceExact(int t) throws IOException {
          return binary.advanceExact(t);
        }

        @Override
        public long cost() {
          return binary.cost();
        }
      };
    }

    static BytesRef firstValue(BytesRef raw) {
      ByteArrayDataInput in = new ByteArrayDataInput(raw.bytes, raw.offset, raw.length);
      int length = in.readVInt();
      return new BytesRef(raw.bytes, in.getPosition(), length);
    }

    /** Serializes/deserializes a {@link FirstValueSortField}. */
    public static final class Provider extends SortFieldProvider {
      public Provider() {
        super(NAME);
      }

      @Override
      public SortField readSortField(DataInput in) throws IOException {
        return new FirstValueSortField(in.readString(), in.readInt() == 1);
      }

      @Override
      public void writeSortField(SortField sf, DataOutput out) throws IOException {
        out.writeString(sf.getField());
        out.writeInt(sf.getReverse() ? 1 : 0);
      }
    }
  }

  /**
   * Example value source that selects the <em>minimum</em> value of a multi-valued binary field.
   * The value is self-describing — a sequence of {@code [vInt length][bytes]} pairs — so the values
   * are scanned to the end of the {@link BytesRef} and the smallest is returned as the sort key; no
   * companion field is needed. The {@link Provider} reconstructs the field when the index sort is
   * read back, so the same key is produced on merge.
   */
  public static final class MinValueSortField extends BinarySortField {
    static final String NAME = "MinValueSortField";

    public MinValueSortField(String field, boolean reverse) {
      super(field, reverse, null, NAME);
    }

    @Override
    protected BinaryDocValues getSortKeyDocValues(LeafReader reader) throws IOException {
      BinaryDocValues binary = DocValues.getBinary(reader, getField());
      return new BinaryDocValues() {
        @Override
        public BytesRef binaryValue() throws IOException {
          return min(binary.binaryValue());
        }

        @Override
        public int docID() {
          return binary.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return binary.nextDoc();
        }

        @Override
        public int advance(int t) throws IOException {
          return binary.advance(t);
        }

        @Override
        public boolean advanceExact(int t) throws IOException {
          return binary.advanceExact(t);
        }

        @Override
        public long cost() {
          return binary.cost();
        }
      };
    }

    /**
     * Returns the minimum of the {@code [vInt length][bytes]} values packed in {@code raw}, as a
     * view into {@code raw} (no copy — the value is only used for comparison, per the contract).
     */
    static BytesRef min(BytesRef raw) {
      ByteArrayDataInput in = new ByteArrayDataInput(raw.bytes, raw.offset, raw.length);
      BytesRef best = null;
      while (in.eof() == false) {
        int length = in.readVInt();
        BytesRef value = new BytesRef(raw.bytes, in.getPosition(), length);
        in.setPosition(in.getPosition() + length);
        if (best == null || value.compareTo(best) < 0) {
          best = value;
        }
      }
      return best;
    }

    /** Serializes/deserializes a {@link MinValueSortField}. */
    public static final class Provider extends SortFieldProvider {
      public Provider() {
        super(NAME);
      }

      @Override
      public SortField readSortField(DataInput in) throws IOException {
        return new MinValueSortField(in.readString(), in.readInt() == 1);
      }

      @Override
      public void writeSortField(SortField sf, DataOutput out) throws IOException {
        out.writeString(sf.getField());
        out.writeInt(sf.getReverse() ? 1 : 0);
      }
    }
  }
}
