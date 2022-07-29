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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

public class TestOrdinalMap extends LuceneTestCase {

  private static final Field ORDINAL_MAP_OWNER_FIELD;

  static {
    try {
      ORDINAL_MAP_OWNER_FIELD = OrdinalMap.class.getDeclaredField("owner");
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private static final RamUsageTester.Accumulator ORDINAL_MAP_ACCUMULATOR =
      new RamUsageTester.Accumulator() {

        @Override
        public long accumulateObject(
            Object o,
            long shallowSize,
            java.util.Map<Field, Object> fieldValues,
            java.util.Collection<Object> queue) {
          if (o == LongValues.ZEROES || o == LongValues.IDENTITY) {
            return 0L;
          }
          if (o instanceof OrdinalMap) {
            fieldValues = new HashMap<>(fieldValues);
            fieldValues.remove(ORDINAL_MAP_OWNER_FIELD);
          }
          return super.accumulateObject(o, shallowSize, fieldValues, queue);
        }
      };

  public void testRamBytesUsed() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setCodec(TestUtil.alwaysDocValuesFormat(TestUtil.getDefaultDocValuesFormat()));
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, cfg);
    final int maxDoc = TestUtil.nextInt(random(), 10, 1000);
    final int maxTermLength = TestUtil.nextInt(random(), 1, 4);
    for (int i = 0; i < maxDoc; ++i) {
      Document d = new Document();
      if (random().nextBoolean()) {
        d.add(
            new SortedDocValuesField(
                "sdv", new BytesRef(TestUtil.randomSimpleString(random(), maxTermLength))));
      }
      final int numSortedSet = random().nextInt(3);
      for (int j = 0; j < numSortedSet; ++j) {
        d.add(
            new SortedSetDocValuesField(
                "ssdv", new BytesRef(TestUtil.randomSimpleString(random(), maxTermLength))));
      }
      iw.addDocument(d);
      if (rarely()) {
        iw.getReader().close();
      }
    }
    iw.commit();
    DirectoryReader r = iw.getReader();
    SortedDocValues sdv = MultiDocValues.getSortedValues(r, "sdv");
    if (sdv instanceof MultiDocValues.MultiSortedDocValues) {
      OrdinalMap map = ((MultiDocValues.MultiSortedDocValues) sdv).mapping;
      assertEquals(RamUsageTester.ramUsed(map, ORDINAL_MAP_ACCUMULATOR), map.ramBytesUsed());
    }
    SortedSetDocValues ssdv = MultiDocValues.getSortedSetValues(r, "ssdv");
    if (ssdv instanceof MultiDocValues.MultiSortedSetDocValues) {
      OrdinalMap map = ((MultiDocValues.MultiSortedSetDocValues) ssdv).mapping;
      assertEquals(RamUsageTester.ramUsed(map, ORDINAL_MAP_ACCUMULATOR), map.ramBytesUsed());
    }
    iw.close();
    r.close();
    dir.close();
  }

  /**
   * Tests the case where one segment contains all of the global ords. In this case, we apply a
   * small optimization and hardcode the first segment indices and global ord deltas as all zeroes.
   */
  public void testOneSegmentWithAllValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setCodec(TestUtil.alwaysDocValuesFormat(TestUtil.getDefaultDocValuesFormat()))
            .setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter iw = new IndexWriter(dir, cfg);

    int numTerms = 1000;
    for (int i = 0; i < numTerms; ++i) {
      Document d = new Document();
      String term = String.valueOf(i);
      d.add(new SortedDocValuesField("sdv", new BytesRef(term)));
      iw.addDocument(d);
    }
    iw.forceMerge(1);

    for (int i = 0; i < 10; ++i) {
      Document d = new Document();
      String term = String.valueOf(random().nextInt(numTerms));
      d.add(new SortedDocValuesField("sdv", new BytesRef(term)));
      iw.addDocument(d);
    }
    iw.commit();

    DirectoryReader r = DirectoryReader.open(iw);
    SortedDocValues sdv = MultiDocValues.getSortedValues(r, "sdv");
    assertNotNull(sdv);
    assertTrue(sdv instanceof MultiDocValues.MultiSortedDocValues);

    // Check that the optimization kicks in.
    OrdinalMap map = ((MultiDocValues.MultiSortedDocValues) sdv).mapping;
    assertEquals(LongValues.ZEROES, map.firstSegments);
    assertEquals(LongValues.ZEROES, map.globalOrdDeltas);

    // Check the map's basic behavior.
    assertEquals(numTerms, (int) map.getValueCount());
    for (int i = 0; i < numTerms; i++) {
      assertEquals(0, map.getFirstSegmentNumber(i));
      assertEquals(i, map.getFirstSegmentOrd(i));
    }

    iw.close();
    r.close();
    dir.close();
  }

  public void testSharedPrefixes() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setCodec(TestUtil.alwaysDocValuesFormat(TestUtil.getDefaultDocValuesFormat()))
            .setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter iw = new IndexWriter(dir, cfg);
    Document doc = new Document();
    SortedDocValuesField sdvf = new SortedDocValuesField("field", new BytesRef());
    doc.add(sdvf);
    final int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      String prefix;
      switch (random().nextInt(3)) {
        case 0:
          prefix = "";
        case 1:
          prefix = "abc";
        case 2:
        default:
          prefix = "xyz";
      }
      String value = prefix + TestUtil.randomSimpleString(random());
      sdvf.setBytesValue(new BytesRef(value));
      iw.addDocument(doc);
      if (i == 500 || random().nextInt(100) == 0) {
        iw.flush();
      }
    }

    iw.close();
    DirectoryReader reader = DirectoryReader.open(dir);
    TermsEnum[] tes = new TermsEnum[reader.leaves().size()];
    long[] weights = new long[reader.leaves().size()];
    for (int i = 0; i < tes.length; ++i) {
      tes[i] = reader.leaves().get(i).reader().getSortedDocValues("field").termsEnum();
      weights[i] = TestUtil.nextInt(random(), 1, 10);
    }
    final int windowSize = TestUtil.nextInt(random(), 5, 50);
    OrdinalMap map = OrdinalMap.build(null, tes, weights, PackedInts.DEFAULT, windowSize);

    Set<BytesRef> allValues = new HashSet<>();
    for (TermsEnum te : tes) {
      te.seekExact(0);
      for (BytesRef term = te.term(); term != null; term = te.next()) {
        allValues.add(BytesRef.deepCopyOf(term));
      }
    }
    // The global map is not missing a value
    assertEquals(allValues.size(), map.getValueCount());

    // The global map returns values in ascending order
    BytesRef previous = null;
    for (int globalOrd = 0; globalOrd < map.getValueCount(); ++globalOrd) {
      int segIndex = map.getFirstSegmentNumber(globalOrd);
      int segOrd = (int) map.getFirstSegmentOrd(globalOrd);
      tes[segIndex].seekExact(segOrd);
      BytesRef current = BytesRef.deepCopyOf(tes[segIndex].term());
      if (globalOrd > 0) {
        assertTrue(previous.compareTo(current) < 0);
      }
      previous = current;
    }

    reader.close();
    dir.close();
  }
}
