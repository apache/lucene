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

package org.apache.lucene.facet.taxonomy;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;

public class TestBackCompatSortedNumericDocValues extends LuceneTestCase {

  private static class FacetsConfigWrapper extends FacetsConfig {
    public BytesRef encodeValues(IntsRef values) {
      return dedupAndEncode(values);
    }
  }

  public void testRandom() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // sorta big scratch so we don't have to think about reallocating:
    IntsRef scratch = new IntsRef(100);

    // used to access default binary encoding easily:
    FacetsConfigWrapper facetsConfig = new FacetsConfigWrapper();

    // keep track of the values we expect to see for each doc:
    Map<String, List<Integer>> expectedValues = new HashMap<>();

    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      int numValues = RandomNumbers.randomIntBetween(random(), 1, 50);
      scratch.length = 0;
      scratch.offset = 0;
      Set<Integer> values = new HashSet<>();
      for (int j = 0; j < numValues; j++) {
        int value = random().nextInt(Integer.MAX_VALUE);
        values.add(value);
        // we might have dups in here, which is fine (encoding takes care of deduping and sorting):
        scratch.ints[j] = value;
        scratch.length++;
      }
      // we expect to get sorted and deduped values back out:
      expectedValues.put(String.valueOf(i), values.stream().sorted().collect(Collectors.toList()));

      Document doc = new Document();
      doc.add(new StoredField("id", String.valueOf(i)));
      doc.add(new BinaryDocValuesField("bdv", facetsConfig.encodeValues(scratch)));
      writer.addDocument(doc);
    }

    writer.forceMerge(1);
    writer.commit();

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    assert reader.leaves().size() == 1;
    BinaryDocValues binaryDocValues = reader.leaves().get(0).reader().getBinaryDocValues("bdv");
    assertNotNull(binaryDocValues);
    SortedNumericDocValues docValues = BackCompatSortedNumericDocValues.wrap(binaryDocValues, null);

    TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), numDocs, Sort.INDEXORDER);

    StoredFields storedFields = reader.storedFields();
    for (ScoreDoc scoreDoc : docs.scoreDocs) {
      String id = storedFields.document(scoreDoc.doc).get("id");
      int docId = scoreDoc.doc;

      int doc;
      if (random().nextBoolean()) {
        doc = docValues.nextDoc();
      } else {
        if (random().nextBoolean()) {
          doc = docValues.advance(docId);
        } else {
          assertTrue(docValues.advanceExact(docId));
          doc = docId;
        }
      }
      assertEquals(docId, doc);
      assertEquals(docId, docValues.docID());

      List<Integer> expected = expectedValues.get(id);
      assertEquals(expected.size(), docValues.docValueCount());
      checkValues(expected, docValues);
    }

    // Run off the end and make sure that case is handled gracefully:
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.nextDoc());

    IOUtils.close(reader, dir);
  }

  private void checkValues(List<Integer> expected, SortedNumericDocValues values)
      throws IOException {
    for (Integer e : expected) {
      assertEquals((long) e, values.nextValue());
    }
  }
}
