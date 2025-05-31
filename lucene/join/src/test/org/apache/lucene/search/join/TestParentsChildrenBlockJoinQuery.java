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
package org.apache.lucene.search.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestParentsChildrenBlockJoinQuery extends LuceneTestCase {

  @Test
  public void testEmptyIndex() throws Exception {
    // No documents to index, just test the query execution
    test(new TestDoc[0][0], new int[0], 10);
  }

  @Test
  public void testOnlyParentDocs() throws Exception {
    // Only parent documents, no children
    TestDoc[][] blocks = {
      {new TestDoc("parent", true)}, {new TestDoc("parent", true)}, {new TestDoc("parent", true)}
    };
    int[] expectedDocIds = new int[0];
    test(blocks, expectedDocIds, 10);
  }

  @Test
  public void testFirstParentWithoutChild() throws Exception {
    // First parent has no children, but the second parent has two children
    TestDoc[][] blocks = {
      {new TestDoc("parent", true)},
      {new TestDoc("child", true), new TestDoc("child", true), new TestDoc("parent", true)}
    };
    int[] expectedDocIds = new int[] {1, 2};
    test(blocks, expectedDocIds, 10);
  }

  @Test
  public void testWithRandomizedIndex() throws Exception {
    // Random child limit between 1 and 10
    int childLimitPerParent = 1 + random().nextInt(10);

    // Create at least 100 parents
    final int numParents = 1 + random().nextInt(10);
    int docId = 0;

    // Array to store test documents: each parent and its children
    TestDoc[][] testDocs = new TestDoc[numParents][];

    // List to store expected matching document IDs
    List<Integer> expectedMatches = new ArrayList<>();
    for (int parentIdx = 0; parentIdx < numParents; parentIdx++) {
      // Randomly decide if parent matches
      boolean matchingParent = random().nextBoolean();

      // Random number of children (0-19)
      int numChildren = random().nextInt(20);
      testDocs[parentIdx] = new TestDoc[numChildren + 1]; // +1 for parent

      int matchingChildrenCount = 0;

      // Create children
      for (int childIdx = 0; childIdx < numChildren; childIdx++) {
        // Randomly decide if child matches
        boolean matchingChild = random().nextBoolean();
        testDocs[parentIdx][childIdx] = new TestDoc("child", matchingChild);

        // If both parent and child match, and we haven't exceeded child limit
        if (matchingChild && matchingParent) {
          matchingChildrenCount++;
          if (matchingChildrenCount <= childLimitPerParent) {
            expectedMatches.add(docId);
          }
        }
        docId++;
      }

      // Add parent document
      testDocs[parentIdx][numChildren] = new TestDoc("parent", matchingParent);
      docId++;
    }

    // Convert expected matches to array and run test
    int[] expectedMatchArray = expectedMatches.stream().mapToInt(Integer::intValue).toArray();
    test(testDocs, expectedMatchArray, childLimitPerParent);
  }

  private void test(TestDoc[][] blocks, int[] expectedDocIds, int childLimitPerParent)
      throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Add documents based on test case
    final List<Document> docs = new ArrayList<>();
    for (TestDoc[] block : blocks) {
      for (TestDoc doc : block) {
        Document document = doc.toDocument();
        docs.add(document);
      }
      writer.addDocuments(docs);
      docs.clear();
    }
    writer.commit();

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    Query parentQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("value", "match")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("type", "parent")), BooleanClause.Occur.MUST)
            .build();
    Query childQuery =
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("value", "match")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("type", "child")), BooleanClause.Occur.MUST)
            .build();

    ParentsChildrenBlockJoinQuery query =
        new ParentsChildrenBlockJoinQuery(
            parentFilter, parentQuery, childQuery, childLimitPerParent);

    TopDocs results = searcher.search(query, (expectedDocIds.length + 1) * 2);

    try {
      assertEquals(expectedDocIds.length, results.totalHits.value());

      // Verify the matching documents
      for (int i = 0; i < expectedDocIds.length; i++) {
        assertEquals(expectedDocIds[i], results.scoreDocs[i].doc);
      }
    } catch (AssertionError e) {
      System.out.println("Test failed. Document structure:");
      System.out.println(visualizeTestDocs(blocks));
      System.out.println("Expected docIds: " + Arrays.toString(expectedDocIds));
      System.out.println("Actual hits: " + results.totalHits.value());

      // Get actual docIds
      int[] actualDocIds = new int[results.scoreDocs.length];
      for (int i = 0; i < results.scoreDocs.length; i++) {
        actualDocIds[i] = results.scoreDocs[i].doc;
      }
      System.out.println("Actual docIds: " + Arrays.toString(actualDocIds));

      // Compare expected vs actual
      Set<Integer> expected = Arrays.stream(expectedDocIds).boxed().collect(Collectors.toSet());
      Set<Integer> actual = Arrays.stream(actualDocIds).boxed().collect(Collectors.toSet());

      Set<Integer> missing = new HashSet<>(expected);
      missing.removeAll(actual);
      if (!missing.isEmpty()) {
        System.out.println("Missing expected docIds: " + missing);
      }

      Set<Integer> unexpected = new HashSet<>(actual);
      unexpected.removeAll(expected);
      if (!unexpected.isEmpty()) {
        System.out.println("Unexpected docIds: " + unexpected);
      }

      throw e;
    }

    reader.close();
    dir.close();
  }

  @Test
  public void testInvalidChildLimit() {
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    Query parentQuery = new TermQuery(new Term("value", "parent"));
    Query childQuery = new TermQuery(new Term("type", "child"));

    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> new ParentsChildrenBlockJoinQuery(parentFilter, parentQuery, childQuery, 0));
    assertTrue(e.getMessage().contains("childLimitPerParent must be > 0"));
  }

  private record TestDoc(String type, boolean isMatch) {
    Document toDocument() {
      Document doc = new Document();
      doc.add(new StringField("type", type, Field.Store.YES));
      if (isMatch) {
        doc.add(new StringField("value", "match", Field.Store.NO));
      } else {
        doc.add(new StringField("value", "nomatch", Field.Store.NO));
      }
      return doc;
    }

    @Override
    public String toString() {
      return type + "(" + (isMatch ? "match" : "nomatch") + ")";
    }
  }

  private String visualizeTestDocs(TestDoc[][] blocks) {
    StringBuilder sb = new StringBuilder();
    sb.append("Test Documents Structure:\n");
    int docId = 0;
    for (int i = 0; i < blocks.length; i++) {
      sb.append("Block ").append(i).append(":\n");
      for (TestDoc doc : blocks[i]) {
        sb.append("  docId=").append(docId++).append(": ").append(doc).append("\n");
      }
    }
    return sb.toString();
  }
}
