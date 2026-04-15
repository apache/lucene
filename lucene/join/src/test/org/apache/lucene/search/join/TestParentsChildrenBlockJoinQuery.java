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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestParentsChildrenBlockJoinQuery extends LuceneTestCase {

  private static RandomIndexWriter createIndexWriter(Directory dir) throws IOException {
    // We need a merge policy that merges segments sequentially.
    // Most tests here merge down to a single segment and assume the order of documents in the
    // segment
    // matches the order in which they were added.
    return new RandomIndexWriter(
        random(), dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()));
  }

  @Test
  public void testEmptyIndex() throws Exception {
    // No documents to index, just test the query execution
    test(new TestDoc[0][0], new int[0], 10);
  }

  @Test
  public void testOnlyParentDocs() throws Exception {
    // Only parent documents, no children
    TestDoc[][] blocks = {
      {new TestDoc("parent", true, 0)},
      {new TestDoc("parent", true, 1)},
      {new TestDoc("parent", true, 2)}
    };
    int[] expectedDocIds = new int[0];
    test(blocks, expectedDocIds, 10);
  }

  @Test
  public void testFirstParentWithoutChild() throws Exception {
    // First parent has no children, but the second parent has two children
    TestDoc[][] blocks = {
      {new TestDoc("parent", true, 0)},
      {new TestDoc("child", true, 1), new TestDoc("child", true, 2), new TestDoc("parent", true, 3)}
    };
    int[] expectedDocIds = new int[] {1, 2};
    test(blocks, expectedDocIds, 10);
  }

  @Test
  public void testWithRandomizedIndex() throws Exception {
    for (int i = 0; i < 10; i++) {
      // Run multiple iterations to ensure randomness
      if (VERBOSE) {
        System.out.println("Running randomized test iteration: " + i);
      }
      runRandomizedTest();
    }
  }

  private void runRandomizedTest() throws Exception {
    // Random child limit between 1 and 10
    int childLimitPerParent = 1 + random().nextInt(10);

    // Create at least 100 parents
    final int numParents = atLeast(100);
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
        testDocs[parentIdx][childIdx] = new TestDoc("child", matchingChild, docId);

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
      testDocs[parentIdx][numChildren] = new TestDoc("parent", matchingParent, docId);
      docId++;
    }

    // Convert expected matches to array and run test
    int[] expectedMatchArray = expectedMatches.stream().mapToInt(Integer::intValue).toArray();
    test(testDocs, expectedMatchArray, childLimitPerParent);
  }

  @Test
  public void testAdvance() throws Exception {
    // Create test blocks with specific structure to test advance()
    TestDoc[][] blocks = new TestDoc[3][];

    // Block 0: 2 children + 1 parent (parent matches)
    blocks[0] = new TestDoc[3];
    blocks[0][0] = new TestDoc("child", true, 0); // docId=0
    blocks[0][1] = new TestDoc("child", true, 1); // docId=1
    blocks[0][2] = new TestDoc("parent", true, 2); // docId=2

    // Block 1: 3 children + 1 parent (parent matches)
    blocks[1] = new TestDoc[4];
    blocks[1][0] = new TestDoc("child", true, 3); // docId=3
    blocks[1][1] = new TestDoc("child", true, 4); // docId=4
    blocks[1][2] = new TestDoc("child", true, 5); // docId=5
    blocks[1][3] = new TestDoc("parent", true, 6); // docId=6

    // Block 2: 2 children + 1 parent (parent matches)
    blocks[2] = new TestDoc[3];
    blocks[2][0] = new TestDoc("child", true, 7); // docId=7
    blocks[2][1] = new TestDoc("child", true, 8); // docId=8
    blocks[2][2] = new TestDoc("parent", true, 9); // docId=9

    final Directory dir = newDirectory();
    final RandomIndexWriter writer = createIndexWriter(dir);

    // Add documents
    List<Document> docs = new ArrayList<>();
    for (TestDoc[] block : blocks) {
      for (TestDoc doc : block) {
        docs.add(doc.toDocument());
      }
      writer.addDocuments(docs);
      docs.clear();
    }
    writer.commit();
    writer.forceMerge(1);

    IndexReader reader = writer.getReader();
    writer.close();

    // Do not use Concurrency.INTRA_SEGMENT as that would break childLimitPerParent counting
    IndexSearcher searcher = newSearcher(reader, false, false, Concurrency.NONE);
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    CheckJoinIndex.check(reader, parentFilter);
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
        new ParentsChildrenBlockJoinQuery(parentFilter, parentQuery, childQuery, 2);

    // Test advance() functionality
    Weight weight =
        searcher.createWeight(
            searcher.rewrite(query), org.apache.lucene.search.ScoreMode.COMPLETE, 1);
    Scorer scorer = weight.scorer(reader.leaves().getFirst());
    assert scorer != null;
    DocIdSetIterator it = scorer.iterator();

    // Test advance to doc 3 (should skip to first child of second parent)
    assertEquals(3, it.advance(3));

    // Test advance to doc 7 (should skip to first child of third parent)
    assertEquals(7, it.advance(7));

    // Test advance beyond last document
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(10));

    reader.close();
    dir.close();
  }

  private void test(TestDoc[][] blocks, int[] expectedDocIds, int childLimitPerParent)
      throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter writer = createIndexWriter(dir);

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
    writer.forceMerge(1);

    IndexReader reader = writer.getReader();
    writer.close();

    // Do not use Concurrency.INTRA_SEGMENT as that would break childLimitPerParent counting
    IndexSearcher searcher = newSearcher(reader, false, false, Concurrency.NONE);
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    CheckJoinIndex.check(reader, parentFilter);
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
      Set<Integer> expectedDocIdSet =
          Arrays.stream(expectedDocIds).boxed().collect(Collectors.toSet());

      // Verify the matching documents
      for (ScoreDoc scoreDoc : results.scoreDocs) {
        Document doc = reader.storedFields().document(scoreDoc.doc);
        String type = Objects.requireNonNull(doc.getField("type")).stringValue();
        Integer id = Objects.requireNonNull(doc.getField("ID")).numericValue().intValue();
        assertEquals("child", type); // All results should be children
        assertTrue(expectedDocIdSet.contains(id));
      }
    } catch (AssertionError e) {
      if (VERBOSE) {
        System.out.println("Test failed. Document structure:");
        System.out.println(visualizeTestDocs(blocks));
        System.out.println("Child limit per parent: " + childLimitPerParent);
        System.out.println("Expected docIds: " + Arrays.toString(expectedDocIds));
        System.out.println("Actual hits: " + results.totalHits.value());

        // Get actual docIds
        int[] actualDocIds = new int[results.scoreDocs.length];
        int i = 0;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
          Document doc = reader.storedFields().document(scoreDoc.doc);
          int id = Objects.requireNonNull(doc.getField("ID")).numericValue().intValue();
          actualDocIds[i++] = id;
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

  @Test
  public void testExplain() throws Exception {
    // Create test blocks with specific structure to test explain()
    TestDoc[][] blocks = new TestDoc[2][];

    // Block 0: 2 children + 1 parent (parent matches)
    blocks[0] = new TestDoc[3];
    blocks[0][0] = new TestDoc("child", true, 0); // docId=0
    blocks[0][1] = new TestDoc("child", true, 1); // docId=1
    blocks[0][2] = new TestDoc("parent", true, 2); // docId=2

    // Block 1: 2 children + 1 parent (parent matches)
    blocks[1] = new TestDoc[3];
    blocks[1][0] = new TestDoc("child", true, 3); // docId=3
    blocks[1][1] = new TestDoc("child", false, 4); // docId=4
    blocks[1][2] = new TestDoc("parent", true, 5); // docId=5

    Directory dir = newDirectory();
    final RandomIndexWriter writer = createIndexWriter(dir);

    // Add documents
    List<Document> docs = new ArrayList<>();
    for (TestDoc[] block : blocks) {
      for (TestDoc doc : block) {
        docs.add(doc.toDocument());
      }
      writer.addDocuments(docs);
      docs.clear();
    }
    writer.commit();
    writer.forceMerge(1);

    IndexReader reader = writer.getReader();
    writer.close();

    // Do not use Concurrency.INTRA_SEGMENT as that would break childLimitPerParent counting
    IndexSearcher searcher = newSearcher(reader, false, false, Concurrency.NONE);
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    CheckJoinIndex.check(reader, parentFilter);
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
        new ParentsChildrenBlockJoinQuery(parentFilter, parentQuery, childQuery, 2);

    // Test explain for a matching child document
    Explanation explanation = searcher.explain(query, 0);
    if (VERBOSE) {
      System.out.println("Explanation for matching child document:");
      System.out.println(explanation);
    }
    assertTrue(explanation.isMatch());

    // Test explain for a non-matching child document
    // Add a non-matching child

    explanation = searcher.explain(query, 4);
    assertFalse(explanation.isMatch());

    reader.close();
    dir.close();
  }

  @Test
  public void testIntraSegmentConcurrencyNotSupported() throws Exception {
    // Create test blocks with specific structure - using a larger number of documents
    final int numParents = atLeast(1000); // Create at least 1000 parents to ensure large segment
    TestDoc[][] blocks = new TestDoc[numParents][];

    int docId = 0;
    for (int parentIdx = 0; parentIdx < numParents; parentIdx++) {
      // Each parent has 5 children
      blocks[parentIdx] = new TestDoc[6]; // 5 children + 1 parent

      // Add children
      for (int childIdx = 0; childIdx < 5; childIdx++) {
        blocks[parentIdx][childIdx] = new TestDoc("child", true, docId++);
      }
      // Add parent
      blocks[parentIdx][5] = new TestDoc("parent", true, docId++);
    }

    Directory dir = newDirectory();
    final RandomIndexWriter writer = createIndexWriter(dir);

    // Add documents
    List<Document> docs = new ArrayList<>();
    for (TestDoc[] block : blocks) {
      for (TestDoc doc : block) {
        docs.add(doc.toDocument());
      }
      writer.addDocuments(docs);
      docs.clear();
    }
    writer.commit();
    writer.forceMerge(1); // Force merge to ensure we have one large segment

    IndexReader reader = writer.getReader();
    writer.close();

    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("type", "parent")));
    CheckJoinIndex.check(reader, parentFilter);
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
        new ParentsChildrenBlockJoinQuery(parentFilter, parentQuery, childQuery, 2);

    for (int i = 0; i < 10; i++) {
      // Verify that searching with IntraSegment concurrency throws the expected exception
      // Note that there are still randomness on whether to slice a single segment, thus we are
      // running the test 10 times
      // And only assert for exception if there are multiple slices
      IndexSearcher searcher = newSearcher(reader, false, false, Concurrency.INTRA_SEGMENT);
      IndexSearcher.LeafSlice[] leafSlices = searcher.getSlices();
      if (leafSlices.length > 1) {
        IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> searcher.search(query, 10));
        assertTrue(
            e.getMessage()
                .contains(
                    "ParentsChildrenBlockJoinQuery does not support intraSegment concurrency."));
      }
    }

    reader.close();
    dir.close();
  }

  private record TestDoc(String type, boolean isMatch, int docID) {
    Document toDocument() {
      Document doc = new Document();
      doc.add(new StringField("type", type, Field.Store.YES));
      doc.add(new StoredField("ID", docID));
      if (isMatch) {
        doc.add(new StringField("value", "match", Field.Store.YES));
      } else {
        doc.add(new StringField("value", "nomatch", Field.Store.YES));
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
