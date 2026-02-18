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
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/** */
public class TestBlockJoinSorting extends LuceneTestCase {

  @Test
  public void testNestedSorting() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    List<Document> docs = new ArrayList<>();
    Document document = new Document();
    document.add(new StringField("field2", "a", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("a")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "b", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("b")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "c", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("c")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "a", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "c", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("c")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "d", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("d")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "e", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("e")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "b", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "e", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("e")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "f", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("f")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "g", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("g")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "c", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "g", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("g")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "h", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("h")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "i", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("i")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "d", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "i", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("i")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "j", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("j")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "k", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("k")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "f", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "k", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("k")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "l", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("l")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "m", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("m")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "g", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    docs.clear();
    document = new Document();
    document.add(new StringField("field2", "m", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("m")));
    document.add(new StringField("filter_1", "T", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "n", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("n")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("field2", "o", Field.Store.NO));
    document.add(new SortedDocValuesField("field2", new BytesRef("o")));
    document.add(new StringField("filter_1", "F", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    document.add(new StringField("field1", "i", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w.w));
    w.close();
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("__type", "parent")));
    CheckJoinIndex.check(searcher.getIndexReader(), parentFilter);
    BitSetProducer childFilter = new QueryBitSetProducer(new PrefixQuery(new Term("field2")));
    ToParentBlockJoinQuery query =
        new ToParentBlockJoinQuery(
            new PrefixQuery(new Term("field2")), parentFilter, ScoreMode.None);

    // Sort by field ascending, order first
    ToParentBlockJoinSortField sortField =
        new ToParentBlockJoinSortField(
            "field2", SortField.Type.STRING, false, parentFilter, childFilter);
    Sort sort = new Sort(sortField);
    TopFieldDocs topDocs = searcher.search(query, 5, sort);
    assertEquals(7, topDocs.totalHits.value());
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(3, topDocs.scoreDocs[0].doc);
    assertEquals("a", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[1].doc);
    assertEquals("c", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[4].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field ascending, order last
    sortField =
        notEqual(
            sortField,
            () ->
                new ToParentBlockJoinSortField(
                    "field2", SortField.Type.STRING, false, true, parentFilter, childFilter));

    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(7, topDocs.totalHits.value());
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(3, topDocs.scoreDocs[0].doc);
    assertEquals("c", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[1].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[4].doc);
    assertEquals("k", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field descending, order last
    sortField =
        notEqual(
            sortField,
            () ->
                new ToParentBlockJoinSortField(
                    "field2", SortField.Type.STRING, true, parentFilter, childFilter));
    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(topDocs.totalHits.value(), 7);
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(27, topDocs.scoreDocs[0].doc);
    assertEquals("o", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(23, topDocs.scoreDocs[1].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(19, topDocs.scoreDocs[2].doc);
    assertEquals("k", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("i", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[4].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    // Sort by field descending, order last, sort filter (filter_1:T)
    BitSetProducer childFilter1T =
        new QueryBitSetProducer(new TermQuery((new Term("filter_1", "T"))));
    query =
        new ToParentBlockJoinQuery(
            new TermQuery((new Term("filter_1", "T"))), parentFilter, ScoreMode.None);

    sortField =
        notEqual(
            sortField,
            () ->
                new ToParentBlockJoinSortField(
                    "field2", SortField.Type.STRING, true, parentFilter, childFilter1T));

    sort = new Sort(sortField);
    topDocs = searcher.search(query, 5, sort);
    assertEquals(6, topDocs.totalHits.value());
    assertEquals(5, topDocs.scoreDocs.length);
    assertEquals(23, topDocs.scoreDocs[0].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString());
    assertEquals(27, topDocs.scoreDocs[1].doc);
    assertEquals("m", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString());
    assertEquals(11, topDocs.scoreDocs[2].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString());
    assertEquals(15, topDocs.scoreDocs[3].doc);
    assertEquals("g", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString());
    assertEquals(7, topDocs.scoreDocs[4].doc);
    assertEquals("e", ((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString());

    sortField =
        notEqual(
            sortField,
            () ->
                new ToParentBlockJoinSortField(
                    "field2",
                    SortField.Type.STRING,
                    true,
                    new QueryBitSetProducer(new TermQuery(new Term("__type", "another"))),
                    childFilter1T));

    searcher.getIndexReader().close();
    dir.close();
  }

  @Test
  public void testParentMissingValueNestedSorting() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    // Parent A (doc 2): children with sort_val 20 and 40 → MIN=20, MAX=40
    List<Document> docs = new ArrayList<>();
    Document document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 20));
    docs.add(document);
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 40));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    // Parent B (doc 5): children with sort_val 10 and 30 → MIN=10, MAX=30
    docs.clear();
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 10));
    docs.add(document);
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 30));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);

    // Parent C (doc 8): children without sort_val → parentMissingValue applies
    docs.clear();
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w.w));
    w.close();
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("__type", "parent")));
    CheckJoinIndex.check(searcher.getIndexReader(), parentFilter);
    BitSetProducer childFilter = new QueryBitSetProducer(new TermQuery(new Term("child", "true")));
    ToParentBlockJoinQuery query =
        new ToParentBlockJoinQuery(
            new TermQuery(new Term("child", "true")), parentFilter, ScoreMode.None);

    // parentMissing=5, reverse=false (MIN, ascending): C(5), B(10), A(20)
    ToParentBlockJoinSortField sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, false, 5, null, parentFilter, childFilter);
    TopFieldDocs topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(3, topDocs.totalHits.value());
    assertEquals(3, topDocs.scoreDocs.length);
    assertEquals(8, topDocs.scoreDocs[0].doc);
    assertEquals(5, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(5, topDocs.scoreDocs[1].doc);
    assertEquals(10, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
    assertEquals(2, topDocs.scoreDocs[2].doc);
    assertEquals(20, (int) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);

    // parentMissing=5, reverse=true (MAX, descending): A(40), B(30), C(5)
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, true, 5, null, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(3, topDocs.totalHits.value());
    assertEquals(3, topDocs.scoreDocs.length);
    assertEquals(2, topDocs.scoreDocs[0].doc);
    assertEquals(40, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(5, topDocs.scoreDocs[1].doc);
    assertEquals(30, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
    assertEquals(8, topDocs.scoreDocs[2].doc);
    assertEquals(5, (int) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);

    // parentMissing=100, reverse=false (MIN, ascending): B(10), A(20), C(100)
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, false, 100, null, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(3, topDocs.totalHits.value());
    assertEquals(3, topDocs.scoreDocs.length);
    assertEquals(5, topDocs.scoreDocs[0].doc);
    assertEquals(10, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(2, topDocs.scoreDocs[1].doc);
    assertEquals(20, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
    assertEquals(8, topDocs.scoreDocs[2].doc);
    assertEquals(100, (int) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);

    // parentMissing=100, reverse=true (MAX, descending): C(100), A(40), B(30)
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, true, 100, null, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(3, topDocs.totalHits.value());
    assertEquals(3, topDocs.scoreDocs.length);
    assertEquals(8, topDocs.scoreDocs[0].doc);
    assertEquals(100, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(2, topDocs.scoreDocs[1].doc);
    assertEquals(40, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);
    assertEquals(5, topDocs.scoreDocs[2].doc);
    assertEquals(30, (int) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]);

    searcher.getIndexReader().close();
    dir.close();
  }

  @Test
  public void testChildMissingValueNestedSorting() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w =
        new RandomIndexWriter(
            random(),
            dir,
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    // Parent A (doc 2): one child with sort_val=30, one child without sort_val
    List<Document> docs = new ArrayList<>();
    Document document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 30));
    docs.add(document);
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    // Parent B (doc 5): children with sort_val=20 and sort_val=40
    docs.clear();
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 20));
    docs.add(document);
    document = new Document();
    document.add(new StringField("child", "true", Field.Store.NO));
    document.add(new NumericDocValuesField("sort_val", 40));
    docs.add(document);
    document = new Document();
    document.add(new StringField("__type", "parent", Field.Store.NO));
    docs.add(document);
    w.addDocuments(docs);
    w.commit();

    IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(w.w));
    w.close();
    BitSetProducer parentFilter =
        new QueryBitSetProducer(new TermQuery(new Term("__type", "parent")));
    CheckJoinIndex.check(searcher.getIndexReader(), parentFilter);
    BitSetProducer childFilter = new QueryBitSetProducer(new TermQuery(new Term("child", "true")));
    ToParentBlockJoinQuery query =
        new ToParentBlockJoinQuery(
            new TermQuery(new Term("child", "true")), parentFilter, ScoreMode.None);

    // childMissing=5, reverse=false (MIN, ascending):
    //   A=MIN(30, 5)=5, B=MIN(20, 40)=20 → [A(5), B(20)]
    ToParentBlockJoinSortField sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, false, null, 5, parentFilter, childFilter);
    TopFieldDocs topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(2, topDocs.totalHits.value());
    assertEquals(2, topDocs.scoreDocs.length);
    assertEquals(2, topDocs.scoreDocs[0].doc);
    assertEquals(5, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(5, topDocs.scoreDocs[1].doc);
    assertEquals(20, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);

    // childMissing=5, reverse=true (MAX, descending):
    //   A=MAX(30, 5)=30, B=MAX(20, 40)=40 → [B(40), A(30)]
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, true, null, 5, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(2, topDocs.totalHits.value());
    assertEquals(2, topDocs.scoreDocs.length);
    assertEquals(5, topDocs.scoreDocs[0].doc);
    assertEquals(40, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(2, topDocs.scoreDocs[1].doc);
    assertEquals(30, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);

    // childMissing=50, reverse=false (MIN, ascending):
    //   A=MIN(30, 50)=30, B=MIN(20, 40)=20 → [B(20), A(30)]
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, false, null, 50, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(2, topDocs.totalHits.value());
    assertEquals(2, topDocs.scoreDocs.length);
    assertEquals(5, topDocs.scoreDocs[0].doc);
    assertEquals(20, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(2, topDocs.scoreDocs[1].doc);
    assertEquals(30, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);

    // childMissing=50, reverse=true (MAX, descending):
    //   A=MAX(30, 50)=50, B=MAX(20, 40)=40 → [A(50), B(40)]
    sortField =
        new ToParentBlockJoinSortField(
            "sort_val", SortField.Type.INT, true, null, 50, parentFilter, childFilter);
    topDocs = searcher.search(query, 10, new Sort(sortField));
    assertEquals(2, topDocs.totalHits.value());
    assertEquals(2, topDocs.scoreDocs.length);
    assertEquals(2, topDocs.scoreDocs[0].doc);
    assertEquals(50, (int) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]);
    assertEquals(5, topDocs.scoreDocs[1].doc);
    assertEquals(40, (int) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]);

    searcher.getIndexReader().close();
    dir.close();
  }

  private ToParentBlockJoinSortField notEqual(
      ToParentBlockJoinSortField old, Supplier<ToParentBlockJoinSortField> create) {
    final ToParentBlockJoinSortField newObj = create.get();
    assertFalse(old.equals(newObj));
    assertNotSame(old, newObj);

    final ToParentBlockJoinSortField bro = create.get();
    assertEquals(newObj, bro);
    assertEquals(newObj.hashCode(), bro.hashCode());
    assertNotSame(bro, newObj);

    assertFalse(old.equals(bro));
    return newObj;
  }
}
