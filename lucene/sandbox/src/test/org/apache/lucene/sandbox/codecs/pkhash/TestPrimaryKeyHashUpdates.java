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
package org.apache.lucene.sandbox.codecs.pkhash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.PerThreadPKLookup;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Updates, deletes and soft updates by id, applied by {@code IndexWriter} to already flushed
 * segments through {@code seekExact} on the hash table (the enum prefers it), checked against a
 * model of which version of each id is live.
 */
public class TestPrimaryKeyHashUpdates extends LuceneTestCase {

  private static final String SOFT_DELETES = "soft_delete";

  public void testUpdatesAndDeletes() throws IOException {
    final Random random = random();
    final Codec codec = TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    final boolean softDeletes = random.nextBoolean();
    final boolean indexSort = random.nextBoolean();
    final int shape = random.nextInt(5);

    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random)).setCodec(codec);
      if (softDeletes) {
        iwc.setSoftDeletesField(SOFT_DELETES);
        if (random.nextBoolean()) {
          // keep soft-deleted versions through merges: their ids then have several documents
          iwc.setMergePolicy(
              new SoftDeletesRetentionMergePolicy(
                  SOFT_DELETES, MatchAllDocsQuery::new, iwc.getMergePolicy()));
        }
      }
      if (indexSort) {
        iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
      }

      // id -> live version, for ids that have one
      final Map<BytesRef, Long> model = new HashMap<>();
      final List<BytesRef> ids = new ArrayList<>();
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        DirectoryReader reader = DirectoryReader.open(writer);
        PerThreadPKLookup lookup = new PerThreadPKLookup(reader, "id");
        long version = 0;
        final int numOps = atLeast(3000);
        for (int op = 0; op < numOps; op++) {
          final BytesRef id;
          if (ids.isEmpty() || random.nextInt(3) == 0) {
            id = PrimaryKeyHashTestUtil.randomKey(random, shape, ids.size());
            ids.add(id);
          } else {
            id = ids.get(random.nextInt(ids.size()));
          }
          final Term term = new Term("id", id);
          final int action = random.nextInt(100);
          if (action < 60) {
            version++;
            Document doc = doc(id, version, random);
            if (softDeletes && random.nextBoolean()) {
              writer.softUpdateDocument(term, doc, new NumericDocValuesField(SOFT_DELETES, 1));
            } else {
              writer.updateDocument(term, doc);
            }
            model.put(id, version);
          } else if (action < 75) {
            if (softDeletes && random.nextBoolean()) {
              // a soft delete: mark the current version, add nothing
              writer.updateDocValues(term, new NumericDocValuesField(SOFT_DELETES, 1));
            } else {
              writer.deleteDocuments(term);
            }
            model.remove(id);
          } else if (action < 85) {
            // not openIfChanged, which can miss doc values updates that flush() wrote
            DirectoryReader newReader = DirectoryReader.open(writer);
            reader.close();
            reader = newReader;
            lookup =
                random.nextBoolean() ? lookup.reopen(reader) : new PerThreadPKLookup(reader, "id");
            verify(reader, lookup, model, ids, random);
          } else if (action < 88) {
            writer.commit();
          } else if (action == 88 && random.nextInt(5) == 0) {
            writer.forceMerge(TestUtil.nextInt(random, 1, 3));
          } else if (action == 89) {
            writer.flush();
          }
        }
        writer.forceMerge(1);
        reader.close();
        reader = DirectoryReader.open(writer);
        verify(reader, new PerThreadPKLookup(reader, "id"), model, ids, random);
        for (LeafReaderContext context : reader.leaves()) {
          PrimaryKeyHashTestUtil.checkField(context.reader(), "id", ids, true);
        }
        reader.close();
      }
    }
  }

  private static Document doc(BytesRef id, long version, Random random) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.NO));
    doc.add(new NumericDocValuesField("version", version));
    doc.add(new NumericDocValuesField("sort", random.nextLong()));
    return doc;
  }

  /** Every id has exactly one live document, with the right version, or none. */
  private static void verify(
      DirectoryReader reader,
      PerThreadPKLookup lookup,
      Map<BytesRef, Long> model,
      List<BytesRef> ids,
      Random random)
      throws IOException {
    // not newSearcher: wrapped readers would hide the hash table
    IndexSearcher searcher = new IndexSearcher(reader);
    for (BytesRef id : ids) {
      if (ids.size() > 100 && random.nextInt(ids.size() / 100) != 0) {
        continue;
      }
      Long version = model.get(id);
      assertEquals(
          id.toString(),
          version == null ? 0 : 1,
          searcher.count(new TermQuery(new Term("id", id))));
      int doc = lookup.lookup(id);
      if (version == null) {
        assertEquals(-1, doc);
      } else {
        assertNotEquals(-1, doc);
        assertEquals(version.longValue(), versionOf(reader, doc));
      }
    }
  }

  private static long versionOf(DirectoryReader reader, int doc) throws IOException {
    LeafReaderContext context = reader.leaves().get(ReaderUtil.subIndex(doc, reader.leaves()));
    NumericDocValues versions = context.reader().getNumericDocValues("version");
    assertTrue(versions.advanceExact(doc - context.docBase));
    return versions.longValue();
  }
}
