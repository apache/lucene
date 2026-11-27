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

package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class TestTermGroupSelector extends BaseGroupSelectorTestCase<BytesRef> {

  public void testRediscoverGroupsAcrossLeaves() throws IOException {
    List<BytesRef> expected = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int leaf = 0; leaf < 5; leaf++) {
          for (int i = 0; i < 30; i++) {
            // Shared values can have different ordinals in different leaves.
            BytesRef group =
                switch (i % 4) {
                  case 0 -> null;
                  case 1 -> new BytesRef("shared");
                  case 2 -> new BytesRef("leaf" + leaf);
                  default -> new BytesRef("rand" + random().nextInt(8));
                };
            Document doc = new Document();
            if (group != null) {
              doc.add(new SortedDocValuesField("group", group));
            }
            expected.add(group);
            writer.addDocument(doc);
          }
          writer.commit();
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        TermGroupSelector selector = new TermGroupSelector("group");
        SearchGroup<BytesRef> shared = new SearchGroup<>();
        shared.groupValue = new BytesRef("shared");
        for (int pass = 0; pass < 4; pass++) {
          boolean restricted = pass >= 2;
          if (pass == 3) {
            selector.setGroups(List.of(shared, new SearchGroup<>()));
          }
          for (var leaf : reader.leaves()) {
            selector.setNextReader(leaf);
            if (pass == 2 && leaf.ord == 0) {
              selector.setGroups(List.of(shared, new SearchGroup<>()));
            }
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
              BytesRef group = expected.get(leaf.docBase + doc);
              boolean accept =
                  restricted ? group == null || shared.groupValue.equals(group) : group != null;
              assertEquals(
                  accept ? GroupSelector.State.ACCEPT : GroupSelector.State.SKIP,
                  selector.advanceTo(doc));
              if (accept || group == null) {
                assertEquals(group, selector.currentValue());
                assertEquals(group, selector.copyValue());
              }
            }
          }
        }
      }
    }
  }

  @Override
  protected void addGroupField(Document document, int id) {
    if (rarely()) {
      return; // missing value
    }
    String groupValue = "group" + random().nextInt(10);
    document.add(new SortedDocValuesField("groupField", new BytesRef(groupValue)));
    document.add(new TextField("groupField", groupValue, Field.Store.NO));
  }

  @Override
  protected GroupSelector<BytesRef> getGroupSelector() {
    return new TermGroupSelector("groupField");
  }

  @Override
  protected Query filterQuery(BytesRef groupValue) {
    if (groupValue == null) {
      return new BooleanQuery.Builder()
          .add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.FILTER)
          .add(new FieldExistsQuery("groupField"), BooleanClause.Occur.MUST_NOT)
          .build();
    }
    return new TermQuery(new Term("groupField", groupValue));
  }
}
