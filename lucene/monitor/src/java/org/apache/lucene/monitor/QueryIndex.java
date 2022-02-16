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

package org.apache.lucene.monitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import org.apache.lucene.index.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

interface QueryIndex extends Closeable {
  void commit(List<MonitorQuery> updates) throws IOException;

  MonitorQuery getQuery(String queryId) throws IOException;

  void scan(QueryCollector matcher) throws IOException;

  long search(WritableQueryIndex.QueryBuilder queryBuilder, QueryCollector matcher)
      throws IOException;

  void purgeCache() throws IOException;

  @Override
  void close() throws IOException;

  int numDocs() throws IOException;

  int cacheSize();

  void deleteQueries(Iterable<String> ids) throws IOException;

  void clear() throws IOException;

  public interface QueryCollector {

    void matchQuery(String id, QueryCacheEntry query, DataValues dataValues) throws IOException;

    default ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  interface QueryBuilder {
    Query buildQuery(BiPredicate<String, BytesRef> termAcceptor) throws IOException;
  }

  public static final class DataValues {
    SortedDocValues queryId;
    SortedDocValues cacheId;
    BinaryDocValues mq;
    Scorable scorer;
    LeafReaderContext ctx;

    void advanceTo(int doc) throws IOException {
      assert scorer.docID() == doc;
      queryId.advanceExact(doc);
      cacheId.advanceExact(doc);
      if (mq != null) {
        mq.advanceExact(doc);
      }
    }
  }

  class QueryTermFilter implements BiPredicate<String, BytesRef> {

    private final Map<String, BytesRefHash> termsHash = new HashMap<>();

    QueryTermFilter(IndexReader reader) throws IOException {
      for (LeafReaderContext ctx : reader.leaves()) {
        for (FieldInfo fi : ctx.reader().getFieldInfos()) {
          BytesRefHash terms = termsHash.computeIfAbsent(fi.name, f -> new BytesRefHash());
          Terms t = ctx.reader().terms(fi.name);
          if (t != null) {
            TermsEnum te = t.iterator();
            BytesRef term;
            while ((term = te.next()) != null) {
              terms.add(term);
            }
          }
        }
      }
    }

    @Override
    public boolean test(String field, BytesRef term) {
      BytesRefHash bytes = termsHash.get(field);
      if (bytes == null) {
        return false;
      }
      return bytes.find(term) != -1;
    }
  }
}
