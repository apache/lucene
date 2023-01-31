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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

abstract class QueryIndex implements Closeable {
  static final class FIELDS {
    static final String query_id = "_query_id";
    static final String cache_id = "_cache_id";
    static final String mq = "_mq";
  }

  protected SearcherManager manager;
  protected QueryDecomposer decomposer;
  protected MonitorQuerySerializer serializer;

  // package-private for testing
  final Map<IndexReader.CacheKey, QueryTermFilter> termFilters = new HashMap<>();

  protected final List<MonitorUpdateListener> listeners = new ArrayList<>();

  abstract void commit(List<MonitorQuery> updates) throws IOException;

  public MonitorQuery getQuery(String queryId) throws IOException {
    if (serializer == null) {
      throw new IllegalStateException(
          "Cannot get queries from an index with no MonitorQuerySerializer");
    }
    BytesRef[] bytesHolder = new BytesRef[1];
    search(
        new TermQuery(new Term(FIELDS.query_id, queryId)),
        (id, query, dataValues) -> bytesHolder[0] = dataValues.mq.binaryValue());
    return serializer.deserialize(bytesHolder[0]);
  }

  public void scan(QueryCollector matcher) throws IOException {
    search(new MatchAllDocsQuery(), matcher);
  }

  long search(final Query query, QueryCollector matcher) throws IOException {
    QueryBuilder builder = termFilter -> query;
    return search(builder, matcher);
  }

  abstract long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException;

  public abstract void purgeCache() throws IOException;

  abstract void purgeCache(CachePopulator populator) throws IOException;

  abstract int numDocs() throws IOException;

  public abstract int cacheSize();

  abstract void deleteQueries(List<String> ids) throws IOException;

  abstract void clear() throws IOException;

  public abstract long getLastPurged();

  public void addListener(MonitorUpdateListener listener) {
    listeners.add(listener);
  }

  public interface QueryCollector {

    void matchQuery(String id, QueryCacheEntry query, DataValues dataValues) throws IOException;

    default ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  interface QueryBuilder {
    Query buildQuery(BiPredicate<String, BytesRef> termAcceptor) throws IOException;
  }

  interface CachePopulator {
    void populateCacheWithIndex(Map<String, QueryCacheEntry> newCache) throws IOException;
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

  static class QueryTermFilter implements BiPredicate<String, BytesRef> {

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
