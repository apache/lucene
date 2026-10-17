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
package org.apache.lucene.benchmark.byTask.tasks;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * Writers and NRT searchers over the join indices at <b>join.dir</b>/{layout}/index (Default:
 * work/join), shared by join tasks and closed with {@link PerfRunData}. Updates are never
 * committed, so the indices stay intact for the next run.
 */
final class JoinIndices implements Closeable {

  /**
   * Searcher that carries the global ordinals of {@link JoinDocMaker#FK} for the mixed index. They
   * are built by the first query on each new searcher, so a cold search after refresh pays for
   * them.
   */
  static final class JoinSearcher extends IndexSearcher {
    private final boolean ordinals;
    private OrdinalMap ordinalMap;
    private boolean built;

    JoinSearcher(IndexReader reader, boolean ordinals) {
      super(reader);
      this.ordinals = ordinals;
    }

    synchronized OrdinalMap ordinalMap() throws IOException {
      if (ordinals && !built) {
        if (MultiDocValues.getSortedValues(getIndexReader(), JoinDocMaker.FK)
            instanceof MultiDocValues.MultiSortedDocValues multi) {
          ordinalMap = multi.mapping; // a single segment needs no ordinal map
        }
        built = true;
      }
      return ordinalMap;
    }
  }

  record Index(Directory dir, IndexWriter writer, SearcherManager manager) {}

  private final Path root;
  private final Map<String, Index> indices = new HashMap<>();

  private JoinIndices(Path root) {
    this.root = root;
  }

  static JoinIndices get(PerfRunData runData) {
    synchronized (runData) {
      JoinIndices indices = (JoinIndices) runData.getPerfObject(JoinIndices.class.getName());
      if (indices == null) {
        indices = new JoinIndices(Path.of(runData.getConfig().get("join.dir", "work/join")));
        runData.setPerfObject(JoinIndices.class.getName(), indices);
      }
      return indices;
    }
  }

  synchronized Index index(String layout) throws IOException {
    Index index = indices.get(layout);
    if (index == null) {
      Directory dir = FSDirectory.open(root.resolve(layout).resolve("index"));
      IndexWriter writer =
          new IndexWriter(
              dir, new IndexWriterConfig().setOpenMode(OpenMode.APPEND).setCommitOnClose(false));
      boolean ordinals = layout.equals("mixed");
      SearcherManager manager =
          new SearcherManager(
              writer,
              new SearcherFactory() {
                @Override
                public IndexSearcher newSearcher(IndexReader reader, IndexReader previous) {
                  return new JoinSearcher(reader, ordinals);
                }
              });
      index = new Index(dir, writer, manager);
      indices.put(layout, index);
    }
    return index;
  }

  @Override
  public synchronized void close() throws IOException {
    for (Index index : indices.values()) {
      IOUtils.close(index.manager(), index.writer()::rollback, index.dir());
    }
  }
}
