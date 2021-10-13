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
package org.apache.lucene.jmh.base.rndgen;

import static org.apache.lucene.jmh.base.BaseBenchState.log;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;

/** The type Queries. */
public class Queries {

  private final Queue<Query> queries = new ConcurrentLinkedQueue<>();

  private final QueryGen queryGen;

  /**
   * Docs docs.
   *
   * @param generator the generator
   * @return the docs
   */
  public static Queries queries(QueryGen generator) {
    return new Queries(generator);
  }

  private Queries(QueryGen queryGen) {
    this.queryGen = queryGen;
  }

  /**
   * Pre generate iterator.
   *
   * @param numQueries the num queries
   * @return the iterator
   * @throws InterruptedException the interrupted exception
   */
  public Iterator<Query> preGenerate(int numQueries) throws InterruptedException {
    log("preGenerate queries " + numQueries + " ...");
    queries.clear();

    for (int i = 0; i < numQueries; i++) {

      Query query = queryGen.generate();
      queries.add(query);
    }

    log(
        "done preGenerateDocs queries="
            + queries.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(queries)));

    return getQueryIterator();
  }

  /**
   * Generated queries iterator.
   *
   * @return the iterator
   */
  public Iterator<Query> generatedDocsIterator() {
    return getQueryIterator();
  }

  private Iterator<Query> getQueryIterator() {
    return new Iterator<>() {
      Iterator<Query> it = queries.iterator();

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Query next() {
        if (!it.hasNext()) {
          it = queries.iterator();
        }
        return it.next();
      }
    };
  }

  /**
   * Query query.
   *
   * @return the query
   */
  public Query query() {
    return queryGen.generate();
  }

  /** The interface Query gen. */
  public interface QueryGen {

    /**
     * Generate query.
     *
     * @return the query
     */
    Query generate();
  }

  /** Clear. */
  public void clear() {
    queries.clear();
  }
}
