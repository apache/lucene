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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;

final class RespellTask extends Task {

  private final Term term;
  private SuggestWord[] answers;

  public RespellTask(Term term) {
    this.term = term;
  }

  @Override
  public Task clone() {
    return new RespellTask(term);
  }

  @Override
  public void go(IndexState state) throws IOException {
    final IndexSearcher searcher = state.mgr.acquire();
    try {
      answers =
          state.spellChecker.suggestSimilar(
              term, 10, searcher.getIndexReader(), SuggestMode.SUGGEST_MORE_POPULAR);
    } finally {
      state.mgr.release(searcher);
    }
    // System.out.println("term=" + term);
    // printResults(System.out, state);
  }

  @Override
  public String toString() {
    return "respell " + term.text();
  }

  @Override
  public String getCategory() {
    return "Respell";
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RespellTask) {
      return term.equals(((RespellTask) other).term);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return term.hashCode();
  }

  @Override
  public long checksum() {
    long sum = 0;
    for (SuggestWord suggest : answers) {
      sum += suggest.string.hashCode() + Integer.valueOf(suggest.freq).hashCode();
    }
    return sum;
  }

  @Override
  public void printResults(PrintStream out, IndexState state) {
    for (SuggestWord suggest : answers) {
      out.println("  " + suggest.string + " freq=" + suggest.freq + " score=" + suggest.score);
    }
  }
}
