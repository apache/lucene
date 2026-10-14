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
package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/** Compares the old per-doc constant-score loop with batched DocIdStream collection. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xmx1g", "-Xms1g", "-XX:+AlwaysPreTouch"})
public class DefaultBulkScorerBenchmark {

  @Param({"1000000"})
  int maxDoc;

  @Param({"0.01", "0.1", "0.5", "1.0"})
  double density;

  @Param({"false", "true"})
  boolean withDeletions;

  private FixedBitSet matchingDocs;
  private FixedBitSet liveDocs;
  private int matchCount;
  private int expectedHitCount;
  private Bits acceptDocs;
  private BitSetIterator baselineIterator;
  private BitSetIterator intoBitSetIterator;
  private BulkScorer baselineScorer;
  private BulkScorer intoBitSetScorer;
  private CountingLeafCollector baselineCollector;
  private DocIdStreamCountingLeafCollector intoBitSetCollector;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    matchingDocs = new FixedBitSet(maxDoc);
    int gap = Math.max(1, (int) Math.round(1d / density));
    for (int doc = 0; doc < maxDoc; doc += gap) {
      matchingDocs.set(doc);
    }
    matchCount = matchingDocs.cardinality();

    liveDocs = new FixedBitSet(maxDoc);
    liveDocs.set(0, maxDoc);

    for (int doc = 0, matchOrd = 0; doc < maxDoc; doc += gap, matchOrd++) {
      if (matchOrd % 10 == 0) {
        liveDocs.clear(doc);
      }
    }

    expectedHitCount = matchCount;
    if (withDeletions) {
      expectedHitCount = 0;
      for (int doc = 0; doc < maxDoc; doc += gap) {
        if (liveDocs.get(doc)) {
          expectedHitCount++;
        }
      }
      acceptDocs = liveDocs;
    } else {
      acceptDocs = null;
    }

    baselineIterator = new BitSetIterator(matchingDocs, matchCount);
    intoBitSetIterator = new BitSetIterator(matchingDocs, matchCount);
    baselineScorer = newBaselineBulkScorer(baselineIterator);
    intoBitSetScorer = newConstantScoreBulkScorer(intoBitSetIterator, maxDoc);
    baselineCollector = new CountingLeafCollector();
    intoBitSetCollector = new DocIdStreamCountingLeafCollector();
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    baselineIterator.setDocId(-1);
    intoBitSetIterator.setDocId(-1);
    baselineCollector.count = 0;
    intoBitSetCollector.count = 0;
  }

  @Benchmark
  public int scoreBaseline() throws IOException {
    baselineScorer.score(baselineCollector, acceptDocs, 0, maxDoc);
    checkCount("baseline", baselineCollector.count);
    return baselineCollector.count;
  }

  @Benchmark
  public int scoreIntoBitSet() throws IOException {
    intoBitSetScorer.score(intoBitSetCollector, acceptDocs, 0, maxDoc);
    checkCount("intoBitSet", intoBitSetCollector.count);
    return intoBitSetCollector.count;
  }

  private void checkCount(String method, int count) {
    if (count != expectedHitCount) {
      throw new AssertionError(
          "Expected " + expectedHitCount + " " + method + " hits but got " + count);
    }
  }

  private static BulkScorer newBaselineBulkScorer(DocIdSetIterator iterator) {
    return new PerDocBulkScorer(
        new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, iterator));
  }

  private static BulkScorer newConstantScoreBulkScorer(DocIdSetIterator iterator, int maxDoc)
      throws IOException {
    return ConstantScoreScorerSupplier.fromIterator(
            iterator, 1f, ScoreMode.COMPLETE_NO_SCORES, maxDoc)
        .bulkScorer();
  }

  private static final class PerDocBulkScorer extends BulkScorer {
    private final Scorer scorer;
    private final DocIdSetIterator iterator;

    private PerDocBulkScorer(Scorer scorer) {
      this.scorer = scorer;
      this.iterator = scorer.iterator();
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      collector.setScorer(scorer);
      if (iterator.docID() < min) {
        if (iterator.docID() == min - 1) {
          iterator.nextDoc();
        } else {
          iterator.advance(min);
        }
      }
      for (int doc = iterator.docID(); doc < max; doc = iterator.nextDoc()) {
        if (acceptDocs == null || acceptDocs.get(doc)) {
          collector.collect(doc);
        }
      }
      return iterator.docID();
    }

    @Override
    public long cost() {
      return iterator.cost();
    }
  }

  private static class CountingLeafCollector implements LeafCollector {
    int count;

    @Override
    public void setScorer(Scorable scorer) {}

    @Override
    public void collect(int doc) {
      count++;
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      throw new AssertionError("Baseline benchmark must use per-doc collection");
    }
  }

  private static final class DocIdStreamCountingLeafCollector extends CountingLeafCollector {
    @Override
    public void collect(int doc) {
      throw new AssertionError("Constant score bulk scoring must use DocIdStream collection");
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
      count += stream.count();
    }
  }
}
