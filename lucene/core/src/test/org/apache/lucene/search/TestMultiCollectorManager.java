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
package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestMultiCollectorManager extends LuceneTestCase {

  @SuppressWarnings("unchecked")
  public void testCollection() throws IOException {
    Directory dir = newDirectory();
    DirectoryReader reader = reader(dir);
    LeafReaderContext ctx = reader.leaves().get(0);

    // Setup two collector managers, one that will only collect even doc ids and one that
    // only collects odd. Create some random doc ids and keep track of the ones that we
    // expect each collector manager to collect:
    Predicate<Integer> evenPredicate = val -> val % 2 == 0;
    Predicate<Integer> oddPredicate = val -> val % 2 == 1;

    SimpleCollectorManager cm1 = new SimpleCollectorManager(evenPredicate);
    SimpleCollectorManager cm2 = new SimpleCollectorManager(oddPredicate);

    for (int iter = 0; iter < 100; iter++) {
      int docs = RandomNumbers.randomIntBetween(random(), 1000, 10000);
      List<Integer> expected = generateDocIds(docs, random());
      List<Integer> expectedEven =
          expected.stream().filter(evenPredicate).collect(Collectors.toList());
      List<Integer> expectedOdd =
          expected.stream().filter(oddPredicate).collect(Collectors.toList());

      // Test only wrapping one of the collector managers:
      MultiCollectorManager mcm = new MultiCollectorManager(cm1);
      Object[] results = (Object[]) collectAll(ctx, expected, mcm);
      assertEquals(1, results.length);
      List<Integer> intResults = (List<Integer>) results[0];
      assertArrayEquals(expectedEven.toArray(new Integer[0]), intResults.toArray(new Integer[0]));

      // Test wrapping both collector managers:
      mcm = new MultiCollectorManager(cm1, cm2);
      results = (Object[]) collectAll(ctx, expected, mcm);
      assertEquals(2, results.length);
      intResults = (List<Integer>) results[0];
      assertArrayEquals(expectedEven.toArray(new Integer[0]), intResults.toArray(new Integer[0]));
      intResults = (List<Integer>) results[1];
      assertArrayEquals(expectedOdd.toArray(new Integer[0]), intResults.toArray(new Integer[0]));
    }

    reader.close();
    dir.close();
  }

  public void testNullCollectorManagers() {
    assertThrows(IllegalArgumentException.class, () -> new MultiCollectorManager(null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new MultiCollectorManager(new SimpleCollectorManager(), null));
  }

  public void testCacheScoresIfNecessary() throws IOException {
    Directory dir = newDirectory();
    DirectoryReader reader = reader(dir);
    LeafReaderContext ctx = reader.leaves().get(0);

    // no collector needs scores => no caching
    CollectorManager<?, ?> cm1 = collectorManager(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    CollectorManager<?, ?> cm2 = collectorManager(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    new MultiCollectorManager(cm1, cm2)
        .newCollector()
        .getLeafCollector(ctx)
        .setScorer(new ScoreAndDoc());

    // only one collector needs scores => no caching
    cm1 = collectorManager(ScoreMode.COMPLETE, ScoreAndDoc.class);
    cm2 = collectorManager(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    new MultiCollectorManager(cm1, cm2)
        .newCollector()
        .getLeafCollector(ctx)
        .setScorer(new ScoreAndDoc());

    // several collectors need scores => caching
    cm1 = collectorManager(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    cm2 = collectorManager(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    new MultiCollectorManager(cm1, cm2)
        .newCollector()
        .getLeafCollector(ctx)
        .setScorer(new ScoreAndDoc());

    reader.close();
    dir.close();
  }

  public void testScoreWrapping() throws IOException {
    Directory dir = newDirectory();
    DirectoryReader reader = reader(dir);
    LeafReaderContext ctx = reader.leaves().get(0);

    // all wrapped collector managers are TOP_SCORE score mode, so they should see a
    // MinCompetitiveScoreAwareScorable passed in as their scorer:
    CollectorManager<?, ?> cm1 =
        collectorManager(
            ScoreMode.TOP_SCORES, MultiCollector.MinCompetitiveScoreAwareScorable.class);
    CollectorManager<?, ?> cm2 =
        collectorManager(
            ScoreMode.TOP_SCORES, MultiCollector.MinCompetitiveScoreAwareScorable.class);
    new MultiCollectorManager(cm1, cm2)
        .newCollector()
        .getLeafCollector(ctx)
        .setScorer(new ScoreAndDoc());

    // both wrapped collector managers need scores, but one is exhaustive, so they should
    // see a ScoreCachingWrappingScorer pass in as their scorer:
    cm1 = collectorManager(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    cm2 = collectorManager(ScoreMode.TOP_SCORES, ScoreCachingWrappingScorer.class);
    new MultiCollectorManager(cm1, cm2)
        .newCollector()
        .getLeafCollector(ctx)
        .setScorer(new ScoreAndDoc());

    reader.close();
    dir.close();
  }

  @SuppressWarnings("unchecked")
  public void testEarlyTermination() throws IOException {
    Directory dir = newDirectory();
    DirectoryReader reader = reader(dir);
    LeafReaderContext ctx = reader.leaves().get(0);

    int docs = RandomNumbers.randomIntBetween(random(), 1000, 10000);
    List<Integer> expected = generateDocIds(docs, random());

    // The first collector manager should collect all docs even though the second throws
    // CollectionTerminatedException immediately:
    SimpleCollectorManager cm1 = new SimpleCollectorManager();
    TerminatingCollectorManager cm2 = new TerminatingCollectorManager();
    MultiCollectorManager mcm = new MultiCollectorManager(cm1, cm2);
    Object[] results = (Object[]) collectAll(ctx, expected, mcm);
    assertEquals(2, results.length);
    List<Integer> intResults = (List<Integer>) results[0];
    assertArrayEquals(expected.toArray(new Integer[0]), intResults.toArray(new Integer[0]));
    assertNull(results[1]);

    // If we wrap multiple collector managers that throw CollectionTerminatedException, the
    // exception should be thrown by the MultiCollectorManager's collector:
    TerminatingCollectorManager cm3 = new TerminatingCollectorManager();
    assertThrows(
        CollectionTerminatedException.class,
        () -> collectAll(ctx, expected, new MultiCollectorManager(cm2, cm3)));

    reader.close();
    dir.close();
  }

  private static DirectoryReader reader(Directory dir) throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    iw.commit();
    DirectoryReader reader = iw.getReader();
    iw.close();

    return reader;
  }

  private static <C extends Collector> Object collectAll(
      LeafReaderContext ctx, List<Integer> values, CollectorManager<C, ?> collectorManager)
      throws IOException {
    List<C> collectors = new ArrayList<>();
    C collector = collectorManager.newCollector();
    collectors.add(collector);
    LeafCollector leafCollector = collector.getLeafCollector(ctx);
    for (Integer v : values) {
      if (random().nextInt(10) == 1) {
        collector = collectorManager.newCollector();
        collectors.add(collector);
        leafCollector = collector.getLeafCollector(ctx);
      }
      leafCollector.collect(v);
    }
    return collectorManager.reduce(collectors);
  }

  /**
   * Generate test doc ids. This will de-dupe and create a sorted list to be more realistic with
   * real-world use-cases. Note that it's possible this will generate fewer than 'count' entries
   * because of de-duping, but that should be quite rare and probably isn't worth worrying about for
   * these testing purposes.
   */
  private List<Integer> generateDocIds(int count, Random random) {
    Set<Integer> generated = new HashSet<>(count);
    for (int i = 0; i < count; i++) {
      generated.add(random.nextInt());
    }

    return generated.stream().sorted().collect(Collectors.toList());
  }

  private static final class SimpleCollectorManager
      implements CollectorManager<SimpleListCollector, List<Integer>> {
    private final Predicate<Integer> predicate;

    SimpleCollectorManager() {
      this.predicate = val -> true;
    }

    SimpleCollectorManager(Predicate<Integer> predicate) {
      this.predicate = predicate;
    }

    @Override
    public SimpleListCollector newCollector() throws IOException {
      return new SimpleListCollector(predicate);
    }

    @Override
    public List<Integer> reduce(Collection<SimpleListCollector> collectors) throws IOException {
      List<Integer> all = new ArrayList<>();
      for (SimpleListCollector c : collectors) {
        all.addAll(c.collected);
      }

      return all;
    }
  }

  private static final class SimpleListCollector implements Collector {
    final Predicate<Integer> predicate;
    final List<Integer> collected = new ArrayList<>();

    SimpleListCollector(Predicate<Integer> predicate) {
      this.predicate = predicate;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new LeafCollector() {
        @Override
        public void setScorer(Scorable scorer) throws IOException {}

        @Override
        public void collect(int doc) throws IOException {
          if (predicate.test(doc)) {
            collected.add(doc);
          }
        }
      };
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

  private static final class TerminatingCollectorManager
      implements CollectorManager<Collector, Object> {

    @Override
    public Collector newCollector() throws IOException {
      return new SimpleCollector() {
        @Override
        public void collect(int doc) throws IOException {
          throw new CollectionTerminatedException();
        }

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE;
        }
      };
    }

    @Override
    public Object reduce(Collection<Collector> collectors) throws IOException {
      return null;
    }
  }

  private static CollectorManager<?, ?> collectorManager(
      ScoreMode scoreMode, Class<?> expectedScorer) {
    return new CollectorManager<Collector, Object>() {

      @Override
      public Collector newCollector() throws IOException {

        return new Collector() {
          @Override
          public ScoreMode scoreMode() {
            return scoreMode;
          }

          @Override
          public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                while (expectedScorer.equals(scorer.getClass()) == false
                    && scorer instanceof FilterScorable) {
                  scorer = ((FilterScorable) scorer).in;
                }
                assertEquals(expectedScorer, scorer.getClass());
              }

              @Override
              public void collect(int doc) throws IOException {}
            };
          }
        };
      }

      @Override
      public Object reduce(Collection<Collector> collectors) throws IOException {
        return null;
      }
    };
  }
}
