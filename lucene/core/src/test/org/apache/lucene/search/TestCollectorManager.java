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

import static org.hamcrest.CoreMatchers.instanceOf;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestCollectorManager extends LuceneTestCase {

  @SuppressWarnings("unchecked")
  public void testCollection() throws IOException {
    Directory dir = newDirectory();
    DirectoryReader reader = reader(dir);
    LeafReaderContext ctx = reader.leaves().get(0);

    // Setup two collectors, one that will only collect even doc ids and one that
    // only collects odd. Create some random doc ids and keep track of the ones that we
    // expect each collector manager to collect:
    Predicate<Integer> evenPredicate = val -> val % 2 == 0;
    Predicate<Integer> oddPredicate = val -> val % 2 == 1;

    CompositeCollectorManager cm =
        new CompositeCollectorManager(Arrays.asList(evenPredicate, oddPredicate));

    for (int iter = 0; iter < 100; iter++) {
      int docs = RandomNumbers.randomIntBetween(random(), 1000, 10000);
      Collection<Integer> expected = generateDocIds(docs, random());
      IntStream expectedEven = expected.stream().filter(evenPredicate).mapToInt(i -> i);
      IntStream expectedOdd = expected.stream().filter(oddPredicate).mapToInt(i -> i);

      // Test only wrapping one of the collector managers:
      Object result = collectAll(ctx, expected, cm);
      assertThat(result, instanceOf(List.class));
      IntStream intResults = ((List<Integer>) result).stream().mapToInt(i -> i);
      assertArrayEquals(
          IntStream.concat(expectedEven, expectedOdd).sorted().toArray(),
          intResults.sorted().toArray());
    }

    reader.close();
    dir.close();
  }

  public void testEmptyCollectors() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CompositeCollectorManager(Collections.emptyList()).newCollector());
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
      LeafReaderContext ctx, Collection<Integer> values, CollectorManager<C, ?> collectorManager)
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
   * Generate test doc ids. This will de-dupe and create a sorted collection to be more realistic
   * with real-world use-cases. Note that it's possible this will generate fewer than 'count'
   * entries because of de-duping, but that should be quite rare and probably isn't worth worrying
   * about for these testing purposes.
   */
  private static SortedSet<Integer> generateDocIds(int count, Random random) {
    SortedSet<Integer> generated = new TreeSet<>();
    for (int i = 0; i < count; i++) {
      generated.add(random.nextInt());
    }
    return generated;
  }

  private static final class CompositeCollectorManager
      implements CollectorManager<Collector, List<Integer>> {
    private final List<Predicate<Integer>> predicates;

    CompositeCollectorManager(List<Predicate<Integer>> predicates) {
      this.predicates = predicates;
    }

    @Override
    public Collector newCollector() throws IOException {
      return MultiCollector.wrap(
          predicates.stream().map(SimpleListCollector::new).toArray(Collector[]::new));
    }

    @Override
    public List<Integer> reduce(Collection<Collector> collectors) throws IOException {
      List<Integer> all = new ArrayList<>();
      for (Collector m : collectors) {
        for (Collector c : ((MultiCollector) m).getCollectors()) {
          all.addAll(((SimpleListCollector) c).collected);
        }
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
}
