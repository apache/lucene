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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.jmh.base.BaseBenchState;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A tool to generate controlled random data for a benchmark. {@link Document}s are created based on
 * supplied FieldDef definitions.
 *
 * <p>You can call getDocument to build and retrieve one {@link Document} at a time, or you can call
 * {@link #preGenerate} to generate the given number of documents in RAM, and then retrieve them via
 * {@link #generatedDocsIterator}.
 */
public class Docs {

  private final Queue<Document> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, FieldDef> fields = Collections.synchronizedMap(new HashMap<>(16));
  private final BenchmarkRandomSource random;

  private ExecutorService executorService;

  /**
   * Docs docs.
   *
   * @return the docs
   */
  public static Docs docs() {
    return new Docs(BaseBenchState.getInitRandomeSeed());
  }

  /**
   * Docs docs.
   *
   * @param seed the seed
   * @return the docs
   */
  public static Docs docs(Long seed) {
    return new Docs(seed);
  }

  private Docs(Long seed) {
    // NOTE: we share across threads - not an issue for rnd benchmark data
    this.random = new BenchmarkRandomSource(new SplittableRandomGenerator(seed));
  }

  /**
   * Pre generate iterator.
   *
   * @param numDocs the num docs
   * @return the iterator
   * @throws InterruptedException the interrupted exception
   */
  public Iterator<Document> preGenerate(int numDocs) throws InterruptedException {
    log("preGenerate docs " + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1, new NamedThreadFactory("JMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          () -> {
            try {
              Document doc = Docs.this.document();
              docs.add(doc);
            } catch (Exception e) {
              e.printStackTrace();
              executorService.shutdownNow();
              throw new RuntimeException(e);
            }
          });
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
    log(
        "done preGenerateDocs docs="
            + docs.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(docs)));

    if (numDocs != docs.size()) {
      throw new IllegalStateException("numDocs != " + docs.size());
    }

    return new Iterator<>() {
      Iterator<Document> it = docs.iterator();

      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public Document next() {
        if (!it.hasNext()) {
          it = docs.iterator();
        }
        return it.next();
      }
    };
  }

  /**
   * Generated docs iterator iterator.
   *
   * @return the iterator
   */
  public Iterator<Document> generatedDocsIterator() {
    return docs.iterator();
  }

  /**
   * Document.
   *
   * @return the document
   */
  public Document document() {
    Document doc = new Document();
    for (Map.Entry<String, FieldDef> entry : fields.entrySet()) {
      doc.add(
          new TextField(
              entry.getKey(),
              (String) entry.getValue().generator.generate(random),
              entry.getValue().properties.contains(Store.YES) ? Store.YES : Store.NO));
    }

    return doc;
  }

  /**
   * Field docs.
   *
   * @param name the name
   * @param generator the generator
   * @param properties the properties
   * @return the docs
   */
  public Docs field(String name, RndGen<?> generator, Object... properties) {
    Set<Object> props = null;
    if (properties != null) {
      props = new HashSet<>(properties.length);
      props.addAll(Arrays.asList(properties));
    }
    FieldDef fieldDef = new FieldDef(generator, props);
    fields.put(name, fieldDef);

    return this;
  }

  /** The type Fld. */
  public static class FieldDef {
    /** The Generator. */
    RndGen<?> generator;
    /** The Properties. */
    Set<Object> properties;

    /**
     * Instantiates a new Fld.
     *
     * @param generator the generator
     * @param properties the properties
     */
    public FieldDef(RndGen<?> generator, Set<Object> properties) {
      this.generator = generator;
      this.properties = properties;
    }
  }

  /** Clear. */
  public void clear() {
    docs.clear();
  }
}
