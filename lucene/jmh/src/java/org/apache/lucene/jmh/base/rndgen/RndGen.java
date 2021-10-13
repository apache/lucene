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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.lucene.jmh.base.BaseBenchState;

/**
 * The type RndGen.
 *
 * @param <T> the type parameter
 */
public abstract class RndGen<T> implements AsString<T> {

  /** The constant OPEN_PAREN. */
  public static final String OPEN_PAREN = " (";

  /** The constant CLOSE_PAREN. */
  public static final char CLOSE_PAREN = ')';

  /** The constant COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED. */
  public static final String COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED =
      " Count types are tracked, limit was reached.\n\n";

  /** The constant RANDOM_DATA_GEN_REPORTS. */
  public static final String RANDOM_DATA_GEN_REPORTS =
      "\n\n\n*****  Random Data Gen Reports *****\n\n";

  /** The constant ONLY. */
  public static final String ONLY = "\n\nOnly ";

  /** The Distribution. */
  protected Distribution distribution = Distribution.UNIFORM;

  /** The Start. */
  protected long start;

  /** The End. */
  protected long end;

  private static final boolean COLLECT_COUNTS =
      Boolean.getBoolean("random.counts") || BaseBenchState.DEBUG;

  static {
    log("random.counts output: " + COLLECT_COUNTS);
  }

  private Describing<T> description;

  private String collectKey;
  /** The constant COUNTS. */
  protected static final Map<String, RandomDataHistogram.Counts> COUNTS =
      new ConcurrentHashMap<>(64);

  private RndCollector<T> collector = null;

  /**
   * Counts report list.
   *
   * @return the list
   */
  public static List<String> countsReport() {
    List<String> reports = new ArrayList<>(COUNTS.size());
    reports.add(RANDOM_DATA_GEN_REPORTS);
    if (COUNTS.size() >= RandomDataHistogram.MAX_TYPES_TO_COLLECT) {
      reports.add(
          ONLY
              + RandomDataHistogram.MAX_TYPES_TO_COLLECT
              + COUNT_TYPES_ARE_TRACKED_LIMIT_WAS_REACHED);
    }
    COUNTS.forEach((k, v) -> reports.add(v.print()));
    return reports;
  }

  /** Instantiates a new RndGen. */
  protected RndGen() {}

  /**
   * Instantiates a new Rnd gen.
   *
   * @param description the description
   */
  public RndGen(String description) {
    this.description = t -> description;
  }

  /**
   * Instantiates a new Rnd gen.
   *
   * @param description the description
   */
  public RndGen(Describing<T> description) {
    this.description = description;
  }

  /**
   * Generate t.
   *
   * @param in the in
   * @return the t
   */
  @SuppressWarnings("unchecked")
  public final T generate(RandomnessSource in) {
    if (BaseBenchState.DEBUG) {
      log("generate " + this + " from " + in.getClass().getSimpleName());
    }
    T val;
    if (start == end && end == 0) {
      val = gen(in);
    } else {
      val = (T) Integer.valueOf((int) in.withDistribution(distribution).next(start, end));
    }
    processRndValue(val, in);
    return val;
  }

  /**
   * Gen t.
   *
   * @param in the in
   * @return the t
   */
  protected abstract T gen(RandomnessSource in);

  /**
   * Process counts t.
   *
   * @param val the val
   * @param in the in
   * @return the t
   */
  protected T processRndValue(T val, RandomnessSource in) {
    if (BaseBenchState.DEBUG) {
      log(
          "processRndValue RndGen="
              + this
              + " Collector="
              + (collector == null ? "none" : collector.getValues())
              + " Val="
              + val);
    }

    if (collector != null) {
      collector.collect(val);
    }

    if (COLLECT_COUNTS) {

      if (description == null || COUNTS.size() > RandomDataHistogram.MAX_TYPES_TO_COLLECT) {
        return val;
      }

      // System.out.println("Add key " + key);
      RandomDataHistogram.Counts newCounts = null;
      RandomDataHistogram.Counts counts;
      collectKey = description.asString(null) + OPEN_PAREN + in.getDistribution() + CLOSE_PAREN;
      counts = COUNTS.get(collectKey);

      if (counts == null) {
        newCounts = new RandomDataHistogram.Counts(collectKey, val instanceof Number);
        counts = COUNTS.putIfAbsent(collectKey, newCounts);
      }

      if (counts == null) {
        newCounts.collect(val);
      } else {
        counts.collect(val);
      }
    }
    return val;
  }

  /**
   * Described as RndGen.
   *
   * @param description the description
   * @return the RndGen
   */
  public RndGen<T> describedAs(String description) {
    this.description = t -> description;
    return this;
  }

  /**
   * Described as RndGen.
   *
   * @param asString the as string
   * @return the RndGen
   */
  public RndGen<T> describedAs(AsString<T> asString) {
    this.description = new DescribingGenerator<T>(asString);
    return this;
  }

  /**
   * Mix RndGen.
   *
   * @param rhs the rhs
   * @return the RndGen
   */
  RndGen<T> mix(RndGen<T> rhs) {
    return mix(rhs, 50);
  }

  /**
   * Mix RndGen.
   *
   * @param rhs the rhs
   * @param weight the weight
   * @return the RndGen
   */
  public RndGen<T> mix(RndGen<T> rhs, int weight) {
    return new RndGen<T>() {
      @Override
      public T gen(RandomnessSource in) {
        while (true) {
          long picked = in.next(0, 99);
          if (picked >= weight) {
            continue;
          }
          return (rhs).generate(in);
        }
      }
    };
  }

  /**
   * Flat map RndGen.
   *
   * @param <R> the type parameter
   * @param mapper the mapper
   * @return the RndGen
   */
  public <R> RndGen<R> flatMap(Function<? super T, RndGen<? extends R>> mapper) {
    return new RndGen<>() {
      @Override
      public R gen(RandomnessSource in) {
        in = in.withDistribution(distribution);
        return mapper.apply(RndGen.this.generate(in)).generate(in);
      }
    };
  }

  /**
   * Map RndGen.
   *
   * @param <R> the type parameter
   * @param mapper the mapper
   * @return the RndGen
   */
  @SuppressWarnings("unchecked")
  public <R> RndGen<R> map(Function<? super T, ? extends R> mapper) {
    return new RndGen<R>((Describing<R>) description) {
      @Override
      public R gen(RandomnessSource in) {
        return mapper.apply(RndGen.this.generate(in));
      }
    };
  }

  /**
   * With distribution RndGen.
   *
   * @param distribution the distribution
   * @return the RndGen
   */
  public RndGen<T> withDistribution(Distribution distribution) {

    this.distribution = distribution;
    if (COLLECT_COUNTS) {
      this.collectKey = description + OPEN_PAREN + distribution + CLOSE_PAREN;
    }
    return this;
  }

  /**
   * With collector rnd gen.
   *
   * @param collector the collector
   * @return the rnd gen
   */
  public RndGen<T> withCollector(RndCollector<T> collector) {
    if (BaseBenchState.DEBUG) {
      log("setting collector on " + this);
    }
    this.collector = collector;
    return this;
  }

  /**
   * To string string.
   *
   * @return the string
   */
  @Override
  public String toString() {
    return "RndGen[desc=" + description.asString(null) + ", dist=" + distribution + ']';
  }

  @Override
  public String asString(T t) {
    if (t == null) {
      return "null";
    }
    return t.toString();
  }

  /**
   * Gets distribution.
   *
   * @return the distribution
   */
  protected Distribution getDistribution() {
    return this.distribution;
  }

  /**
   * Gets description.
   *
   * @return the description
   */
  protected String getDescription() {
    return description.asString(null);
  }
}

/**
 * The interface Describing.
 *
 * @param <G> the type parameter
 */
interface Describing<G> {

  /**
   * As string string.
   *
   * @param t the t
   * @return the string
   */
  String asString(G t);
}

/**
 * The type Solr describing generator.
 *
 * @param <G> the type parameter
 */
class DescribingGenerator<G> implements Describing<G> {

  private final AsString<G> toString;

  /**
   * Instantiates a new Solr describing generator.
   *
   * @param toString the to string
   */
  public DescribingGenerator(AsString<G> toString) {
    this.toString = toString;
  }

  @Override
  public String asString(G t) {
    return toString.asString(t);
  }
}
