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

package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import java.lang.StackWalker.StackFrame;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;

/**
 * A provider of vectorization implementations. Depending on the Java version and availability of
 * vectorization modules in the Java runtime this class provides optimized implementations (using
 * SIMD) of several algorithms used throughout Apache Lucene. *
 *
 * @lucene.internal
 */
public abstract class VectorizationProvider {
  /**
   * Prefer any provider that is available and supported. If nothing is, try panama, followed by
   * default
   */
  public static final String DEFAULT_VECTORIZATION_PROVIDER_ORDER_PREFERENCE = "*,panama,default";

  public static final String VECTORIZATION_PROVIDER_ORDER_PREFERENCE_PROPERTY =
      "lucene.vectorization";

  /**
   * Returns the default instance of the provider matching vectorization possibilities of actual
   * runtime.
   *
   * @throws UnsupportedOperationException if the singleton getter is not called from known Lucene
   *     classes.
   */
  public static VectorizationProvider getInstance() {
    ensureCaller();
    return Objects.requireNonNull(
        Holder.INSTANCE, "call to getInstance() from subclass of VectorizationProvider");
  }

  protected VectorizationProvider() {
    // no instance/subclass except from this package
  }

  /**
   * Returns a singleton (stateless) {@link VectorUtilSupport} to support SIMD usage in {@link
   * VectorUtil}.
   */
  public abstract VectorUtilSupport getVectorUtilSupport();

  /** Returns a FlatVectorsScorer that supports the Lucene99 format. */
  public abstract FlatVectorsScorer getLucene99FlatVectorsScorer();

  /** Returns a FlatVectorsScorer that supports the Lucene99 format. */
  public abstract FlatVectorsScorer getLucene99ScalarQuantizedVectorsScorer();

  /** Create a new {@link PostingDecodingUtil} for the given {@link IndexInput}. */
  public abstract PostingDecodingUtil newPostingDecodingUtil(IndexInput input) throws IOException;

  // *** Lookup mechanism: ***

  // visible for tests
  static VectorizationProvider lookup(String providerPreference) {
    var implementations =
        ServiceLoader.load(VectorizationProviderService.class).stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toMap(e -> e.name(), e -> e));

    implementations = new TreeMap<>(implementations);

    ArrayDeque<VectorizationProviderService> inReversePreferenceOrder = new ArrayDeque<>();

    // force a specific implementation here or give order of preference.
    var options = Arrays.asList(providerPreference.split(",")).reversed().iterator();

    while (options.hasNext()) {
      var s = options.next();
      if (s.equals("*")) {
        implementations.values().forEach(inReversePreferenceOrder::addFirst);
        break;
      } else {
        var impl = implementations.remove(s);
        if (impl != null) {
          inReversePreferenceOrder.addFirst(impl);
        }
      }
    }

    while (!inReversePreferenceOrder.isEmpty()) {
      var impl = inReversePreferenceOrder.removeFirst();
      if (impl.isUsable()) {
        return impl.newInstance();
      }
    }

    throw new RuntimeException(
        "No vectorization provider matches this preference: " + providerPreference);
  }

  // add all possible callers here as FQCN:
  private static final Set<String> VALID_CALLERS =
      Set.of(
          "org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil",
          "org.apache.lucene.util.VectorUtil",
          "org.apache.lucene.codecs.lucene104.Lucene104PostingsReader",
          "org.apache.lucene.codecs.lucene104.PostingIndexInput",
          "org.apache.lucene.tests.util.TestSysoutsLimits");

  private static void ensureCaller() {
    final boolean validCaller =
        StackWalker.getInstance()
            .walk(
                s ->
                    s.skip(2)
                        .limit(1)
                        .map(StackFrame::getClassName)
                        .allMatch(VALID_CALLERS::contains));
    if (!validCaller) {
      throw new UnsupportedOperationException(
          "VectorizationProvider is internal and can only be used by known Lucene classes.");
    }
  }

  /** This static holder class prevents classloading deadlock. */
  private static final class Holder {
    private Holder() {}

    static final VectorizationProvider INSTANCE =
        lookup(
            System.getProperty(
                VECTORIZATION_PROVIDER_ORDER_PREFERENCE_PROPERTY,
                DEFAULT_VECTORIZATION_PROVIDER_ORDER_PREFERENCE));
  }
}
