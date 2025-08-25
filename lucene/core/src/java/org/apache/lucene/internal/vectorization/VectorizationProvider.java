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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;

/**
 * A provider of vectorization implementations. Depending on the Java version and availability of
 * vectorization modules in the Java runtime this class provides optimized implementations (using
 * SIMD) of several algorithms used throughout Apache Lucene.
 *
 * <p>Expert: set the {@value #UPPER_JAVA_FEATURE_VERSION_SYSPROP} system property to increase the
 * set of Java versions this class will provide optimized implementations for.
 *
 * @lucene.internal
 */
public abstract class VectorizationProvider {

  static final OptionalInt TESTS_VECTOR_SIZE;
  static final int UPPER_JAVA_FEATURE_VERSION = getUpperJavaFeatureVersion();

  static {
    var vs = OptionalInt.empty();
    try {
      vs =
          Stream.ofNullable(System.getProperty("tests.vectorsize"))
              .filter(Predicate.not(Set.of("", "default")::contains))
              .mapToInt(Integer::parseInt)
              .findAny();
    } catch (SecurityException _) {
      // ignored
    }
    TESTS_VECTOR_SIZE = vs;
  }

  private static final String UPPER_JAVA_FEATURE_VERSION_SYSPROP =
      "org.apache.lucene.vectorization.upperJavaFeatureVersion";
  private static final int DEFAULT_UPPER_JAVA_FEATURE_VERSION = 24;

  private static int getUpperJavaFeatureVersion() {
    int runtimeVersion = DEFAULT_UPPER_JAVA_FEATURE_VERSION;
    try {
      String str = System.getProperty(UPPER_JAVA_FEATURE_VERSION_SYSPROP);
      if (str != null) {
        runtimeVersion = Math.max(Integer.parseInt(str), runtimeVersion);
      }
    } catch (NumberFormatException | SecurityException _) {
      Logger.getLogger(VectorizationProvider.class.getName())
          .warning(
              "Cannot read sysprop "
                  + UPPER_JAVA_FEATURE_VERSION_SYSPROP
                  + ", so the default value will be used.");
    }
    return runtimeVersion;
  }

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

  VectorizationProvider() {
    // no instance/subclass except from this package
  }

  /**
   * Returns a singleton (stateless) {@link VectorUtilSupport} to support SIMD usage in {@link
   * VectorUtil}.
   */
  public abstract VectorUtilSupport getVectorUtilSupport();

  /** Returns a FlatVectorsScorer that supports the Lucene99 format. */
  public abstract FlatVectorsScorer getLucene99FlatVectorsScorer();

  /** Create a new {@link PostingDecodingUtil} for the given {@link IndexInput}. */
  public abstract PostingDecodingUtil newPostingDecodingUtil(IndexInput input) throws IOException;

  // *** Lookup mechanism: ***

  private static final Logger LOG = Logger.getLogger(VectorizationProvider.class.getName());

  // visible for tests
  static VectorizationProvider lookup(boolean testMode) {
    final int runtimeVersion = Runtime.version().feature();
    assert runtimeVersion >= 21;
    if (runtimeVersion <= UPPER_JAVA_FEATURE_VERSION) {
      // only use vector module with Hotspot VM
      if (!Constants.IS_HOTSPOT_VM) {
        LOG.warning(
            "Java runtime is not using Hotspot VM; Java vector incubator API can't be enabled.");
        return new DefaultVectorizationProvider();
      }
      // don't use vector module with JVMCI (it does not work)
      if (Constants.IS_JVMCI_VM) {
        LOG.warning(
            "Java runtime is using JVMCI Compiler; Java vector incubator API can't be enabled.");
        return new DefaultVectorizationProvider();
      }
      // is the incubator module present and readable (JVM providers may to exclude them or it is
      // build with jlink)
      final var vectorMod = lookupVectorModule();
      if (vectorMod.isEmpty()) {
        LOG.warning(
            "Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.");
        return new DefaultVectorizationProvider();
      }
      vectorMod.ifPresent(VectorizationProvider.class.getModule()::addReads);
      // check for testMode and otherwise fallback to default if slowness could happen
      if (!testMode) {
        if (TESTS_VECTOR_SIZE.isPresent()) {
          LOG.warning(
              "Vector bitsize enforcement; using default vectorization provider outside of testMode");
          return new DefaultVectorizationProvider();
        }
        if (Constants.IS_CLIENT_VM) {
          LOG.warning("C2 compiler is disabled; Java vector incubator API can't be enabled");
          return new DefaultVectorizationProvider();
        }
      }
      try {
        // we use method handles with lookup, so we do not need to deal with setAccessible as we
        // have private access through the lookup:
        final var lookup = MethodHandles.lookup();
        final var cls =
            lookup.findClass(
                "org.apache.lucene.internal.vectorization.PanamaVectorizationProvider");
        final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
        try {
          return (VectorizationProvider) constr.invoke();
        } catch (UnsupportedOperationException uoe) {
          // not supported because preferred vector size too small or similar
          LOG.warning("Java vector incubator API was not enabled. " + uoe.getMessage());
          return new DefaultVectorizationProvider();
        } catch (RuntimeException | Error e) {
          throw e;
        } catch (Throwable th) {
          throw new AssertionError(th);
        }
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new LinkageError(
            "PanamaVectorizationProvider is missing correctly typed constructor", e);
      } catch (ClassNotFoundException cnfe) {
        throw new LinkageError("PanamaVectorizationProvider is missing in Lucene JAR file", cnfe);
      }
    } else {
      LOG.warning(
          "You are running with unsupported Java "
              + runtimeVersion
              + ". To make full use of the Vector API, please update Apache Lucene.");
    }
    return new DefaultVectorizationProvider();
  }

  /**
   * Looks up the vector module from Lucene's {@link ModuleLayer} or the root layer (if unnamed).
   */
  private static Optional<Module> lookupVectorModule() {
    return Optional.ofNullable(VectorizationProvider.class.getModule().getLayer())
        .orElse(ModuleLayer.boot())
        .findModule("jdk.incubator.vector");
  }

  // add all possible callers here as FQCN:
  private static final Set<String> VALID_CALLERS =
      Set.of(
          "org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil",
          "org.apache.lucene.util.VectorUtil",
          "org.apache.lucene.codecs.lucene103.Lucene103PostingsReader",
          "org.apache.lucene.codecs.lucene103.PostingIndexInput",
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

    static final VectorizationProvider INSTANCE = lookup(false);
  }
}
