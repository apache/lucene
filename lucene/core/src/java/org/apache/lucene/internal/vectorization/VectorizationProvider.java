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

import java.lang.Runtime.Version;
import java.lang.StackWalker.StackFrame;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;

/**
 * A provider of vectorization implementations. Depending on the Java version and availability of
 * vectorization modules in the Java runtime this class provides optimized implementations (using
 * SIMD) of several algorithms used throughout Apache Lucene.
 *
 * @lucene.internal
 */
public abstract class VectorizationProvider {

  static final OptionalInt TESTS_VECTOR_SIZE;
  static final boolean TESTS_FORCE_INTEGER_VECTORS;

  static {
    var vs = OptionalInt.empty();
    try {
      vs =
          Stream.ofNullable(System.getProperty("tests.vectorsize"))
              .filter(Predicate.not(Set.of("", "default")::contains))
              .mapToInt(Integer::parseInt)
              .findAny();
    } catch (
        @SuppressWarnings("unused")
        SecurityException se) {
      // ignored
    }
    TESTS_VECTOR_SIZE = vs;

    boolean enforce = false;
    try {
      enforce = Boolean.getBoolean("tests.forceintegervectors");
    } catch (
        @SuppressWarnings("unused")
        SecurityException se) {
      // ignored
    }
    TESTS_FORCE_INTEGER_VECTORS = enforce;
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

  // *** Lookup mechanism: ***

  private static final Logger LOG = Logger.getLogger(VectorizationProvider.class.getName());

  /** The minimal version of Java that has the bugfix for JDK-8301190. */
  private static final Version VERSION_JDK8301190_FIXED = Version.parse("20.0.2");

  // visible for tests
  static VectorizationProvider lookup(boolean testMode) {
    final int runtimeVersion = Runtime.version().feature();
    if (runtimeVersion >= 20 && runtimeVersion <= 22) {
      // is locale sane (only buggy in Java 20)
      if (isAffectedByJDK8301190()) {
        LOG.warning(
            "Java runtime is using a buggy default locale; Java vector incubator API can't be enabled: "
                + Locale.getDefault());
        return new DefaultVectorizationProvider();
      }
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
        if (TESTS_VECTOR_SIZE.isPresent() || TESTS_FORCE_INTEGER_VECTORS) {
          LOG.warning(
              "Vector bitsize and/or integer vectors enforcement; using default vectorization provider outside of testMode");
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
    } else if (runtimeVersion >= 23) {
      LOG.warning(
          "You are running with Java 23 or later. To make full use of the Vector API, please update Apache Lucene.");
    } else if (lookupVectorModule().isPresent()) {
      LOG.warning(
          "Java vector incubator module was enabled by command line flags, but your Java version is too old: "
              + runtimeVersion);
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

  /**
   * Check if runtime is affected by JDK-8301190 (avoids assertion when default language is say
   * "tr").
   */
  private static boolean isAffectedByJDK8301190() {
    return VERSION_JDK8301190_FIXED.compareToIgnoreOptional(Runtime.version()) > 0
        && !Objects.equals("I", "i".toUpperCase(Locale.getDefault()));
  }

  // add all possible callers here as FQCN:
  private static final Set<String> VALID_CALLERS =
      Set.of(
          "org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil",
          "org.apache.lucene.util.VectorUtil");

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
