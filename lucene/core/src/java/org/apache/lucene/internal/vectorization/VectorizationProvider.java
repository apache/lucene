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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.VectorUtil;

/**
 * A provider of vectorization implementations. Depending on the Java version and availability of
 * vectorization modules in the Java runtime this class provides optimized implementations (using
 * SIMD) of several algorithms used throughout Apache Lucene.
 *
 * @lucene.internal
 */
public abstract class VectorizationProvider {

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

  // *** Lookup mechanism: ***

  private static final Logger LOG = Logger.getLogger(VectorizationProvider.class.getName());

  /** The minimal version of Java that has the bugfix for JDK-8301190. */
  private static final Version VERSION_JDK8301190_FIXED = Version.parse("20.0.2");

  // visible for tests
  static VectorizationProvider lookup(boolean testMode) {
    final int runtimeVersion = Runtime.version().feature();
    if (runtimeVersion >= 20 && runtimeVersion <= 21) {
      // is locale sane (only buggy in Java 20)
      if (isAffectedByJDK8301190()) {
        LOG.warning(
            "Java runtime is using a buggy default locale; Java vector incubator API can't be enabled: "
                + Locale.getDefault());
        return new DefaultVectorizationProvider();
      }
      // is the incubator module present and readable (JVM providers may to exclude them or it is
      // build with jlink)
      if (!vectorModulePresentAndReadable()) {
        LOG.warning(
            "Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.");
        return new DefaultVectorizationProvider();
      }
      if (!testMode && isClientVM()) {
        LOG.warning("C2 compiler is disabled; Java vector incubator API can't be enabled");
        return new DefaultVectorizationProvider();
      }
      try {
        // we use method handles with lookup, so we do not need to deal with setAccessible as we
        // have private access through the lookup:
        final var lookup = MethodHandles.lookup();
        final var cls =
            lookup.findClass(
                "org.apache.lucene.internal.vectorization.PanamaVectorizationProvider");
        final var constr =
            lookup.findConstructor(cls, MethodType.methodType(void.class, boolean.class));
        try {
          return (VectorizationProvider) constr.invoke(testMode);
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
    } else if (runtimeVersion >= 22) {
      LOG.warning(
          "You are running with Java 22 or later. To make full use of the Vector API, please update Apache Lucene.");
    }
    return new DefaultVectorizationProvider();
  }

  private static boolean vectorModulePresentAndReadable() {
    var opt =
        ModuleLayer.boot().modules().stream()
            .filter(m -> m.getName().equals("jdk.incubator.vector"))
            .findFirst();
    if (opt.isPresent()) {
      VectorizationProvider.class.getModule().addReads(opt.get());
      return true;
    }
    return false;
  }

  /**
   * Check if runtime is affected by JDK-8301190 (avoids assertion when default language is say
   * "tr").
   */
  private static boolean isAffectedByJDK8301190() {
    return VERSION_JDK8301190_FIXED.compareToIgnoreOptional(Runtime.version()) > 0
        && !Objects.equals("I", "i".toUpperCase(Locale.getDefault()));
  }

  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "security manager")
  private static boolean isClientVM() {
    try {
      final PrivilegedAction<Boolean> action =
          () -> System.getProperty("java.vm.info", "").contains("emulated-client");
      return AccessController.doPrivileged(action);
    } catch (
        @SuppressWarnings("unused")
        SecurityException e) {
      LOG.warning(
          "SecurityManager denies permission to 'java.vm.info' system property, so state of C2 compiler can't be detected. "
              + "In case of performance issues allow access to this property.");
      return false;
    }
  }

  // add all possible callers here as FQCN:
  private static final Set<String> VALID_CALLERS = Set.of("org.apache.lucene.util.VectorUtil");

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
