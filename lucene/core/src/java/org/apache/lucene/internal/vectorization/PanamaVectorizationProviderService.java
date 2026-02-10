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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import java.util.logging.Logger;
import org.apache.lucene.util.Constants;

/**
 * Expert: set the {@value #UPPER_JAVA_FEATURE_VERSION_SYSPROP} system property to increase the set
 * of Java versions this class will provide optimized implementations for.
 */
public class PanamaVectorizationProviderService implements VectorizationProviderService {
  //
  // All the panama vectorization classes should live in a separate module, really. However, this
  // would require another jar and would complicate life for many people. It is also de-facto
  // default on all modern JVMs, so we will let it live in the core.
  //

  private static final Logger LOG =
      Logger.getLogger(PanamaVectorizationProviderService.class.getName());

  private static final String UPPER_JAVA_FEATURE_VERSION_SYSPROP =
      "org.apache.lucene.vectorization.upperJavaFeatureVersion";

  private static final int DEFAULT_UPPER_JAVA_FEATURE_VERSION = 25;

  static final int UPPER_JAVA_FEATURE_VERSION = getUpperJavaFeatureVersion();

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
   * Looks up the vector module from Lucene's {@link ModuleLayer} or the root layer (if unnamed).
   */
  private static Optional<Module> lookupVectorModule() {
    return Optional.ofNullable(VectorizationProvider.class.getModule().getLayer())
        .orElse(ModuleLayer.boot())
        .findModule("jdk.incubator.vector");
  }

  @Override
  public boolean isUsable() {
    final int runtimeVersion = Runtime.version().feature();
    assert runtimeVersion >= 21;

    if (runtimeVersion > UPPER_JAVA_FEATURE_VERSION) {
      LOG.warning(
          "You are running with unsupported Java "
              + runtimeVersion
              + ". To make full use of the Vector API, please update Apache Lucene.");
      return false;
    }

    // only use vector module with Hotspot VM
    if (!Constants.IS_HOTSPOT_VM) {
      LOG.warning(
          "Java runtime is not using Hotspot VM; Java vector incubator API can't be enabled.");
      return false;
    }

    // don't use vector module with JVMCI (it does not work)
    if (Constants.IS_JVMCI_VM) {
      LOG.warning(
          "Java runtime is using JVMCI Compiler; Java vector incubator API can't be enabled.");
      return false;
    }

    if (Constants.IS_CLIENT_VM) {
      LOG.warning("C2 compiler is disabled; Java vector incubator API can't be enabled");
      return false;
    }

    // is the incubator module present and readable (JVM providers may to exclude them, or it is
    // built with jlink)
    final var vectorMod = lookupVectorModule();
    if (vectorMod.isEmpty()) {
      LOG.warning(
          "Java vector incubator module is not readable. For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API.");
      return false;
    }
    vectorMod.ifPresent(VectorizationProvider.class.getModule()::addReads);

    try {
      return newInstance() != null;
    } catch (Throwable ex) {
      throw new RuntimeException(
          "All the conditions to run panama seem to be met but the vectorization provider cannot be instantiated?",
          ex);
    }
  }

  @Override
  public String name() {
    return "panama";
  }

  @Override
  public VectorizationProvider newInstance() {
    // This is intentional because the implementation lives in multi-release JAR space; we may
    // substitute implementations for different JDKs until the API stabilizes. IDEs and gradle
    // don't support mr-jars too well so this is easier.

    try {
      final var lookup = MethodHandles.lookup();
      final var cls =
          lookup.findClass(
              "org.apache.lucene.internal.vectorization.panama.PanamaVectorizationProvider");
      final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
      return (VectorizationProvider) constr.invoke();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
}
