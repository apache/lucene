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
import org.apache.lucene.util.Constants;

// this should live in a separate module, really. but for now - since we use
// mr-jars, we have to look up the class by reflection (it isn't visible from this module).
public class PanamaVectorizationProviderService implements VectorizationProviderService {
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

    // only use vector module with Hotspot VM
    if (!Constants.IS_HOTSPOT_VM) {
      return false;
    }

    // don't use vector module with JVMCI (it does not work)
    if (Constants.IS_JVMCI_VM) {
      return false;
    }

    // is the incubator module present and readable (JVM providers may to exclude them or it is
    // build with jlink)
    final var vectorMod = lookupVectorModule();
    if (vectorMod.isEmpty()) {
      return false;
    }
    vectorMod.ifPresent(VectorizationProvider.class.getModule()::addReads);

    // TODO: check for testMode and otherwise fallback to default if slowness could happen

    try {
      return newInstance() != null;
    } catch (Throwable _) {
      return false;
    }
  }

  @Override
  public String name() {
    return "panama";
  }

  @Override
  public VectorizationProvider newInstance() {
    try {
      final var lookup = MethodHandles.lookup();
      final var cls =
          lookup.findClass(
              "org.apache.lucene.internal.vectorization.panama.PanamaVectorizationProvider");
      final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
      return (VectorizationProvider) constr.invoke();
    } catch (Throwable t) {
      // TODO: we should probably check what happened more thoroughly...
      throw new RuntimeException(t);
    }
  }
}
