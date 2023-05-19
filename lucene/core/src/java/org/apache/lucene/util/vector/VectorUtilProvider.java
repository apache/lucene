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

package org.apache.lucene.util.vector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.logging.Logger;

public interface VectorUtilProvider {

  /** Calculates the dot product of the given float arrays. */
  float dotProduct(float[] a, float[] b);

  // -- provider lookup mechanism

  Logger LOG = Logger.getLogger(VectorUtilProvider.class.getName());

  static VectorUtilProvider lookup() {
    final int runtimeVersion = Runtime.version().feature();
    if (runtimeVersion == 20 && vectorModulePresentAndReadable()) {
      try {
        final var lookup = MethodHandles.lookup();
        final var cls = lookup.findClass("org.apache.lucene.util.vector.PanamaVectorUtilProvider");
        // we use method handles, so we do not need to deal with setAccessible as we have private
        // access through the lookup:
        final var constr = lookup.findConstructor(cls, MethodType.methodType(void.class));
        try {
          return (VectorUtilProvider) constr.invoke();
        } catch (RuntimeException | Error e) {
          throw e;
        } catch (Throwable th) {
          throw new AssertionError(th);
        }
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new LinkageError(
            "PanamaVectorUtilProvider is missing correctly typed constructor", e);
      } catch (ClassNotFoundException cnfe) {
        throw new LinkageError("PanamaVectorUtilProvider is missing in Lucene JAR file", cnfe);
      }
    } else if (runtimeVersion >= 21) {
      LOG.warning(
          "You are running with Java 21 or later. To make full use of the Vector API, please update Apache Lucene.");
    }
    return new DefaultVectorUtilProvider();
  }

  static boolean vectorModulePresentAndReadable() {
    var opt =
        ModuleLayer.boot().modules().stream()
            .filter(m -> m.getName().equals("jdk.incubator.vector"))
            .findFirst();
    if (opt.isPresent()) {
      VectorUtilProvider.class.getModule().addReads(opt.get());
      return true;
    }
    return false;
  }
}
