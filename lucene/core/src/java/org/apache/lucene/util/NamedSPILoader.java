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
package org.apache.lucene.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Helper class for loading named SPIs from classpath (e.g. Codec, PostingsFormat).
 *
 * @lucene.internal
 */
public final class NamedSPILoader<S extends NamedSPILoader.NamedSPI> implements Iterable<S> {

  private volatile Map<String, S> services = Collections.emptyMap();
  private final Class<S> clazz;

  public NamedSPILoader(Class<S> clazz) {
    this(clazz, null);
  }

  public NamedSPILoader(Class<S> clazz, ClassLoader classloader) {
    this.clazz = clazz;
    // if clazz' classloader is not a parent of the given one, we scan clazz's classloader, too:
    final ClassLoader clazzClassloader = clazz.getClassLoader();
    if (classloader == null) {
      classloader = clazzClassloader;
    }
    if (clazzClassloader != null
        && !ClassLoaderUtils.isParentClassLoader(clazzClassloader, classloader)) {
      reload(clazzClassloader);
    }
    reload(classloader);
  }

  /**
   * Reloads the internal SPI list from the given {@link ClassLoader}. Changes to the service list
   * are visible after the method ends, all iterators ({@link #iterator()},...) stay consistent.
   *
   * <p><b>NOTE:</b> Only new service providers are added, existing ones are never removed or
   * replaced.
   *
   * <p><em>This method is expensive and should only be called for discovery of new service
   * providers on the given classpath/classloader!</em>
   */
  public void reload(ClassLoader classloader) {
    Objects.requireNonNull(classloader, "classloader");
    final Map<String, S> services = this.services;
    final LinkedHashMap<String, S> newServices = new LinkedHashMap<>();
    for (final S service : ServiceLoader.load(clazz, classloader)) {
      final String name = service.getName();
      // only add the first one for each name, later services will be ignored
      // unless the later-found service allows to replace the previous.
      var prevNew = newServices.get(name);
      if (prevNew == null || service.replace(prevNew)) {
        if (!services.containsKey(name)) {
          checkServiceName(name);
          newServices.put(name, service);
        }
      }
    }
    Map<String, S> finalServices = new LinkedHashMap<>(services.size() + newServices.size());
    finalServices.putAll(services);
    finalServices.putAll(newServices);
    this.services = Collections.unmodifiableMap(finalServices);
  }

  /** Validates that a service name meets the requirements of {@link NamedSPI} */
  public static void checkServiceName(String name) {
    // based on harmony charset.java
    if (name.length() >= 128) {
      throw new IllegalArgumentException(
          "Illegal service name: '" + name + "' is too long (must be < 128 chars).");
    }
    for (int i = 0, len = name.length(); i < len; i++) {
      char c = name.charAt(i);
      if (!isLetterOrDigit(c)) {
        throw new IllegalArgumentException(
            "Illegal service name: '" + name + "' must be simple ascii alphanumeric.");
      }
    }
  }

  /** Checks whether a character is a letter or digit (ascii) which are defined in the spec. */
  private static boolean isLetterOrDigit(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9');
  }

  public S lookup(String name) {
    final S service = services.get(name);
    if (service != null) return service;
    throw new IllegalArgumentException(
        "An SPI class of type "
            + clazz.getName()
            + " with name '"
            + name
            + "' does not exist."
            + "  You need to add the corresponding JAR file supporting this SPI to your classpath."
            + "  The current classpath supports the following names: "
            + availableServices());
  }

  public Set<String> availableServices() {
    return services.keySet();
  }

  @Override
  public Iterator<S> iterator() {
    return services.values().iterator();
  }

  /**
   * Interface to support {@link NamedSPILoader#lookup(String)} by name.
   *
   * <p>Names must be all ascii alphanumeric, and less than 128 characters in length.
   */
  public interface NamedSPI {
    String getName();

    /**
     * Allows a service provider found during the load iteration to replace another provider found
     * earlier during the same load iteration, before the providers becomes externally visible, via
     * {@link #iterator()} or {@link #availableServices()}.
     *
     * <p>This allows for finer-grain selection within a particular load iteration, between new
     * service providers of the same name found later in that iteration.
     *
     * <p>Note, this replacement does not break the externally observable invariant of {@link
     * #reload(ClassLoader) reload} - that only new service providers are added, existing ones are
     * never removed or replaced. The replacement here is within the same load iteration.
     *
     * <p>Among the usages, this mechanism can be used to facilitate ordering between service
     * providers within the same module layer, since the iteration order of services within a module
     * layer is undefined. Whereas applications deployed as unnamed modules can depend upon the
     * ordering within the classpath .
     *
     * <p>The default implementation returns false - do not replace.
     *
     * @param previous the previous service provider, never null
     */
    default boolean replace(NamedSPI previous) {
      return false;
    }
  }
}
