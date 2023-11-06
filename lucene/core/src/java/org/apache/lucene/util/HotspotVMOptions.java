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

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;

/** Accessor to get Hotspot VM Options (if available). */
final class HotspotVMOptions {
  private HotspotVMOptions() {} // can't construct

  /** True iff the Java VM is based on Hotspot and has the Hotspot MX bean readable by Lucene */
  public static final boolean IS_HOTSPOT_VM;

  /**
   * Returns an optional with the value of a Hotspot VM option. If the VM option does not exist or
   * is not readable, returns an empty optional.
   */
  public static Optional<String> get(String name) {
    return ACCESSOR.apply(Objects.requireNonNull(name, "name"));
  }

  private static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
  private static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";
  private static final Function<String, Optional<String>> ACCESSOR;

  static {
    boolean isHotspot = false;
    Function<String, Optional<String>> accessor = name -> Optional.empty();
    try {
      final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
      // we use reflection for this, because the management factory is not part
      // of java.base module:
      final Object hotSpotBean =
          Class.forName(MANAGEMENT_FACTORY_CLASS)
              .getMethod("getPlatformMXBean", Class.class)
              .invoke(null, beanClazz);
      if (hotSpotBean != null) {
        final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
        final Method getValueMethod = getVMOptionMethod.getReturnType().getMethod("getValue");
        isHotspot = true;
        accessor =
            name -> {
              try {
                final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, name);
                return Optional.of(getValueMethod.invoke(vmOption).toString());
              } catch (@SuppressWarnings("unused")
                  ReflectiveOperationException
                  | RuntimeException e) {
                return Optional.empty();
              }
            };
      }
    } catch (@SuppressWarnings("unused") ReflectiveOperationException | RuntimeException e) {
      isHotspot = false;
      final Logger log = Logger.getLogger(HotspotVMOptions.class.getName());
      final Module module = HotspotVMOptions.class.getModule();
      final ModuleLayer layer = module.getLayer();
      // classpath / unnamed module has no layer, so we need to check:
      if (layer != null
          && layer.findModule("jdk.management").map(module::canRead).orElse(false) == false) {
        log.warning(
            "Lucene cannot access JVM internals to optimize algorithms or calculate object sizes, unless the 'jdk.management' Java module "
                + "is readable [please add 'jdk.management' to modular application either by command line or its module descriptor].");
      } else {
        log.warning(
            "Lucene cannot optimize algorithms or calculate object sizes for JVMs that are not based on Hotspot or a compatible implementation.");
      }
    }
    IS_HOTSPOT_VM = isHotspot;
    ACCESSOR = accessor;
  }
}
