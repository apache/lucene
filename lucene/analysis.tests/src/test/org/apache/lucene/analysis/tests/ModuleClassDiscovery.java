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
package org.apache.lucene.analysis.tests;

import java.io.IOException;
import java.lang.module.ResolvedModule;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Assert;

/** Discovers all classes from the module graph and loads them (without initialization) */
abstract class ModuleClassDiscovery {

  private static final Module THIS_MODULE = ModuleClassDiscovery.class.getModule();
  private static final ModuleLayer LAYER = THIS_MODULE.getLayer();
  private static final SortedMap<String, ResolvedModule> ALL_ANALYSIS_MODULES;

  private static final Predicate<String> ALLOW_MODULES =
      name ->
          name.equals("org.apache.lucene.core") || name.startsWith("org.apache.lucene.analysis.");

  static {
    Assert.assertTrue(
        "Analysis integration tests must run in Java Module System as named module",
        THIS_MODULE.isNamed());
    Assert.assertNotNull("Module layer is missing", LAYER);

    var mods = new TreeMap<String, ResolvedModule>();
    discoverAnalysisModules(LAYER, mods);
    ALL_ANALYSIS_MODULES = Collections.unmodifiableSortedMap(mods);
    if (LuceneTestCase.VERBOSE) {
      System.out.println(
          "Discovered the following analysis modules: " + ALL_ANALYSIS_MODULES.keySet());
    }
  }

  private static void discoverAnalysisModules(
      ModuleLayer layer, Map<String, ResolvedModule> result) {
    for (var mod : layer.configuration().modules()) {
      String name = mod.name();
      if (ALLOW_MODULES.test(name) && !Objects.equals(name, THIS_MODULE.getName())) {
        result.put(name, mod);
      }
    }
    for (var parent : layer.parents()) {
      discoverAnalysisModules(parent, result);
    }
  }

  /** Finds all classes in package across all analysis modules */
  public static List<Class<?>> getClassesForPackage(String pkgname) throws IOException {
    final var prefix = pkgname.concat(".");
    final var classes = new ArrayList<Class<?>>();
    for (var resolvedModule : ALL_ANALYSIS_MODULES.values()) {
      final var module = LAYER.findModule(resolvedModule.name()).orElseThrow();
      try (var reader = resolvedModule.reference().open()) {
        reader
            .list()
            .filter(entry -> entry.endsWith(".class"))
            .map(entry -> entry.substring(0, entry.length() - 6).replace('/', '.'))
            .filter(clazzname -> clazzname.startsWith(prefix))
            .sorted()
            .map(
                clazzname ->
                    Objects.requireNonNull(
                        Class.forName(module, clazzname),
                        "Class '" + clazzname + "' not found in module '" + module.getName() + "'"))
            .forEach(classes::add);
      }
    }
    Assert.assertFalse("No classes found in package:" + pkgname, classes.isEmpty());
    return classes;
  }
}
