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
package org.apache.lucene.distribution;

import java.io.IOException;
import java.io.InputStream;
import java.lang.module.Configuration;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Sanity checks concerning the distribution's binary artifacts (modules).
 *
 * <p>We do <em>not</em> want this module to depend on any Lucene classes (including the test
 * framework) so that there is no risk of accidental classpath space pollution. This also means the
 * default {@code LuceneTestCase} configuration setup is not used (you have to annotate test for
 * JUnit, for example).
 */
public class TestModularLayer {
  /** A path to a directory with an expanded Lucene distribution. */
  private static final String DISTRIBUTION_PROPERTY = "lucene.distribution.dir";

  /** The expected distribution version of Lucene modules. */
  private static final String VERSION_PROPERTY = "lucene.distribution.version";

  /**
   * {@link ModuleFinder} resolving both {@link #coreModulesFinder} and {@link
   * #thirdPartyModulesFinder}.
   */
  private static ModuleFinder allModulesFinder;

  /** Only core Lucene modules, no third party modules. */
  private static Set<ModuleReference> allCoreModules;

  /** {@link ModuleFinder} resolving only the Lucene modules. */
  private static ModuleFinder coreModulesFinder;

  /** {@link ModuleFinder} resolving only the third party modules. */
  private static ModuleFinder thirdPartyModulesFinder;

  /** Ensure Lucene classes are not visible. */
  @BeforeClass
  public static void checkLuceneNotInClasspath() {
    Assertions.assertThatThrownBy(
            () -> {
              Class.forName("org.apache.lucene.index.IndexWriter");
            })
        .isInstanceOf(ClassNotFoundException.class);
  }

  /**
   * We accept external properties that point to the assembled set of distribution modules and to
   * their expected version. These properties are collected and passed by gradle but can be provided
   * manually (for IDE launches).
   */
  @BeforeClass
  public static void checkModulePathProvided() {
    String modulesPropertyValue = System.getProperty(DISTRIBUTION_PROPERTY);
    if (modulesPropertyValue == null) {
      throw new AssertionError(DISTRIBUTION_PROPERTY + " property is required for this test.");
    }

    Path modulesPath = Paths.get(modulesPropertyValue).resolve("modules");
    if (!Files.isDirectory(modulesPath)) {
      throw new AssertionError(
          DISTRIBUTION_PROPERTY
              + " property does not point to a directory where this path is present: "
              + modulesPath.toAbsolutePath());
    }

    Path thirdPartyModulesPath = Paths.get(modulesPropertyValue).resolve("modules-thirdparty");
    if (!Files.isDirectory(thirdPartyModulesPath)) {
      throw new AssertionError(
          DISTRIBUTION_PROPERTY
              + " property does not point to a directory where this path is present: "
              + thirdPartyModulesPath.toAbsolutePath());
    }

    coreModulesFinder = ModuleFinder.of(modulesPath);
    allCoreModules = coreModulesFinder.findAll();

    thirdPartyModulesFinder = ModuleFinder.of(thirdPartyModulesPath);

    allModulesFinder = ModuleFinder.compose(coreModulesFinder, thirdPartyModulesFinder);
  }

  /** Make sure all module names remain constant, even if we reorganize the build. */
  @Test
  public void testExpectedDistributionModuleNames() {
    Assertions.assertThat(
            allCoreModules.stream().map(module -> module.descriptor().name()).sorted())
        .containsExactly(
            "org.apache.lucene.analysis.common",
            "org.apache.lucene.analysis.icu",
            "org.apache.lucene.analysis.kuromoji",
            "org.apache.lucene.analysis.morfologik",
            "org.apache.lucene.analysis.nori",
            "org.apache.lucene.analysis.opennlp",
            "org.apache.lucene.analysis.phonetic",
            "org.apache.lucene.analysis.smartcn",
            "org.apache.lucene.analysis.stempel",
            "org.apache.lucene.backward_codecs",
            "org.apache.lucene.benchmark",
            "org.apache.lucene.classification",
            "org.apache.lucene.codecs",
            "org.apache.lucene.core",
            "org.apache.lucene.demo",
            "org.apache.lucene.expressions",
            "org.apache.lucene.facet",
            "org.apache.lucene.grouping",
            "org.apache.lucene.highlighter",
            "org.apache.lucene.join",
            "org.apache.lucene.luke",
            "org.apache.lucene.memory",
            "org.apache.lucene.misc",
            "org.apache.lucene.monitor",
            "org.apache.lucene.queries",
            "org.apache.lucene.queryparser",
            "org.apache.lucene.replicator",
            "org.apache.lucene.sandbox",
            "org.apache.lucene.spatial3d",
            "org.apache.lucene.spatial_extras",
            "org.apache.lucene.suggest");
  }

  /** Ensure all modules have the same (expected) version. */
  @Test
  public void testAllModulesHaveExpectedVersion() {
    String luceneBuildVersion = System.getProperty(VERSION_PROPERTY);
    Assumptions.assumeThat(luceneBuildVersion).isNotNull();
    for (var module : allCoreModules) {
      Assertions.assertThat(module.descriptor().rawVersion().orElse(null))
          .as("Version of module: " + module.descriptor().name())
          .isEqualTo(luceneBuildVersion);
    }
  }

  /** Ensure SPIs are equal for the module and classpath layer. */
  @Test
  public void testModularAndClasspathProvidersAreConsistent() throws IOException {
    for (var module : allCoreModules) {
      TreeMap<String, TreeSet<String>> modularProviders = getModularServiceProviders(module);
      TreeMap<String, TreeSet<String>> classpathProviders = getClasspathServiceProviders(module);

      // Compare services first so that the exception is shorter.
      Assertions.assertThat(modularProviders.keySet())
          .as("Modular services in module: " + module.descriptor().name())
          .containsExactlyInAnyOrderElementsOf(classpathProviders.keySet());

      // We're sure the services correspond to each other. Now, for each service, compare the
      // providers.
      for (var service : modularProviders.keySet()) {
        Assertions.assertThat(modularProviders.get(service))
            .as(
                "Modular providers of service "
                    + service
                    + " in module: "
                    + module.descriptor().name())
            .containsExactlyInAnyOrderElementsOf(classpathProviders.get(service));
      }
    }
  }

  private TreeMap<String, TreeSet<String>> getClasspathServiceProviders(ModuleReference module)
      throws IOException {
    TreeMap<String, TreeSet<String>> services = new TreeMap<>();
    Pattern serviceEntryPattern = Pattern.compile("META-INF/services/(?<serviceName>.+)");
    try (ModuleReader reader = module.open();
        Stream<String> entryStream = reader.list()) {
      List<String> serviceProviderEntryList =
          entryStream
              .filter(entry -> serviceEntryPattern.matcher(entry).find())
              .collect(Collectors.toList());

      for (String entry : serviceProviderEntryList) {
        List<String> implementations;
        try (InputStream is = reader.open(entry).get()) {
          implementations =
              Arrays.stream(new String(is.readAllBytes(), StandardCharsets.UTF_8).split("\n"))
                  .map(String::trim)
                  .filter(line -> !line.isBlank() && !line.startsWith("#"))
                  .collect(Collectors.toList());
        }

        Matcher matcher = serviceEntryPattern.matcher(entry);
        if (!matcher.find()) {
          throw new AssertionError("Impossible.");
        }
        String service = matcher.group("serviceName");
        services.computeIfAbsent(service, k -> new TreeSet<>()).addAll(implementations);
      }
    }

    return services;
  }

  private static TreeMap<String, TreeSet<String>> getModularServiceProviders(
      ModuleReference module) {
    return module.descriptor().provides().stream()
        .collect(
            Collectors.toMap(
                ModuleDescriptor.Provides::service,
                provides -> new TreeSet<>(provides.providers()),
                (k, v) -> {
                  throw new RuntimeException();
                },
                TreeMap::new));
  }

  /** This tracks our progress of transition to modules. */
  @Test
  public void testAllJarsAreNamedModules() {
    Assertions.assertThat(
            allCoreModules.stream()
                .map(
                    module ->
                        String.format(
                            Locale.ROOT,
                            "%-10s %s",
                            module.descriptor().isAutomatic() ? "AUTOMATIC" : "NAMED",
                            module.descriptor().name()))
                .sorted())
        .containsExactly(
            "NAMED      org.apache.lucene.analysis.common",
            "NAMED      org.apache.lucene.analysis.icu",
            "NAMED      org.apache.lucene.analysis.kuromoji",
            "NAMED      org.apache.lucene.analysis.morfologik",
            "NAMED      org.apache.lucene.analysis.nori",
            "NAMED      org.apache.lucene.analysis.opennlp",
            "NAMED      org.apache.lucene.analysis.phonetic",
            "NAMED      org.apache.lucene.analysis.smartcn",
            "NAMED      org.apache.lucene.analysis.stempel",
            "NAMED      org.apache.lucene.backward_codecs",
            "NAMED      org.apache.lucene.benchmark",
            "NAMED      org.apache.lucene.classification",
            "NAMED      org.apache.lucene.codecs",
            "NAMED      org.apache.lucene.core",
            "NAMED      org.apache.lucene.demo",
            "NAMED      org.apache.lucene.expressions",
            "NAMED      org.apache.lucene.facet",
            "NAMED      org.apache.lucene.grouping",
            "NAMED      org.apache.lucene.highlighter",
            "NAMED      org.apache.lucene.join",
            "NAMED      org.apache.lucene.luke",
            "NAMED      org.apache.lucene.memory",
            "NAMED      org.apache.lucene.misc",
            "NAMED      org.apache.lucene.monitor",
            "NAMED      org.apache.lucene.queries",
            "NAMED      org.apache.lucene.queryparser",
            "NAMED      org.apache.lucene.replicator",
            "NAMED      org.apache.lucene.sandbox",
            "NAMED      org.apache.lucene.spatial3d",
            "NAMED      org.apache.lucene.spatial_extras",
            "NAMED      org.apache.lucene.suggest");
  }

  /** Tries to actually load/ resolve the modules and verify runtime modular checks. */
  @Test
  public void testRuntimeModuleCheckAssertions() throws Exception {
    String luceneCoreModule = "org.apache.lucene.core";

    ModuleLayer parent = ModuleLayer.boot();
    Configuration configuration =
        parent
            .configuration()
            .resolve(allModulesFinder, ModuleFinder.of(), Set.of(luceneCoreModule));
    ClassLoader classLoader = new URLClassLoader(new URL[] {}, ClassLoader.getSystemClassLoader());
    ModuleLayer layer = parent.defineModulesWithOneLoader(configuration, classLoader);
    Module luceneCore = layer.findModule(luceneCoreModule).get();

    @SuppressWarnings("unchecked")
    Supplier<Map<String, String>> instance =
        (Supplier<Map<String, String>>)
            Class.forName(luceneCore, "org.apache.lucene.internal.ModuleCheck")
                .getDeclaredConstructor()
                .newInstance();

    Assertions.assertThat(instance.get())
        .containsExactlyInAnyOrderEntriesOf(
            Map.ofEntries(
                Map.entry("hello", "world"),
                // I assume all test platforms will actually be able to support unmap... We could
                // replace this with a more direct test trying to access Unsafe but it seems less
                // obvious.
                Map.entry("unmap.supported", "true")));
  }
}
