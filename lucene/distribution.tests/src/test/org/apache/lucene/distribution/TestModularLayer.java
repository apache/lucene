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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.AfterClass;
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
public class TestModularLayer extends AbstractLuceneDistributionTest {
  /** All Lucene modules (including the test framework), no third party modules. */
  private static Set<ModuleReference> allLuceneModules;

  /** {@link ModuleFinder} resolving only the core Lucene modules. */
  private static ModuleFinder allLuceneModulesFinder;

  /** {@link ModuleFinder} resolving Lucene core and third party dependencies. */
  private static ModuleFinder luceneCoreAndThirdPartyModulesFinder;

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

    Path modulesPath = getDistributionPath().resolve("modules");
    if (!Files.isDirectory(modulesPath)) {
      throw new AssertionError(
          DISTRIBUTION_PROPERTY
              + " property does not point to a directory where this path is present: "
              + modulesPath.toAbsolutePath());
    }

    Path testModulesPath = Paths.get(modulesPropertyValue).resolve("modules-test-framework");
    if (!Files.isDirectory(testModulesPath)) {
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

    allLuceneModulesFinder = ModuleFinder.of(modulesPath, testModulesPath);
    allLuceneModules = allLuceneModulesFinder.findAll();

    luceneCoreAndThirdPartyModulesFinder = ModuleFinder.of(modulesPath, thirdPartyModulesPath);
  }

  @AfterClass
  public static void cleanup() {
    allLuceneModules = null;
    luceneCoreAndThirdPartyModulesFinder = null;
    allLuceneModulesFinder = null;
  }

  /** Make sure all published module names remain constant, even if we reorganize the build. */
  @Test
  public void testExpectedDistributionModuleNames() {
    Assertions.assertThat(
            allLuceneModules.stream().map(module -> module.descriptor().name()).sorted())
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
            "org.apache.lucene.suggest",
            "org.apache.lucene.test_framework");
  }

  /** Try to instantiate the demo classes so that we make sure their module layer is complete. */
  @Test
  public void testDemoClassesCanBeLoaded() {
    ModuleLayer bootLayer = ModuleLayer.boot();
    Assertions.assertThatNoException()
        .isThrownBy(
            () -> {
              String demoModuleId = "org.apache.lucene.demo";

              Configuration configuration =
                  bootLayer
                      .configuration()
                      .resolve(
                          luceneCoreAndThirdPartyModulesFinder,
                          ModuleFinder.of(),
                          List.of(demoModuleId));

              ModuleLayer layer =
                  bootLayer.defineModulesWithOneLoader(
                      configuration, ClassLoader.getSystemClassLoader());

              for (String className :
                  List.of(
                      "org.apache.lucene.demo.IndexFiles",
                      "org.apache.lucene.demo.SearchFiles",
                      "org.apache.lucene.index.CheckIndex")) {
                Assertions.assertThat(layer.findLoader(demoModuleId).loadClass(className))
                    .isNotNull();
              }
            });
  }

  /** Make sure we don't publish automatic modules. */
  @Test
  public void testAllCoreModulesAreNamedModules() {
    Assertions.assertThat(allLuceneModules)
        .allSatisfy(
            module -> {
              Assertions.assertThat(module.descriptor().isAutomatic())
                  .as(module.descriptor().name())
                  .isFalse();
            });
  }

  /** Ensure all modules have the same (expected) version. */
  @Test
  public void testAllModulesHaveExpectedVersion() {
    String luceneBuildVersion = System.getProperty(VERSION_PROPERTY);
    Assumptions.assumeThat(luceneBuildVersion).isNotNull();
    for (var module : allLuceneModules) {
      Assertions.assertThat(module.descriptor().rawVersion().orElse(null))
          .as("Version of module: " + module.descriptor().name())
          .isEqualTo(luceneBuildVersion);
    }
  }

  /** Ensure SPIs are equal for the module and classpath layer. */
  @Test
  public void testModularAndClasspathProvidersAreConsistent() throws IOException {
    for (var module : allLuceneModules) {
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

  /**
   * Ensure all exported packages in the descriptor are in sync with the module's Java classes.
   *
   * <p>This test should be progressively tuned so that certain internal packages are hidden in the
   * module layer.
   */
  @Test
  public void testAllExportedPackagesInSync() throws IOException {
    for (var module : allLuceneModules) {
      Set<String> jarPackages = getJarPackages(module, entry -> true);
      Set<ModuleDescriptor.Exports> moduleExports = new HashSet<>(module.descriptor().exports());

      if (module.descriptor().name().equals("org.apache.lucene.luke")) {
        jarPackages.removeIf(
            entry -> {
              // Luke's packages are not exported.
              return entry.startsWith("org.apache.lucene.luke");
            });
      }

      if (module.descriptor().name().equals("org.apache.lucene.core")) {
        // Internal packages should not be exported to unqualified targets.
        jarPackages.removeIf(
            entry -> {
              return entry.startsWith("org.apache.lucene.internal");
            });

        // Internal packages should use qualified exports.
        moduleExports.removeIf(
            export -> {
              boolean isInternal = export.source().startsWith("org.apache.lucene.internal");
              if (isInternal) {
                Assertions.assertThat(export.targets())
                    .containsExactlyInAnyOrder("org.apache.lucene.test_framework");
              }
              return isInternal;
            });
      }

      Assertions.assertThat(moduleExports)
          .as("Exported packages in module: " + module.descriptor().name())
          .allSatisfy(
              export -> {
                Assertions.assertThat(export.targets())
                    .as("We only support unqualified exports for now?")
                    .isEmpty();
              })
          .map(ModuleDescriptor.Exports::source)
          .containsExactlyInAnyOrderElementsOf(jarPackages);
    }
  }

  /** This test ensures that all analysis modules open their resources files to core. */
  @Test
  public void testAllOpenAnalysisPackagesInSync() throws IOException {
    for (var module : allLuceneModules) {
      if (false == module.descriptor().name().startsWith("org.apache.lucene.analysis.")) {
        continue; // at moment we only want to open resources inside analysis packages
      }

      // We only collect resources from the JAR file which are:
      // - stopword files (*.txt)
      // - ICU break iterator rules (*.brk)
      var filter = Pattern.compile("/[^/]+\\.(txt|brk)$");
      Set<String> jarPackages = getJarPackages(module, filter.asPredicate());
      Set<ModuleDescriptor.Opens> moduleOpens = module.descriptor().opens();

      Assertions.assertThat(moduleOpens)
          .as("Open packages in module: " + module.descriptor().name())
          .allSatisfy(
              export -> {
                Assertions.assertThat(export.targets())
                    .as("Opens should only be targeted to Lucene Core.")
                    .containsExactly("org.apache.lucene.core");
              })
          .map(ModuleDescriptor.Opens::source)
          .containsExactlyInAnyOrderElementsOf(jarPackages);
    }
  }

  private Set<String> getJarPackages(ModuleReference module, Predicate<String> entryFilter)
      throws IOException {
    try (ModuleReader reader = module.open()) {
      return reader
          .list()
          .filter(
              entry ->
                  !entry.startsWith("META-INF/")
                      && !entry.equals("module-info.class")
                      && !entry.endsWith("/")
                      && entryFilter.test(entry))
          .map(entry -> entry.replaceAll("/[^/]+$", ""))
          .map(entry -> entry.replace('/', '.'))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }
}
