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
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
public class TestModularLayer {
  /** A path to a directory with an expanded Lucene distribution. */
  private static final String DISTRIBUTION_PROPERTY = "lucene.distribution.dir";

  /** The expected distribution version of Lucene modules. */
  private static final String VERSION_PROPERTY = "lucene.distribution.version";

  /** Only core Lucene modules, no third party modules. */
  private static Set<ModuleReference> allCoreModules;

  /** {@link ModuleFinder} resolving only the Lucene modules. */
  private static ModuleFinder coreModulesFinder;

  /** Ensure Lucene classes are not directly visible. */
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
  }

  @AfterClass
  public static void cleanup() {
    allCoreModules = null;
    coreModulesFinder = null;
  }

  /** Make sure all published module names remain constant, even if we reorganize the build. */
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

  /** Make sure we don't publish automatic modules. */
  @Test
  public void testAllCoreModulesAreNamedModules() {
    Assertions.assertThat(allCoreModules)
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

  /**
   * Ensure all exported packages in the descriptor are in sync with the module's Java classes.
   *
   * <p>This test should be progressively tuned so that certain internal packages are hidden in the
   * module layer.
   */
  @Test
  public void testAllOpenPackagesInSync() throws IOException {
    for (var module : allCoreModules) {
      Set<String> jarPackages = getJarPackages(module);

      if (module.descriptor().name().equals("org.apache.lucene.luke")) {
        jarPackages.removeIf(
            entry -> {
              // Luke's packages are not exported.
              return entry.startsWith("org.apache.lucene.luke");
            });
      }

      Set<ModuleDescriptor.Exports> moduleExports = module.descriptor().exports();
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

  private Set<String> getJarPackages(ModuleReference module) throws IOException {
    try (ModuleReader reader = module.open()) {
      return reader
          .list()
          .filter(
              entry ->
                  !entry.startsWith("META-INF/")
                      && !entry.equals("module-info.class")
                      && !entry.endsWith("/"))
          .map(entry -> entry.replaceAll("/[^/]+$", ""))
          .map(entry -> entry.replace('/', '.'))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }
}
