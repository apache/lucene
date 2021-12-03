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
  /** A path to a directory with Lucene distribution's modules. */
  private static final String MODULES_PROPERTY = "lucene.distribution.modules";

  /** The expected distribution version of Lucene modules. */
  private static final String VERSION_PROPERTY = "lucene.distribution.version";

  /** Resolved modules from {@link #MODULES_PROPERTY}. */
  private static Set<ModuleReference> allModules;

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
    String modulesPropertyValue = System.getProperty(MODULES_PROPERTY);
    if (modulesPropertyValue == null) {
      throw new AssertionError(MODULES_PROPERTY + " property is required for this test.");
    }

    Path modulesPath = Paths.get(modulesPropertyValue);
    if (!Files.isDirectory(modulesPath)) {
      throw new AssertionError(
          MODULES_PROPERTY
              + " property does not point to a directory: "
              + modulesPath.toAbsolutePath());
    }

    allModules = ModuleFinder.of(modulesPath).findAll();
  }

  /** Make sure all module names remain constant, even if we reorganize the build. */
  @Test
  public void testExpectedDistributionModuleNames() {
    Assertions.assertThat(allModules.stream().map(module -> module.descriptor().name()).sorted())
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
    for (var module : allModules) {
      Assertions.assertThat(module.descriptor().rawVersion().orElse(null))
          .as("Version of module: " + module.descriptor().name())
          .isEqualTo(luceneBuildVersion);
    }
  }

  /** Ensure SPIs are equal for the module and classpath layer. */
  @Test
  public void testModularAndClasspathProvidersAreConsistent() throws IOException {
    for (var module : allModules) {
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
}
