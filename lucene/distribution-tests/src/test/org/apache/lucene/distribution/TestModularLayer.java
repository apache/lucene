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

import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/** Sanity checks concerning the distribution's binary artifacts (modules). */
public class TestModularLayer {
  /**
   * A path to a directory with Lucene distribution's modules. This property is passed by gradle
   * build (but can be provided manually if you run from an IDE).
   */
  private static String MODULES_PROPERTY = "lucene.distribution.modules";

  /** The expected distribution version of Lucene modules. */
  private static String VERSION_PROPERTY = "lucene.distribution.version";

  /** Resolved modules from {@link #MODULES_PROPERTY}. */
  private static Set<ModuleReference> allModules;

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
  @Ignore("LUCENE-10267: Awaits fix or a workaround for gradle bug.")
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
}
