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
package org.apache.lucene.gradle.plugins;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.VersionCatalog;
import org.gradle.api.artifacts.VersionCatalogsExtension;

/** Some common scaffolding and utilities for Lucene gradle plugins. */
public abstract class LuceneGradlePlugin implements Plugin<Project> {
  /** Return the version catalog used in the project. */
  protected static VersionCatalog getVersionCatalog(Project project) {
    var versionCatalogExt =
        project.getRootProject().getExtensions().getByType(VersionCatalogsExtension.class);
    return versionCatalogExt.named("libs");
  }

  /**
   * Ensure that {@code project} already has plugin {@code clazz} applied, throw an exception
   * otherwise.
   */
  protected final <T extends Plugin<?>> void requiresAppliedPlugin(
      Project project, Class<T> clazz) {
    var plugin = project.getPlugins().findPlugin(clazz);
    if (plugin == null) {
      throw new GradleException(
          String.format(
              Locale.ROOT,
              "Gradle plugin '%s' requires plugin '%s' to be already applied to project: %s",
              getClass().getSimpleName(),
              clazz.getSimpleName(),
              project.getPath()));
    }
  }

  /** Return the main Lucene project path (root path of the repository checkout). */
  protected static Path getProjectRootPath(Project project) {
    return project.getLayout().getSettingsDirectory().getAsFile().toPath();
  }

  /** Ensure the plugin is applied to the root project only, not subprojects. */
  protected final void applicableToRootProjectOnly(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException(
          "This plugin is applicable to the rootProject only: " + getClass().getSimpleName());
    }
  }

  /**
   * Returns a filesystem path to a given resource that the plugin uses. At the moment, these
   * resources are located under the top-level {@code gradle/} folder.
   */
  protected static Path gradlePluginResource(Project project, String relativePath) {
    return project
        .getLayout()
        .getSettingsDirectory()
        .dir("gradle")
        .getAsFile()
        .toPath()
        .resolve(relativePath);
  }

  /** Utility method returning {@link BuildOptionsExtension}. */
  protected static BuildOptionsExtension getBuildOptions(Project project) {
    return project.getExtensions().getByType(BuildOptionsExtension.class);
  }

  /**
   * Return the {@link org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension} with
   * global Lucene constants.
   */
  protected static LuceneBuildGlobalsExtension getLuceneBuildGlobals(Project project) {
    return project.getExtensions().getByType(LuceneBuildGlobalsExtension.class);
  }
}
