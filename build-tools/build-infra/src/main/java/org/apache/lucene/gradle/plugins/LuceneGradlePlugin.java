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

import java.util.Locale;
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
  protected <T extends Plugin<?>> void requiresAppliedPlugin(Project project, Class<T> clazz) {
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
}
