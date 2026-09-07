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
package org.apache.lucene.gradle.plugins.misc;

import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.bundling.Jar;

public class PyLuceneHelperPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    var publishedProjects = getLuceneBuildGlobals(project).getPublishedProjects();
    if (publishedProjects.contains(project)) {
      project
          .getTasks()
          .register(
              "collectRuntimeJars",
              Sync.class,
              task -> {
                // Collect our own artifact.
                task.from(project.getTasks().withType(Jar.class).getByName("jar").getOutputs());

                // Collect all dependencies, excluding cross-module dependencies.
                task.from(
                    project.getConfigurations().getByName("runtimeClasspath"),
                    spec -> spec.exclude("lucene-*"));

                task.into(project.getLayout().getBuildDirectory().dir("runtimeJars"));
              });
    }
  }
}
