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
package org.apache.lucene.gradle.plugins.java;

import java.io.File;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.language.jvm.tasks.ProcessResources;

/** Configure up non-standard, legacy folder structure in Lucene projects. */
public class JavaFolderLayoutPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    JavaPluginExtension javaExt = project.getExtensions().getByType(JavaPluginExtension.class);
    SourceSetContainer sourceSets = javaExt.getSourceSets();

    SourceSet mainSrcSet = sourceSets.named("main").get();
    mainSrcSet.java(srcSetDir -> srcSetDir.setSrcDirs(List.of("src/java")));
    mainSrcSet.resources(resources -> resources.setSrcDirs(List.of("src/resources")));

    // point build-infra-shadow's main sources at build-infra sources so that we can
    // reapply the build to ourselves.
    if (project.getPath().equals(":lucene:build-tools:build-infra-shadow")
        && !getLuceneBuildGlobals(project).getIntellijIdea().get().isIdea()) {
      mainSrcSet.java(
          srcSetDir -> {
            srcSetDir.setSrcDirs(
                List.of(
                    getProjectRootPath(project)
                        .resolve("build-tools/build-infra/src/main/java")
                        .toFile()));
          });
    }

    SourceSet testSrcSet = sourceSets.named("test").get();
    testSrcSet.java(srcSetDir -> srcSetDir.setSrcDirs(List.of("src/test")));
    testSrcSet.resources(resources -> resources.setSrcDirs(List.of("src/test-files")));

    project
        .getTasks()
        .withType(ProcessResources.class)
        .named("processTestResources")
        .configure(
            task -> {
              task.from("src/test").exclude("**/*.java");
            });

    // if 'src/tools' exists, add it as a separate sourceSet.
    File toolsSrc = project.file("src/tools/java");
    if (toolsSrc.exists()) {
      sourceSets
          .create("tools")
          .java(
              srcSet -> {
                srcSet.setSrcDirs(List.of(toolsSrc));
              });

      // and inherit any dependencies for this source set from the main source set.
      ConfigurationContainer configurations = project.getConfigurations();
      configurations
          .named("toolsImplementation")
          .configure(
              conf -> {
                conf.extendsFrom(configurations.named("implementation").get());
              });
    }
  }
}
