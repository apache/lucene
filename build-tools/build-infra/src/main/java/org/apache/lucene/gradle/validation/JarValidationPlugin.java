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

package org.apache.lucene.gradle.validation;

import java.util.ArrayList;
import java.util.List;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;

public abstract class JarValidationPlugin implements Plugin<Project> {

  @Override
  public void apply(Project project) {
    List<JarInfo> jarInfos = new ArrayList<>();
    project.getExtensions().add("jarInfos", jarInfos);

    project
        .getTasks()
        .register(
            "collectJarInfos",
            GenerateJarInfosTask.class,
            task -> {
              // For Java projects, add all dependencies from each source set
              project
                  .getPlugins()
                  .withType(
                      JavaPlugin.class,
                      _ -> {
                        var java = project.getExtensions().getByType(JavaPluginExtension.class);
                        java.getSourceSets()
                            .configureEach(
                                sourceSet -> {
                                  var compileClassPathConfiguration =
                                      project
                                          .getConfigurations()
                                          .getByName(
                                              sourceSet.getCompileClasspathConfigurationName());
                                  var runtimeClassPathConfiguration =
                                      project
                                          .getConfigurations()
                                          .getByName(
                                              sourceSet.getRuntimeClasspathConfigurationName());
                                  task.getDependencies().from(compileClassPathConfiguration);
                                  task.getDependencies().from(runtimeClassPathConfiguration);

                                  task.getRuntimeArtifacts()
                                      .add(
                                          compileClassPathConfiguration
                                              .getIncoming()
                                              .getArtifacts());
                                  task.getRuntimeArtifacts()
                                      .add(
                                          runtimeClassPathConfiguration
                                              .getIncoming()
                                              .getArtifacts());
                                });
                      });
              // TODO remove ones downstream dependencies have been ported to file dependencies
              task.getOutputs().upToDateWhen(_ -> false);
              task.getOutputFile()
                  .set(project.getLayout().getBuildDirectory().file("reports/jarInfos.json"));
            });

    project
        .getTasks()
        .withType(JarValidationTask.class)
        .configureEach(task -> task.setJarInfos(jarInfos));
    project
        .getTasks()
        .register(
            "validateJarLicenses",
            JarLicenseValidationTask.class,
            task ->
                task.getLicenseDir()
                    .fileValue(
                        project
                            .getLayout()
                            .getSettingsDirectory()
                            .dir("lucene/licenses")
                            .getAsFile()));
  }
}
