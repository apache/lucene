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
package org.apache.lucene.gradle.plugins.ide;

import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.plugins.ide.idea.model.IdeaModel;

/** Adds some development support for IntelliJ Idea IDE. */
public class IdeaSupportPlugin extends LuceneGradlePlugin {

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    var intellijIdea = getLuceneBuildGlobals(rootProject).getIntellijIdea().get();
    if (intellijIdea.isIdea()) {
      var logger = rootProject.getLogger();
      logger.warn("IntelliJ Idea IDE detected.");

      rootProject.allprojects(
          project -> {
            project.getPlugins().apply("idea");

            var idea = project.getExtensions().getByType(IdeaModel.class);
            var module = idea.getModule();
            module.setOutputDir(project.file("build/idea/classes/main"));
            module.setTestOutputDir(project.file("build/idea/classes/test"));
            module.setDownloadSources(true);
          });
    }

    if (intellijIdea.isIdeaSync()) {
      rootProject.allprojects(
          project -> {
            // disable all MR-JAR folders by hiding them from IDE after evaluation:
            project
                .getPlugins()
                .withType(JavaPlugin.class)
                .configureEach(
                    _ -> {
                      var sourceSets =
                          project
                              .getExtensions()
                              .getByType(JavaPluginExtension.class)
                              .getSourceSets();
                      sourceSets
                          .matching(
                              sourceSet -> {
                                return sourceSet.getName().matches("main\\d+");
                              })
                          .configureEach(
                              sourceSet -> {
                                project
                                    .getLogger()
                                    .lifecycle(
                                        "Skipping MR-JAR sourceSet on IntelliJ Idea: "
                                            + sourceSet.getName());
                                sourceSet.getJava().setSrcDirs(List.of());
                                sourceSet.getResources().setSrcDirs(List.of());
                              });
                    });
          });
    }

    if (intellijIdea.isIdeaBuild()) {
      // Skip certain long tasks that are dependencies of 'assemble' if we're building from within
      // IntelliJ.
      rootProject.allprojects(
          project -> {
            project
                .getTasks()
                .matching(task -> task.getName().equals("renderSiteJavadoc"))
                .configureEach(
                    task -> {
                      project.getLogger().lifecycle("Skipping task on IntelliJ: " + task.getPath());
                      task.setEnabled(false);
                    });
          });
    }
  }
}
