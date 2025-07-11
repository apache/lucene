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

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsPlugin;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.astgrep.AstGrepPlugin;
import org.apache.lucene.gradle.plugins.eclint.EditorConfigLintPlugin;
import org.apache.lucene.gradle.plugins.gitgrep.GitGrepPlugin;
import org.apache.lucene.gradle.plugins.gitinfo.GitInfoPlugin;
import org.apache.lucene.gradle.plugins.globals.RegisterBuildGlobalsPlugin;
import org.apache.lucene.gradle.plugins.regenerate.RegenerateTasksSupportPlugin;
import org.gradle.api.Project;
import org.gradle.api.initialization.IncludedBuild;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.PluginContainer;
import org.gradle.api.tasks.TaskContainer;

/** Initialization of the root project, globals, etc. */
public class RootProjectSetupPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    // Register the build options plugin early, for everything.
    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              project.getPlugins().apply(BuildOptionsPlugin.class);
            });

    // Register other root-level plugins.
    PluginContainer plugins = rootProject.getPlugins();
    plugins.apply(RegisterBuildGlobalsPlugin.class);
    plugins.apply(RegenerateTasksSupportPlugin.class);
    plugins.apply(GitInfoPlugin.class);
    plugins.apply(GitGrepPlugin.class);
    plugins.apply(AstGrepPlugin.class);
    plugins.apply(EditorConfigLintPlugin.class);

    // wire up included composite builds to validation tasks.
    connectCompositeTasksToMainBuild(rootProject);

    // TODO: this shouldn't be here but it's used in multiple scripts that are racy in
    // lazy-evaluation.
    {
      Project project = rootProject.project(":lucene:core");
      var ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
      ext.set("mrjarJavaVersions", List.of(24));
      ext.set("apijars", project.getLayout().getProjectDirectory().dir("src/generated/jdk"));
    }
  }

  private void connectCompositeTasksToMainBuild(Project rootProject) {
    TaskContainer tasks = rootProject.getTasks();
    Collection<IncludedBuild> includedBuilds = rootProject.getGradle().getIncludedBuilds();

    // Wire up top-level tidy and check from composite builds to the main project.
    tasks
        .matching(task -> task.getName().equals("tidy"))
        .configureEach(
            task -> {
              task.dependsOn(includedBuilds.stream().map(build -> build.task(":tidy")).toList());
            });

    tasks
        .named("check")
        .configure(
            task -> {
              task.dependsOn(includedBuilds.stream().map(build -> build.task(":check")).toList());
            });
  }
}
