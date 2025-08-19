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
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.astgrep.AstGrepPlugin;
import org.apache.lucene.gradle.plugins.eclint.EditorConfigLintPlugin;
import org.apache.lucene.gradle.plugins.gitgrep.GitGrepPlugin;
import org.apache.lucene.gradle.plugins.gitinfo.GitInfoPlugin;
import org.apache.lucene.gradle.plugins.globals.RegisterBuildGlobalsPlugin;
import org.apache.lucene.gradle.plugins.hacks.DumpGradleStateOnStalledBuildsPlugin;
import org.apache.lucene.gradle.plugins.hacks.HacksPlugin;
import org.apache.lucene.gradle.plugins.hacks.TuneJvmOptionsPlugin;
import org.apache.lucene.gradle.plugins.hacks.WipeGradleTempPlugin;
import org.apache.lucene.gradle.plugins.help.BuildOptionGroupsPlugin;
import org.apache.lucene.gradle.plugins.ide.EclipseSupportPlugin;
import org.apache.lucene.gradle.plugins.ide.IdeaSupportPlugin;
import org.apache.lucene.gradle.plugins.regenerate.RegenerateTasksSupportPlugin;
import org.apache.lucene.gradle.plugins.spotless.GradleGroovyFormatPlugin;
import org.apache.lucene.gradle.plugins.spotless.ValidateSourcePatternsPlugin;
import org.gradle.api.Project;
import org.gradle.api.file.DuplicatesStrategy;
import org.gradle.api.initialization.IncludedBuild;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.PluginContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.bundling.AbstractArchiveTask;

/** Initialization of the root project, globals, etc. */
public class RootProjectSetupPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    // Register these plugins early, for everything.
    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              project.getPlugins().apply(BasePlugin.class);
              project.getPlugins().apply(BuildOptionsPlugin.class);
              project.getPlugins().apply(BuildOptionGroupsPlugin.class);
            });

    // Register other root-level plugins.
    PluginContainer plugins = rootProject.getPlugins();
    plugins.apply(RegisterBuildGlobalsPlugin.class);
    plugins.apply(GitInfoPlugin.class);

    // Apply common configuration to all projects.
    rootProject.getAllprojects().forEach(this::applyCommonConfiguration);

    // Register more root-level plugins
    plugins.apply(RegenerateTasksSupportPlugin.class);
    plugins.apply(GitGrepPlugin.class);
    plugins.apply(AstGrepPlugin.class);
    plugins.apply(EditorConfigLintPlugin.class);
    plugins.apply(HelpPlugin.class);
    plugins.apply(HacksPlugin.class);
    plugins.apply(WipeGradleTempPlugin.class);
    plugins.apply(GradleGroovyFormatPlugin.class);
    plugins.apply(ValidateSourcePatternsPlugin.class);
    plugins.apply(ConfigureLockFilePlugin.class);
    plugins.apply(CheckGradlewScriptsTweakedPlugin.class);

    plugins.apply(EclipseSupportPlugin.class);
    plugins.apply(IdeaSupportPlugin.class);

    plugins.apply(MeasureTaskTimesPlugin.class);
    plugins.apply(DumpGradleStateOnStalledBuildsPlugin.class);

    // Apply more convention plugins to all projects.
    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              project.getPlugins().apply(TuneJvmOptionsPlugin.class);
            });

    // wire up included composite builds to validation tasks.
    connectCompositeTasksToMainBuild(rootProject);
  }

  private void applyCommonConfiguration(Project project) {
    project.setGroup("org.apache.lucene");
    if (project != project.getRootProject()) {
      project.setVersion(project.getRootProject().getVersion());
    }

    project.getRepositories().mavenCentral();

    // Common archive artifact naming.
    var baseExt = project.getExtensions().getByType(BasePluginExtension.class);
    baseExt.getArchivesName().convention(project.getPath().replaceAll("^:", "").replace(':', '-'));

    // Register these tasks for each project, even if they don't use them.
    project
        .getTasks()
        .register(
            "tidy",
            task -> {
              task.setDescription(
                  "Applies all code formatters and other enforced cleanups to the project.");
              task.setGroup("verification");
            });

    // Try to ensure builds are reproducible.
    project
        .getTasks()
        .withType(AbstractArchiveTask.class)
        .configureEach(
            task -> {
              task.setDuplicatesStrategy(DuplicatesStrategy.FAIL);
              task.setPreserveFileTimestamps(false);
              task.setReproducibleFileOrder(true);
              task.dirPermissions(
                  permissions -> {
                    permissions.unix(0755);
                  });
              task.filePermissions(
                  permissions -> {
                    permissions.unix(0644);
                  });
            });
  }

  private void connectCompositeTasksToMainBuild(Project rootProject) {
    TaskContainer tasks = rootProject.getTasks();
    Collection<IncludedBuild> includedBuilds = rootProject.getGradle().getIncludedBuilds();

    // Wire up top-level tidy and check from composite builds to the main project.
    tasks
        .named("tidy")
        .configure(
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
