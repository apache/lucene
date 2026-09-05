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
package org.apache.lucene.gradle.plugins.spotless;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.misc.CheckEnvironmentPlugin;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;

/**
 * This adds automatic (and enforced) code formatting using google-java-format (LUCENE-9564,
 * GITHUB-14824).
 */
public class GoogleJavaFormatPlugin extends LuceneGradlePlugin {
  private static final int DEFAULT_BATCH_SIZE = 5;
  private static final String GJF_BATCH_SIZE_OPTION = "lucene.gjf.batchSize";

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    TaskContainer tasks = project.getTasks();

    TaskProvider<ApplyGoogleJavaFormatTask> applyTask =
        tasks.register("applyGoogleJavaFormat", ApplyGoogleJavaFormatTask.class);

    TaskProvider<CheckGoogleJavaFormatTask> checkTask =
        tasks.register(
            "checkGoogleJavaFormat",
            CheckGoogleJavaFormatTask.class,
            task -> {
              Object isCiBuild = project.getRootProject().findProperty("isCIBuild");
              task.getColorizedOutput().set(!(isCiBuild instanceof Boolean && (Boolean) isCiBuild));
              task.mustRunAfter(applyTask);
            });

    Provider<Integer> batchSizeOption =
        project
            .getExtensions()
            .getByType(BuildOptionsExtension.class)
            .addIntOption(
                GJF_BATCH_SIZE_OPTION,
                "Sets the batch size for google-java-format tasks.",
                DEFAULT_BATCH_SIZE);

    // Connect to check and tidy tasks.
    tasks.named("tidy", tidy -> tidy.dependsOn(applyTask));
    tasks.named("check", check -> check.dependsOn(checkTask));

    var fileStates = project.getLayout().getBuildDirectory().file("gjf-file-states.json");
    for (var t : List.of(applyTask, checkTask)) {
      t.configure(
          task -> {
            task.getBatchSize().set(batchSizeOption);
            task.dependsOn(":" + CheckEnvironmentPlugin.TASK_CHECK_JDK_INTERNALS_EXPOSED_TO_GRADLE);
            task.getFileStateCache().set(fileStates);
          });
    }

    // Configure details depending on the project.
    for (var t : List.of(applyTask, checkTask)) {
      t.configure(
          task -> {
            var srcTree =
                project.getPath().equals(":build-tools:build-infra-shadow")
                    ? project.getRootProject().fileTree("build-tools/build-infra/src")
                    : project.fileTree("src");

            srcTree.include("**/*.java");
            configureExclusions(project, srcTree);

            task.getSourceFiles().setFrom(srcTree);
          });
    }
  }

  private void configureExclusions(Project project, ConfigurableFileTree ftree) {
    switch (project.getPath()) {
      case ":lucene:analysis:common":
        // These two cause stack overflow errors in google java format. Just leave them.
        ftree.exclude("**/HTMLStripCharFilter.java", "**/UAX29URLEmailTokenizerImpl.java");
        break;
    }
  }
}
