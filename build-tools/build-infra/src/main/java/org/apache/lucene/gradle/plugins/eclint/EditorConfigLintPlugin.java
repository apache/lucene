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
package org.apache.lucene.gradle.plugins.eclint;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

/**
 * Applies (the rust version of) {@code eclint} to verify editor config settings.
 *
 * @see "https://gitlab.com/greut/eclint"
 */
public class EditorConfigLintPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    var optionName = "lucene.tool.eclint";
    var tasks = project.getTasks();

    var eclintToolOption =
        project
            .getExtensions()
            .getByType(BuildOptionsExtension.class)
            .addOption(optionName, "External eclint executable (path or name)");

    var verifyEcLintTask = tasks.register("verifyEcLint", Exec.class);

    var applyEcLintTask =
        tasks.register(
            "applyEcLint",
            Exec.class,
            task -> {
              task.args("-fix");
            });

    // Common configuration.
    List.of(verifyEcLintTask, applyEcLintTask)
        .forEach(
            taskProv -> {
              taskProv.configure(
                  task -> {
                    if (!eclintToolOption.isPresent()) {
                      task.getLogger()
                          .warn(
                              "The eclint tool location is not set ('{}' option), will not apply eclint checks.",
                              optionName);
                      task.setEnabled(false);
                    }

                    task.setIgnoreExitValue(false);
                    if (eclintToolOption.isPresent()) {
                      task.setExecutable(eclintToolOption.get());
                    }
                    task.setWorkingDir(project.getLayout().getProjectDirectory());
                  });
            });

    tasks
        .matching(task -> task.getName().equals("check"))
        .configureEach(
            task -> {
              task.dependsOn(verifyEcLintTask);
            });

    tasks
        .matching(task -> task.getName().equals("tidy"))
        .configureEach(
            task -> {
              task.dependsOn(applyEcLintTask);
            });
  }
}
