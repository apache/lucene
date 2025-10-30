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
package org.apache.lucene.gradle.plugins.astgrep;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.gradle.plugins.misc.QuietExec;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

public class AstGrepPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException("This plugin can be applied to the root project only.");
    }

    var optionName = "lucene.tool.ast-grep";
    var tasks = project.getTasks();

    var astToolOption =
        project
            .getExtensions()
            .getByType(BuildOptionsExtension.class)
            .addOption(optionName, "External ast-grep executable (path or name)");

    var testAstGrepRules =
        tasks.register(
            "testAstGrepRules",
            QuietExec.class,
            (task) -> {
              task.setArgs(
                  List.of(
                      "test",
                      "--skip-snapshot-tests",
                      "-c",
                      "gradle/validation/ast-grep/sgconfig.yml"));
            });

    var applyAstGrepRulesTask =
        tasks.register(
            "applyAstGrepRules",
            Exec.class,
            task -> {
              task.mustRunAfter(testAstGrepRules);

              if (!astToolOption.isPresent()) {
                task.getLogger()
                    .warn(
                        "The ast-grep tool location is not set ('{}' option), will not apply"
                            + " ast-grep rules.",
                        optionName);
              }

              var args = new ArrayList<String>();
              // fail on any rule match regardless of severity level. Scan hidden files/directories
              // too.
              args.addAll(
                  List.of(
                      "scan",
                      "-c",
                      "gradle/validation/ast-grep/sgconfig.yml",
                      "--error",
                      "--no-ignore",
                      "hidden"));
              // use the github format when being run as a workflow
              if (System.getenv("CI") != null && System.getenv("GITHUB_WORKFLOW") != null) {
                args.addAll(List.of("--format", "github"));
              }
              task.setArgs(args);
            });

    // Common configuration.
    List.of(testAstGrepRules, applyAstGrepRulesTask)
        .forEach(
            taskProv -> {
              taskProv.configure(
                  task -> {
                    if (!astToolOption.isPresent()) {
                      task.setEnabled(false);
                    }

                    if (astToolOption.isPresent()) {
                      task.setExecutable(astToolOption.get());
                    }
                    task.setWorkingDir(project.getLayout().getProjectDirectory());
                  });
            });

    tasks
        .matching(task -> task.getName().equals("check"))
        .configureEach(
            task -> {
              task.dependsOn(testAstGrepRules);
              task.dependsOn(applyAstGrepRulesTask);
            });
  }
}
