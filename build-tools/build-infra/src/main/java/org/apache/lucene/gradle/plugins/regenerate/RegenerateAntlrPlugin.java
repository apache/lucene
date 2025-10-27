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
package org.apache.lucene.gradle.plugins.regenerate;

import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.MinimalExternalModuleDependency;
import org.gradle.api.provider.Provider;

/** Adds tasks that regenerate antlr4 grammars. */
public class RegenerateAntlrPlugin extends LuceneGradlePlugin {

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    configureAntlr(project.project(":lucene:expressions"));
  }

  private void configureAntlr(Project project) {
    var antlr = project.getConfigurations().create("antlr");

    Provider<MinimalExternalModuleDependency> antlrCore =
        getVersionCatalog(project).findLibrary("antlr-core").get();

    project.getDependencies().add(antlr.getName(), antlrCore);

    project
        .getTasks()
        .register(
            "regenerateAntlr",
            task -> {
              var buildGlobals = getLuceneBuildGlobals(project);

              task.setDescription("Regenerate Javascript.g4");
              task.dependsOn(antlr);

              String[] generatedPatterns = {
                "**/JavascriptBaseVisitor.java",
                "**/JavascriptLexer.java",
                "**/JavascriptParser.java",
                "**/JavascriptVisitor.java",
              };

              var sourceDir = "src/java/org/apache/lucene/expressions/js";
              var inputFiles = project.file(sourceDir + "/Javascript.g4");
              var tempOutput =
                  project.getLayout().getBuildDirectory().dir("antlr").get().getAsFile();
              var outputFiles = project.fileTree(sourceDir, cfg -> cfg.include(generatedPatterns));

              task.getInputs().property("antlr-version", antlrCore.get().getVersion());
              task.getInputs().files(inputFiles);
              task.getOutputs().files(outputFiles);
              task.doFirst(
                  _ -> {
                    buildGlobals.getFileOps().delete(tempOutput);
                    buildGlobals
                        .getExecOps()
                        .javaexec(
                            spec -> {
                              spec.getMainClass().set("org.antlr.v4.Tool");
                              spec.setClasspath(antlr);
                              spec.setIgnoreExitValue(false);
                              spec.setArgs(
                                  List.of(
                                      "-no-listener",
                                      "-visitor",
                                      "-package",
                                      "org.apache.lucene.expressions.js",
                                      "-o",
                                      tempOutput,
                                      inputFiles));
                            });

                    var generatedFiles =
                        project.fileTree(tempOutput, cfg -> cfg.include(generatedPatterns));
                    for (var file : generatedFiles) {
                      buildGlobals.modifyFile(
                          file,
                          text -> {
                            text =
                                text.replaceAll("public ((interface|class) Javascript\\w+)", "$1");
                            text =
                                text.replaceAll(
                                    "// Generated from .*",
                                    """
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

                                    // ANTLR GENERATED CODE: DO NOT EDIT.
                                    """);
                            return text;
                          });
                    }

                    buildGlobals
                        .getFileOps()
                        .copy(
                            spec -> {
                              spec.from(tempOutput);
                              spec.into(sourceDir);
                              spec.include(generatedPatterns);
                            });
                  });
            });
  }
}
