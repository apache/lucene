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

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.JavaCompile;

/** Sets up various JavaCompile task options. */
public class JavacConfigurationPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    JavaVersion minJavaVersion = getLuceneBuildGlobals(project).getMinJavaVersion().get();

    BuildOptionsExtension buildOptions =
        project.getExtensions().getByType(BuildOptionsExtension.class);
    Provider<Boolean> failOnWarningsOption =
        buildOptions.addBooleanOption(
            "javac.failOnWarnings", "Triggers failures on javac warnings.", true);

    JavaPluginExtension javaExtension =
        project.getExtensions().getByType(JavaPluginExtension.class);
    javaExtension.setSourceCompatibility(minJavaVersion);
    javaExtension.setTargetCompatibility(minJavaVersion);

    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            task -> {
              CompileOptions options = task.getOptions();

              // Use 'release' flag instead of 'source' and 'target'
              List<String> compilerArgs = options.getCompilerArgs();

              compilerArgs.addAll(List.of("--release", minJavaVersion.toString()));

              options.setEncoding("UTF-8");

              // Configure warnings. Use 'javac --help-lint' to get the supported list
              compilerArgs.addAll(
                  List.of(
                      "-Xlint:auxiliaryclass",
                      "-Xlint:cast",
                      "-Xlint:classfile",
                      "-Xlint:dangling-doc-comments",
                      "-Xlint:-deprecation",
                      "-Xlint:dep-ann",
                      "-Xlint:divzero",
                      "-Xlint:empty",
                      // TODO: uh-oh we have broken APIs.
                      "-Xlint:-exports",
                      "-Xlint:fallthrough",
                      "-Xlint:finally",
                      "-Xlint:incubating",
                      // TODO: there are problems
                      "-Xlint:-lossy-conversions",
                      // TODO: there are problems
                      "-Xlint:-missing-explicit-ctor",
                      "-Xlint:module",
                      "-Xlint:opens",
                      "-Xlint:options",
                      "-Xlint:output-file-clash",
                      "-Xlint:overloads",
                      "-Xlint:overrides",
                      "-Xlint:path",
                      "-Xlint:processing",
                      "-Xlint:rawtypes",
                      "-Xlint:removal",
                      "-Xlint:requires-automatic",
                      "-Xlint:requires-transitive-automatic",
                      "-Xlint:-serial",
                      "-Xlint:static",
                      "-Xlint:strictfp",
                      "-Xlint:synchronization",
                      "-Xlint:text-blocks",
                      // TODO: there are problems
                      "-Xlint:-this-escape",
                      "-Xlint:try",
                      "-Xlint:unchecked",
                      "-Xlint:varargs",
                      "-Xlint:preview",
                      "-Xlint:restricted",
                      "-Xdoclint:all/protected",
                      "-Xdoclint:-missing",
                      "-Xdoclint:-accessibility"));

              // we can't use this linter option
              // because of https://github.com/apache/lucene/issues/14941
              if (project.getPath().equals(":build-tools:build-infra-shadow")) {
                compilerArgs.remove("-Xlint:path");
              }

              if (project.getPath().equals(":lucene:benchmark-jmh")) {
                // Ignore, JMH benchmarks use JMH preprocessor and incubating modules.
              } else {
                // proc:none was added because of LOG4J2-1925 / JDK-8186647
                compilerArgs.add("-proc:none");

                if (failOnWarningsOption.get()) {
                  compilerArgs.add("-Werror");
                }
              }
            });
  }
}
