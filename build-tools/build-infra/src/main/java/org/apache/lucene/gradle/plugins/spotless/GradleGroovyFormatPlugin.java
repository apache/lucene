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

import com.diffplug.gradle.spotless.GroovyGradleExtension;
import com.diffplug.gradle.spotless.SpotlessExtension;
import com.diffplug.gradle.spotless.SpotlessPlugin;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;

/** This adds automatic gradle/groovy code formatting and format compliance checks. */
public class GradleGroovyFormatPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    project.getPlugins().apply(SpotlessPlugin.class);

    // register an option to turn this formatting on or off (it is heavy).
    String spotlessGradleScriptsOptionName = "lucene.spotlessGradleScripts";
    Provider<Boolean> spotlessGradleScriptsOption =
        getBuildOptions(project)
            .addBooleanOption(
                spotlessGradleScriptsOptionName,
                "Enable formatting and validation of groovy/gradle scripts (you may want to turn it on locally if you"
                    + " work with gradle/groovy scripts)",
                false);

    if (!spotlessGradleScriptsOption.get()) {
      // register empty stubs for the corresponding set of spotless tasks, if disabled.
      registerEmptyStubs(project, spotlessGradleScriptsOptionName);
    } else {
      var spotless = project.getExtensions().getByType(SpotlessExtension.class);
      spotless.format(
          "gradleScripts",
          GroovyGradleExtension.class,
          ext -> {
            ext.greclipse();
            ext.leadingTabsToSpaces(2);
            ext.trimTrailingWhitespace();
            ext.endWithNewline();
            ext.target("build-tools/**/*.gradle", "build-tools/**/*.groovy", "**/build.gradle");
          });
    }

    TaskContainer tasks = project.getTasks();
    tasks.named(
        "spotlessGradleScripts",
        task -> {
          task.mustRunAfter(
              List.of(
                  ":lucene:build-tools:build-infra-shadow:applyGoogleJavaFormat",
                  ":lucene:build-tools:build-infra-shadow:compileJava",
                  ":lucene:build-tools:build-infra-shadow:pluginDescriptors",
                  ":lucene:build-tools:build-infra-shadow:pluginUnderTestMetadata",
                  ":lucene:build-tools:missing-doclet:applyGoogleJavaFormat",
                  ":lucene:build-tools:missing-doclet:compileJava",
                  ":lucene:build-tools:missing-doclet:renderJavadoc"));
        });

    tasks.named("tidy").configure(task -> task.dependsOn(":spotlessGradleScriptsApply"));
    tasks.named("check").configure(task -> task.dependsOn(":spotlessGradleScriptsCheck"));
  }

  private static void registerEmptyStubs(Project project, String spotlessGradleScriptsOptionName) {
    for (var taskName :
        List.of(
            "spotlessGradleScripts", "spotlessGradleScriptsApply", "spotlessGradleScriptsCheck")) {
      project
          .getTasks()
          .register(
              taskName,
              spec -> {
                spec.doFirst(
                    t -> {
                      t.getLogger()
                          .info(
                              "Spotless is turned off for gradle/groovy scripts. CI "
                                  + "checks may not pass if you have formatting "
                                  + "differences (enable '"
                                  + spotlessGradleScriptsOptionName
                                  + "' build option to check"
                                  + "and apply gradle/groovy formatting).");
                    });
              });
    }
  }
}
