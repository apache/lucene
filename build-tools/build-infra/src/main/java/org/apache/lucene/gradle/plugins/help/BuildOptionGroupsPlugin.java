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
package org.apache.lucene.gradle.plugins.help;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/** Group related Lucene build options into higher level categories. */
public class BuildOptionGroupsPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    project
        .getTasks()
        .withType(BuildOptionsTask.class)
        .configureEach(
            task -> {
              task.optionGroups(
                  optionGroups -> {
                    optionGroups.group("Lucene version strings", "version\\.(.*)");

                    optionGroups.group("IDE-tweaking options", "eclipse\\.(.+)");

                    optionGroups.group(
                        "Optional testing and test resources",
                        explicitList(
                            "tests.hunspell.regressions",
                            "validation.errorprone",
                            "hunspell.corpora",
                            "hunspell.dictionaries",
                            "hunspell.repo.path",
                            "validation.owasp",
                            "validation.owasp.apikey",
                            "validation.owasp.threshold"));

                    optionGroups.group("Test profiling", "tests\\.profile\\.(.*)");

                    optionGroups.group(
                        "Test repetition control",
                        explicitList("tests.iters", "tests.dups", "tests.failfast"));

                    optionGroups.group(
                        "Test randomization and all test-related options", "tests\\.(.*)");

                    optionGroups.group(
                        "Local tool paths",
                        "(lucene\\.tool\\.(.*))|" + explicitList("runtime.java.home"));

                    optionGroups.group(
                        "Options configuring the :lucene:benchmark:run task",
                        explicitList("maxHeapSize", "standardOutput", "taskAlg"));

                    optionGroups.group(
                        "Options useful for release managers",
                        explicitList("lucene.javadoc.url", "sign", "useGpg"));

                    optionGroups.group(
                        "Build control and information",
                        explicitList(
                            "lucene.spotlessGradleScripts",
                            "lucene.gjf.batchSize",
                            "task.times",
                            "javac.failOnWarnings",
                            "tests.slowestSuites",
                            "tests.slowestSuites.minTime",
                            "tests.slowestTests",
                            "tests.slowestTests.minTime"));
                  });
            });
  }

  private static String explicitList(String... explicitOptions) {
    return Stream.of(explicitOptions)
        .map(opt -> "(" + Pattern.quote(opt) + ")")
        .collect(Collectors.joining("|"));
  }
}
