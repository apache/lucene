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
package org.apache.lucene.gradle.plugins.hacks;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.java.RenderJavadocTaskBase;
import org.gradle.api.Project;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.process.CommandLineArgumentProvider;

/**
 * This plugin tunes JVM options for tasks that fork short-lived java subprocesses (if there are
 * such tasks).
 *
 * @see "https://github.com/apache/lucene/pull/33"
 */
public class TuneJvmOptionsPlugin extends LuceneGradlePlugin {

  private static final List<String> vmOpts =
      List.of("-XX:+UseParallelGC", "-XX:TieredStopAtLevel=1", "-XX:ActiveProcessorCount=1");

  @Override
  public void apply(Project project) {
    var tasks = project.getTasks();

    // Inject vm options into custom javadoc rendering.
    tasks
        .withType(RenderJavadocTaskBase.class)
        .configureEach(
            task -> {
              task.getExtraOpts().addAll(vmOpts.stream().map(it -> "-J" + it).toList());
            });

    // Inject vm options into any JavaExec task... We could narrow it
    // down, but I don't think there is any harm in keeping it broad.
    if (project.getPath().equals(":lucene:benchmark")) {
      // Skip this tuning in the benchmarks project.
    } else {
      tasks
          .withType(JavaExec.class)
          .configureEach(
              task -> {
                List<String> jvmArgs = new ArrayList<>(task.getJvmArgs());
                jvmArgs.addAll(vmOpts);
                task.setJvmArgs(jvmArgs);
              });
    }

    // Tweak javac to not be too resource-hungry.
    // This applies to any JVM when javac runs forked (e.g. error-prone)
    // Avoiding the fork entirely is best.
    tasks
        .withType(JavaCompile.class)
        .configureEach(
            task -> {
              task.getOptions()
                  .getForkOptions()
                  .getJvmArgumentProviders()
                  .add(
                      new CommandLineArgumentProvider() {
                        @Override
                        public Iterable<String> asArguments() {
                          // Gradle bug: https://github.com/gradle/gradle/issues/22746
                          //
                          // Evaluation of this block is delayed until execution time when
                          // we know which "mode" java compiler task will pick and can set arguments
                          // accordingly.
                          //
                          // There is a side effect to this that arguments passed via the provider
                          // are not part of up-to-date checks but these are internal JVM flags so
                          // we
                          // don't care.
                          //
                          // Pass VM options via -J when a custom javaHome is used, and we're in
                          // fork mode.
                          if (task.getOptions().isFork()
                              && task.getOptions().getForkOptions().getJavaHome() != null) {
                            return vmOpts.stream().map(it -> "-J" + it).toList();
                          } else {
                            return vmOpts;
                          }
                        }
                      });
            });
  }
}
