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

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.StartParameter;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.artifacts.VersionCatalog;
import org.gradle.api.tasks.wrapper.Wrapper;
import org.gradle.util.GradleVersion;

/**
 * This checks build environment sanity: that we're running the desired version of Gradle, that the
 * JVM is supported, etc.
 */
public class CheckEnvironmentPlugin extends LuceneGradlePlugin {
  public static final String TASK_CHECK_JDK_INTERNALS_EXPOSED_TO_GRADLE =
      "checkJdkInternalsExportedToGradle";

  public static final String TASK_DISPLAY_GRADLE_DIAGNOSTICS = "displayGradleDiagnostics";

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    VersionCatalog versionCatalog = getVersionCatalog(rootProject);
    String expectedGradleVersion = versionCatalog.findVersion("minGradle").get().toString();

    rootProject
        .getTasks()
        .withType(Wrapper.class)
        .named("wrapper")
        .configure(
            task -> {
              task.setDistributionType(Wrapper.DistributionType.BIN);
              task.setGradleVersion(expectedGradleVersion);
            });

    JavaVersion currentJavaVersion = JavaVersion.current();
    JavaVersion minJavaVersion = JavaVersion.toVersion(versionCatalog.findVersion("minJava").get());

    if (currentJavaVersion.compareTo(minJavaVersion) < 0) {
      throw new GradleException(
          String.format(
              Locale.ROOT,
              "At least Java %s is required, you are running Java %s " + "[%s %s]",
              minJavaVersion,
              currentJavaVersion,
              System.getProperty("java.vm.name"),
              System.getProperty("java.vm.version")));
    }

    if (Runtime.version().pre().isPresent()) {
      throw new GradleException(
          String.format(
              Locale.ROOT,
              "You are running Gradle with an EA version of Java - this is not supported. "
                  + "To test Lucene compatibility with EA or pre-release versions, see this help guide: "
                  + " './gradlew helpJvms'. Detected Java version: "
                  + "[%s %s]",
              System.getProperty("java.vm.name"),
              System.getProperty("java.vm.version")));
    }

    // Unless we're regenerating the wrapper, ensure we have the exact expected gradle version.
    if (!rootProject.getGradle().getStartParameter().getTaskNames().contains("wrapper")) {
      GradleVersion currentGradleVersion = GradleVersion.current();
      if (!currentGradleVersion.equals(GradleVersion.version(expectedGradleVersion))) {
        if (currentGradleVersion
            .getBaseVersion()
            .equals(GradleVersion.version(expectedGradleVersion).getBaseVersion())) {
          rootProject
              .getLogger()
              .warn(
                  "Gradle {} is required but base version of this gradle matches, proceeding ("
                      + "this gradle is {}).",
                  expectedGradleVersion,
                  currentGradleVersion);
        } else {
          throw new GradleException(
              "Gradle "
                  + expectedGradleVersion
                  + " is required (hint: use the ./gradlew script), "
                  + "this gradle is "
                  + currentGradleVersion
                  + ".");
        }
      }
    }

    rootProject
        .getTasks()
        .register(
            TASK_CHECK_JDK_INTERNALS_EXPOSED_TO_GRADLE,
            task -> {
              task.doFirst(
                  _ -> {
                    var jdkCompilerModule =
                        ModuleLayer.boot().findModule("jdk.compiler").orElseThrow();
                    var gradleModule = getClass().getModule();
                    var internalsExported =
                        Set.of(
                                "com.sun.tools.javac.api",
                                "com.sun.tools.javac.file",
                                "com.sun.tools.javac.parser",
                                "com.sun.tools.javac.tree",
                                "com.sun.tools.javac.util")
                            .stream()
                            .allMatch(pkg -> jdkCompilerModule.isExported(pkg, gradleModule));

                    if (!internalsExported) {
                      throw new GradleException(
                          "Certain gradle tasks and plugins require access to jdk.compiler"
                              + " internals, your gradle.properties might have just been generated or could be"
                              + " out of sync (see gradle/template.gradle.properties)");
                    }
                  });
            });

    rootProject
        .getTasks()
        .register(
            TASK_DISPLAY_GRADLE_DIAGNOSTICS,
            task -> {
              task.doFirst(
                  t -> {
                    StartParameter startParameter = t.getProject().getGradle().getStartParameter();

                    var logger = t.getLogger();
                    logger.lifecycle(
                        Stream.of(
                                "max workers: " + startParameter.getMaxWorkerCount(),
                                "tests.jvms: "
                                    + getBuildOptions(t.getProject().project(":lucene:core"))
                                        .getOption("tests.jvms")
                                        .asStringProvider()
                                        .get(),
                                "cache dir: " + startParameter.getProjectCacheDir(),
                                "current dir: " + startParameter.getCurrentDir(),
                                "user home dir: " + startParameter.getGradleUserHomeDir())
                            .map(v -> "  - " + v)
                            .collect(Collectors.joining("\n")));
                  });
            });
  }
}
