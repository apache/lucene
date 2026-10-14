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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.Delete;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.testing.Test;

/**
 * Try to clean up gradle's temp files and other junk it creates.
 *
 * @see "LUCENE-9471"
 */
public abstract class WipeGradleTempPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    Gradle gradle = rootProject.getGradle();
    gradle
        .getSharedServices()
        .registerIfAbsent(
            "gradleTempCleanupService",
            TempCleanupService.class,
            service -> {
              service.parameters(
                  params -> {
                    params.getProjectRootDir().set(getProjectRootPath(rootProject).toFile());
                    File gradleUserHome = gradle.getGradleUserHomeDir();
                    params.getUserHomeDir().set(gradleUserHome);
                    params
                        .getGradleDaemonDir()
                        .set(
                            gradleUserHome
                                .toPath()
                                .resolve("daemon")
                                .resolve(gradle.getGradleVersion())
                                .toFile());
                  });
            })
        .get(); // make sure it'll run.

    rootProject
        .getAllprojects()
        .forEach(
            project -> {
              project
                  .getPlugins()
                  .withType(JavaPlugin.class)
                  .configureEach(
                      _ -> {
                        wipeAfterTests(project);
                      });
            });
  }

  private void wipeAfterTests(Project project) {
    TaskContainer tasks = project.getTasks();
    TaskCollection<Test> testTasks = tasks.withType(Test.class);
    var wipeTaskTempTask =
        tasks.register(
            "wipeTaskTemp",
            Delete.class,
            task -> {
              task.delete(
                  testTasks.stream()
                      .map(
                          test -> {
                            return project.fileTree(
                                test.getTemporaryDir(),
                                fileTree -> {
                                  fileTree.include("jar_extract*");
                                });
                          })
                      .toList());
            });

    testTasks.configureEach(
        task -> {
          task.finalizedBy(wipeTaskTempTask);
        });
  }

  public abstract static class TempCleanupService
      implements BuildService<TempCleanupServiceParams>, AutoCloseable {
    private static final Logger LOGGER = Logging.getLogger(TempCleanupService.class);

    @Override
    public void close() {
      cleanUpRedirectedTmpDir(getParameters().getProjectRootDir().get());
      cleanUpGradlesTempDir(
          getParameters().getUserHomeDir().get(), getParameters().getGradleDaemonDir().get());
    }

    private void cleanUpGradlesTempDir(Directory gradleUserHomeDir, Directory gradleDaemonDir) {
      try {
        // clean up any files older than 3 hours from the user's gradle temp. the time
        // limit is added so that we don't interfere with any concurrent builds... just in
        // case.
        Instant deadline = Instant.now().minus(3, ChronoUnit.HOURS);
        List<Stream<Path>> toDelete = new ArrayList<>();

        Path gradleUserHome = gradleUserHomeDir.getAsFile().toPath();

        var gradleTmp = gradleUserHome.resolve(".tmp");
        LOGGER.info("Cleaning up gradle's user temp dir: {}", gradleTmp);
        if (Files.exists(gradleTmp)) {
          toDelete.add(Files.list(gradleTmp));
        }

        var daemonDir = gradleDaemonDir.getAsFile().toPath();
        LOGGER.info("Cleaning up gradle's daemon logs dir: {}", daemonDir);
        if (Files.exists(daemonDir)) {
          toDelete.add(Files.list(daemonDir).filter(path -> path.toString().endsWith(".out.log")));
        }

        for (Stream<Path> stream : toDelete) {
          try {
            for (var path : stream.toList()) {
              if (!Files.isRegularFile(path)) {
                continue;
              }
              if (!Files.getLastModifiedTime(path).toInstant().isBefore(deadline)) {
                continue;
              }
              Files.deleteIfExists(path);
            }
          } finally {
            stream.close();
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void cleanUpRedirectedTmpDir(Directory projectRootDir) {
      // Clean up the java.io.tmpdir we've redirected gradle to use (LUCENE-9471).
      // these are still used and populated with junk.
      try {
        try (var files = Files.list(projectRootDir.dir(".gradle/tmp").getAsFile().toPath())) {
          var workerPaths =
              files
                  .filter(
                      path -> path.getFileName().toString().startsWith("gradle-worker-classpath"))
                  .toList();
          for (var path : workerPaths) {
            Files.delete(path);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public interface TempCleanupServiceParams extends BuildServiceParameters {
    DirectoryProperty getProjectRootDir();

    DirectoryProperty getUserHomeDir();

    DirectoryProperty getGradleDaemonDir();
  }
}
