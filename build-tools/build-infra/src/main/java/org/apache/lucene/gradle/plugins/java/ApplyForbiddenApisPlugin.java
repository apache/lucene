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

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;
import de.thetaphi.forbiddenapis.gradle.ForbiddenApisPlugin;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.result.DependencyResult;
import org.gradle.api.artifacts.result.ResolvedComponentResult;
import org.gradle.api.artifacts.result.ResolvedDependencyResult;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskContainer;

/**
 * This configures the application of forbidden-API signature files.
 *
 * @see "https://github.com/policeman-tools/forbidden-apis"
 */
public class ApplyForbiddenApisPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    project.getPlugins().apply(ForbiddenApisPlugin.class);

    JavaPluginExtension javaExt = project.getExtensions().getByType(JavaPluginExtension.class);
    SourceSetContainer sourceSets = javaExt.getSourceSets();

    Path forbiddenApisDir = gradlePluginResource(project, "validation/forbidden-apis");

    TaskContainer tasks = project.getTasks();

    TaskCollection<CheckForbiddenApis> allForbiddenApisTasks =
        tasks.withType(CheckForbiddenApis.class);

    // common configuration of all source sets.
    allForbiddenApisTasks.configureEach(
        task -> {
          task.getBundledSignatures()
              .addAll(
                  List.of("jdk-unsafe", "jdk-deprecated", "jdk-non-portable", "jdk-reflection"));

          task.getSuppressAnnotations().add("**.SuppressForbidden");

          // apply logger restrictions to all modules except Luke.
          if (!project.getPath().equals(":lucene:luke")) {
            task.setSignaturesFiles(
                task.getSignaturesFiles()
                    .plus(
                        project.files(
                            forbiddenApisDir.resolve("non-standard/logging.txt").toFile())));
          }
        });

    // Configure defaults for main source sets
    allForbiddenApisTasks
        .matching(task -> task.getName().matches("forbiddenApisMain\\d*|forbiddenApisTools"))
        .configureEach(
            task -> {
              task.getBundledSignatures().add("jdk-system-out");

              String ruleGroup =
                  switch (project.getPath()) {
                    case ":lucene:build-tools:build-infra-shadow" -> "main-build-infra";
                    default -> "main";
                  };

              addCompileConfigurationSignatureFiles(
                  project, task, sourceSets, ruleGroup, forbiddenApisDir);
            });

    // Configure defaults for test source sets
    allForbiddenApisTasks
        .matching(task -> task.getName().matches("forbiddenApisTest|forbiddenApisTestFixtures"))
        .configureEach(
            task -> {
              String ruleGroup = "test";
              addCompileConfigurationSignatureFiles(
                  project, task, sourceSets, ruleGroup, forbiddenApisDir);
            });

    // Configure defaults for the MR-JAR feature sourceSets: add vector code signatures files
    allForbiddenApisTasks
        .matching(task -> task.getName().matches("forbiddenApisMain\\d+"))
        .configureEach(
            task -> {
              // trick forbiddenapis jdk-nonportable signatures which report the following a
              // violation:
              // Due to a bug in forbiddenapis we add them first to suppress them later:
              // https://github.com/policeman-tools/forbidden-apis/issues/267
              var vectorExclusions = Set.of("jdk.internal.vm.vector.VectorSupport$Vector");
              task.getSignatures().addAll(vectorExclusions);
              task.getSignaturesWithSeveritySuppress().addAll(vectorExclusions);
              // add specific vector-incubator signatures
              task.setSignaturesFiles(
                  task.getSignaturesFiles()
                      .plus(
                          project.files(
                              forbiddenApisDir
                                  .resolve("non-standard/incubator-vector.txt")
                                  .toFile())));
            });

    // Configure non-standard, per-project stuff.
    var forbiddenApisMainTask = allForbiddenApisTasks.named("forbiddenApisMain");

    switch (project.getPath()) {
      case ":lucene:build-tools:missing-doclet", ":lucene:build-tools:build-infra-shadow":
        forbiddenApisMainTask.configure(
            task -> {
              task.getBundledSignatures().removeAll(Set.of("jdk-non-portable", "jdk-system-out"));
            });
        break;

      case ":lucene:demo", ":lucene:benchmark", ":lucene:test-framework":
        forbiddenApisMainTask.configure(
            task -> {
              task.getBundledSignatures().remove("jdk-system-out");
            });
        break;

      case ":lucene:analysis:common":
        forbiddenApisMainTask.configure(
            task -> {
              task.exclude("org/tartarus/**");
            });
        break;
    }
  }

  /**
   * Adds signature files of dependencies derived from the compile configuration associated with the
   * source set the forbidden apis task was created for.
   */
  private void addCompileConfigurationSignatureFiles(
      Project project,
      CheckForbiddenApis task,
      SourceSetContainer sourceSets,
      String ruleGroup,
      Path forbiddenApisDir) {
    if (!task.getName().startsWith("forbiddenApis")) {
      throw new GradleException(
          "Expect all forbidden-apis task to be named forbiddenApisXYZ,"
              + "where XYZ is the sourceSet's compile classpath configuration to inspect: "
              + task.getName());
    }

    // Figure out the source set and configuration name.
    String sourceSetName;
    {
      String name = task.getName().replaceAll("^forbiddenApis", "");
      sourceSetName = name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1);
    }

    var compileConfigurationName =
        sourceSets.named(sourceSetName).get().getCompileClasspathConfigurationName();

    var allDependenciesProvider =
        project
            .getConfigurations()
            .named(compileConfigurationName)
            .flatMap(conf -> conf.getIncoming().getResolutionResult().getRootComponent())
            .map(ApplyForbiddenApisPlugin::getAllDependencies);

    // collect signature files for all other dependencies.
    Provider<List<File>> signatureFiles =
        allDependenciesProvider.map(
            allDependencies -> {
              return collectSignatureLocations(ruleGroup, allDependencies).stream()
                  .map(forbiddenApisDir::resolve)
                  .map(Path::toFile)
                  .toList();
            });

    var allSignatureFiles = project.files(signatureFiles);
    var existingSignatureFiles = allSignatureFiles.filter(File::exists);

    task.getInputs().files(existingSignatureFiles);
    task.setSignaturesFiles(task.getSignaturesFiles().plus(existingSignatureFiles));

    addBuiltInSignatures(task, allDependenciesProvider);

    if (task.getLogger().isInfoEnabled()) {
      task.doFirst(
          t -> {
            t.getLogger()
                .info(
                    "All expected signature file locations for source set '{}', configuration '{}':\n{}",
                    sourceSetName,
                    compileConfigurationName,
                    allSignatureFiles.getFiles().stream()
                        .map(f -> "  " + f)
                        .collect(Collectors.joining("\n")));

            t.getLogger()
                .info(
                    "Existing signature files for source set '{}':\n{}",
                    sourceSetName,
                    allSignatureFiles.getFiles().stream()
                        .filter(File::exists)
                        .map(f -> "  " + f)
                        .collect(Collectors.joining("\n")));
          });
    }
  }

  /**
   * This is hacky; certain signatures are built into forbidden-api checker. Unfortunately, the
   * task's setBundledSignatures doesn't accept providers and if we try to instantiate the
   * dependency graph early, it breaks the build... I don't know how to fix this better than by
   * moving to execution stage.
   */
  private static void addBuiltInSignatures(
      CheckForbiddenApis task,
      Provider<HashSet<ResolvedDependencyResult>> allDependenciesProvider) {
    task.doFirst(
        _ -> {
          var builtInSignatureProvider =
              allDependenciesProvider.map(
                  allDependencies -> {
                    return allDependencies.stream()
                        .map(
                            dep -> {
                              return dep.getSelected().getModuleVersion();
                            })
                        .filter(
                            moduleVersion -> {
                              return moduleVersion.getGroup().equals("commons-io")
                                  && moduleVersion.getName().equals("commons-io");
                            })
                        .map(
                            moduleVersion ->
                                moduleVersion.getName() + "-unsafe-" + moduleVersion.getVersion())
                        .sorted()
                        .toList();
                  });
          task.getBundledSignatures().addAll(builtInSignatureProvider.get());
        });
  }

  private Set<String> collectSignatureLocations(
      String ruleGroup, Set<ResolvedDependencyResult> allDependencies) {

    Set<String> signatureLocations = new TreeSet<>();
    signatureLocations.add("all/defaults.txt");
    signatureLocations.add(ruleGroup + "/defaults.txt");

    allDependencies.stream()
        .flatMap(
            dep -> {
              var moduleVersion = dep.getSelected().getModuleVersion();
              var ruleFileName = moduleVersion.getGroup() + "." + moduleVersion.getName() + ".txt";
              return Set.of("all/" + ruleFileName, ruleGroup + "/" + ruleFileName).stream();
            })
        .forEach(signatureLocations::add);

    return signatureLocations;
  }

  private static HashSet<ResolvedDependencyResult> getAllDependencies(
      ResolvedComponentResult graphRoot) {
    HashSet<ResolvedDependencyResult> allResolved = new HashSet<>();
    ArrayDeque<DependencyResult> queue = new ArrayDeque<>(graphRoot.getDependencies());

    while (!queue.isEmpty()) {
      var dep = queue.removeFirst();
      if (dep instanceof ResolvedDependencyResult resolvedDep) {
        if (allResolved.add(resolvedDep)) {
          queue.addAll(resolvedDep.getSelected().getDependencies());
        }
      } else {
        throw new GradleException("Unresolved dependency, can't apply forbidden APIs: " + dep);
      }
    }
    return allResolved;
  }
}
