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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;

/** Configures miscellaneous aspects required to support the java module system layer. */
public class ModularPathsPlugin extends LuceneGradlePlugin {
  public static final String MODULAR_PATHS_EXTENSION_NAME = "modularPaths";
  public static final String MODULAR_PATHS_EXTENSION_ECJ_NAME = "modularPathsForEcj";

  private static final Set<String> ECJ_CLASSPATH_ONLY_PROJECTS =
      Set.of(":lucene:spatial-extras", ":lucene:spatial3d");

  private static final Set<String> TEST_CLASSPATH_ONLY_PROJECTS =
      Set.of(":lucene:core", ":lucene:codecs", ":lucene:test-framework");

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    // We won't be using gradle's built-in automatic module finder.
    var javaPluginExtension = project.getExtensions().getByType(JavaPluginExtension.class);
    javaPluginExtension.getModularity().getInferModulePath().set(false);

    var ideaSync = getLuceneBuildGlobals(project).getIntellijIdea().get().isIdeaSync();

    //
    // Configure modular extensions for each source set.
    //
    SourceSetContainer sourceSets = javaPluginExtension.getSourceSets();
    sourceSets.configureEach(
        sourceSet -> {
          // Create and register a source set extension for manipulating classpath/ module-path
          ModularPathsExtension modularPaths = new ModularPathsExtension(project, sourceSet);
          sourceSet.getExtensions().add(MODULAR_PATHS_EXTENSION_NAME, modularPaths);

          // LUCENE-10344: We have to provide a special-case extension for ECJ because it does
          // not support all the module-specific javac options.
          ModularPathsExtension modularPathsForEcj = modularPaths;
          if (SourceSet.TEST_SOURCE_SET_NAME.equals(sourceSet.getName())
              && ECJ_CLASSPATH_ONLY_PROJECTS.contains(project.getPath())) {
            modularPathsForEcj =
                modularPaths.cloneWithMode(ModularPathsExtension.Mode.CLASSPATH_ONLY);
          }
          sourceSet.getExtensions().add(MODULAR_PATHS_EXTENSION_ECJ_NAME, modularPathsForEcj);

          // TODO: the tests of these projects currently don't compile or work in
          // module-path mode. Make the modular paths extension use class path only.
          if (SourceSet.TEST_SOURCE_SET_NAME.equals(sourceSet.getName())
              && TEST_CLASSPATH_ONLY_PROJECTS.contains(project.getPath())) {
            modularPaths.setMode(ModularPathsExtension.Mode.CLASSPATH_ONLY);
          }

          // Configure the JavaCompile task associated with this source set.
          project
              .getTasks()
              .named(sourceSet.getCompileJavaTaskName(), JavaCompile.class)
              .configure(
                  task -> {
                    task.dependsOn(modularPaths.getCompileModulePathConfiguration());

                    // GH-12742: add the modular path as inputs so that if anything changes, the
                    // task
                    // is not up to date and is re-run. I [dw] believe this should be a
                    // @Classpath parameter
                    // on the task itself... but I don't know how to implement this on an
                    // existing class.
                    // this is a workaround but should work just fine though.
                    task.getInputs().files(modularPaths.getCompileModulePathConfiguration());

                    // LUCENE-10327: don't allow gradle to emit an empty sourcepath as it would
                    // break
                    // compilation of modules.
                    task.getOptions().setSourcepath(sourceSet.getJava().getSourceDirectories());

                    // Add modular dependencies and their transitive dependencies to module
                    // path.
                    task.getOptions()
                        .getCompilerArgumentProviders()
                        .add(modularPaths.getCompilationArguments());

                    // LUCENE-10304: if we modify the classpath here, IntelliJ no longer sees
                    // the dependencies as compile-time
                    // dependencies, don't know why.
                    if (!ideaSync) {
                      task.setClasspath(modularPaths.getCompilationClasspath());
                    }

                    Property<String> debugInfo =
                        project
                            .getObjects()
                            .property(String.class)
                            .convention(
                                project
                                    .getProviders()
                                    .provider(modularPaths::getCompilationPathDebugInfo));

                    task.doFirst(t -> t.getLogger().info(debugInfo.get()));
                  });

          // For source sets that contain a module descriptor, configure a jar task that
          // combines classes and resources into a single module.
          if (!SourceSet.MAIN_SOURCE_SET_NAME.equals(sourceSet.getName())) {
            project
                .getTasks()
                .register(
                    sourceSet.getJarTaskName(),
                    Jar.class,
                    jar -> {
                      jar.getArchiveClassifier().set(sourceSet.getName());
                      jar.from(sourceSet.getOutput());
                    });
          }
        });

    // Connect modular configurations between their "test" and "main" source sets, this reflects
    // the conventions set by the Java plugin.
    ConfigurationContainer configs = project.getConfigurations();
    configs.getByName("moduleTestApi").extendsFrom(configs.getByName("moduleApi"));
    configs
        .getByName("moduleTestImplementation")
        .extendsFrom(configs.getByName("moduleImplementation"));
    configs.getByName("moduleTestRuntimeOnly").extendsFrom(configs.getByName("moduleRuntimeOnly"));
    configs.getByName("moduleTestCompileOnly").extendsFrom(configs.getByName("moduleCompileOnly"));

    // Gradle's java plugin sets the compile and runtime classpath to be a combination
    // of configuration dependencies and source set's outputs. For source sets with modules,
    // this leads to split class and resource folders.
    //
    // We tweak the default source set path configurations here by assembling jar task outputs
    // of the respective source set, instead of their source set output folders. We also attach
    // the main source set's jar to the modular test implementation configuration.
    SourceSet main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
    SourceSet test = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME);

    boolean mainIsEmpty = main.getAllJava().isEmpty();
    boolean mainIsModular =
        ((ModularPathsExtension) main.getExtensions().getByName(MODULAR_PATHS_EXTENSION_NAME))
            .hasModuleDescriptor;
    boolean testIsModular =
        ((ModularPathsExtension) test.getExtensions().getByName(MODULAR_PATHS_EXTENSION_NAME))
            .hasModuleDescriptor;

    // LUCENE-10304: if we modify the classpath here, IntelliJ no longer sees the dependencies as
    // compile-time dependencies, don't know why.
    if (!ideaSync) {
      // Consider combinations of modular/classpath between main and test.
      if (testIsModular) {
        if (mainIsModular || mainIsEmpty) {
          // Task providers for jar outputs (using task providers so we depend on their outputs
          // lazily).
          var mainJarTask = project.getTasks().named(main.getJarTaskName(), Jar.class);
          var testJarTask = project.getTasks().named(test.getJarTaskName(), Jar.class);

          // If main is empty, omit its jar outputs.
          FileCollection mainJarOutputs =
              mainIsEmpty
                  ? project.files()
                  : project.getObjects().fileCollection().from(mainJarTask);

          FileCollection testJarOutputs = project.getObjects().fileCollection().from(testJarTask);

          // Attach jars to modular test configurations.
          project.getDependencies().add("moduleTestImplementation", mainJarOutputs);
          project.getDependencies().add("moduleTestRuntimeOnly", testJarOutputs);

          // Fully modular tests - must have no split packages, proper access, etc.
          // Work around the split classes/resources problem by adjusting classpaths to
          // rely on JARs rather than source set output folders.
          test.setCompileClasspath(
              project
                  .getObjects()
                  .fileCollection()
                  .from(
                      mainJarOutputs,
                      configs.getByName(test.getCompileClasspathConfigurationName())));

          test.setRuntimeClasspath(
              project
                  .getObjects()
                  .fileCollection()
                  .from(
                      mainJarOutputs,
                      testJarOutputs,
                      configs.getByName(test.getRuntimeClasspathConfigurationName())));

        } else {
          // This combination of options simply does not make any sense (in my opinion).
          throw new GradleException(
              "Test source set is modular and main source set is class-based, this makes no sense: "
                  + project.getPath());
        }
      } else {
        // if (mainIsModular) {
        //   This combination is a potential candidate for patching the main sourceset's module with
        //   test classes. I could not resolve all the difficulties that arise when you try to do it
        //   though - either a separate module descriptor is needed that opens test packages, adds
        //   dependencies via requires clauses or a series of jvm arguments (--add-reads,
        //   --add-opens, etc.) has to be generated and maintained. This is  very low-level (ECJ
        //   doesn't support a full set of these instructions, for example).
        //
        //   Fall back to classpath mode.
        // } else {
        //   This is the 'plain old classpath' mode: neither the main source set nor the test set
        //   are modular.
        // }
      }
    }

    project
        .getTasks()
        .withType(Test.class)
        .matching(it -> it.getName().matches("test(_[0-9]+)?"))
        .configureEach(t -> configureTestTaskForSourceSet(t, sourceSets));

    // Configure module versions.
    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            task -> {
              // TODO: LUCENE-10267: Gradle bug workaround. Remove when upstream is fixed.
              String projectVersion = String.valueOf(project.getVersion());

              task.getOptions()
                  .getCompilerArgumentProviders()
                  .add(
                      () -> {
                        if (task.getClasspath().isEmpty()) {
                          return Arrays.asList("--module-version", projectVersion);
                        } else {
                          return List.of();
                        }
                      });

              task.getOptions().getJavaModuleVersion().set(projectVersion);
            });
  }

  /**
   * Configures the {@link Test} task associated with the provided source set to use module paths.
   *
   * <p>There is no explicit connection between source sets and test tasks so there is no way (?) to
   * do this automatically, convention-style.
   *
   * <p>This closure can be used to configure a different task, with a different source set, should
   * we have the need for it.
   */
  private void configureTestTaskForSourceSet(Test task, SourceSetContainer sourceSets) {
    var project = task.getProject();

    // Default: configure for SourceSet 'test'. You can call this for other pairs if needed.
    SourceSet testSourceSet = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME);

    ModularPathsExtension modularPaths =
        ((ModularPathsExtension)
            testSourceSet.getExtensions().getByName(MODULAR_PATHS_EXTENSION_NAME));
    task.dependsOn(modularPaths);

    // Add modular dependencies (and transitives) to module path.
    task.getJvmArgumentProviders().add(modularPaths.getRuntimeArguments());

    // Modify default classpath for tests.
    task.setClasspath(modularPaths.getTestRuntimeClasspath());

    // Pass all the required properties for tests which fork the JVM. We don't use
    // regular system properties here because this could affect task up-to-date checks.
    File forkProperties = new File(task.getTemporaryDir(), "jvm-forking.properties");
    task.getJvmArgumentProviders()
        .add(() -> List.of("-Dtests.jvmForkArgsFile=" + forkProperties.getAbsolutePath()));

    // Before execution: compute full JVM args list to be used by forked processes
    // and persist to file.
    ListProperty<String> jvmArgsProperty =
        project
            .getObjects()
            .listProperty(String.class)
            .value(
                project
                    .getProviders()
                    .provider(
                        () -> {
                          List<String> args = new ArrayList<>();

                          // Start with runtime module-path args
                          modularPaths.getRuntimeArguments().asArguments().forEach(args::add);

                          String cp = modularPaths.getRuntimeClasspath().getAsPath();
                          if (!cp.isBlank()) {
                            args.add("-cp");
                            args.add(cp);
                          }

                          // Sanity check: no newlines in individual args
                          for (String s : args) {
                            if (s.contains("\n")) {
                              throw new GradleException("LF in a forked jvm property?: " + s);
                            }
                          }

                          return args;
                        }));
    task.doFirst(
        _ -> {
          try {
            Files.createDirectories(forkProperties.toPath().getParent());
            Files.writeString(forkProperties.toPath(), String.join("\n", jvmArgsProperty.get()));
          } catch (IOException e) {
            throw new GradleException("Failed to write fork properties file: " + forkProperties, e);
          }
        });

    Property<String> debugInfo =
        project
            .getObjects()
            .property(String.class)
            .convention(project.getProviders().provider(modularPaths::getRuntimePathDebugInfo));
    task.doFirst(t -> t.getLogger().info(debugInfo.get()));
  }
}
