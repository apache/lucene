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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.SourceSet;
import org.gradle.process.CommandLineArgumentProvider;

/**
 * @see ModularPathsPlugin
 */
public class ModularPathsExtension implements Cloneable, Iterable<Configuration> {
  /** Determines how paths are split between module path and classpath. */
  public enum Mode {
    /**
     * Dependencies and source set outputs are placed on classpath, even if declared on modular
     * configurations.
     */
    CLASSPATH_ONLY,

    /**
     * Dependencies from modular configurations are placed on module path. Source set outputs are
     * placed on classpath.
     */
    DEPENDENCIES_ON_MODULE_PATH
  }

  private final Project project;
  private final SourceSet sourceSet;

  private final Configuration compileModulePathConfiguration;
  private final Configuration runtimeModulePathConfiguration;
  private final Configuration modulePatchOnlyConfiguration;

  /** The mode of splitting paths for this source set. */
  private Mode mode = Mode.DEPENDENCIES_ON_MODULE_PATH;

  /** More verbose debugging for paths. */
  private boolean debugPaths;

  final boolean hasModuleDescriptor;

  /**
   * A list of module name - path provider entries that will be converted into {@code
   * --patch-module} options.
   */
  private final List<Map.Entry<String, Provider<? extends FileSystemLocation>>> modulePatches =
      new ArrayList<>();

  public ModularPathsExtension(Project project, SourceSet sourceSet) {
    this.project = project;
    this.sourceSet = sourceSet;

    // enable to debug paths.
    this.debugPaths = false;
    this.hasModuleDescriptor =
        sourceSet.getAllJava().getSrcDirs().stream()
            .map(dir -> new File(dir, "module-info.java"))
            .anyMatch(File::exists);

    ConfigurationContainer configurations = project.getConfigurations();

    // Create modular configurations for gradle's java plugin convention configurations.
    Configuration moduleApi =
        createModuleConfigurationForConvention(sourceSet.getApiConfigurationName());
    Configuration moduleImplementation =
        createModuleConfigurationForConvention(sourceSet.getImplementationConfigurationName());
    Configuration moduleRuntimeOnly =
        createModuleConfigurationForConvention(sourceSet.getRuntimeOnlyConfigurationName());
    Configuration moduleCompileOnly =
        createModuleConfigurationForConvention(sourceSet.getCompileOnlyConfigurationName());

    // Apply hierarchy relationships to modular configurations.
    moduleImplementation.extendsFrom(moduleApi);

    // Patched modules have to end up in the implementation configuration for IDEs.
    String patchOnlyName =
        SourceSet.isMain(sourceSet) ? "patchOnly" : (sourceSet.getName() + "PatchOnly");
    Configuration modulePatchOnly = createModuleConfigurationForConvention(patchOnlyName);
    modulePatchOnly.setCanBeResolved(true);
    moduleImplementation.extendsFrom(modulePatchOnly);
    this.modulePatchOnlyConfiguration = modulePatchOnly;

    // This part of convention configurations seems like a very esoteric use case, leave out for
    // now.
    // sourceSet.compileOnlyApiConfigurationName

    // We have to ensure configurations are using assembled resources and classes (jar variant) as a
    // single module can't be expanded into multiple folders.
    Consumer<Configuration> ensureJarVariant =
        (Configuration c) ->
            c.getAttributes()
                .attribute(
                    LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                    project.getObjects().named(LibraryElements.class, LibraryElements.JAR));

    // Set up compilation/runtime classpath configurations to prefer JAR variant as well.
    ensureJarVariant.accept(
        configurations.maybeCreate(sourceSet.getCompileClasspathConfigurationName()));
    ensureJarVariant.accept(
        configurations.maybeCreate(sourceSet.getRuntimeClasspathConfigurationName()));

    // A factory that creates resolvable module configurations mirroring the convention ones.
    Function<String, Configuration> createResolvableModuleConfiguration =
        (String configurationName) -> {
          Configuration conventionConfiguration = configurations.maybeCreate(configurationName);
          Configuration moduleConfiguration =
              configurations.maybeCreate(
                  moduleConfigurationNameFor(conventionConfiguration.getName()));
          moduleConfiguration.setCanBeConsumed(false);
          moduleConfiguration.setCanBeResolved(true);
          ensureJarVariant.accept(moduleConfiguration);
          project
              .getLogger()
              .info(
                  "Created resolvable module configuration for '{}': {}",
                  conventionConfiguration.getName(),
                  moduleConfiguration.getName());
          return moduleConfiguration;
        };

    this.compileModulePathConfiguration =
        createResolvableModuleConfiguration.apply(sourceSet.getCompileClasspathConfigurationName());
    this.compileModulePathConfiguration.extendsFrom(moduleCompileOnly, moduleImplementation);

    this.runtimeModulePathConfiguration =
        createResolvableModuleConfiguration.apply(sourceSet.getRuntimeClasspathConfigurationName());
    this.runtimeModulePathConfiguration.extendsFrom(moduleRuntimeOnly, moduleImplementation);
  }

  /**
   * Adds {@code --patch-module} option for the provided module name and the provider of a folder or
   * JAR file.
   */
  public void patchModule(String moduleName, Provider<? extends FileSystemLocation> pathProvider) {
    modulePatches.add(Map.entry(moduleName, pathProvider));
  }

  public void patchModule(String moduleName, FileCollection singleFileCollection) {
    patchModule(
        moduleName,
        singleFileCollection
            .getElements()
            .map(
                elements -> {
                  if (elements.size() != 1) {
                    throw new RuntimeException(
                        "Expected a single file as an argument for patchModule.");
                  }
                  return elements.iterator().next();
                }));
  }

  private FileCollection getCompilationModulePath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      return project.files();
    }
    return compileModulePathConfiguration.minus(modulePatchOnlyConfiguration);
  }

  private FileCollection getRuntimeModulePath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      if (hasModuleDescriptor) {
        throw new GradleException(
            "Source set contains a module but classpath-only dependencies requested: "
                + project.getPath()
                + ", source set '"
                + sourceSet.getName()
                + "'");
      }
      return project.files();
    }
    return runtimeModulePathConfiguration.minus(modulePatchOnlyConfiguration);
  }

  private FileCollection removeNonExisting(FileCollection fc) {
    return fc.filter(file -> file.exists());
  }

  public FileCollection getCompilationClasspath() {
    var compileClasspath = sourceSet.getCompileClasspath();

    if (mode == Mode.CLASSPATH_ONLY) {
      return removeNonExisting(sourceSet.getCompileClasspath());
    }

    return removeNonExisting(
        compileClasspath.minus(compileModulePathConfiguration).minus(modulePatchOnlyConfiguration));
  }

  public CommandLineArgumentProvider getCompilationArguments() {
    return new CompilationArgumentsProvider(
        getCompilationModulePath(), hasModuleDescriptor, modulePatches);
  }

  /** Keep this class static to avoid references to the outer class. */
  public record CompilationArgumentsProvider(
      FileCollection modulePath,
      boolean hasModuleDescriptor,
      List<Map.Entry<String, Provider<? extends FileSystemLocation>>> modulePatches)
      implements CommandLineArgumentProvider {

    @Override
    public Iterable<String> asArguments() {
      if (modulePath.isEmpty()) {
        return List.of();
      }

      List<String> extraArgs = new ArrayList<>();
      extraArgs.add("--module-path");
      extraArgs.add(joinPaths(modulePath));

      if (!hasModuleDescriptor) {
        // We're compiling what appears to be a non-module source set so we'll
        // bring everything on module path in the resolution graph,
        // otherwise modular dependencies wouldn't be part of the resolved module graph and this
        // would result in class-not-found compilation problems.
        extraArgs.add("--add-modules");
        extraArgs.add("ALL-MODULE-PATH");
      }

      // Add module-patching.
      extraArgs.addAll(getPatchModuleArguments(modulePatches));

      return extraArgs;
    }
  }

  public FileCollection getRuntimeClasspath() {
    var runtimeClasspath = sourceSet.getRuntimeClasspath();

    if (mode == Mode.CLASSPATH_ONLY) {
      return runtimeClasspath;
    }
    return runtimeClasspath.minus(getRuntimeModulePath()).minus(modulePatchOnlyConfiguration);
  }

  /**
   * Returns the runtime classpath for test tasks.
   *
   * <p>Ensures junit remains on the classpath for the Gradle test worker. Explanation: the plain
   * runtimeClasspath would also filter out junit dependencies, which must be on the Test.classpath
   * as the gradle test worker depends on it. Not having junit explicitly on the classpath would
   * result in a deprecation warning about junit not being on the classpath and junit being
   * indirectly added by gradle internal logic.
   */
  public FileCollection getTestRuntimeClasspath() {
    var runtimeClasspath = sourceSet.getRuntimeClasspath();

    if (mode == Mode.CLASSPATH_ONLY) {
      return runtimeClasspath;
    }
    return runtimeClasspath
        .minus(getRuntimeModulePath())
        .minus(modulePatchOnlyConfiguration)
        .plus(runtimeModulePathConfiguration.filter(file -> file.getName().contains("junit")));
  }

  public CommandLineArgumentProvider getRuntimeArguments() {
    return new RuntimeArgumentsProvider(getRuntimeModulePath(), hasModuleDescriptor, modulePatches);
  }

  /** Keep this class static to avoid references to the outer class. */
  public record RuntimeArgumentsProvider(
      FileCollection modulePath,
      boolean hasModuleDescriptor,
      List<Map.Entry<String, Provider<? extends FileSystemLocation>>> modulePatches)
      implements CommandLineArgumentProvider {

    @Override
    public Iterable<String> asArguments() {
      if (modulePath.isEmpty()) {
        return List.of();
      }

      List<String> extraArgs = new ArrayList<>();

      // Add source set outputs to module path.
      extraArgs.add("--module-path");
      extraArgs.add(joinPaths(modulePath));

      // Ideally, we should only add the sourceset's module here, everything else would be resolved
      // via the module descriptor. But this would require parsing the module descriptor and may
      // cause JVM version conflicts so keeping it simple.
      extraArgs.add("--add-modules");
      extraArgs.add("ALL-MODULE-PATH");

      // Add module-patching.
      extraArgs.addAll(getPatchModuleArguments(modulePatches));

      return extraArgs;
    }
  }

  private static List<String> getPatchModuleArguments(
      List<Map.Entry<String, Provider<? extends FileSystemLocation>>> modulePatches) {
    List<String> args = new ArrayList<>();
    for (var e : modulePatches) {
      args.add("--patch-module");
      args.add(e.getKey() + "=" + e.getValue().get().getAsFile());
    }
    return args;
  }

  private static String toList(FileCollection files) {
    if (files.isEmpty()) {
      return " [empty]";
    }

    return files.getFiles().stream()
        .map(f -> "    " + f.getPath())
        .sorted()
        .collect(Collectors.joining("\n"));
  }

  private static String toList(
      List<Map.Entry<String, Provider<? extends FileSystemLocation>>> patches) {
    if (patches.isEmpty()) {
      return " [empty]";
    }
    return patches.stream()
        .map(e -> "    " + e.getKey() + "=" + e.getValue().get().getAsFile())
        .collect(Collectors.joining("\n"));
  }

  public String getCompilationPathDebugInfo() {
    return "Modular extension, compilation paths, source set="
        + (sourceSet.getName() + (hasModuleDescriptor ? " (module)" : ""))
        + (", mode=" + mode + ":\n")
        + ("  Module path:" + toList(getCompilationModulePath()) + "\n")
        + ("  Class path: " + toList(getCompilationClasspath()) + "\n")
        + ("  Patches:    " + toList(modulePatches));
  }

  public String getRuntimePathDebugInfo() {
    return "Modular extension, runtime paths, source set="
        + (sourceSet.getName() + (hasModuleDescriptor ? " (module)" : ""))
        + (", mode=" + mode + ":\n")
        + ("  Module path:" + toList(getRuntimeModulePath()) + "\n")
        + ("  Class path: " + toList(getRuntimeClasspath()) + "\n")
        + ("  Patches   : " + toList(modulePatches));
  }

  @Override
  public ModularPathsExtension clone() {
    try {
      return (ModularPathsExtension) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(e);
    }
  }

  public ModularPathsExtension cloneWithMode(Mode newMode) {
    ModularPathsExtension cloned = this.clone();
    cloned.mode = newMode;
    return cloned;
  }

  /** Map convention configuration names to "modular" corresponding configurations. */
  public static String moduleConfigurationNameFor(String configurationName) {
    return "module" + capitalizeFirst(configurationName).replace("Classpath", "Path");
  }

  /** Create module configuration for the corresponding convention configuration. */
  private Configuration createModuleConfigurationForConvention(String configurationName) {
    ConfigurationContainer configurations = project.getConfigurations();
    Configuration conventionConfiguration = configurations.maybeCreate(configurationName);
    Configuration moduleConfiguration =
        configurations.maybeCreate(moduleConfigurationNameFor(configurationName));
    moduleConfiguration.setCanBeConsumed(false);
    moduleConfiguration.setCanBeResolved(false);
    conventionConfiguration.extendsFrom(moduleConfiguration);

    project
        .getLogger()
        .info(
            "Created module configuration for '{}': {}",
            conventionConfiguration.getName(),
            moduleConfiguration.getName());

    return moduleConfiguration;
  }

  /** Provide internal dependencies for tasks willing to depend on this modular paths object. */
  @Override
  public Iterator<Configuration> iterator() {
    return Arrays.asList(compileModulePathConfiguration, runtimeModulePathConfiguration).iterator();
  }

  // --- getters/setters/extras ---

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public boolean isDebugPaths() {
    return debugPaths;
  }

  public void setDebugPaths(boolean debugPaths) {
    this.debugPaths = debugPaths;
  }

  public Configuration getCompileModulePathConfiguration() {
    return compileModulePathConfiguration;
  }

  public Configuration getRuntimeModulePathConfiguration() {
    return runtimeModulePathConfiguration;
  }

  public Configuration getModulePatchOnlyConfiguration() {
    return modulePatchOnlyConfiguration;
  }

  private static String capitalizeFirst(String s) {
    if (s == null || s.isEmpty()) return s;
    if (Character.isUpperCase(s.charAt(0))) return s;
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  private static String joinPaths(FileCollection fc) {
    return fc.getFiles().stream()
        .map(File::getPath)
        .collect(Collectors.joining(File.pathSeparator));
  }
}
