
package org.apache.lucene.gradle.modules

import org.gradle.api.*
import org.gradle.api.file.*
import org.gradle.api.tasks.*
import org.gradle.api.artifacts.*
import org.gradle.api.provider.*
import org.gradle.api.attributes.*
import org.gradle.api.logging.*
import org.gradle.process.*
import java.nio.file.*
import java.nio.*

//
// For a source set, create explicit configurations for declaring modular dependencies.
//
// These "modular" configurations correspond 1:1 to Gradle's conventions but have a 'module' prefix
// and a capitalized remaining part of the conventional name. For example, an 'api' configuration in
// the main source set would have a corresponding 'moduleApi' configuration for declaring modular
// dependencies.
//
// Gradle's java plugin "convention" configurations extend from their modular counterparts
// so all dependencies end up on classpath by default for backward compatibility with other
// tasks and gradle infrastructure.
//
// At the same time, we also know which dependencies (and their transitive graph of dependencies!)
// should be placed on module-path only.
//
// Note that an explicit configuration of modular dependencies also opens up the possibility of automatically
// validating whether the dependency configuration for a gradle project is consistent with the information in
// the module-info descriptor because there is a (nearly?) direct correspondence between the two:
//
// moduleApi            - 'requires transitive'
// moduleImplementation - 'requires'
// moduleCompileOnly    - 'requires static'
//
class ModularPathsExtension implements Iterable<Object> {
  /**
   * Determines how paths are split between module path and classpath.
   */
  enum Mode {
    /**
     * Dependencies and source set outputs are placed on classpath, even if declared on modular
     * configurations. This would be the 'default' backward-compatible mode.
     */
    CLASSPATH_ONLY,

    /**
     * Dependencies from modular configurations are placed on module path. Source set outputs
     * are placed on classpath.
     */
    DEPENDENCIES_ON_MODULE_PATH
  }

  final Project project
  final SourceSet sourceSet
  final Configuration compileModulePathConfiguration
  final Configuration runtimeModulePathConfiguration
  final Configuration modulePatchOnlyConfiguration

  // The mode of splitting paths for this source set.
  Mode mode = Mode.DEPENDENCIES_ON_MODULE_PATH

  /**
   * A list of module name - path provider entries that will be converted
   * into {@code --patch-module} options.
   */
  private List<Map.Entry<String, Provider<Path>>> modulePatches = new ArrayList<>()

  private ModularPathsExtension(Project project,
                                SourceSet sourceSet,
                                Configuration compileModulePathConfiguration,
                                Configuration runtimeModulePathConfiguration,
                                Configuration modulePatchOnlyConfiguration,
                                Mode mode
  ) {
    this.project = project;
    this.sourceSet = sourceSet;
    this.compileModulePathConfiguration = compileModulePathConfiguration
    this.runtimeModulePathConfiguration = runtimeModulePathConfiguration
    this.modulePatchOnlyConfiguration = modulePatchOnlyConfiguration
    this.mode = mode
  }

  ModularPathsExtension(Project project, SourceSet sourceSet) {
    this.project = project
    this.sourceSet = sourceSet

    ConfigurationContainer configurations = project.configurations

    // Create modular configurations for gradle's java plugin convention configurations.
    Configuration moduleApi = createModuleConfigurationForConvention(sourceSet.apiConfigurationName)
    Configuration moduleImplementation = createModuleConfigurationForConvention(sourceSet.implementationConfigurationName)
    Configuration moduleRuntimeOnly = createModuleConfigurationForConvention(sourceSet.runtimeOnlyConfigurationName)
    Configuration moduleCompileOnly = createModuleConfigurationForConvention(sourceSet.compileOnlyConfigurationName)

    // Apply hierarchy relationships to modular configurations.
    moduleImplementation.extendsFrom(moduleApi)

    // Patched modules have to end up in the implementation configuration for IDEs, which
    // otherwise get terribly confused.
    Configuration modulePatchOnly = createModuleConfigurationForConvention(
        SourceSet.isMain(sourceSet) ? "patchOnly" : sourceSet.name + "PatchOnly")
    modulePatchOnly.canBeResolved(true)
    moduleImplementation.extendsFrom(modulePatchOnly)
    this.modulePatchOnlyConfiguration = modulePatchOnly

    // This part of convention configurations seems like a very esoteric use case, leave out for now.
    // sourceSet.compileOnlyApiConfigurationName

    // We have to ensure configurations are using assembled resources and classes (jar variant) as a single
    // module can't be expanded into multiple folders.
    Closure<Void> ensureJarVariant = { Configuration c ->
      c.attributes.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, project.objects.named(LibraryElements, LibraryElements.JAR))
    }

    // Set up compilation module path configuration combining corresponding convention configurations.
    Closure<Configuration> createResolvableModuleConfiguration = { String configurationName ->
      Configuration conventionConfiguration = configurations.maybeCreate(configurationName)
      Configuration moduleConfiguration = configurations.maybeCreate(moduleConfigurationNameFor(conventionConfiguration.name))
      moduleConfiguration.canBeConsumed(false)
      moduleConfiguration.canBeResolved(true)
      ensureJarVariant(moduleConfiguration)

      project.logger.info("Created resolvable module configuration for '${conventionConfiguration.name}': ${moduleConfiguration.name}")
      return moduleConfiguration
    }

    ensureJarVariant(configurations.maybeCreate(sourceSet.compileClasspathConfigurationName))
    ensureJarVariant(configurations.maybeCreate(sourceSet.runtimeClasspathConfigurationName))

    this.compileModulePathConfiguration = createResolvableModuleConfiguration(sourceSet.compileClasspathConfigurationName)
    compileModulePathConfiguration.extendsFrom(moduleCompileOnly, moduleImplementation)

    this.runtimeModulePathConfiguration = createResolvableModuleConfiguration(sourceSet.runtimeClasspathConfigurationName)
    runtimeModulePathConfiguration.extendsFrom(moduleRuntimeOnly, moduleImplementation)
  }

  /**
   * Adds {@code --patch-module} option for the provided module name and the provider of a
   * folder or JAR file.
   *
   * @param moduleName
   * @param pathProvider
   */
  void patchModule(String moduleName, Provider<Path> pathProvider) {
    modulePatches.add(Map.entry(moduleName, pathProvider));
  }

  private FileCollection getCompilationModulePath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      return project.files()
    }
    return compileModulePathConfiguration - modulePatchOnlyConfiguration
  }

  private FileCollection getRuntimeModulePath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      if (hasModuleDescriptor()) {
        // The source set is itself a module.
        throw new GradleException("Source set contains a module but classpath-only" +
            " dependencies requested: ${project.path}, source set '${sourceSet.name}'")
      }

      return project.files()
    }

    return runtimeModulePathConfiguration - modulePatchOnlyConfiguration
  }

  FileCollection getCompilationClasspath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      return sourceSet.compileClasspath
    }

    // Modify the default classpath by removing anything already placed on module path.
    // Use a lazy provider to delay computation.
    project.files({ ->
      return sourceSet.compileClasspath - compileModulePathConfiguration - modulePatchOnlyConfiguration
    })
  }

  CommandLineArgumentProvider getCompilationArguments() {
    return new CommandLineArgumentProvider() {
      @Override
      Iterable<String> asArguments() {
        FileCollection modulePath = ModularPathsExtension.this.compilationModulePath

        if (modulePath.isEmpty()) {
          return []
        }

        ArrayList<String> extraArgs = []
        extraArgs += ["--module-path", modulePath.join(File.pathSeparator)]

        if (!hasModuleDescriptor()) {
          // We're compiling what appears to be a non-module source set so we'll
          // bring everything on module path in the resolution graph,
          // otherwise modular dependencies wouldn't be part of the resolved module graph and this
          // would result in class-not-found compilation problems.
          extraArgs += ["--add-modules", "ALL-MODULE-PATH"]
        }

        // Add module-patching.
        extraArgs += getPatchModuleArguments(modulePatches)

        return extraArgs
      }
    }
  }

  FileCollection getRuntimeClasspath() {
    if (mode == Mode.CLASSPATH_ONLY) {
      return sourceSet.runtimeClasspath
    }

    // Modify the default classpath by removing anything already placed on module path.
    // Use a lazy provider to delay computation.
    project.files({ ->
      return sourceSet.runtimeClasspath - runtimeModulePath - modulePatchOnlyConfiguration
    })
  }

  CommandLineArgumentProvider getRuntimeArguments() {
    return new CommandLineArgumentProvider() {
      @Override
      Iterable<String> asArguments() {
        FileCollection modulePath = ModularPathsExtension.this.runtimeModulePath

        if (modulePath.isEmpty()) {
          return []
        }

        def extraArgs = []

        // Add source set outputs to module path.
        extraArgs += ["--module-path", modulePath.files.join(File.pathSeparator)]

        // Ideally, we should only add the sourceset's module here, everything else would be resolved via the
        // module descriptor. But this would require parsing the module descriptor and may cause JVM version conflicts
        // so keeping it simple.
        extraArgs += ["--add-modules", "ALL-MODULE-PATH"]

        // Add module-patching.
        extraArgs += getPatchModuleArguments(modulePatches)

        return extraArgs
      }
    }
  }

  boolean hasModuleDescriptor() {
    return sourceSet.allJava.srcDirs.stream()
        .map(dir -> new File(dir, "module-info.java"))
        .anyMatch(file -> file.exists())
  }

  private List<String> getPatchModuleArguments(List<Map.Entry<String, Provider<Path>>> patches) {
    def args = []
    patches.each {
      args.add("--patch-module");
      args.add(it.key + "=" + it.value.get())
    }
    return args
  }

  private static String toList(FileCollection files) {
    return files.isEmpty() ? " [empty]" : ("\n    " + files.sort().join("\n    "))
  }

  private static String toList(List<Map.Entry<String, Provider<Path>>> patches) {
    return patches.isEmpty() ? " [empty]" : ("\n    " + patches.collect {"${it.key}=${it.value.get()}"}.join("\n    "))
  }

  public void logCompilationPaths(Logger logger) {
    def value = "Modular extension, compilation paths, source set=${sourceSet.name}${hasModuleDescriptor() ? " (module)" : ""}, mode=${mode}:\n" +
        "  Module path:${toList(compilationModulePath)}\n" +
        "  Class path: ${toList(compilationClasspath)}\n" +
        "  Patches:    ${toList(modulePatches)}"

    if (debugPaths) {
      logger.lifecycle(value)
    } else {
      logger.info(value)
    }
  }

  public void logRuntimePaths(Logger logger) {
    def value = "Modular extension, runtime paths, source set=${sourceSet.name}${hasModuleDescriptor() ? " (module)" : ""}, mode=${mode}:\n" +
        "  Module path:${toList(runtimeModulePath)}\n" +
        "  Class path: ${toList(runtimeClasspath)}\n" +
        "  Patches   : ${toList(modulePatches)}"

    if (debugPaths) {
      logger.lifecycle(value)
    } else {
      logger.info(value)
    }
  }

  private boolean getDebugPaths() {
    return Boolean.parseBoolean(project.propertyOrDefault('build.debug.paths', 'false'))
  }

  ModularPathsExtension cloneWithMode(Mode newMode) {
    return new ModularPathsExtension(project,
        sourceSet,
        compileModulePathConfiguration,
        runtimeModulePathConfiguration,
        modulePatchOnlyConfiguration,
        newMode);
  }

  // Map convention configuration names to "modular" corresponding configurations.
  static String moduleConfigurationNameFor(String configurationName) {
    return "module" + configurationName.capitalize().replace("Classpath", "Path")
  }

  // Create module configuration for the corresponding convention configuration.
  private Configuration createModuleConfigurationForConvention(String configurationName) {
    ConfigurationContainer configurations = project.configurations
    Configuration conventionConfiguration = configurations.maybeCreate(configurationName)
    Configuration moduleConfiguration = configurations.maybeCreate(moduleConfigurationNameFor(configurationName))
    moduleConfiguration.canBeConsumed(false)
    moduleConfiguration.canBeResolved(false)
    conventionConfiguration.extendsFrom(moduleConfiguration)

    project.logger.info("Created module configuration for '${conventionConfiguration.name}': ${moduleConfiguration.name}")
    return moduleConfiguration
  }

  /**
   * Provide internal dependencies for tasks willing to depend on this modular paths object.
   */
  @Override
  Iterator<Object> iterator() {
    return [
        compileModulePathConfiguration,
        runtimeModulePathConfiguration
    ].iterator()
  }
}