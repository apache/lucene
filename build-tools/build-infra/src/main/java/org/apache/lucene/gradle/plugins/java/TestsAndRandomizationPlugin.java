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
import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsPlugin;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.apache.tools.ant.types.Commandline;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.invocation.Gradle;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.logging.TestExceptionFormat;
import org.gradle.api.tasks.testing.logging.TestLogEvent;
import org.gradle.process.CommandLineArgumentProvider;

/** Sets up gradle's Test task configuration, including all kinds of randomized options */
public class TestsAndRandomizationPlugin extends LuceneGradlePlugin {
  public static class RootHooksPlugin extends LuceneGradlePlugin {
    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      project
          .getTasks()
          .register(
              "warnForcedLimitedParallelism",
              task -> {
                task.doFirst(
                    t -> {
                      t.getLogger()
                          .warn(
                              "'tests.jvm' build option forced to 1 because tests.verbose is true.");
                    });
              });

      project
          .getTasks()
          .register(
              "showTestsSeed",
              task -> {
                var testsSeedOption =
                    project
                        .getExtensions()
                        .getByType(BuildOptionsExtension.class)
                        .getOption("tests.seed");

                String seedSource =
                    switch (testsSeedOption.getSource()) {
                      case GRADLE_PROPERTY -> "project property";
                      case SYSTEM_PROPERTY -> "system property";
                      case ENVIRONMENT_VARIABLE -> "environment variable";
                      case EXPLICIT_VALUE -> "explicit value";
                      case COMPUTED_VALUE -> "picked at random";
                      case BUILD_OPTIONS_FILE -> BuildOptionsPlugin.BUILD_OPTIONS_FILE + " file";
                      case LOCAL_BUILD_OPTIONS_FILE ->
                          BuildOptionsPlugin.LOCAL_BUILD_OPTIONS_FILE + " file";
                    };

                task.doFirst(
                    t -> {
                      t.getLogger()
                          .lifecycle(
                              "Running tests with root randomization seed tests.seed="
                                  + testsSeedOption.asStringProvider().get()
                                  + ", source: "
                                  + seedSource);
                    });
              });
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    // Add warning task at the top level project so that we only emit it once.
    project.getRootProject().getPlugins().apply(RootHooksPlugin.class);

    // Pass certain build options to the test JVM as system properties
    LinkedHashSet<String> optionsInheritedAsProperties = new LinkedHashSet<>();

    BuildOptionsExtension buildOptions = getBuildOptions(project);
    LuceneBuildGlobalsExtension buildGlobals =
        project.getExtensions().getByType(LuceneBuildGlobalsExtension.class);

    // JVM options
    Provider<String> minHeapSizeOption =
        buildOptions.addOption("tests.minheapsize", "Minimum heap size for test JVMs", "256m");
    Provider<String> heapSizeOption =
        buildOptions.addOption("tests.heapsize", "Heap size for test JVMs", "512m");

    // Vectorization-related options.
    boolean defaultVectorizationEnabled =
        addVectorizationOptions(project, buildGlobals, buildOptions, optionsInheritedAsProperties);

    // Verbatim JVM arguments; make it also accept TEST_JVM_ARGS env. variable.
    Provider<String> jvmArgsOption =
        project
            .getProviders()
            .environmentVariable("TEST_JVM_ARGS")
            .orElse(
                buildOptions.addOption(
                    "tests.jvmargs",
                    "Arguments passed to each forked test JVM.",
                    project.provider(
                        () -> {
                          return (buildGlobals.isCIBuild || defaultVectorizationEnabled)
                              ? ""
                              : "-XX:TieredStopAtLevel=1 -XX:+UseParallelGC -XX:ActiveProcessorCount=1";
                        })));

    // cwd and tmp dir for forked JVMs.
    var buildDirectory = project.getLayout().getBuildDirectory();
    Provider<Directory> workDirOption =
        buildOptions.addDirOption(
            "tests.workDir",
            "Working directory for forked test JVMs.",
            buildDirectory.dir("tests-cwd"));
    Provider<Directory> tmpDirOption =
        buildOptions.addDirOption(
            "tests.tmpDir",
            "Temp directory for forked test JVMs.",
            buildDirectory.dir("tests-tmp"));
    File testsCwd = workDirOption.get().getAsFile();
    File testsTmpDir = workDirOption.get().getAsFile();

    // Asserts, debug output.
    Provider<Boolean> verboseOption =
        buildOptions.addBooleanOption(
            "tests.verbose",
            "Enables verbose test output mode (emits full test outputs immediately).",
            false);
    Provider<Boolean> haltOnFailureOption =
        buildOptions.addBooleanOption(
            "tests.haltonfailure", "Halt processing early on test failure.", false);
    Provider<Boolean> failFastOption =
        buildOptions.addBooleanOption("tests.failfast", "Stop the build early on failure.", false);
    Provider<Boolean> rerunOption =
        buildOptions.addBooleanOption(
            "tests.rerun",
            "Always rerun the test task, even if nothing has changed on input.",
            true);

    // How many testing JVM forks to create
    Provider<Integer> jvmsOption =
        buildOptions.addIntOption(
            "tests.jvms",
            "The number of forked test JVMs",
            project
                .getProviders()
                .provider(
                    () -> {
                      return ((int)
                          Math.max(
                              1, Math.min(Runtime.getRuntime().availableProcessors() / 2.0, 4.0)));
                    }));

    // GITHUB#13986: Allow easier configuration of the Panama Vectorization provider with newer Java
    // versions
    Provider<Integer> upperJavaFeatureVersionOption =
        buildOptions.addIntOption(
            "tests.upperJavaFeatureVersion",
            "Min JDK feature version to configure the Panama Vectorization provider");

    // Test reiteration, filtering and component randomization options.

    // Propagate root seed so that it is visible and reported as an option in subprojects.
    if (project != project.getRootProject()) {
      buildOptions.addOption(
          "tests.seed",
          "The \"root\" randomization seed for options and test parameters.",
          buildGlobals.getRootSeed());
    }
    optionsInheritedAsProperties.add("tests.seed");

    buildOptions.addIntOption("tests.iters", "Duplicate (re-run) each test case N times.");
    optionsInheritedAsProperties.add("tests.iters");

    buildOptions.addIntOption("tests.multiplier", "Value multiplier for randomized tests.");
    optionsInheritedAsProperties.add("tests.multiplier");

    buildOptions.addIntOption("tests.maxfailures", "Skip tests after a given number of failures.");
    optionsInheritedAsProperties.add("tests.maxfailures");

    buildOptions.addIntOption("tests.timeoutSuite", "Timeout (in millis) for an entire suite.");
    optionsInheritedAsProperties.add("tests.timeoutSuite");

    Provider<Boolean> assertsOption =
        buildOptions.addBooleanOption(
            "tests.asserts", "Enables or disables assertions mode.", true);
    optionsInheritedAsProperties.add("tests.asserts");

    buildOptions.addBooleanOption(
        "tests.infostream", "Enables or disables infostream logs.", false);
    optionsInheritedAsProperties.add("tests.infostream");

    buildOptions.addBooleanOption(
        "tests.leaveTemporary", "Leave temporary directories after tests complete.", false);
    optionsInheritedAsProperties.add("tests.leaveTemporary");

    buildOptions.addOption("tests.codec", "Sets the codec tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.codec");

    buildOptions.addOption(
        "tests.directory", "Sets the Directory implementation tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.directory");

    buildOptions.addOption(
        "tests.postingsformat", "Sets the postings format tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.postingsformat");

    buildOptions.addOption(
        "tests.docvaluesformat", "Sets the doc values format tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.docvaluesformat");

    buildOptions.addOption(
        "tests.locale", "Sets the default locale tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.locale");

    buildOptions.addOption(
        "tests.timezone", "Sets the default time zone tests should run with.", "random");
    optionsInheritedAsProperties.add("tests.timezone");

    buildOptions.addOption("tests.filter", "Applies a test filter (see ./gradlew :helpTests).");
    optionsInheritedAsProperties.add("tests.filter");

    buildOptions.addBooleanOption("tests.nightly", "Enables or disables @Nightly tests.", false);
    buildOptions.addBooleanOption("tests.monster", "Enables or disables @Monster tests.", false);
    buildOptions.addBooleanOption(
        "tests.awaitsfix", "Enables or disables @AwaitsFix tests.", false);
    optionsInheritedAsProperties.addAll(
        List.of("tests.nightly", "tests.monster", "tests.awaitsfix"));

    buildOptions.addBooleanOption(
        "tests.gui",
        "Enables or disables @RequiresGUI tests.",
        project.getProviders().provider(() -> buildGlobals.isCIBuild));

    buildOptions.addOption(
        "tests.file.encoding",
        "Sets the default file.encoding on test JVM.",
        project
            .getProviders()
            .provider(
                () -> {
                  return RandomPicks.randomFrom(
                      new Random(buildGlobals.getProjectSeedAsLong().get()),
                      List.of("US-ASCII", "ISO-8859-1", "UTF-8"));
                }));

    buildOptions.addBooleanOption(
        "tests.faiss.run", "Explicitly run tests for the Faiss codec.", false);
    optionsInheritedAsProperties.add("tests.faiss.run");

    buildOptions.addOption(
        "tests.linedocsfile", "Line docs test data file path.", "europarl.lines.txt.gz");
    optionsInheritedAsProperties.add("tests.linedocsfile");

    buildOptions.addOption(
        "tests.LUCENE_VERSION", "Base Lucene version for tests.", buildGlobals.baseVersion);
    optionsInheritedAsProperties.add("tests.LUCENE_VERSION");

    buildOptions.addOption("tests.bwcdir", "Test data for backward-compatibility indexes.");
    optionsInheritedAsProperties.add("tests.bwcdir");

    // If we're running in verbose mode and:
    // 1) worker count > 1
    // 2) number of 'test' tasks in the build is > 1
    // then the output would very likely be mangled on the
    // console. Fail and let the user know what to do.
    boolean verboseMode = verboseOption.get();
    if (verboseMode) {
      Gradle gradle = project.getGradle();
      if (gradle.getStartParameter().getMaxWorkerCount() > 1) {
        gradle
            .getTaskGraph()
            .whenReady(
                graph -> {
                  var testTasks =
                      graph.getAllTasks().stream().filter(task -> task instanceof Test).count();
                  if (testTasks > 1) {
                    throw new GradleException(
                        "Run your tests in verbose mode only with --max-workers=1 option passed to gradle.");
                  }
                });
      }
    }

    Provider<Integer> minMajorVersion =
        upperJavaFeatureVersionOption.map(
            ver -> Integer.parseInt(JavaVersion.toVersion(ver).getMajorVersion()));
    var altJvmExt =
        project
            .getRootProject()
            .getExtensions()
            .getByType(AlternativeJdkSupportPlugin.AltJvmExtension.class);
    JavaVersion runtimeJava = altJvmExt.getCompilationJvmVersion().get();

    // JDK versions where the vector module is still incubating.
    boolean incubatorJavaVersion =
        Set.of("21", "22", "23", "24", "25").contains(runtimeJava.getMajorVersion());

    // if the vector module is in incubator, pass lint flags to suppress excessive warnings.
    if (incubatorJavaVersion) {
      project
          .getTasks()
          .withType(JavaCompile.class)
          .configureEach(
              task -> {
                task.getOptions().getCompilerArgs().add("-Xlint:-incubating");
              });
    }

    project
        .getTasks()
        .withType(Test.class)
        .configureEach(
            task -> {
              // Running any test task should first display the root randomization seed.
              task.dependsOn(":showTestsSeed");

              File testOutputsDir =
                  task.getReports()
                      .getJunitXml()
                      .getOutputLocation()
                      .dir("outputs")
                      .get()
                      .getAsFile();

              task.getExtensions()
                  .getByType(ExtraPropertiesExtension.class)
                  .set("testOutputsDir", testOutputsDir);

              // LUCENE-9660: Make it possible to always rerun tests, even if they're incrementally
              // up-to-date.
              if (rerunOption.get()) {
                task.getOutputs().upToDateWhen(_ -> false);
              }

              int maxParallelForks = jvmsOption.get();
              if (verboseMode && maxParallelForks != 1) {
                task.dependsOn(":warnForcedLimitedParallelism");
                maxParallelForks = 1;
              }
              task.setMaxParallelForks(maxParallelForks);

              if (failFastOption.get()) {
                task.setFailFast(true);
              }

              task.setWorkingDir(testsCwd);
              task.useJUnit();

              task.setMinHeapSize(minHeapSizeOption.get());
              task.setMaxHeapSize(heapSizeOption.get());

              task.setIgnoreFailures(!haltOnFailureOption.get());

              if (assertsOption.get()) {
                task.jvmArgs("-ea", "-esa");
              } else {
                task.setEnableAssertions(false);
              }

              // Lucene needs to optional modules at runtime, which we want to enforce for testing
              // (if the runner JVM does not support them, it will fail tests):
              task.jvmArgs("--add-modules", "jdk.management");

              // dump heap on OOM.
              task.jvmArgs("-XX:+HeapDumpOnOutOfMemoryError");

              // Enable the vector incubator module on supported Java versions:
              boolean manualMinMajorVersion =
                  minMajorVersion.isPresent()
                      && Integer.parseInt(runtimeJava.getMajorVersion()) <= minMajorVersion.get();
              if (incubatorJavaVersion || manualMinMajorVersion) {
                task.jvmArgs("--add-modules", "jdk.incubator.vector");
                if (manualMinMajorVersion) {
                  task.systemProperty(
                      "org.apache.lucene.vectorization.upperJavaFeatureVersion",
                      Integer.toString(minMajorVersion.get()));
                }
              }

              task.jvmArgs(
                  "--enable-native-access="
                      + switch (project.getPath()) {
                        case ":lucene:codecs",
                            ":lucene:core",
                            ":lucene:distribution.tests",
                            ":lucene:test-framework" ->
                            "ALL-UNNAMED";
                        default -> "org.apache.lucene.core";
                      });

              var loggingFileProvider =
                  project.getObjects().newInstance(LoggingFileArgumentProvider.class);
              Path loggingConfigFile =
                  super.gradlePluginResource(project, "testing/logging.properties");
              loggingFileProvider.getLoggingConfigFile().set(loggingConfigFile.toFile());
              loggingFileProvider.getTempDir().set(tmpDirOption.get());
              task.getJvmArgumentProviders().add(loggingFileProvider);

              task.systemProperty("java.awt.headless", "true");
              task.systemProperty("jdk.map.althashing.threshold", "0");

              if (!Os.isFamily(Os.FAMILY_WINDOWS)) {
                task.systemProperty("java.security.egd", "file:/dev/./urandom");
              }

              // Turn jenkins blood red for hashmap bugs
              task.systemProperty("jdk.map.althashing.threshold", "0");

              // Pass certain buildOptions as system properties
              var sysProps = task.getSystemProperties().keySet();
              for (String key : optionsInheritedAsProperties) {
                Provider<String> option = buildOptions.optionValue(key);
                if (option.isPresent() && !sysProps.contains(key)) {
                  task.systemProperty(key, buildOptions.optionValue(key).get());
                }
              }

              // Set up cwd and temp locations.
              task.systemProperty("java.io.tmpdir", testsTmpDir);

              task.doFirst(
                  _ -> {
                    testsCwd.mkdirs();
                    testsTmpDir.mkdirs();
                  });

              task.jvmArgs((Object[]) Commandline.translateCommandline(jvmArgsOption.get()));

              // Disable HTML report generation. The reports are big and slow to generate.
              task.getReports().getHtml().getRequired().set(false);

              // Set up logging.
              var logging = task.getTestLogging();
              logging.events(TestLogEvent.FAILED);
              logging.setExceptionFormat(TestExceptionFormat.FULL);
              logging.setShowExceptions(true);
              logging.setShowCauses(true);
              logging.setShowStackTraces(true);
              logging.getStackTraceFilters().clear();
              logging.setShowStandardStreams(false);

              // Disable automatic test class detection, rely on class names only. This is needed
              // for testing
              // against JDKs where the bytecode is unparseable by Gradle, for example.
              // We require all tests to start with Test*, this simplifies include patterns greatly.
              task.setScanForTestClasses(false);
              task.include("**/Test*.class");
              task.exclude("**/*$*");

              // Set up custom test output handler.
              task.doFirst(
                  _ -> {
                    project.delete(testOutputsDir);
                  });

              var spillDir = task.getTemporaryDir().toPath();
              var listener =
                  new ErrorReportingTestListener(
                      task.getTestLogging(), spillDir, testOutputsDir.toPath(), verboseMode);
              task.addTestOutputListener(listener);
              task.addTestListener(listener);

              task.doFirst(
                  _ -> {
                    task.getLogger()
                        .info(
                            "Test folders for {}: cwd={}, tmp={}",
                            task.getPath(),
                            testsCwd,
                            testsTmpDir);
                  });
            });
  }

  public abstract static class LoggingFileArgumentProvider implements CommandLineArgumentProvider {
    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getLoggingConfigFile();

    @Internal
    public abstract DirectoryProperty getTempDir();

    @Override
    public Iterable<String> asArguments() {
      return List.of(
          "-Djava.util.logging.config.file="
              + getLoggingConfigFile().getAsFile().get().getAbsolutePath(),
          "-DtempDir=" + getTempDir().get().getAsFile().getAbsolutePath());
    }
  }

  private static boolean addVectorizationOptions(
      Project project,
      LuceneBuildGlobalsExtension buildGlobals,
      BuildOptionsExtension buildOptions,
      LinkedHashSet<String> optionsInheritedAsProperties) {
    String randomVectorSize =
        RandomPicks.randomFrom(
            new Random(buildGlobals.getProjectSeedAsLong().get()),
            List.of("default", "128", "256", "512"));

    Provider<Boolean> defaultVectorizationOption =
        buildOptions.addBooleanOption(
            "tests.defaultvectorization",
            "Uses defaults for running tests with correct JVM settings to test Panama vectorization (tests.jvmargs, tests.vectorsize, tests.forceintegervectors).",
            false);
    buildOptions.addOption(
        "tests.vectorsize",
        "Sets preferred vector size in bits.",
        project.provider(() -> defaultVectorizationOption.get() ? "default" : randomVectorSize));

    buildOptions.addBooleanOption(
        "tests.forceintegervectors",
        "Forces use of integer vectors even when slow.",
        project.provider(
            () -> defaultVectorizationOption.get() ? false : (randomVectorSize != "default")));

    optionsInheritedAsProperties.addAll(List.of("tests.vectorsize", "tests.forceintegervectors"));

    return defaultVectorizationOption.get();
  }
}
