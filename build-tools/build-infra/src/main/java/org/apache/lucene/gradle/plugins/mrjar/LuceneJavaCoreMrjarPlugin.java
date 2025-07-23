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
package org.apache.lucene.gradle.plugins.mrjar;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;
import java.util.List;
import java.util.Map;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.JavaExec;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaLauncher;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.process.CommandLineArgumentProvider;

/**
 * Sets up the infrastructure required to extract API stubs from newer JDKs and compile
 * multi-release JARs against those stubs.
 */
public class LuceneJavaCoreMrjarPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    if (!project.getPath().equals(":lucene:core")) {
      throw new GradleException("This plugin needs to be applied to :lucene:core only.");
    }

    requiresAppliedPlugin(project, JavaPlugin.class);

    project.getExtensions().create(MrJarsExtension.NAME, MrJarsExtension.class);
  }

  static void setupMrJarInfrastructure(Project project, List<Integer> mrjarJavaVersions) {
    Directory apijars = project.getLayout().getProjectDirectory().dir("src/generated/jdk");

    var javaExt = project.getExtensions().getByType(JavaPluginExtension.class);

    for (var jdkVersion : mrjarJavaVersions) {
      SourceSet sourceSet = javaExt.getSourceSets().create("main" + jdkVersion);
      sourceSet.getJava().setSrcDirs(List.of("src/java" + jdkVersion));

      var configurations = project.getConfigurations();
      var confName = "main" + jdkVersion + "Implementation";
      var conf = configurations.maybeCreate(confName);
      conf.extendsFrom(configurations.maybeCreate("implementation"));

      project
          .getDependencies()
          .add(confName, javaExt.getSourceSets().getByName("main").getOutput());

      RegularFile apijar = apijars.file("jdk" + jdkVersion + "-api.jar");

      // include api jar in forbidden APIs.
      project
          .getTasks()
          .withType(CheckForbiddenApis.class)
          .named("forbiddenApisMain" + jdkVersion)
          .configure(
              spec -> {
                spec.setClasspath(spec.getClasspath().plus(project.files(apijar)));
              });

      // configure javac.
      project
          .getTasks()
          .withType(JavaCompile.class)
          .named("compileMain" + jdkVersion + "Java")
          .configure(
              spec -> {
                // TODO: this depends on the order of argument configuration...
                List<String> compilerArgs = spec.getOptions().getCompilerArgs();
                int releaseIndex = compilerArgs.indexOf("--release");
                assert releaseIndex >= 0;
                compilerArgs.subList(releaseIndex, releaseIndex + 2).clear();

                // Remove conflicting options for the linter. #14782
                compilerArgs.remove("-Xlint:options");
                compilerArgs.add("-Xlint:-options");

                compilerArgs.addAll(
                    List.of(
                        "--add-exports",
                        "java.base/java.lang.foreign=ALL-UNNAMED",
                        // for compilation, we patch the incubator packages into java.base, this has
                        // no effect on resulting class files:
                        "--add-exports",
                        "java.base/jdk.incubator.vector=ALL-UNNAMED"));

                var argsProvider = project.getObjects().newInstance(CompilerArgsProvider.class);
                argsProvider.getApiJarFile().set(apijar);
                spec.getOptions().getCompilerArgumentProviders().add(argsProvider);
              });
    }

    // configure jar task to include mr-jar classes.
    var buildGlobals = getLuceneBuildGlobals(project);
    project
        .getTasks()
        .withType(Jar.class)
        .configureEach(
            spec -> {
              boolean needMRJAR = false;
              int minJavaVersion =
                  Integer.parseInt(buildGlobals.getMinJavaVersion().get().getMajorVersion());

              for (var jdkVersion : mrjarJavaVersions) {
                // the sourceSet which corresponds to the minimum/base Java version
                // will copy its output to the root of this JAR,
                // all other sourceSets will go into MR-JAR folders.
                boolean isBaseVersion = jdkVersion <= minJavaVersion;
                spec.into(
                    isBaseVersion ? "" : "META-INF/versions/" + jdkVersion,
                    copySpec -> {
                      copySpec.from(
                          javaExt.getSourceSets().getByName("main" + jdkVersion).getOutput());
                    });
                needMRJAR |= !isBaseVersion;
              }

              if (needMRJAR) {
                spec.getManifest().getAttributes().put("Multi-Release", "true");
              }
            });

    // add regeneration of the apijar(s).
    var javaToolchains = project.getExtensions().getByType(JavaToolchainService.class);
    for (var jdkVersion : mrjarJavaVersions) {
      var generateJdkApiJarTask =
          project
              .getTasks()
              .register(
                  "generateJdkApiJar" + jdkVersion,
                  JavaExec.class,
                  spec -> {
                    spec.setDescription(
                        "Regenerate the API-only JAR file with public Panama Vector API from JDK "
                            + jdkVersion);
                    spec.setGroup("generation");

                    Property<JavaLauncher> javaLauncher = spec.getJavaLauncher();
                    javaLauncher.set(
                        javaToolchains.launcherFor(
                            toolchainSpec -> {
                              toolchainSpec
                                  .getLanguageVersion()
                                  .set(JavaLanguageVersion.of(jdkVersion));
                            }));

                    spec.setOnlyIf(
                        t -> {
                          try {
                            return javaLauncher.isPresent() && javaLauncher.get() != null;
                          } catch (Exception e) {
                            var logger = t.getLogger();
                            logger.warn(
                                "Launcher for Java {} is not available; skipping regeneration of Panama Vector API JAR.",
                                jdkVersion);
                            logger.warn("Error: {}", e.getCause().getMessage());
                            logger.warn(
                                "Please make sure to point env 'JAVA{}_HOME' to exactly JDK version {} or enable Gradle toolchain auto-download.",
                                jdkVersion,
                                jdkVersion);
                            return false;
                          }
                        });

                    spec.getMainClass()
                        .set(
                            getProjectRootPath(project)
                                .resolve(
                                    "build-tools/build-infra/src/main/java/org/apache/lucene/gradle/plugins/mrjar/ExtractJdkApis.java")
                                .toString());

                    spec.getSystemProperties()
                        .putAll(
                            Map.of(
                                "user.timezone", "UTC",
                                "file.encoding", "UTF-8"));

                    RegularFile apijar = apijars.file("jdk" + jdkVersion + "-api.jar");
                    spec.setArgs(
                        List.of(
                            buildGlobals.getMinJavaVersion().get().toString(),
                            jdkVersion.toString(),
                            apijar.getAsFile().toString()));
                  });

      project
          .getTasks()
          .named("regenerate")
          .configure(
              t -> {
                t.dependsOn(generateJdkApiJarTask);
              });
    }
  }

  public abstract static class CompilerArgsProvider implements CommandLineArgumentProvider {
    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    abstract RegularFileProperty getApiJarFile();

    @Override
    public Iterable<String> asArguments() {
      return List.of(
          "--patch-module", "java.base=" + getApiJarFile().get().getAsFile().getAbsolutePath());
    }
  }
}
