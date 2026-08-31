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
import java.util.Locale;
import java.util.function.Function;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.testing.Test;
import org.gradle.internal.jvm.JavaInfo;
import org.gradle.internal.jvm.Jvm;
import org.gradle.internal.jvm.inspection.JvmInstallationMetadata;
import org.gradle.internal.jvm.inspection.JvmMetadataDetector;
import org.gradle.jvm.toolchain.internal.InstallationLocation;

// I failed to set it up leveraging Gradle's toolchains because
// a toolchain spec is not flexible enough to provide an exact location of the JVM to be used;
// if you have two identical JVM lang. versions in auto-discovered JVMs, an arbitrary one is used
// (?).
// This situation is not uncommon when debugging low-level stuff (hand-compiled JVM binaries).

/** This adds support for compiling and testing against a different Java runtime. */
public abstract class AlternativeJdkSupportPlugin extends LuceneGradlePlugin {
  public abstract static class AltJvmExtension {
    public abstract Property<Boolean> getAltJvmUsed();

    public abstract Property<JavaInfo> getCompilationJvm();

    public abstract Property<JavaInfo> getGradleJvm();

    public abstract Property<JavaVersion> getCompilationJvmVersion();
  }

  public abstract static class RootHooksPlugin extends LuceneGradlePlugin {
    @Inject
    public abstract JvmMetadataDetector getJvmMetadataDetector();

    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      Provider<Directory> runtimeJavaHomeOption =
          getBuildOptions(project)
              .addDirOption(
                  "runtime.java.home",
                  "Home directory path to an alternative compilation/ runtime JDK.");

      // we used to have "RUNTIME_JAVA_HOME" uppercase env variable support so keep this option too.
      var legacyUpperCase =
          project
              .getLayout()
              .getProjectDirectory()
              .dir(project.getProviders().environmentVariable("RUNTIME_JAVA_HOME"));
      runtimeJavaHomeOption = legacyUpperCase.orElse(runtimeJavaHomeOption);

      JavaInfo jvmGradle = Jvm.current();
      JavaInfo jvmCurrent =
          runtimeJavaHomeOption.isPresent()
              ? Jvm.forHome(runtimeJavaHomeOption.get().getAsFile())
              : jvmGradle;

      var altJvmExt = project.getExtensions().create("altJvmExtension", AltJvmExtension.class);

      boolean altJvmUsed = !jvmGradle.getJavaHome().equals(jvmCurrent.getJavaHome());

      JvmMetadataDetector jvmDetector = getJvmMetadataDetector();
      altJvmExt.getAltJvmUsed().convention(altJvmUsed).finalizeValue();
      altJvmExt.getCompilationJvm().convention(jvmCurrent).finalizeValue();
      altJvmExt.getGradleJvm().convention(jvmGradle).finalizeValue();
      altJvmExt
          .getCompilationJvmVersion()
          .convention(
              jvmDetector
                  .getMetadata(
                      InstallationLocation.userDefined(jvmCurrent.getJavaHome(), "specific path"))
                  .getLanguageVersion())
          .finalizeValue();

      Function<JavaInfo, String> jvmInfo =
          javaInfo -> {
            JvmInstallationMetadata jvmMetadata =
                jvmDetector.getMetadata(
                    InstallationLocation.userDefined(javaInfo.getJavaHome(), "specific path"));
            return String.format(
                Locale.ROOT,
                "%s (%s %s, home at: %s)",
                jvmMetadata.getLanguageVersion(),
                jvmMetadata.getDisplayName(),
                jvmMetadata.getRuntimeVersion(),
                jvmMetadata.getJavaHome());
          };

      project
          .getTasks()
          .register(
              "altJvmWarning",
              task -> {
                task.doFirst(
                    t -> {
                      t.getLogger()
                          .warn(
                              "NOTE: Alternative java toolchain will be used for compilation and tests:\nProject will use {}\nGradle runs with {}",
                              jvmInfo.apply(altJvmExt.getCompilationJvm().get()),
                              jvmInfo.apply(altJvmExt.getGradleJvm().get()));
                    });
              });
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    // Set up the globals on the root project first.
    project.getRootProject().getPlugins().apply(RootHooksPlugin.class);

    var altJvmExt = project.getRootProject().getExtensions().getByType(AltJvmExtension.class);
    if (altJvmExt.getAltJvmUsed().get()) {
      // Set up custom JVM for tests
      project
          .getTasks()
          .withType(Test.class)
          .configureEach(
              task -> {
                task.dependsOn(":altJvmWarning");
                task.executable(altJvmExt.getCompilationJvm().get().getJavaExecutable());
              });

      // Set up javac compilation tasks
      project
          .getTasks()
          .withType(JavaCompile.class)
          .configureEach(
              task -> {
                task.dependsOn(":altJvmWarning");
                task.getOptions().setFork(true);
                task.getOptions()
                    .getForkOptions()
                    .setJavaHome(altJvmExt.getCompilationJvm().get().getJavaHome());
              });

      // Set up javadoc compilation.
      File javadocExecutable = altJvmExt.getCompilationJvm().get().getJavadocExecutable();
      project
          .getTasks()
          .withType(RenderJavadocTaskBase.class)
          .configureEach(
              task -> {
                task.dependsOn(":altJvmWarning");
                task.getExecutable().set(javadocExecutable.toString());
              });
    }
  }
}
