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
package org.apache.lucene.gradle.plugins.ide;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.gradle.SuppressForbidden;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.java.EcjLintPlugin;
import org.apache.tools.ant.filters.ReplaceTokens;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.VersionCatalog;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Delete;
import org.gradle.api.tasks.Sync;
import org.gradle.plugins.ide.eclipse.EclipsePlugin;
import org.gradle.plugins.ide.eclipse.GenerateEclipseClasspath;
import org.gradle.plugins.ide.eclipse.GenerateEclipseJdt;
import org.gradle.plugins.ide.eclipse.model.AbstractClasspathEntry;
import org.gradle.plugins.ide.eclipse.model.Classpath;
import org.gradle.plugins.ide.eclipse.model.ClasspathEntry;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.eclipse.model.SourceFolder;

/** Adds (limited) development support for Eclipse IDE. */
public class EclipseSupportPlugin extends LuceneGradlePlugin {
  public static final String OPT_ECLIPSE_JAVA_VERSION = "eclipse.javaVersion";
  public static final String OPT_ECLIPSE_ERRORS = "eclipse.errors";

  @Override
  public void apply(Project rootProject) {
    applicableToRootProjectOnly(rootProject);

    // Always add configurable build options so that they show up in the
    // option list.
    var buildOptions = getBuildOptions(rootProject);
    Provider<String> eclipseJavaVersionOption =
        buildOptions.addOption(
            OPT_ECLIPSE_JAVA_VERSION,
            "Set Java version for Eclipse IDE",
            getLuceneBuildGlobals(rootProject).getMinJavaVersion().map(JavaVersion::toString));
    buildOptions.addOption(
        OPT_ECLIPSE_ERRORS, "Sets eclipse IDE lint level (ignore/warning/error)", "warning");

    // Only apply Eclipse configuration setup if an explicit 'eclipse' parameter is invoked.
    if (rootProject.getGradle().getStartParameter().getTaskNames().contains("eclipse")) {
      configureEclipseIdeSettings(rootProject, eclipseJavaVersionOption.get());
    }
  }

  @SuppressForbidden(reason = "Ant's ReplaceTokens filter requires a Hashtable as parameter")
  private static <K, V> Hashtable<K, V> newHashtable(Map<K, V> map) {
    return new Hashtable<>(map);
  }

  private void configureEclipseIdeSettings(Project project, String eclipseJavaVersion) {
    var plugins = project.getPlugins();
    plugins.apply("java-base");
    plugins.apply(EclipsePlugin.class);

    var eclipse = project.getExtensions().getByType(EclipseModel.class);
    eclipse.getProject().setName("Apache Lucene " + project.getVersion());

    var jdt = eclipse.getJdt();
    jdt.setSourceCompatibility(eclipseJavaVersion);
    jdt.setTargetCompatibility(eclipseJavaVersion);
    jdt.setJavaRuntimeName("JavaSE-" + eclipseJavaVersion);

    var luceneEclipseJdt =
        project
            .getTasks()
            .register(
                "luceneEclipseJdt",
                Sync.class,
                task -> {
                  task.setDescription("Generates the Eclipse JDT settings file.");

                  Path ecjLintFile =
                      gradlePluginResource(project, EcjLintPlugin.ECJ_LINT_PREFS_PATH);
                  var errorMode = getBuildOptions(project).optionValue(OPT_ECLIPSE_ERRORS).get();

                  var inputs = task.getInputs();
                  inputs.file(ecjLintFile);
                  inputs.property("errorMode", errorMode);
                  inputs.property("eclipseJavaVersion", eclipseJavaVersion);

                  task.from(gradlePluginResource(project, "ide/eclipse/dot.settings"));
                  task.into(project.file(".settings"));

                  task.setFilteringCharset("UTF-8");

                  String ecjLintProps;
                  try {
                    ecjLintProps =
                        Files.readString(ecjLintFile).replaceAll("=error\\b", "=" + errorMode);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                  task.filter(
                      Map.of(
                          "tokens",
                          newHashtable(
                              Map.ofEntries(
                                  Map.entry("eclipseJavaVersion", eclipseJavaVersion),
                                  Map.entry("ecj-lint-config", ecjLintProps)))),
                      ReplaceTokens.class);

                  task.doLast(
                      t -> {
                        var logger = t.getLogger();
                        logger.lifecycle(
                            "Eclipse config for Java {} written with ECJ errors configured as '{}'. "
                                + "Change by altering the build option '{}' to ignore/warning/error: "
                                + "-P{}=ignore",
                            eclipseJavaVersion,
                            errorMode,
                            OPT_ECLIPSE_ERRORS,
                            OPT_ECLIPSE_ERRORS);
                        logger.lifecycle(
                            "To edit classes of MR-JARs for a specific Java version, regenerate"
                                + " Eclipse options using: -P{}=24",
                            OPT_ECLIPSE_JAVA_VERSION);
                      });
                });

    var tasks = project.getTasks();
    tasks
        .withType(GenerateEclipseJdt.class)
        .named("eclipseJdt")
        .configure(
            task -> {
              task.setEnabled(false);
              task.dependsOn(luceneEclipseJdt);
            });

    var wipePreviousConfiguration =
        tasks.register(
            "wipePreviousConfiguration",
            Delete.class,
            task -> {
              Path rootDir = getProjectRootPath(project);
              task.delete(rootDir.resolve(".classpath"), rootDir.resolve(".project"));
            });

    // Add gradle plugin portal to the source repository list and
    // apply any ecj source repository hackery the same way as everywhere.
    project.getRepositories().gradlePluginPortal();
    project.apply(conf -> conf.from(project.file("build-tools/build-infra/ecj-source.gradle")));

    tasks
        .withType(GenerateEclipseClasspath.class)
        .named("eclipseClasspath")
        .configure(
            task -> {
              task.dependsOn(wipePreviousConfiguration);

              var classpath = task.getClasspath();
              classpath.setDefaultOutputDir(project.file("build/eclipse"));
              classpath.setDownloadJavadoc(false);
              classpath.setDownloadSources(true);

              // Collect dependencies from selected configurations
              // and copy them over to a local configuration.
              List<Dependency> subdependencies = new ArrayList<>();
              project
                  .getAllprojects()
                  .forEach(
                      p -> {
                        if (p.getPlugins().hasPlugin(JavaPlugin.class)) {
                          subdependencies.addAll(
                              p.getConfigurations()
                                  .getByName("compileClasspath")
                                  .getAllDependencies());
                          subdependencies.addAll(
                              p.getConfigurations()
                                  .getByName("testCompileClasspath")
                                  .getAllDependencies());
                        }
                      });

              var subconf =
                  project
                      .getConfigurations()
                      .detachedConfiguration(subdependencies.toArray(Dependency[]::new));
              classpath.setPlusConfigurations(List.of(subconf));

              classpath
                  .getFile()
                  .whenMerged(
                      cp ->
                          configureClasspath(
                              project, (org.gradle.plugins.ide.eclipse.model.Classpath) cp));

              task.getInputs().property("eclipseJavaVersion", eclipseJavaVersion);
            });
  }

  private void configureClasspath(Project project, Classpath cp) {
    // wipe references to sources and cross-project refs.
    cp.getEntries()
        .removeIf(
            (ClasspathEntry e) -> {
              return e.getKind().equals("src")
                  || (e.getKind().equals("lib")
                      && ((AbstractClasspathEntry) e).getPath().matches("(.+)lucene-.*\\.jar"));
            });

    // Wipe attributes which only contain a blank "gradle_used_by_scope" for every single library.
    VersionCatalog versionCatalog = getVersionCatalog(project);
    String expectedGradleVersion = versionCatalog.findVersion("minGradle").get().toString();

    for (ClasspathEntry entry : cp.getEntries()) {
      if (entry instanceof AbstractClasspathEntry cpEntry && entry.getKind().equals("lib")) {
        cpEntry.getEntryAttributes().clear();
        if (cpEntry.getPath().matches(".*gradle-api-.*\\.jar")) {
          String javadocUrl = "https://docs.gradle.org/" + expectedGradleVersion + "/javadoc";
          cpEntry.getEntryAttributes().put("javadoc_location", javadocUrl);
        }
      }
    }

    // When this is called, all projects should have all the plugins applied (after the xml is
    // merged).
    String eclipseJavaVersion =
        getBuildOptions(project).optionValue(OPT_ECLIPSE_JAVA_VERSION).get();
    Set<String> sourceSetNames =
        Set.of("main", "test", "main" + eclipseJavaVersion, "test" + eclipseJavaVersion, "tools");
    Path rootDir = getProjectRootPath(project);

    var sourceFolderList =
        project.getAllprojects().stream()
            .filter(p -> p.getPlugins().hasPlugin(JavaPlugin.class))
            .flatMap(
                p ->
                    p.getExtensions().getByType(JavaPluginExtension.class).getSourceSets().stream())
            .filter(s -> sourceSetNames.contains(s.getName()))
            .flatMap(
                ss ->
                    Stream.concat(
                        ss.getJava().getSrcDirs().stream(),
                        ss.getResources().getSrcDirs().stream()))
            .filter(File::exists)
            .map(dir -> rootDir.relativize(dir.toPath()).toString())
            .sorted()
            .map(
                path -> {
                  var sourceFolder = new SourceFolder(path, "build/eclipse/" + path);
                  sourceFolder.setExcludes(List.of("module-info.java"));
                  return sourceFolder;
                })
            .toList();
    cp.getEntries().addAll(sourceFolderList);
  }
}
