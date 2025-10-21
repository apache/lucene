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
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.documentation.DocumentationConfigPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.javadoc.Javadoc;

/** Configures all projects to manually invoke Javadoc instead of relying on gradle's defaults. */
public class RenderJavadocPlugin extends LuceneGradlePlugin {
  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    var resources =
        getProjectRootPath(project).resolve("gradle/documentation/render-javadoc").toFile();

    var missingdocletConfiguration = project.getConfigurations().create("missingdoclet");
    project.getDependencies().add("missingdoclet", project.project(":build-tools:missing-doclet"));

    var relativeDocPath = project.getPath().replaceFirst(":\\w+:", "").replace(':', '/');

    TaskContainer tasks = project.getTasks();
    var renderJavadoc =
        tasks.register(
            "renderJavadoc",
            RenderJavadocTask.class,
            task -> {
              task.setGroup("documentation");
              task.setDescription(
                  "Generates Javadoc API documentation for each module. This directly invokes javadoc tool.");

              task.getTaskResources().set(resources);

              SourceSet mainSrcSet =
                  project
                      .getExtensions()
                      .getByType(JavaPluginExtension.class)
                      .getSourceSets()
                      .getByName("main");

              var compileCp = mainSrcSet.getCompileClasspath();
              task.dependsOn(compileCp);
              task.getClasspath().from(compileCp);
              task.getSrcDirSet().set(mainSrcSet.getJava());

              JavaVersion minJavaVersion = getLuceneBuildGlobals(project).getMinJavaVersion().get();
              task.getReleaseVersion().set(minJavaVersion);

              var javadocOutputDir =
                  project
                      .getProviders()
                      .provider(
                          () ->
                              tasks
                                  .withType(Javadoc.class)
                                  .named("javadoc")
                                  .get()
                                  .getDestinationDir());
              task.getOutputDir().set(project.getLayout().dir(javadocOutputDir));
            });

    // We disable the default javadoc task and have our own
    // javadoc rendering task below. The default javadoc task
    // will just invoke 'renderJavadoc' (to allow people to call
    // conventional task name).
    tasks
        .named("javadoc")
        .configure(
            task -> {
              task.setEnabled(false);
              task.dependsOn(renderJavadoc);
            });

    // TODO, NOCOMMIT: implement this.
    /*
       if (project.path == ':lucene:luke' || !(project in rootProject.ext.mavenProjects)) {
         // These projects are not part of the public API so we don't render their javadocs
         // as part of the site's creation. Linting happens via javac
       } else {
         tasks.register("renderSiteJavadoc", RenderJavadocTask, {
           description = "Generates Javadoc API documentation for the site (relative links)."
           group = "documentation"

           taskResources = resources
           dependsOn sourceSets.main.compileClasspath
           classpath = sourceSets.main.compileClasspath
           srcDirSet = sourceSets.main.java
           releaseVersion = minJavaVersion

           relativeProjectLinks = true

           enableSearch = true

           // Place the documentation under the documentation directory.
           // docroot is defined in 'documentation.gradle'
           outputDir = project.docroot.toPath().resolve(project.ext.relativeDocPath).toFile()
         })
       }
    */

    // Set up titles and link up some offline docs for all documentation
    // (they may be unused but this doesn't do any harm).
    String minJava = getVersionCatalog(project).findVersion("minJava").get().toString();
    Path javaJavadocPackages = resources.toPath().resolve("java-" + minJava + "/");
    if (!Files.exists(javaJavadocPackages)) {
      throw new GradleException(
          "Prefetched javadoc element-list is missing at "
              + javaJavadocPackages
              + ", "
              + "create this directory and fetch the element-list file from "
              + "from https://docs.oracle.com/en/java/javase/" + minJava + "/docs/api/element-list");
    }

    String junitVersion = getVersionCatalog(project).findVersion("junit").get().toString();
    Path junitJavadocPackages = resources.toPath().resolve("junit-" + junitVersion + "/");
    if (!Files.exists(junitJavadocPackages)) {
      throw new GradleException(
          "Prefetched javadoc package-list is missing at "
              + junitJavadocPackages
              + ", "
              + "create this directory and fetch the package-list file from "
              + "from https://junit.org/junit4/javadoc/" + junitVersion + "/package-list");
    }

    project
        .getTasks()
        .withType(RenderJavadocTask.class)
        .configureEach(
            task -> {
              task.getTitle()
                  .set("Lucene " + project.getVersion() + " " + project.getName() + " API");

              task.getOfflineLinks()
                  .put(
                      "https://docs.oracle.com/en/java/javase/" + minJava + "/docs/api/",
                      javaJavadocPackages.toFile());
              task.getOfflineLinks()
                  .put(
                      "https://junit.org/junit4/javadoc/" + junitVersion + "/",
                      junitJavadocPackages.toFile());

              task.getLuceneDocUrl()
                  .set(
                      getBuildOptions(project.getRootProject())
                          .getOption(DocumentationConfigPlugin.OPT_JAVADOC_URL)
                          .asStringProvider());

              // Set up custom doclet.
              task.dependsOn(missingdocletConfiguration);
              task.getDocletpath().from(missingdocletConfiguration);
            });

    // Configure project-specific tweaks and to-dos.
    project
        .getTasks()
        .withType(RenderJavadocTask.class)
        .configureEach(
            task -> {
              // TODO: fix these
              switch (task.getProject().getPath()) {
                case ":lucene:core":
                  task.getJavadocMissingLevel().set("class");
                  task.getJavadocMissingMethod()
                      .set(
                          List.of(
                              "org.apache.lucene.util.automaton",
                              "org.apache.lucene.analysis.standard",
                              "org.apache.lucene.analysis.tokenattributes",
                              "org.apache.lucene.document",
                              "org.apache.lucene.search.similarities",
                              "org.apache.lucene.index",
                              "org.apache.lucene.codecs",
                              "org.apache.lucene.codecs.lucene50",
                              "org.apache.lucene.codecs.lucene60",
                              "org.apache.lucene.codecs.lucene80",
                              "org.apache.lucene.codecs.lucene84",
                              "org.apache.lucene.codecs.lucene86",
                              "org.apache.lucene.codecs.lucene87",
                              "org.apache.lucene.codecs.perfield"));
                  break;

                case ":lucene:analysis:common":
                case ":lucene:analysis:kuromoji":
                case ":lucene:analysis:nori":
                case ":lucene:analysis:opennlp":
                case ":lucene:analysis:smartcn":
                case ":lucene:benchmark":
                case ":lucene:codecs":
                case ":lucene:grouping":
                case ":lucene:highlighter":
                case ":lucene:luke":
                case ":lucene:misc":
                case ":lucene:monitor":
                case ":lucene:queries":
                case ":lucene:queryparser":
                case ":lucene:replicator":
                case ":lucene:sandbox":
                case ":lucene:spatial-extras":
                case ":lucene:spatial-test-fixtures":
                case ":lucene:test-framework":
                  task.getJavadocMissingLevel().set("class");
                  break;

                case ":lucene:analysis:icu":
                case ":lucene:analysis:morfologik":
                case ":lucene:analysis:phonetic":
                case ":lucene:analysis:stempel":
                case ":lucene:backward-codecs":
                case ":lucene:classification":
                case ":lucene:expressions":
                case ":lucene:facet":
                case ":lucene:join":
                case ":lucene:spatial3d":
                case ":lucene:suggest":
                  task.getJavadocMissingLevel().set("method");
                  break;

                case ":lucene:demo":
                  task.getJavadocMissingLevel().set("method");
                  // For the demo, we link the example source in the javadocs, as it's ref'ed
                  // elsewhere
                  task.getLinksource().set(true);
                  break;
              }
            });

    // TODO: NOCOMMIT
    /*
    // Add cross-project documentation task dependencies:
    // - each RenderJavaDocs task gets a dependency to all tasks with the same name in its dependencies
    // - the dependency is using dependsOn with a closure to enable lazy evaluation
    configure(subprojects) {
      project.tasks.withType(RenderJavadocTask).configureEach { task ->
        task.dependsOn {
          task.project.configurations.implementation.allDependencies.withType(ProjectDependency).collect { dep ->
            return dep.path + ":" + task.name
          }
        }
      }
    }
    */
  }

  @CacheableTask
  public abstract static class RenderJavadocTask extends RenderJavadocTaskBase {
    @Inject
    public abstract FileOperations getFileOps();

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    @SkipWhenEmpty
    public abstract Property<SourceDirectorySet> getSrcDirSet();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    @CompileClasspath
    public abstract ConfigurableFileCollection getClasspath();

    @CompileClasspath
    public abstract ConfigurableFileCollection getDocletpath();

    @Input
    public abstract Property<String> getTitle();

    @Input
    public abstract Property<Boolean> getLinksource();

    @Input
    public abstract Property<Boolean> getEnableSearch();

    @Input
    public abstract Property<Boolean> getRelativeProjectLinks();

    @Input
    public abstract Property<JavaVersion> getReleaseVersion();

    @Internal
    public abstract MapProperty<String, File> getOfflineLinks();

    // Computes cacheable inputs from the map in offlineLinks.
    @Nested
    public List<OfflineLink> getCacheableOfflineLinks() {
      return getOfflineLinks().get().entrySet().stream()
          .map(
              e ->
                  getProject()
                      .getObjects()
                      .newInstance(OfflineLink.class, e.getKey(), e.getValue()))
          .toList();
    }

    @Input
    @Optional
    public abstract Property<String> getLuceneDocUrl();

    // default is to require full javadocs
    @Input
    public abstract Property<String> getJavadocMissingLevel();

    // anything in these packages is checked with level=method. This allows iteratively fixing one
    // package at a time.
    @Input
    public abstract ListProperty<String> getJavadocMissingMethod();

    // default is not to ignore any elements, should only be used to workaround split packages
    @Input
    public abstract ListProperty<String> getJavadocMissingIgnore();

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract DirectoryProperty getTaskResources();

    public RenderJavadocTask() {
      getLinksource().convention(false);
      getEnableSearch().convention(false);
      getRelativeProjectLinks().convention(false);
      getJavadocMissingLevel().convention("parameter");
    }

    @TaskAction
    public void render() throws IOException {
      List<File> srcDirs =
          getSrcDirSet().get().getSourceDirectories().getFiles().stream()
              .filter(f -> Files.exists(f.toPath()))
              .toList();

      Path optionsFile = getTemporaryDir().toPath().resolve("javadoc-options.txt");
      Files.createDirectories(optionsFile.getParent());

      // create the directory, so relative link calculation knows that it's a directory:
      Files.createDirectories(getOutputDir().get().getAsFile().toPath());

      List<Object> opts = new ArrayList<>();

      var overviewFiles =
          srcDirs.stream()
              .map(dir -> dir.toPath().resolve("overview.html"))
              .filter(Files::exists)
              .toList();
      if (overviewFiles.size() != 1) {
        throw new GradleException("Must be exactly one overview.html file: " + overviewFiles);
      }

      opts.add(
          List.of("-overview", getProject().file(overviewFiles.getFirst().toFile()).toString()));
      opts.add(List.of("-d", getOutputDir().get().getAsFile().toString()));
      opts.add("-protected");

      opts.add(List.of("-encoding", "UTF-8"));
      opts.add(List.of("-charset", "UTF-8"));
      opts.add(List.of("-docencoding", "UTF-8"));

      if (!getEnableSearch().getOrElse(false)) {
        opts.add("-noindex");
      }

      opts.add("-author");
      opts.add("-version");
      if (getLinksource().get()) {
        opts.add("-linksource");
      }

      opts.add("-use");

      opts.add(List.of("-locale", "en_US"));

      opts.add(List.of("-windowtitle", getTitle().get()));
      opts.add(List.of("-doctitle", getTitle().get()));

      if (!getClasspath().isEmpty()) {
        opts.add(List.of("-classpath", getClasspath().getAsPath()));
      }

      var buildGlobals = getLuceneBuildGlobals(getProject());
      opts.add(
          List.of(
              "-bottom",
              "<i>Copyright &copy; 2000-"
                  + buildGlobals.buildYear
                  + " Apache Software Foundation. All Rights Reserved.</i>"));

      opts.add(
          List.of(
              "-tag",
              "lucene.experimental:a:WARNING: This API is experimental and might change in incompatible ways in the next release."));
      opts.add(
          List.of(
              "-tag",
              "lucene.internal:a:NOTE: This API is for internal purposes only and might change in incompatible ways in the next release."));
      opts.add(
          List.of(
              "-tag",
              "lucene.spi:t:SPI Name (case-insensitive: if the name is 'htmlStrip', 'htmlstrip' can be used when looking up the service)."));

      opts.add(List.of("-doclet", "org.apache.lucene.missingdoclet.MissingDoclet"));
      opts.add(List.of("-docletpath", getDocletpath().getAsPath()));

      opts.add(List.of("--missing-level", getJavadocMissingLevel().get()));

      if (getJavadocMissingIgnore().isPresent()) {
        opts.add(List.of("--missing-ignore", String.join(",", getJavadocMissingIgnore().get())));
      }

      if (getJavadocMissingMethod().isPresent()) {
        opts.add(List.of("--missing-method", String.join(",", getJavadocMissingMethod().get())));
      }

      opts.add("-quiet");

      // Add all extra options, if any.
      opts.addAll(getExtraOpts().getOrElse(List.of()));

      Map<String, File> allOfflineLinks = new LinkedHashMap<>();
      allOfflineLinks.putAll(getOfflineLinks().get());

      // TODO, NOCOMMIT: Resolve inter-project links:
      allOfflineLinks.forEach(
          (url, dir) -> {
            // Some sanity check/ validation here to ensure dir/package-list or dir/element-list is
            // present.
            if (!Files.exists(dir.toPath().resolve("package-list"))
                && !Files.exists(dir.toPath().resolve("element-list"))) {
              throw new GradleException(
                  "Expected pre-rendered package-list or element-list at: " + dir);
            }
            getLogger().info("Linking ${url} to ${dir}");
            opts.add(List.of("-linkoffline", url, dir.toString()));
          });

      opts.add(List.of("--release", getReleaseVersion().get().getMajorVersion()));

      opts.add("-Xdoclint:all,-missing");

      // Increase Javadoc's heap.
      opts.add("-J-Xmx512m");

      // Force locale to be "en_US" (fix for: https://bugs.openjdk.java.net/browse/JDK-8222793)
      opts.add("-J-Duser.language=en");
      opts.add("-J-Duser.country=US");

      // -J options have to be passed on command line, they are not interpreted if passed via args
      // file.
      var jOpts =
          opts.stream()
              .filter(opt -> opt instanceof String && ((String) opt).startsWith("-J"))
              .toList();
      opts.removeAll(jOpts);

      // Collect all source files, for now excluding module descriptors.
      opts.addAll(
          srcDirs.stream()
              .flatMap(
                  dir ->
                      getProject()
                          .fileTree(
                              dir,
                              cfg -> {
                                cfg.include("**/*.java");
                                cfg.exclude("**/module-info.java");
                              })
                          .getFiles()
                          .stream())
              .map(File::toString)
              .toList());

      // handle doc-files manually since in explicit source file mode javadoc does not copy them.
      for (var dir : srcDirs) {
        getFileOps()
            .copy(
                spec -> {
                  spec.into(getOutputDir());
                  spec.from(
                      dir,
                      cfg -> {
                        cfg.include("**/doc-files/**");
                      });
                });
      }

      // Temporary file that holds all javadoc options for the current task (except jOpts)
      try (var writer = Files.newBufferedWriter(optionsFile)) {

        // escapes an option with single quotes or whitespace to be passed in the options.txt file
        // for
        Function<String, String> escapeJavadocOption =
            s -> {
              if (Pattern.compile("[ '\"]").matcher(s).find()) {
                String escaped = s.replaceAll("[\\\\'\"]", "\\$0");
                return "'" + escaped + "'";
              } else {
                return s;
              }
            };

        for (var entry : opts) {
          if (entry instanceof List<?> asList) {
            writer.write(
                asList.stream()
                    .map(v -> escapeJavadocOption.apply(v.toString()))
                    .collect(Collectors.joining(" ")));
          } else {
            writer.write(escapeJavadocOption.apply(entry.toString()));
          }
          writer.write("\n");
        }
      }

      var javadocCmd = getProject().file(getExecutable().get());
      getLogger().info("Javadoc executable used: ${javadocCmd}");

      buildGlobals.quietExec(
          this,
          execSpec -> {
            execSpec.executable(javadocCmd);
            execSpec.args("@" + optionsFile);
            execSpec.args(jOpts);
          });

      /*
      TODO: implement this.

          // append some special table css, prettify css
          ant.concat(destfile: "${outputDir}/stylesheet.css", append: "true", fixlastline: "true", encoding: "UTF-8") {
            filelist(dir: taskResources, files:
            [
              "table_padding.css",
              "custom_styles.css",
              "prettify/prettify.css"
            ].join(" ")
            )
          }

          // append prettify to scripts
          ant.concat(destfile: "${outputDir}/script.js", append: "true", fixlastline: "true", encoding: "UTF-8") {
            filelist(dir: project.file("${taskResources}/prettify"), files: "prettify.js inject-javadocs.js")
          }

          ant.fixcrlf(srcdir: outputDir, includes: "stylesheet.css script.js", eol: "lf", fixlast: "true", encoding: "UTF-8")
       */
    }
  }

  public abstract static class OfflineLink implements Serializable {
    @Input
    public abstract Property<String> getUrl();

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract DirectoryProperty getLocation();

    @Inject
    public OfflineLink(String url, File location) {
      this.getUrl().set(url);
      this.getLocation().set(location);
    }
  }
}
