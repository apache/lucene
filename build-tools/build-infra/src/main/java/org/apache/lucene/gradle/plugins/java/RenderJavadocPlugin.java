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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.documentation.DocumentationConfigPlugin;
import org.apache.tools.ant.taskdefs.Concat;
import org.apache.tools.ant.taskdefs.FixCRLF;
import org.apache.tools.ant.types.FileList;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFile;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.internal.file.FileOperations;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
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

    // Add a rendering task that produces the output suitable for the Lucene site.
    Set<Project> publishedProjects = getLuceneBuildGlobals(project).getPublishedProjects();

    if (project.getPath().equals(":lucene:luke") || !(publishedProjects.contains(project))) {
      // These projects are not part of the public API so we don't render their javadocs
      // as part of the site's creation.
    } else {
      tasks.register(
          "renderSiteJavadoc",
          RenderJavadocTask.class,
          task -> {
            task.setGroup("documentation");
            task.setDescription(
                "Generates Javadoc API documentation for the site (relative links).");

            task.getTaskResources().set(resources);

            {
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
            }

            JavaVersion minJavaVersion = getLuceneBuildGlobals(project).getMinJavaVersion().get();
            task.getReleaseVersion().set(minJavaVersion);

            // site-creation specific settings.
            task.getRelativeProjectLinks().set(true);
            task.getEnableSearch().set(true);
            // Place the documentation under the documentation directory.
            task.getOutputDir()
                .set(
                    DocumentationConfigPlugin.getDocumentationRoot(project)
                        .toPath()
                        .resolve(DocumentationConfigPlugin.relativeDocPath(project))
                        .toFile());
          });
    }

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
              + "from https://docs.oracle.com/en/java/javase/"
              + minJava
              + "/docs/api/element-list");
    }

    String junitVersion = getVersionCatalog(project).findVersion("junit").get().toString();
    Path junitJavadocPackages = resources.toPath().resolve("junit-" + junitVersion + "/");
    if (!Files.exists(junitJavadocPackages)) {
      throw new GradleException(
          "Prefetched javadoc package-list is missing at "
              + junitJavadocPackages
              + ", "
              + "create this directory and fetch the package-list file from "
              + "from https://junit.org/junit4/javadoc/"
              + junitVersion
              + "/package-list");
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

    // Add cross-project documentation task dependencies:
    // - each RenderJavaDocs task gets a dependency to all tasks with the same name
    //   present in this project's dependency configuration 'implementation'.
    // - a lazy provider is used to collect these dependencies.
    project
        .getTasks()
        .withType(RenderJavadocTask.class)
        .configureEach(
            task -> {
              task.dependsOn(
                  project
                      .getProviders()
                      .provider(
                          () -> {
                            var allDeps =
                                task.getProject()
                                    .getConfigurations()
                                    .getByName("implementation")
                                    .getAllDependencies();
                            var subtasks =
                                allDeps.withType(ProjectDependency.class).stream()
                                    .map(dep -> dep.getPath() + ":" + task.getName())
                                    .toList();

                            task.getLogger()
                                .info(
                                    "Task {} depends on -> {}",
                                    task.getPath(),
                                    String.join(", ", subtasks));

                            return subtasks;
                          }));
            });
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
      getJavadocMissingIgnore().convention(List.of());
    }

    @TaskAction
    public void render() throws IOException {
      List<File> srcDirs =
          getSrcDirSet().get().getSourceDirectories().getFiles().stream()
              .filter(f -> Files.exists(f.toPath()))
              .toList();

      Path optionsFile = getTemporaryDir().toPath().resolve("javadoc-options.txt");
      Files.createDirectories(optionsFile.getParent());

      // if we are re-rendering, wipe any previous data.
      getFileOps().delete(getOutputDir().get().getAsFile());

      // create the directory, so relative link calculation knows that it's a directory.
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

      var missingIgnored = getJavadocMissingIgnore().getOrElse(List.of());
      if (!missingIgnored.isEmpty()) {
        opts.add(List.of("--missing-ignore", String.join(",", missingIgnored)));
      }

      var missingMethod = getJavadocMissingMethod().getOrElse(List.of());
      if (!missingMethod.isEmpty()) {
        opts.add(List.of("--missing-method", String.join(",", missingMethod)));
      }

      opts.add("-quiet");

      // Add all extra options, if any.
      opts.addAll(getExtraOpts().getOrElse(List.of()));

      Map<String, File> allOfflineLinks = new LinkedHashMap<>();
      allOfflineLinks.putAll(getOfflineLinks().get());

      addOfflineOrRelativeLinksToDependencies(opts, allOfflineLinks);

      allOfflineLinks.forEach(
          (url, dir) -> {
            // Some sanity check/ validation here to ensure dir/package-list or dir/element-list is
            // present.
            if (!Files.exists(dir.toPath().resolve("package-list"))
                && !Files.exists(dir.toPath().resolve("element-list"))) {
              throw new GradleException(
                  "Expected pre-rendered package-list or element-list at: " + dir);
            }
            getLogger().info("Offline link: {} to {}", url, dir);
            opts.add(List.of("-linkoffline", url, dir.toString()));
          });

      opts.add(List.of("--release", getReleaseVersion().get().getMajorVersion()));

      opts.add("-Xdoclint:all,-missing");

      // Increase Javadoc's heap.
      opts.add("-J-Xmx512m");

      // Force locale to be "en_US" (fix for: https://bugs.openjdk.java.net/browse/JDK-8222793)
      opts.add("-J-Duser.language=en");
      opts.add("-J-Duser.country=US");

      // add custom scripts and css.
      {
        // append some special table css, prettify css.
        Provider<RegularFile> customCss =
            getOutputDir().file("resource-files/lucene-stylesheet.css");
        concat(
            customCss,
            getTaskResources(),
            "table_padding.css",
            "custom_styles.css",
            "prettify/prism.css");

        // append prettify to scripts
        Provider<RegularFile> customScript = getOutputDir().file("script-files/lucene-script.js");
        concat(customScript, getTaskResources().dir("prettify"), "prism.js");

        opts.add(List.of("--add-script", customScript.get().getAsFile().toString()));
        opts.add(List.of("--add-stylesheet", customCss.get().getAsFile().toString()));
      }

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
                String escaped = s.replaceAll("[\\\\'\"]", "\\\\$0");
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
      getLogger().info("Javadoc executable used: " + javadocCmd);

      buildGlobals.quietExec(
          this,
          execSpec -> {
            execSpec.executable(javadocCmd);
            execSpec.args("@" + optionsFile);
            execSpec.args(jOpts);
          });
    }

    private void concat(
        Provider<RegularFile> targetFile, Provider<Directory> dir, String... files) {
      var concat = (Concat) getAnt().getAntProject().createTask("concat");
      concat.setDestfile(targetFile.get().getAsFile());
      concat.setAppend(true);
      concat.setFixLastLine(true);
      concat.setEncoding("UTF-8");
      concat.addFilelist(fileList(dir, files));
      concat.execute();

      var fixcrlf = (FixCRLF) getAnt().getAntProject().createTask("fixcrlf");
      fixcrlf.setSrcdir(dir.get().getAsFile());
      fixcrlf.setIncludes(String.join(" ", files));
      fixcrlf.setEncoding("UTF-8");
      fixcrlf.setFixlast(true);
      var lf = new FixCRLF.CrLf();
      lf.setValue("lf");
      fixcrlf.setEol(lf);
      fixcrlf.execute();
    }

    private FileList fileList(Provider<Directory> dir, String... files) {
      var fileList = new FileList();
      fileList.setDir(dir.get().getAsFile());
      fileList.setFiles(String.join(" ", files));
      return fileList;
    }

    /**
     * Resolve inter-project links:
     *
     * <ul>
     *   <li>find all (enabled) tasks from the subgraph of tasks this task depends on (with same
     *       name)
     *   <li>sort these tasks, ordering the 'core' first, then lexigraphically by path
     *   <li>for each task, get the output dir to create relative or absolute link.
     * </ul>
     */
    private void addOfflineOrRelativeLinksToDependencies(
        List<Object> opts, Map<String, File> allOfflineLinks) {
      List<RenderJavadocTask> sortedTaskDeps;
      {
        Set<RenderJavadocTask> taskDeps = new HashSet<>();
        var taskGraph = getProject().getGradle().getTaskGraph();
        ArrayDeque<Task> remaining = new ArrayDeque<>(List.of(this));
        var thisTaskName = getName();
        while (!remaining.isEmpty()) {
          var task = remaining.pop();
          taskGraph.getDependencies(task).stream()
              .filter(t -> t.getName().equals(thisTaskName) && t.getEnabled())
              .forEach(
                  t -> {
                    if (taskDeps.add(((RenderJavadocTask) t))) {
                      remaining.add(t);
                    }
                  });
        }

        sortedTaskDeps =
            taskDeps.stream()
                .sorted(
                    Comparator.<Task, Boolean>comparing(
                            t -> !t.getProject().getName().equals("core"))
                        .thenComparing(Task::getPath))
                .toList();
      }

      for (var otherTask : sortedTaskDeps) {
        Project otherProject = otherTask.getProject();
        // For relative links we compute the actual relative link between projects.
        if (getRelativeProjectLinks().getOrElse(true)) {
          Path pathTo = otherTask.getOutputDir().get().getAsFile().toPath().toAbsolutePath();
          Path pathFrom = getOutputDir().get().getAsFile().toPath().toAbsolutePath();
          String relative = pathFrom.relativize(pathTo).toString().replace(File.separatorChar, '/');
          getLogger().info("Relative link: {} to {}", otherProject.getPath(), relative);
          opts.add(List.of("-link", relative));
        } else {
          // For absolute links, we determine the target URL by assembling the full URL (if base is
          // available).
          if (getLuceneDocUrl().isPresent()) {
            allOfflineLinks.put(
                getLuceneDocUrl().get()
                    + "/"
                    + DocumentationConfigPlugin.relativeDocPath(otherProject),
                otherTask.getOutputDir().get().getAsFile());
          } else {
            // Ignore, not linking relative modules at all.
          }
        }
      }
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
