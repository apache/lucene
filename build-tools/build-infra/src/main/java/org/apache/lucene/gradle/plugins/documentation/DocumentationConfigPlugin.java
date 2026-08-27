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
package org.apache.lucene.gradle.plugins.documentation;

import com.vladsch.flexmark.ast.Heading;
import com.vladsch.flexmark.ext.abbreviation.AbbreviationExtension;
import com.vladsch.flexmark.ext.attributes.AttributesExtension;
import com.vladsch.flexmark.ext.autolink.AutolinkExtension;
import com.vladsch.flexmark.ext.tables.TablesExtension;
import com.vladsch.flexmark.html.HtmlRenderer;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.parser.ParserEmulationProfile;
import com.vladsch.flexmark.util.ast.Document;
import com.vladsch.flexmark.util.data.MutableDataSet;
import com.vladsch.flexmark.util.sequence.Escaping;
import groovy.text.SimpleTemplateEngine;
import java.io.File;
import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.globals.LuceneBuildGlobalsExtension;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskContainer;

/** Configures documentation generation and other documentation-related aspects. */
public class DocumentationConfigPlugin extends LuceneGradlePlugin {
  public static final String OPT_JAVADOC_URL = "lucene.javadoc.url";

  @Override
  public void apply(Project project) {
    applicableToRootProjectOnly(project);

    LuceneBuildGlobalsExtension buildGlobals = getLuceneBuildGlobals(project);

    getBuildOptions(project)
        .addOption(
            OPT_JAVADOC_URL,
            "External Javadoc URL for documentation generator.",
            project
                .getProviders()
                .provider(
                    () -> {
                      if (buildGlobals.snapshotBuild) {
                        // non-release build does not cross-link between modules.
                        return null;
                      } else {
                        // release build
                        var urlVersion = buildGlobals.baseVersion.replace('.', '_');
                        return "https://lucene.apache.org/core/" + urlVersion;
                      }
                    }));

    TaskContainer tasks = project.getTasks();
    var documentationTask =
        tasks.register(
            "documentation",
            task -> {
              task.setGroup("documentation");
              task.setDescription("Generate all documentation");

              task.dependsOn(":lucene:documentation:assemble");
            });

    tasks.named("assemble").configure(task -> task.dependsOn(documentationTask));

    project.configure(
        List.of(project.project(":lucene:documentation")), this::configureDocumentationProject);
  }

  private void configureDocumentationProject(Project project) {
    File docroot = getDocumentationRoot(project);
    File markdownSrc = project.file("src/markdown");
    File assets = project.file("src/assets");

    var tasks = project.getTasks();

    configureMarkdownConversion(markdownSrc, docroot, project);

    var copyDocumentationAssets =
        tasks.register(
            "copyDocumentationAssets",
            Copy.class,
            task -> {
              task.setIncludeEmptyDirs(false);
              task.from(assets);
              task.into(docroot);
            });

    var documentationTask =
        tasks.register(
            "documentation",
            task -> {
              task.setGroup("documentation");
              task.setDescription("Generate Lucene documentation");

              task.dependsOn(
                  project
                      .getProviders()
                      .provider(
                          () ->
                              project.project(":lucene").getSubprojects().stream()
                                  .flatMap(
                                      prj ->
                                          prj
                                              .getTasks()
                                              .matching(
                                                  t -> t.getName().equals("renderSiteJavadoc"))
                                              .stream())
                                  .toList()));

              task.dependsOn(
                  copyDocumentationAssets,
                  "changesToHtml",
                  "markdownToHtml",
                  "createDocumentationIndex");
            });

    // Limit building the full documentation to CI runs only.
    // This is very costly so we only validate the build machinery there.
    var buildGlobals = getLuceneBuildGlobals(project);
    if (buildGlobals.isCIBuild) {
      tasks.named("check").configure(task -> task.dependsOn(documentationTask));
    }

    // assemble always builds the docs.
    tasks.named("assemble").configure(task -> task.dependsOn(documentationTask));

    // Expose the documentation as an artifact.
    String confName = "site";
    project.getConfigurations().create(confName);
    project
        .getArtifacts()
        .add(
            confName,
            docroot,
            configurablePublishArtifact -> {
              configurablePublishArtifact.builtBy(documentationTask);
            });
  }

  public static File getDocumentationRoot(Project project) {
    return project
        .project(":lucene:documentation")
        .getLayout()
        .getBuildDirectory()
        .dir("site")
        .get()
        .getAsFile();
  }

  private void configureMarkdownConversion(File markdownSrc, File docroot, Project project) {
    TaskContainer tasks = project.getTasks();
    var markdownToHtml =
        tasks.register(
            "markdownToHtml",
            Copy.class,
            task -> {
              task.dependsOn("copyDocumentationAssets");

              task.from(
                  project.project(":lucene").getLayout().getProjectDirectory().getAsFile(),
                  copySpec -> {
                    copySpec.include("MIGRATE.md");
                    copySpec.include("JRE_VERSION_MIGRATION.md");
                    copySpec.include("SYSTEM_REQUIREMENTS.md");
                  });

              task.setFilteringCharset("UTF-8");
              task.setIncludeEmptyDirs(false);

              task.rename(Pattern.compile("\\.md$"), ".html");
              task.filter(MarkdownFilterImpl.class);

              task.into(docroot);
            });

    tasks.register(
        "createDocumentationIndex",
        MarkdownTemplateTask.class,
        task -> {
          task.dependsOn(markdownToHtml);

          task.getOutputFile().set(docroot.toPath().resolve("index.html").toFile());
          task.getTemplateFile().set(markdownSrc.toPath().resolve("index.template.md").toFile());

          // list all properties used by the template here to allow uptodate checks to be correct:
          task.getInputs().property("version", project.getVersion());

          var defaultCodecFile =
              project.project(":lucene:core").file("src/java/org/apache/lucene/codecs/Codec.java");
          task.getInputs().file(defaultCodecFile);

          var buildGlobals = getLuceneBuildGlobals(project);
          task.getBinding()
              .put(
                  "project",
                  Map.ofEntries(
                      Map.entry("version", project.getVersion().toString()),
                      Map.entry("majorVersion", buildGlobals.majorVersion)));

          task.getBinding()
              .put(
                  "defaultCodecPackage",
                  project
                      .getProviders()
                      .provider(
                          () -> {
                            var regex =
                                Pattern.compile(
                                    "Codec defaultCodec = LOADER\\.lookup\\((?<codec>\"([^\"]+)\")\\);");
                            var matcher =
                                regex.matcher(Files.readString(defaultCodecFile.toPath()));
                            if (!matcher.find()) {
                              throw new GradleException(
                                  "Cannot determine default codec from file: " + defaultCodecFile);
                            }
                            return matcher.group("codec").toLowerCase(Locale.ROOT);
                          }));

          task.withProjectList();
        });
  }

  /**
   * A filter that can be used with gradle's "copy" task to transforms Markdown files into HTML
   * (adding HTML header, styling,...).
   */
  public static final class MarkdownFilterImpl extends FilterReader {
    public MarkdownFilterImpl(Reader reader) throws IOException {
      // this is not really a filter: it reads the whole file in ctor,
      // converts it and provides result downstream as a StringReader
      super(new StringReader(convert(reader.readAllAsString())));
    }

    public static String convert(String markdownSource) {
      // replace LUCENE or SOLR issue numbers with a markdown link
      markdownSource =
          markdownSource.replaceAll(
              "(?s)\\b(LUCENE|SOLR)-\\d+\\b", "[$0](https://issues.apache.org/jira/browse/$0)");
      // then follow with github issues/ pull requests.
      markdownSource =
          markdownSource.replaceAll(
              "(?s)\\b(GITHUB#|GH-)(\\d+)\\b", "[$0](https://github.com/apache/lucene/issues/$2)");

      // convert the markdown
      MutableDataSet options = new MutableDataSet();
      options.setFrom(ParserEmulationProfile.MARKDOWN);
      options.set(
          Parser.EXTENSIONS,
          List.of(
              AbbreviationExtension.create(),
              AutolinkExtension.create(),
              AttributesExtension.create(),
              TablesExtension.create()));

      options.set(HtmlRenderer.RENDER_HEADER_ID, true);
      options.set(HtmlRenderer.MAX_TRAILING_BLANK_LINES, 0);
      Document parsed = Parser.builder(options).build().parse(markdownSource);

      StringBuilder html = new StringBuilder("<html>\n<head>\n");
      var headingNode = parsed.getFirstChildAny(Heading.class);
      if (headingNode != null) {
        var title = ((Heading) headingNode).getText();
        html.append("<title>").append(Escaping.escapeHtml(title, false)).append("</title>\n");
      }
      html.append(
          """
          <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
          </head>
          <body>
          """);
      HtmlRenderer.builder(options).build().render(parsed, html);
      html.append("</body>\n</html>\n");
      return html.toString();
    }
  }

  /**
   * Applies a binding of variables using a template and produces Markdown, which is converted to
   * HTML.
   */
  public abstract static class MarkdownTemplateTask extends DefaultTask {
    @InputFile
    public abstract RegularFileProperty getTemplateFile();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @Input
    @Optional
    public abstract MapProperty<String, Object> getBinding();

    @TaskAction
    public void transform() throws IOException, ClassNotFoundException {
      var engine = new SimpleTemplateEngine();
      HashMap<String, Object> resolvedBinding = new HashMap<>(getBinding().get());
      String markdown =
          engine
              .createTemplate(Files.readString(getTemplateFile().get().getAsFile().toPath()))
              .make(resolvedBinding)
              .toString();

      var outputPath = getOutputFile().get().getAsFile().toPath();
      Files.createDirectories(outputPath.getParent());
      Files.writeString(outputPath, MarkdownFilterImpl.convert(markdown));
    }

    /**
     * adds a property "projectList" containing all subprojects with javadocs as markdown bullet
     * list
     */
    public void withProjectList() {
      var projectListProvider =
          getProject()
              .getProviders()
              .provider(
                  () -> {
                    var projectList =
                        getProject().project(":lucene").getSubprojects().stream()
                            .filter(
                                p ->
                                    p
                                        .getTasks()
                                        .matching(t -> t.getName().equals("renderSiteJavadoc"))
                                        .stream()
                                        .findAny()
                                        .isPresent())
                            .sorted(
                                Comparator.comparing((Project t) -> !t.getName().equals("core"))
                                    .thenComparing(t -> t.getName().equals("test-framework"))
                                    .thenComparing(Project::getPath))
                            .toList();

                    return projectList.stream()
                        .map(
                            project -> {
                              var text =
                                  String.format(
                                      Locale.ROOT,
                                      "**[%s](%s/index.html):** %s",
                                      relativeDocPath(project).replace('/', '-'),
                                      relativeDocPath(project),
                                      project.getDescription());
                              if (project.getName().equals("core")) {
                                text = text + " {style='font-size:larger; margin-bottom:.5em'}";
                              }
                              return "* " + text;
                            })
                        .collect(Collectors.joining("\n"));
                  });

      getBinding().put("projectList", projectListProvider);
    }
  }

  public static String relativeDocPath(Project p) {
    return p.getPath().replaceFirst(":\\w+:", "").replace(':', '/');
  }
}
