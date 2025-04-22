package org.apache.lucene.gradle.buildoptions;

import java.util.Comparator;
import javax.inject.Inject;
import org.gradle.api.DefaultTask;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.logging.text.StyledTextOutput;
import org.gradle.internal.logging.text.StyledTextOutput.Style;
import org.gradle.internal.logging.text.StyledTextOutputFactory;

/** Displays a list of all build options and their current values declared in the project. */
public abstract class BuildOptionsTask extends DefaultTask {
  public static final String BUILD_OPTIONS_TASK_GROUP = "Build options";
  public static final String NAME = "buildOptions";

  private final String projectName;

  static final Style normal = Style.Normal;
  static final Style computed = Style.Identifier;
  static final Style overridden = Style.FailureHeader;
  static final Style comment = Style.ProgressStatus;

  @Inject
  protected abstract StyledTextOutputFactory getOutputFactory();

  @Input
  public NamedDomainObjectContainer<BuildOption> getAllBuildOptions() {
    var optsExtension = getProject().getExtensions().getByType(BuildOptionsExtension.class);
    return optsExtension.getAllOptions();
  }

  @Inject
  public BuildOptionsTask(Project project) {
    setDescription("Shows configurable options");
    setGroup(BUILD_OPTIONS_TASK_GROUP);
    this.projectName =
        (project == project.getRootProject() ? ": (the root project)" : project.getPath());
  }

  @TaskAction
  public void exec() {
    var out = getOutputFactory().create(this.getClass());

    out.append("Configurable build options in ")
        .withStyle(Style.Identifier)
        .append(projectName)
        .append("\n\n");

    final int keyWidth =
        getAllBuildOptions().stream().mapToInt(opt -> opt.getName().length()).max().orElse(1);
    final String keyFmt = "%-" + keyWidth + "s = ";

    getAllBuildOptions().stream()
        .sorted(Comparator.comparing(BuildOption::getName))
        .forEach(
            opt -> {
              var value = opt.getValue();

              // boolean isComputed =
              // boolean isOverriden =
              // var overrideSource =
              String valueSource = null;
              var valueStyle = normal;
              if (!value.isPresent()) {
                valueStyle = comment;
              } else {
                var optionValue = value.get();
                if (optionValue.source() == BuildOptionValueSource.COMPUTED_VALUE) {
                  valueStyle = computed;
                  valueSource = "computed value";
                } else if (!optionValue.defaultValue()) {
                  valueStyle = overridden;
                  valueSource =
                      switch (optionValue.source()) {
                        case GRADLE_PROPERTY -> "project property";
                        case SYSTEM_PROPERTY -> "system property";
                        case ENVIRONMENT_VARIABLE -> "environment variable";
                        case EXPLICIT_VALUE -> "explicit value";
                        case COMPUTED_VALUE -> throw new RuntimeException("Unreachable");
                        case BUILD_OPTIONS_FILE -> BuildOptionsPlugin.BUILD_OPTIONS_FILE + " file";
                        case LOCAL_BUILD_OPTIONS_FILE ->
                            BuildOptionsPlugin.LOCAL_BUILD_OPTIONS_FILE + " file";
                      };
                }
              }

              out.format(keyFmt, opt.getName());
              out.withStyle(valueStyle).format("%-8s", value.isPresent() ? value.get() : "[empty]");
              out.withStyle(comment).append(" # ");
              if (valueSource != null) {
                out.withStyle(valueStyle).append("(source: ").append(valueSource).append(") ");
              }
              out.withStyle(comment).append(opt.getDescription());
              out.append("\n");
            });

    out.println();
    printLegend(out);
  }

  private void printLegend(StyledTextOutput out) {
    out.append("Option value colors legend: ");
    out.withStyle(normal).append("default value");
    out.append(", ");
    out.withStyle(computed).append("computed value");
    out.append(", ");
    out.withStyle(overridden).append("overridden value");
    out.append(", ");
    out.withStyle(comment).append("no value");
    out.println();
  }
}
