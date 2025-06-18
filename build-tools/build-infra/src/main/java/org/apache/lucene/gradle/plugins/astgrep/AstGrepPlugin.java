package org.apache.lucene.gradle.plugins.astgrep;

import com.carrotsearch.gradle.buildinfra.buildoptions.BuildOptionsExtension;
import java.util.List;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

public class AstGrepPlugin implements Plugin<Project> {
  @Override
  public void apply(Project project) {
    if (project != project.getRootProject()) {
      throw new GradleException("This plugin can be applied to the root project only.");
    }

    var optionName = "lucene.tool.ast-grep";
    var tasks = project.getTasks();
    var applyAstGrepRulesTask =
        tasks.register(
            "applyAstGrepRules",
            Exec.class,
            task -> {
              var option =
                  project
                      .getExtensions()
                      .getByType(BuildOptionsExtension.class)
                      .getOption(optionName)
                      .asStringProvider();

              if (!option.isPresent()) {
                task.getLogger()
                    .warn(
                        "The ast-grep tool location is not set ('{}' option), will not apply ast-grep rules.",
                        optionName);
                task.setEnabled(false);
              }

              task.setIgnoreExitValue(false);
              if (option.isPresent()) {
                task.setExecutable(option.get());
              }
              task.setWorkingDir(project.getLayout().getProjectDirectory());
              task.setArgs(List.of("scan", "-c", "gradle/validation/ast-grep/sgconfig.yml"));
            });
    tasks
        .named("check")
        .configure(
            task -> {
              task.dependsOn(applyAstGrepRulesTask);
            });
  }
}
