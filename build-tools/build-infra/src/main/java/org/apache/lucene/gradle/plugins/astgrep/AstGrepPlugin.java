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

    var astToolOption =
        project
            .getExtensions()
            .getByType(BuildOptionsExtension.class)
            .addOption(optionName, "External ast-grep executable (path or name)");

    var testAstGrepRules =
        tasks.register(
            "testAstGrepRules",
            Exec.class,
            (task) -> {
              task.setArgs(
                  List.of(
                      "test",
                      "--skip-snapshot-tests",
                      "-c",
                      "gradle/validation/ast-grep/sgconfig.yml"));
            });

    var applyAstGrepRulesTask =
        tasks.register(
            "applyAstGrepRules",
            Exec.class,
            task -> {
              task.dependsOn(testAstGrepRules);

              if (!astToolOption.isPresent()) {
                task.getLogger()
                    .warn(
                        "The ast-grep tool location is not set ('{}' option), will not apply ast-grep rules.",
                        optionName);
              }

              task.setArgs(List.of("scan", "-c", "gradle/validation/ast-grep/sgconfig.yml"));
            });

    // Common configuration.
    List.of(testAstGrepRules, applyAstGrepRulesTask)
        .forEach(
            taskProv -> {
              taskProv.configure(
                  task -> {
                    if (!astToolOption.isPresent()) {
                      task.setEnabled(false);
                    }

                    task.setIgnoreExitValue(false);
                    if (astToolOption.isPresent()) {
                      task.setExecutable(astToolOption.get());
                    }
                    task.setWorkingDir(project.getLayout().getProjectDirectory());
                  });
            });

    tasks
        .matching(task -> task.getName().equals("check") || task.getName().equals("tidy"))
        .configureEach(
            task -> {
              task.dependsOn(applyAstGrepRulesTask);
            });
  }
}
