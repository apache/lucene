package org.apache.lucene.gradle.buildoptions;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.gradle.api.Describable;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.provider.ValueSource;
import org.gradle.api.provider.ValueSourceParameters;
import org.jetbrains.annotations.NotNull;

/**
 * A plugin providing {@code buildOptions} extension with overrideable key-value options that may
 * affect the build. For example, a random {@code tests.seed} or {@code tests.filter}.
 */
public class BuildOptionsPlugin implements Plugin<Project> {
  public static final String BUILD_OPTIONS_FILE = "build-options.properties";
  public static final String LOCAL_BUILD_OPTIONS_FILE = "build-options.local.properties";
  public static final String OPTIONS_EXTENSION_NAME = "buildOptions";

  @Inject
  public BuildOptionsPlugin() {}

  public abstract static class OptionFileValueSource
      implements ValueSource<String, OptionFileValueSource.Parameters>, Describable {

    @Nullable
    public String obtain() {
      return getParameters().getValue().getOrNull();
    }

    public String getDisplayName() {
      return String.format(
          Locale.ROOT,
          "override of '%s' in property file %s",
          getParameters().getName().get(),
          getParameters().getSourceFile().get());
    }

    public abstract static class Parameters implements ValueSourceParameters {
      abstract Property<String> getSourceFile();

      abstract Property<String> getValue();

      abstract Property<String> getName();
    }
  }

  @Override
  public void apply(Project project) {
    BuildOptionsExtension options = project.getObjects().newInstance(BuildOptionsExtension.class);
    project.getExtensions().add(OPTIONS_EXTENSION_NAME, options);

    Map<String, String> buildOptionsFile = readBuildOptions(project, BUILD_OPTIONS_FILE);
    Map<String, String> localBuildOptionsFile = readBuildOptions(project, LOCAL_BUILD_OPTIONS_FILE);
    options
        .getAllOptions()
        .whenObjectAdded(
            option -> {
              var providers = project.getProviders();
              var optionName = option.getName();
              option
                  .getValue()
                  .convention(
                      providers
                          .systemProperty(optionName)
                          .map(
                              v ->
                                  new BuildOptionValue(
                                      v, false, BuildOptionValueSource.SYSTEM_PROPERTY))
                          .orElse(
                              providers
                                  .gradleProperty(optionName)
                                  .map(
                                      v ->
                                          new BuildOptionValue(
                                              v, false, BuildOptionValueSource.GRADLE_PROPERTY)))
                          .orElse(
                              providers
                                  .environmentVariable(optionName)
                                  .map(
                                      v ->
                                          new BuildOptionValue(
                                              v,
                                              false,
                                              BuildOptionValueSource.ENVIRONMENT_VARIABLE)))
                          .orElse(
                              fromLocalFile(
                                  providers,
                                  optionName,
                                  localBuildOptionsFile,
                                  BuildOptionValueSource.LOCAL_BUILD_OPTIONS_FILE,
                                  LOCAL_BUILD_OPTIONS_FILE))
                          .orElse(
                              fromLocalFile(
                                  providers,
                                  optionName,
                                  buildOptionsFile,
                                  BuildOptionValueSource.BUILD_OPTIONS_FILE,
                                  BUILD_OPTIONS_FILE))
                          .orElse(option.getDefaultValue()));
            });

    project.getTasks().register(BuildOptionsTask.NAME, BuildOptionsTask.class);
  }

  private static @NotNull Provider<BuildOptionValue> fromLocalFile(
      ProviderFactory providers,
      String optionName,
      Map<String, String> localOptions,
      BuildOptionValueSource source,
      String sourceFile) {
    return providers
        .of(
            OptionFileValueSource.class,
            valueSource -> {
              OptionFileValueSource.Parameters params = valueSource.getParameters();
              params.getSourceFile().set(sourceFile);
              params.getName().set(optionName);
              if (localOptions.containsKey(optionName)) {
                params.getValue().set(localOptions.get(optionName));
              }
            })
        .map(v -> new BuildOptionValue(v, false, source));
  }

  private static @NotNull Map<String, String> readBuildOptions(
      Project project, String buildOptionsFile) {
    Map<String, String> localOptions = new TreeMap<>();
    var localOptionsFile =
        project.getRootProject().getLayout().getProjectDirectory().file(buildOptionsFile);
    if (localOptionsFile.getAsFile().exists()) {
      try (var is = Files.newInputStream(localOptionsFile.getAsFile().toPath())) {
        var v = new Properties();
        v.load(is);
        v.stringPropertyNames().forEach(key -> localOptions.put(key, v.getProperty(key)));
      } catch (IOException e) {
        throw new GradleException("Can't read the " + buildOptionsFile + " file.", e);
      }
    }
    return localOptions;
  }
}
