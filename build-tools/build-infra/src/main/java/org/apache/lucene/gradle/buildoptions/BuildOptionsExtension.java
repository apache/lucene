package org.apache.lucene.gradle.buildoptions;

import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.provider.Provider;

public abstract class BuildOptionsExtension {
  abstract NamedDomainObjectContainer<BuildOption> getAllOptions();

  /** Returns a lazy provider for the given option. */
  public Provider<String> optionValue(String name) {
    return getAllOptions().named(name).flatMap(BuildOption::asStringProvider);
  }

  public BuildOption getOption(String name) {
    return getAllOptions().named(name).get();
  }

  public boolean hasOption(String name) {
    return getAllOptions().findByName(name) != null;
  }

  public Provider<String> getAt(String name) {
    return optionValue(name);
  }

  public Provider<String> propertyMissing(String name) {
    return optionValue(name);
  }

  /** Build option with the default value. */
  public BuildOption addOption(String name, String description, String defaultValue) {
    return getAllOptions()
        .create(
            name,
            opt -> {
              opt.setDescription(description);
              opt.getDefaultValue()
                  .set(
                      new BuildOptionValue(
                          defaultValue, true, BuildOptionValueSource.EXPLICIT_VALUE));
            });
  }

  /** Build option with some dynamically computed value. */
  public BuildOption addOption(
      String name, String description, Provider<String> defaultValueProvider) {
    return getAllOptions()
        .create(
            name,
            opt -> {
              opt.setDescription(description);
              opt.getDefaultValue()
                  .set(
                      defaultValueProvider.map(
                          value ->
                              new BuildOptionValue(
                                  value, true, BuildOptionValueSource.COMPUTED_VALUE)));
            });
  }

  /** Build option without any default value. */
  public BuildOption addOption(String name, String description) {
    return getAllOptions().create(name, opt -> opt.setDescription(description));
  }
}
