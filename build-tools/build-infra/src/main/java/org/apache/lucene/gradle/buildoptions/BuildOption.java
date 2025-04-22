package org.apache.lucene.gradle.buildoptions;

import java.util.Locale;
import org.gradle.api.GradleException;
import org.gradle.api.Named;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;

public abstract class BuildOption implements Named {
  public abstract Property<BuildOptionValue> getValue();

  abstract Property<BuildOptionValue> getDefaultValue();

  public abstract String getDescription();

  abstract void setDescription(String description);

  public final Provider<Boolean> asBooleanProvider() {
    return getValue()
        .map(
            value -> {
              String v = value.value().toLowerCase(Locale.ROOT);
              if (v.equals("true") || v.equals("false")) {
                return Boolean.parseBoolean(v);
              }
              throw new GradleException("Not a Boolean value: " + value);
            });
  }

  public final Provider<String> asStringProvider() {
    return getValue().map(BuildOptionValue::value);
  }

  public final Provider<Integer> asIntProvider() {
    return getValue()
        .map(
            value -> {
              try {
                return Integer.parseInt(value.value());
              } catch (NumberFormatException e) {
                throw new GradleException("Not a Integer value: " + value.value());
              }
            });
  }
}
