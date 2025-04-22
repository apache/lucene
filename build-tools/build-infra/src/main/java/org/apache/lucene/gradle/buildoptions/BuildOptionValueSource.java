package org.apache.lucene.gradle.buildoptions;

public enum BuildOptionValueSource {
  GRADLE_PROPERTY,
  SYSTEM_PROPERTY,
  ENVIRONMENT_VARIABLE,
  EXPLICIT_VALUE,
  COMPUTED_VALUE,
  BUILD_OPTIONS_FILE,
  LOCAL_BUILD_OPTIONS_FILE
}
