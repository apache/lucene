package org.apache.lucene.analysis.morpheme.dict;

import org.apache.lucene.util.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DictionaryResourceLoader {

  /** Used to specify where (dictionary) resources get loaded from. */
  public enum ResourceScheme {
    CLASSPATH,
    FILE
  }

  private final ResourceScheme resourceScheme;
  private final String resourcePath;
  private final Class<?> ownerClass;

  public DictionaryResourceLoader(ResourceScheme resourceScheme, String resourcePath, Class<?> ownerClass) {
    this.resourceScheme = resourceScheme;
    this.resourcePath = resourcePath;
    // the actual owner of the resources; can resident in another module.
    this.ownerClass = ownerClass;
  }

  public final InputStream getResource(String suffix) throws IOException {
    return switch (resourceScheme) {
      case CLASSPATH -> getClassResource(resourcePath + suffix, ownerClass);
      case FILE -> Files.newInputStream(Paths.get(resourcePath + suffix));
      default -> throw new IllegalStateException("unknown resource scheme " + resourceScheme);
    };
  }

  private static InputStream getClassResource(String path, Class<?> clazz) throws IOException {
    // TODO: may not work on module-mode
    return IOUtils.requireResourceNonNull(clazz.getResourceAsStream(path), path);
  }

}
