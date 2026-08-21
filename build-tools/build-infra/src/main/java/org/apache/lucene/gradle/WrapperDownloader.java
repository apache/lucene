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
package org.apache.lucene.gradle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipException;

/**
 * Standalone Gradle bootstrap, launched by {@code gradlew}/{@code gradlew.bat} directly from source
 * ({@code java WrapperDownloader.java ...}) instead of {@code gradle-wrapper.jar}. It reads {@code
 * gradle/wrapper/gradle-wrapper.properties}, downloads, verifies and unpacks the Gradle
 * distribution into the Gradle user home (same directory layout and hashing as Gradle's own
 * wrapper, so installations are shared with IDEs) and then hands over to {@code
 * org.gradle.launcher.GradleMain}.
 *
 * <p>The distribution URL can be overridden with the {@value #DISTRIBUTION_URL_PROPERTY} system
 * property (for example {@code GRADLE_OPTS="-Dgradle.wrapper.distributionUrl=..."}, {@code
 * ./gradlew -D...} or {@code systemProp.gradle.wrapper.distributionUrl=...} in {@code
 * gradle.properties}). The value may contain {@code ${gradleVersion}} and {@code
 * ${distributionType}} placeholders, expanded from the versioned {@code distributionUrl}. The
 * {@code distributionSha256Sum} is still verified.
 *
 * <p>This class must have no dependencies outside of the JDK and must stay compilable with JDK 11
 * (no records, text blocks, switch expressions or pattern matching) so that {@link #checkVersion()}
 * can print a friendly error when the script is run on an old JDK.
 */
public class WrapperDownloader {
  /**
   * Copied to keep the class isolated from any other classes.
   *
   * @see "https://github.com/apache/lucene/issues/15399"
   */
  @Retention(RetentionPolicy.CLASS)
  @Target({ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
  private @interface SuppressForbidden {
    /** A reason for suppressing should always be given. */
    String reason();
  }

  /** System property with the project (root) directory, set by the launch scripts. */
  public static final String PROJECT_DIR_PROPERTY = "lucene.wrapper.projectDir";

  /** System property overriding the {@code distributionUrl} from gradle-wrapper.properties. */
  public static final String DISTRIBUTION_URL_PROPERTY = "gradle.wrapper.distributionUrl";

  private static final String GRADLE_USER_HOME_PROPERTY = "gradle.user.home";
  private static final Pattern DISTRIBUTION_NAME =
      Pattern.compile("gradle-(?<version>.+?)-(?<type>bin|all)\\.zip");
  private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)\\}");
  private static final boolean IS_WINDOWS =
      System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("windows");

  private final Path projectDir;
  private Path gradleUserHome;
  private boolean quiet;

  public static void main(String[] args) {
    Path launcherJar;
    try {
      checkVersion();
      String projectDir = System.getProperty(PROJECT_DIR_PROPERTY);
      if (projectDir == null) {
        throw new IOException("Missing system property: " + PROJECT_DIR_PROPERTY);
      }
      launcherJar = new WrapperDownloader(Paths.get(projectDir)).installDistribution(args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage());
      System.exit(3);
      return;
    }

    try {
      launchGradle(launcherJar, args);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      System.exit(3);
    }
  }

  public static void checkVersion() {
    int major = Runtime.version().feature();
    if (major < 25 || major > 26) {
      throw new IllegalStateException("java version must be 25..26, your version: " + major);
    }
  }

  WrapperDownloader(Path projectDir) {
    this.projectDir = projectDir.toAbsolutePath().normalize();
  }

  /** Installs the distribution if needed and returns the path to its launcher jar. */
  Path installDistribution(String[] args) throws Exception {
    applySystemProperties(args);

    Path wrapperProperties = projectDir.resolve("gradle/wrapper/gradle-wrapper.properties");
    Properties props = loadProperties(wrapperProperties);
    String distributionUrl = props.getProperty("distributionUrl");
    if (distributionUrl == null) {
      throw new IOException("No 'distributionUrl' in " + wrapperProperties);
    }
    String override = System.getProperty(DISTRIBUTION_URL_PROPERTY, "").trim();
    if (!override.isEmpty()) {
      distributionUrl = expandTemplate(override, distributionUrl);
      log(
          "Using Gradle distribution URL from "
              + DISTRIBUTION_URL_PROPERTY
              + ": "
              + distributionUrl);
    }
    URI distribution = new URI(distributionUrl);
    if (distribution.getScheme() == null) {
      // relative url: resolve against the properties file's directory.
      distribution = wrapperProperties.resolveSibling(distribution.getSchemeSpecificPart()).toUri();
    }

    // Local paths, identical to Gradle's: <base>/<path>/<name>/<md5 of the url as base36>.
    String zipName = distribution.getPath().replaceAll(".*/", "");
    String distName = zipName.replaceAll("\\.[^.]*$", "");
    String hash =
        new BigInteger(
                1,
                MessageDigest.getInstance("MD5")
                    .digest(withoutUserInfo(distribution).getBytes(StandardCharsets.UTF_8)))
            .toString(36);
    Path distDir =
        baseDir(props.getProperty("distributionBase", "GRADLE_USER_HOME"))
            .resolve(props.getProperty("distributionPath", "wrapper/dists"))
            .resolve(distName)
            .resolve(hash);
    Path zip =
        baseDir(props.getProperty("zipStoreBase", "GRADLE_USER_HOME"))
            .resolve(props.getProperty("zipStorePath", "wrapper/dists"))
            .resolve(distName)
            .resolve(hash)
            .resolve(zipName);
    Path marker = zip.resolveSibling(zipName + ".ok");
    Path lockFile = zip.resolveSibling(zipName + ".lck");

    String sha256 = props.getProperty("distributionSha256Sum");
    int timeout = Integer.parseInt(props.getProperty("networkTimeout", "10000").trim());
    int retries = Integer.parseInt(props.getProperty("retries", "0").trim());
    int backOffMs = Integer.parseInt(props.getProperty("retryBackOffMs", "500").trim());

    // Serialize concurrent installations of the same distribution with a lock file.
    Files.createDirectories(lockFile.getParent());
    long deadline = System.nanoTime() + 120_000_000_000L;
    while (true) {
      try (FileChannel channel =
          FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        FileLock lock = channel.tryLock();
        if (lock != null) {
          try {
            Path launcherJar = Files.exists(marker) ? findLauncherJar(distDir) : null;
            if (launcherJar == null) {
              launcherJar =
                  install(distribution, zip, distDir, sha256, timeout, retries, backOffMs);
              Files.deleteIfExists(zip);
              if (!Files.exists(marker)) {
                Files.createFile(marker);
              }
            }
            return launcherJar;
          } finally {
            lock.release();
          }
        }
      }
      if (System.nanoTime() > deadline) {
        throw new IOException("Timed out waiting for the lock on " + lockFile);
      }
      sleep(200);
    }
  }

  /**
   * Sets {@code systemProp.*} properties from the project's and the user's gradle.properties and
   * {@code -D} arguments (in this order of precedence); also determines the gradle user home from
   * {@code -g/--gradle-user-home}, the {@code gradle.user.home} property or {@code
   * GRADLE_USER_HOME}.
   */
  private void applySystemProperties(String[] args) throws IOException {
    String userHomeArg = null;
    Map<String, String> cliProps = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      String next = i + 1 < args.length ? args[i + 1] : null;
      if (arg.equals("-q") || arg.equals("--quiet")) {
        quiet = true;
      } else if ((arg.equals("-g") || arg.equals("--gradle-user-home")) && next != null) {
        userHomeArg = args[++i];
      } else if (arg.startsWith("--gradle-user-home=")) {
        userHomeArg = arg.substring("--gradle-user-home=".length());
      } else if (arg.startsWith("-g") && !arg.startsWith("--")) {
        userHomeArg = arg.substring(2);
      } else if (arg.equals("-D") && next != null) {
        putProperty(cliProps, args[++i]);
      } else if (arg.startsWith("-D")) {
        putProperty(cliProps, arg.substring(2));
      }
    }

    Map<String, String> projectProps =
        systemPropertiesFrom(projectDir.resolve("gradle.properties"));
    Map<String, String> merged = new HashMap<>(projectProps);
    merged.putAll(cliProps);
    if (merged.containsKey(GRADLE_USER_HOME_PROPERTY)) {
      System.setProperty(GRADLE_USER_HOME_PROPERTY, merged.get(GRADLE_USER_HOME_PROPERTY));
    }

    if (userHomeArg != null) {
      gradleUserHome = Paths.get(userHomeArg);
    } else if (System.getProperty(GRADLE_USER_HOME_PROPERTY) != null) {
      gradleUserHome = Paths.get(System.getProperty(GRADLE_USER_HOME_PROPERTY));
    } else if (System.getenv("GRADLE_USER_HOME") != null) {
      gradleUserHome = Paths.get(System.getenv("GRADLE_USER_HOME"));
    } else {
      gradleUserHome = Paths.get(System.getProperty("user.home"), ".gradle");
    }

    Map<String, String> userProps =
        systemPropertiesFrom(gradleUserHome.resolve("gradle.properties"));
    // The gradle user home cannot be changed from within the gradle user home.
    userProps.remove(GRADLE_USER_HOME_PROPERTY);
    merged = new HashMap<>(projectProps);
    merged.putAll(userProps);
    merged.putAll(cliProps);
    System.getProperties().putAll(merged);
  }

  private static void putProperty(Map<String, String> props, String keyValue) {
    int eq = keyValue.indexOf('=');
    props.put(
        eq < 0 ? keyValue : keyValue.substring(0, eq), eq < 0 ? "" : keyValue.substring(eq + 1));
  }

  private static Map<String, String> systemPropertiesFrom(Path propertiesFile) throws IOException {
    Map<String, String> result = new HashMap<>();
    if (Files.isRegularFile(propertiesFile)) {
      Properties props = loadProperties(propertiesFile);
      for (String name : props.stringPropertyNames()) {
        if (name.startsWith("systemProp.") && name.length() > "systemProp.".length()) {
          result.put(name.substring("systemProp.".length()), props.getProperty(name));
        }
      }
    }
    return result;
  }

  private static Properties loadProperties(Path propertiesFile) throws IOException {
    Properties props = new Properties();
    try (Reader reader = Files.newBufferedReader(propertiesFile, StandardCharsets.UTF_8)) {
      props.load(reader);
    }
    return props;
  }

  /** Expands {@code ${gradleVersion}} and {@code ${distributionType}} in an override template. */
  static String expandTemplate(String template, String distributionUrl) throws IOException {
    Matcher placeholders = PLACEHOLDER.matcher(template);
    if (!placeholders.find()) {
      return template;
    }
    Matcher name =
        DISTRIBUTION_NAME.matcher(distributionUrl.replaceAll("\\?.*", "").replaceAll(".*/", ""));
    if (!name.matches()) {
      throw new IOException(
          "Can't expand placeholders in "
              + DISTRIBUTION_URL_PROPERTY
              + ", the distribution URL does not look like gradle-<version>-<type>.zip: "
              + distributionUrl);
    }
    Map<String, String> values = new HashMap<>();
    values.put("gradleVersion", name.group("version"));
    values.put("distributionType", name.group("type"));

    StringBuilder sb = new StringBuilder();
    placeholders.reset();
    while (placeholders.find()) {
      String value = values.get(placeholders.group(1));
      if (value == null) {
        throw new IOException(
            "Unknown placeholder "
                + placeholders.group()
                + " in "
                + DISTRIBUTION_URL_PROPERTY
                + " (supported: ${gradleVersion}, ${distributionType}): "
                + template);
      }
      placeholders.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    return placeholders.appendTail(sb).toString();
  }

  private Path baseDir(String base) throws IOException {
    switch (base) {
      case "GRADLE_USER_HOME":
        return gradleUserHome;
      case "PROJECT":
        return projectDir;
      default:
        throw new IOException("Unknown base directory: " + base);
    }
  }

  private static String withoutUserInfo(URI uri) throws URISyntaxException {
    return new URI(
            uri.getScheme(),
            null,
            uri.getHost(),
            uri.getPort(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment())
        .toASCIIString();
  }

  /** Downloads (unless already present), verifies and unpacks the distribution. */
  private Path install(
      URI distribution,
      Path zip,
      Path distDir,
      String sha256,
      int timeout,
      int retries,
      int backOffMs)
      throws Exception {
    for (int zipAttempt = 1; ; zipAttempt++) {
      if (!Files.isRegularFile(zip)) {
        download(distribution, zip, timeout, retries, backOffMs);
      }
      if (sha256 != null) {
        String actual = sha256(zip);
        if (!sha256.trim().equalsIgnoreCase(actual)) {
          Files.delete(zip);
          throw new IOException(
              "Checksum mismatch for the Gradle distribution downloaded from "
                  + withoutUserInfo(distribution)
                  + " (expected: "
                  + sha256
                  + ", actual: "
                  + actual
                  + "). Check 'distributionSha256Sum' in gradle-wrapper.properties"
                  + " and https://gradle.org/release-checksums/");
        }
      }
      try {
        // Remove whatever is left from previous (broken) installations and unpack.
        if (Files.isDirectory(distDir)) {
          try (Stream<Path> s = Files.list(distDir)) {
            for (Path p : s.filter(Files::isDirectory).collect(Collectors.toList())) {
              deleteRecursively(p);
            }
          }
        }
        unzip(zip, distDir);
      } catch (ZipException e) {
        Files.deleteIfExists(zip);
        if (zipAttempt >= 3) {
          throw new IOException(
              "Downloaded distribution " + zip + " is not a valid zip file: " + e);
        }
        log("Downloaded distribution " + zip + " is not a valid zip file, retrying.");
        continue;
      }

      Path launcherJar = findLauncherJar(distDir);
      if (launcherJar == null) {
        throw new IOException("Distribution " + distribution + " does not contain Gradle?");
      }
      Path gradleCommand = launcherJar.getParent().resolveSibling("bin").resolve("gradle");
      if (!IS_WINDOWS && Files.exists(gradleCommand)) {
        try {
          Files.setPosixFilePermissions(
              gradleCommand, PosixFilePermissions.fromString("rwxr-xr-x"));
        } catch (IOException | UnsupportedOperationException e) {
          log("Could not set executable permissions for: " + gradleCommand + " (" + e + ")");
        }
      }
      return launcherJar;
    }
  }

  private void download(URI distribution, Path zip, int timeout, int retries, long backOffMs)
      throws Exception {
    URL url = new URI(withoutUserInfo(distribution)).toURL();
    Path part = zip.resolveSibling(zip.getFileName() + ".part");
    Files.createDirectories(zip.getParent());
    for (int attempt = 1; ; attempt++) {
      log("Downloading " + url);
      try {
        // Proxies are configured globally using the HTTP(S) proxy system properties.
        URLConnection conn = url.openConnection();
        conn.setRequestProperty("User-Agent", "gradlew");
        conn.setConnectTimeout(timeout);
        conn.setReadTimeout(timeout);
        if (conn instanceof HttpURLConnection
            && ((HttpURLConnection) conn).getResponseCode() != HttpURLConnection.HTTP_OK) {
          throw new IOException(
              "Server returned HTTP " + ((HttpURLConnection) conn).getResponseCode());
        }
        try (InputStream in = conn.getInputStream();
            OutputStream out = Files.newOutputStream(part)) {
          byte[] buffer = new byte[64 * 1024];
          long total = 0;
          for (int n; (n = in.read(buffer)) != -1; total += n) {
            out.write(buffer, 0, n);
            if (!quiet && (total + n) / (1024 * 1024) > total / (1024 * 1024)) {
              System.err.print('.');
              System.err.flush();
            }
          }
        }
        log("");
        Files.move(part, zip, StandardCopyOption.REPLACE_EXISTING);
        return;
      } catch (IOException e) {
        log("");
        Files.deleteIfExists(part);
        if (attempt > retries) {
          throw new IOException("Could not download " + url + ": " + e.getMessage(), e);
        }
        log("Download failed (" + e.getMessage() + "), retrying in " + backOffMs + " ms.");
        sleep(backOffMs);
        backOffMs *= 2;
      }
    }
  }

  private static String sha256(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream in = Files.newInputStream(path)) {
      byte[] buffer = new byte[64 * 1024];
      for (int n; (n = in.read(buffer)) != -1; ) {
        digest.update(buffer, 0, n);
      }
    }
    return String.format(Locale.ROOT, "%064x", new BigInteger(1, digest.digest()));
  }

  private static void unzip(Path zip, Path dest) throws IOException {
    // The zip file system throws a ZipException on corrupted archives.
    try (FileSystem zipFs = FileSystems.newFileSystem(zip, (ClassLoader) null)) {
      for (Path root : zipFs.getRootDirectories()) {
        try (Stream<Path> entries = Files.walk(root)) {
          for (Path entry : entries.collect(Collectors.toList())) {
            String name = root.relativize(entry).toString();
            // zip-slip check.
            if (name.startsWith("/")
                || name.startsWith("\\")
                || (IS_WINDOWS && name.contains(":"))
                || Stream.of(name.replace('\\', '/').split("/")).anyMatch(".."::equals)) {
              throw new IOException("Unsafe zip entry: " + name);
            }
            Path target = dest.resolve(name);
            if (Files.isDirectory(entry)) {
              Files.createDirectories(target);
            } else {
              Files.copy(entry, target, StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
      }
    }
  }

  private static void deleteRecursively(Path dir) throws IOException {
    try (Stream<Path> s = Files.walk(dir)) {
      for (Path p : s.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
        Files.delete(p);
      }
    }
  }

  /** Returns {@code lib/gradle-launcher-*.jar} of the single directory under distDir, or null. */
  private static Path findLauncherJar(Path distDir) throws IOException {
    if (!Files.isDirectory(distDir)) {
      return null;
    }
    List<Path> candidates = new ArrayList<>();
    try (Stream<Path> dirs = Files.list(distDir)) {
      for (Path gradleHome : dirs.filter(Files::isDirectory).collect(Collectors.toList())) {
        Path lib = gradleHome.resolve("lib");
        if (Files.isDirectory(lib)) {
          try (Stream<Path> jars = Files.list(lib)) {
            jars.filter(p -> p.getFileName().toString().matches("gradle-launcher-.*\\.jar"))
                .forEach(candidates::add);
          }
        }
      }
    }
    return candidates.size() == 1 ? candidates.get(0) : null;
  }

  @SuppressForbidden(reason = "Gradle's launcher relies on the context class loader being set.")
  private static void launchGradle(Path launcherJar, String[] args) throws Throwable {
    // The launcher jar's manifest Class-Path pulls in the rest of the distribution.
    URLClassLoader classLoader =
        new URLClassLoader(
            new URL[] {launcherJar.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      classLoader
          .loadClass("org.gradle.launcher.GradleMain")
          .getMethod("main", String[].class)
          .invoke(null, new Object[] {args});
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
    classLoader.close();
  }

  private void log(String message) {
    if (!quiet) {
      System.err.println(message);
    }
  }

  @SuppressForbidden(reason = "Valid use of thread.sleep.")
  private static void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
}
