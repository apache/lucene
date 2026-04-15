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
package org.apache.lucene.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.ParseException;
import java.util.Locale;
import java.util.jar.Manifest;

/**
 * Use by certain classes to match version compatibility across releases of Lucene.
 *
 * <p><b>WARNING</b>: When changing the version parameter that you supply to components in Lucene,
 * do not simply change the version at search-time, but instead also adjust your indexing code to
 * match, and re-index.
 */
public final class Version {

  /**
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_0_0 = new Version(10, 0, 0);

  /**
   * Match settings and bugs in Lucene's 10.1.0 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_1_0 = new Version(10, 1, 0);

  /**
   * Match settings and bugs in Lucene's 10.2.0 release.
   *
   * @deprecated Use latest
   * @deprecated (10.3.0) Use latest
   */
  @Deprecated public static final Version LUCENE_10_2_0 = new Version(10, 2, 0);

  /**
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_2_1 = new Version(10, 2, 1);

  /**
   * Match settings and bugs in Lucene's 10.2.2 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_2_2 = new Version(10, 2, 2);

  /**
   * Match settings and bugs in Lucene's 10.3.0 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_3_0 = new Version(10, 3, 0);

  /**
   * Match settings and bugs in Lucene's 10.3.1 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_3_1 = new Version(10, 3, 1);

  /**
   * Match settings and bugs in Lucene's 10.3.2 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_3_2 = new Version(10, 3, 2);

  /**
   * Match settings and bugs in Lucene's 10.4.0 release.
   *
   * @deprecated Use latest
   * @deprecated (10.5.0) Use latest
   */
  @Deprecated public static final Version LUCENE_10_4_0 = new Version(10, 4, 0);

  /**
   * Match settings and bugs in Lucene's 10.5.0 release.
   *
   * @deprecated Use latest
   */
  @Deprecated public static final Version LUCENE_10_5_0 = new Version(10, 5, 0);

  /**
   * Match settings and bugs in Lucene's 11.0.0 release.
   *
   * <p>Use this to get the latest &amp; greatest settings, bug fixes, etc, for Lucene.
   */
  public static final Version LUCENE_11_0_0 = new Version(11, 0, 0);

  // To add a new version:
  //  * Only add above this comment
  //  * If the new version is the newest, change LATEST below and deprecate the previous LATEST

  /**
   * <b>WARNING</b>: if you use this setting, and then upgrade to a newer release of Lucene, sizable
   * changes may happen. If backwards compatibility is important then you should instead explicitly
   * specify an actual version.
   *
   * <p>If you use this constant then you may need to <b>re-index all of your documents</b> when
   * upgrading Lucene, as the way text is indexed may have changed. Additionally, you may need to
   * <b>re-test your entire application</b> to ensure it behaves as expected, as some defaults may
   * have changed and may break functionality in your application.
   */
  public static final Version LATEST = LUCENE_11_0_0;

  /**
   * Constant for backwards compatibility.
   *
   * @deprecated Use {@link #LATEST}
   */
  @Deprecated public static final Version LUCENE_CURRENT = LATEST;

  /**
   * Constant for the minimal supported major version number of an index. This version is defined by
   * the major version number that initially created the index.
   *
   * <p>This constant is manually controlled and should only be bumped when format changes make it
   * impossible to safely read older indexes. Examples include:
   *
   * <ul>
   *   <li>Lossy encoding changes (e.g., norms format changes that cannot be recovered)
   *   <li>Index-level format changes that prevent reading (e.g., segments_N file format)
   *   <li>Critical corruption bugs that make older indexes potentially invalid
   * </ul>
   *
   * <p>This constant should NOT be bumped automatically with major version number releases. The
   * goal is to allow users to upgrade across multiple major version numbers when safe to do so.
   *
   * <p><b>Two-tier version policy:</b>
   *
   * <ul>
   *   <li><b>Index opening policy:</b> An index can be opened if its {@code
   *       indexCreatedVersionMajor} is >= this constant, regardless of how many major version
   *       numbers have been released since.
   *   <li><b>Codec reader policy:</b> Segment codecs are only shipped for the current major version
   *       number and the immediately previous major version number. When no format breaks occur
   *       between consecutive major version numbers, the previous major version number reader can
   *       read segments from older major version numbers that use the same format.
   * </ul>
   *
   * <p><b>When to bump this constant:</b>
   *
   * <ul>
   *   <li>When introducing an incompatible on-disk format change
   *   <li>When fixing a critical bug that makes older indexes potentially corrupt
   *   <li>When the maintenance burden of supporting older creation versions becomes too high
   * </ul>
   *
   * <p><b>How to bump this constant:</b>
   *
   * <ol>
   *   <li>Set the value to the last major version number before the breaking change
   *   <li>Update tests to reflect the new minimum
   *   <li>Update CHANGES.txt and MIGRATE.md with upgrade instructions
   *   <li>Ensure IndexFormatTooOldException messages reference the new minimum
   * </ol>
   *
   * <p><b>Example:</b> If a breaking change is introduced in version 15.0.0, set this constant to
   * 14, which will prevent indexes created with major version numbers 13 and earlier from being
   * opened.
   *
   * @since 11.0.0
   */
  public static final int MIN_SUPPORTED_MAJOR = 10;

  /**
   * @see #getPackageImplementationVersion()
   */
  @SuppressWarnings("NonFinalStaticField")
  private static String implementationVersion;

  /**
   * Parse a version number of the form {@code "major.minor.bugfix.prerelease"}.
   *
   * <p>Part {@code ".bugfix"} and part {@code ".prerelease"} are optional. Note that this is
   * forwards compatible: the parsed version does not have to exist as a constant.
   *
   * @lucene.internal
   */
  public static Version parse(String version) throws ParseException {

    StrictStringTokenizer tokens = new StrictStringTokenizer(version, '.');
    if (tokens.hasMoreTokens() == false) {
      throw new ParseException(
          "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int major;
    String token = tokens.nextToken();
    try {
      major = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p =
          new ParseException(
              "Failed to parse major version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    if (tokens.hasMoreTokens() == false) {
      throw new ParseException(
          "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int minor;
    token = tokens.nextToken();
    try {
      minor = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p =
          new ParseException(
              "Failed to parse minor version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    int bugfix = 0;
    int prerelease = 0;
    if (tokens.hasMoreTokens()) {

      token = tokens.nextToken();
      try {
        bugfix = Integer.parseInt(token);
      } catch (NumberFormatException nfe) {
        ParseException p =
            new ParseException(
                "Failed to parse bugfix version from \"" + token + "\" (got: " + version + ")", 0);
        p.initCause(nfe);
        throw p;
      }

      if (tokens.hasMoreTokens()) {
        token = tokens.nextToken();
        try {
          prerelease = Integer.parseInt(token);
        } catch (NumberFormatException nfe) {
          ParseException p =
              new ParseException(
                  "Failed to parse prerelease version from \""
                      + token
                      + "\" (got: "
                      + version
                      + ")",
                  0);
          p.initCause(nfe);
          throw p;
        }
        if (prerelease == 0) {
          throw new ParseException(
              "Invalid value "
                  + prerelease
                  + " for prerelease; should be 1 or 2 (got: "
                  + version
                  + ")",
              0);
        }

        if (tokens.hasMoreTokens()) {
          // Too many tokens!
          throw new ParseException(
              "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
        }
      }
    }

    try {
      return new Version(major, minor, bugfix, prerelease);
    } catch (IllegalArgumentException iae) {
      ParseException pe =
          new ParseException(
              "failed to parse version string \"" + version + "\": " + iae.getMessage(), 0);
      pe.initCause(iae);
      throw pe;
    }
  }

  /**
   * Parse the given version number as a constant or dot based version.
   *
   * <p>This method allows to use {@code "LUCENE_X_Y"} constant names, or version numbers in the
   * format {@code "x.y.z"}.
   *
   * @lucene.internal
   */
  public static Version parseLeniently(String version) throws ParseException {
    String versionOrig = version;
    version = version.toUpperCase(Locale.ROOT);
    switch (version) {
      case "LATEST":
      case "LUCENE_CURRENT":
        return LATEST;
      default:
        version =
            version
                .replaceFirst("^LUCENE_(\\d+)_(\\d+)_(\\d+)$", "$1.$2.$3")
                .replaceFirst("^LUCENE_(\\d+)_(\\d+)$", "$1.$2.0")
                .replaceFirst("^LUCENE_(\\d)(\\d)$", "$1.$2.0");
        try {
          return parse(version);
        } catch (ParseException pe) {
          ParseException pe2 =
              new ParseException(
                  "failed to parse lenient version string \""
                      + versionOrig
                      + "\": "
                      + pe.getMessage(),
                  0);
          pe2.initCause(pe);
          throw pe2;
        }
    }
  }

  /**
   * Returns a new version based on raw numbers
   *
   * @lucene.internal
   */
  public static Version fromBits(int major, int minor, int bugfix) {
    return new Version(major, minor, bugfix);
  }

  /** Major version, the difference between stable and trunk */
  public final int major;

  /** Minor version, incremented within the stable branch */
  public final int minor;

  /** Bugfix number, incremented on release branches */
  public final int bugfix;

  /** Prerelease version, currently 0 (alpha), 1 (beta), or 2 (final) */
  public final int prerelease;

  // stores the version pieces, with most significant pieces in high bits
  // ie:  | 1 byte | 1 byte | 1 byte |   2 bits   |
  //         major   minor    bugfix   prerelease
  private final int encodedValue;

  private Version(int major, int minor, int bugfix) {
    this(major, minor, bugfix, 0);
  }

  private Version(int major, int minor, int bugfix, int prerelease) {
    this.major = major;
    this.minor = minor;
    this.bugfix = bugfix;
    this.prerelease = prerelease;
    // NOTE: do not enforce major version so we remain future proof, except to
    // make sure it fits in the 8 bits we encode it into:
    if (major > 255 || major < 0) {
      throw new IllegalArgumentException("Illegal major version: " + major);
    }
    if (minor > 255 || minor < 0) {
      throw new IllegalArgumentException("Illegal minor version: " + minor);
    }
    if (bugfix > 255 || bugfix < 0) {
      throw new IllegalArgumentException("Illegal bugfix version: " + bugfix);
    }
    if (prerelease > 2 || prerelease < 0) {
      throw new IllegalArgumentException("Illegal prerelease version: " + prerelease);
    }
    if (prerelease != 0 && (minor != 0 || bugfix != 0)) {
      throw new IllegalArgumentException(
          "Prerelease version only supported with major release (got prerelease: "
              + prerelease
              + ", minor: "
              + minor
              + ", bugfix: "
              + bugfix
              + ")");
    }

    encodedValue = major << 18 | minor << 10 | bugfix << 2 | prerelease;

    assert encodedIsValid();
  }

  /** Returns true if this version is the same or after the version from the argument. */
  public boolean onOrAfter(Version other) {
    return encodedValue >= other.encodedValue;
  }

  @Override
  public String toString() {
    if (prerelease == 0) {
      return "" + major + "." + minor + "." + bugfix;
    }
    return "" + major + "." + minor + "." + bugfix + "." + prerelease;
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o instanceof Version && ((Version) o).encodedValue == encodedValue;
  }

  // Used only by assert:
  private boolean encodedIsValid() {
    assert major == ((encodedValue >>> 18) & 0xFF);
    assert minor == ((encodedValue >>> 10) & 0xFF);
    assert bugfix == ((encodedValue >>> 2) & 0xFF);
    assert prerelease == (encodedValue & 0x03);
    return true;
  }

  @Override
  public int hashCode() {
    return encodedValue;
  }

  /**
   * Return Lucene's full implementation version. This version is saved in Lucene's metadata at
   * build time (JAR manifest, module info). If it is not available, an {@code unknown}
   * implementation version is returned.
   *
   * @return Lucene implementation version string, never {@code null}.
   */
  public static String getPackageImplementationVersion() {
    // Initialize the lazy value.
    synchronized (Version.class) {
      if (implementationVersion == null) {
        String version;

        Package p = Version.class.getPackage();
        version = p.getImplementationVersion();

        if (version == null) {
          var module = Version.class.getModule();
          if (module.isNamed()) {
            // Running as a module? Try parsing the manifest manually.
            try (var is = module.getResourceAsStream("/META-INF/MANIFEST.MF")) {
              if (is != null) {
                Manifest m = new Manifest(is);
                version = m.getMainAttributes().getValue("Implementation-Version");
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        }

        if (version == null) {
          version = "unknown";
        }

        implementationVersion = version;
      }

      return implementationVersion;
    }
  }
}
