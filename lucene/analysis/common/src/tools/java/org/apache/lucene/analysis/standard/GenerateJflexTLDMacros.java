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
package org.apache.lucene.analysis.standard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Generates a file containing JFlex macros to accept valid ASCII TLDs (top level domains), for
 * inclusion in JFlex grammars that can accept domain names.
 *
 * <p>The IANA TLD Database is queried via HTTP from URL cmdline arg #0, the response is parsed, and
 * the results are written out to a file containing a JFlex macro that will accept all valid
 * ASCII-only TLDs, including punycode forms of internationalized TLDs (output file cmdline arg #1).
 */
public class GenerateJflexTLDMacros {

  public static void main(String... args) throws Exception {
    if (args.length != 3 || args[0].equals("--help") || args[0].equals("-help")) {
      System.err.println("Cmd line params:");
      System.err.println(
          "  java "
              + GenerateJflexTLDMacros.class.getName()
              + "<ZoneFileURL> <JFlexOutputFile> <TLDListFile>");
      System.exit(1);
    }
    new GenerateJflexTLDMacros(args[0], args[1], args[2]).execute();
  }

  private static final String NL = "\n";

  private static final String APACHE_LICENSE =
      "/*"
          + NL
          + " * Licensed to the Apache Software Foundation (ASF) under one or more"
          + NL
          + " * contributor license agreements.  See the NOTICE file distributed with"
          + NL
          + " * this work for additional information regarding copyright ownership."
          + NL
          + " * The ASF licenses this file to You under the Apache License, Version 2.0"
          + NL
          + " * (the \"License\"); you may not use this file except in compliance with"
          + NL
          + " * the License.  You may obtain a copy of the License at"
          + NL
          + " *"
          + NL
          + " *     http://www.apache.org/licenses/LICENSE-2.0"
          + NL
          + " *"
          + NL
          + " * Unless required by applicable law or agreed to in writing, software"
          + NL
          + " * distributed under the License is distributed on an \"AS IS\" BASIS,"
          + NL
          + " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied."
          + NL
          + " * See the License for the specific language governing permissions and"
          + NL
          + " * limitations under the License."
          + NL
          + " */"
          + NL;

  private final URL tldFileURL;
  private long tldFileLastModified = -1L;
  private final Path tldListFile;
  private final Path jflexMacroFile;

  private final SortedMap<String, Boolean> processedTLDsLongestFirst =
      new TreeMap<>(
          Comparator.comparing(String::length).reversed().thenComparing(String::compareTo));

  // list position indicates suffix length
  private final List<SortedSet<String>> TLDsBySuffixLength = new ArrayList<>();

  public GenerateJflexTLDMacros(String tldFileURL, String jflexFile, String tldListFile)
      throws Exception {
    this.tldFileURL = new URL(tldFileURL);
    this.jflexMacroFile = Paths.get(jflexFile);
    this.tldListFile = Paths.get(tldListFile);
  }

  /**
   * Downloads the IANA TLD Database, extracts the ASCII TLDs, then writes a set of JFlex macros
   * accepting any of them case-insensitively out to the specified output file.
   *
   * @throws IOException if there is a problem either downloading the database or writing out the
   *     output file.
   */
  public void execute() throws IOException {
    getIANATLDDatabase();
    partitionTLDprefixesBySuffixLength();
    writeOutput();
    int totalDomains = 0;
    for (int suffixLength = 0; suffixLength < TLDsBySuffixLength.size(); ++suffixLength) {
      int domainsAtThisSuffixLength = TLDsBySuffixLength.get(suffixLength).size();
      totalDomains += domainsAtThisSuffixLength;
      System.out.printf(
          Locale.ROOT, "%30s: %4d TLDs%n", getMacroName(suffixLength), domainsAtThisSuffixLength);
    }
    System.out.printf(Locale.ROOT, "%30s: %4d TLDs%n", "Total", totalDomains);
  }

  /**
   * Downloads the IANA TLD Database.
   *
   * @throws java.io.IOException if there is a problem downloading the database
   */
  private void getIANATLDDatabase() throws IOException {
    final URLConnection connection = tldFileURL.openConnection();
    connection.setUseCaches(false);
    connection.addRequestProperty("Cache-Control", "no-cache");
    connection.connect();
    tldFileLastModified = connection.getLastModified();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(connection.getInputStream(), StandardCharsets.US_ASCII))) {
      String line;
      while (null != (line = reader.readLine())) {
        if (line.startsWith("#")) {
          continue;
        }
        processedTLDsLongestFirst.put(line.toLowerCase(Locale.ROOT), Boolean.FALSE);
      }
    }
    System.out.println(
        "Found "
            + processedTLDsLongestFirst.size()
            + " TLDs in IANA TLD Database at "
            + tldFileURL);
  }

  /**
   * Partition TLDs by whether they are prefixes of other TLDs and then by suffix length. We only
   * care about TLDs that are prefixes and are exactly one character shorter than another TLD. See
   * LUCENE-8278 and LUCENE-5391.
   */
  @SuppressWarnings(
      "ModifyCollectionInEnhancedForLoop") // it looks like it works because it is a sorted map!
  private void partitionTLDprefixesBySuffixLength() {
    TLDsBySuffixLength.add(new TreeSet<>()); // initialize set for zero-suffix TLDs
    for (SortedMap.Entry<String, Boolean> entry : processedTLDsLongestFirst.entrySet()) {
      String TLD = entry.getKey();
      if (entry.getValue()) {
        // System.out.println("Skipping already processed: " + TLD);
        continue;
      }
      // System.out.println("Adding zero-suffix TLD: " + TLD);
      TLDsBySuffixLength.get(0).add(TLD);
      for (int suffixLength = 1; (TLD.length() - suffixLength) >= 2; ++suffixLength) {
        String TLDprefix = TLD.substring(0, TLD.length() - suffixLength);
        if (false == processedTLDsLongestFirst.containsKey(TLDprefix)) {
          // System.out.println("Ignoring non-TLD prefix: " + TLDprefix);
          break; // shorter prefixes can be ignored
        }
        if (processedTLDsLongestFirst.get(TLDprefix)) {
          // System.out.println("Skipping already processed prefix: " + TLDprefix);
          break; // shorter prefixes have already been processed
        }

        processedTLDsLongestFirst.put(TLDprefix, true); // mark as processed
        if (TLDsBySuffixLength.size() == suffixLength) TLDsBySuffixLength.add(new TreeSet<>());
        SortedSet<String> TLDbucket = TLDsBySuffixLength.get(suffixLength);
        TLDbucket.add(TLDprefix);
        // System.out.println("Adding TLD prefix of " + TLD + " with suffix length " + suffixLength
        // + ": " + TLDprefix);
      }
    }
  }

  /**
   * Writes a file containing a JFlex macro that will accept any of the given TLDs
   * case-insensitively.
   */
  private void writeOutput() throws IOException {
    Files.writeString(
        tldListFile,
        "# Generated from IANA TLD Database (gradlew generateTlds)."
            + processedTLDsLongestFirst.keySet().stream()
                .sorted()
                .collect(Collectors.joining("\n")),
        StandardCharsets.UTF_8);

    final DateFormat dateFormat =
        DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL, Locale.ROOT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    try (Writer writer =
        new OutputStreamWriter(Files.newOutputStream(jflexMacroFile), StandardCharsets.UTF_8)) {
      writer.write(APACHE_LICENSE);
      writer.write("// Generated from IANA TLD Database <");
      writer.write(tldFileURL.toString());
      writer.write(">");
      writer.write(NL);
      if (tldFileLastModified > 0L) {
        writer.write("// file version from ");
        writer.write(dateFormat.format(tldFileLastModified));
        writer.write(NL);
      }
      writer.write("// generated by ");
      writer.write(this.getClass().getName());
      writer.write(NL);
      writer.write(NL);

      for (int i = 0; i < TLDsBySuffixLength.size(); ++i) {
        String macroName = getMacroName(i);
        writer.write("// LUCENE-8278: ");
        if (i == 0) {
          writer.write(
              "None of the TLDs in {"
                  + macroName
                  + "} is a 1-character-shorter prefix of another TLD");
        } else {
          writer.write("Each TLD in {" + macroName + "} is a prefix of another TLD by");
          writer.write(" " + i + " character");
          if (i > 1) {
            writer.write("s");
          }
        }
        writer.write(NL);
        writeTLDmacro(writer, macroName, TLDsBySuffixLength.get(i));
      }
    }
  }

  private String getMacroName(int suffixLength) {
    return "ASCIITLD" + (suffixLength > 0 ? "prefix_" + suffixLength + "CharSuffix" : "");
  }

  private void writeTLDmacro(Writer writer, String macroName, SortedSet<String> TLDs)
      throws IOException {
    writer.write(macroName);
    writer.write(" = \".\" (");
    writer.write(NL);

    boolean isFirst = true;
    for (String TLD : TLDs) {
      writer.write("  ");
      if (isFirst) {
        isFirst = false;
        writer.write("  ");
      } else {
        writer.write("| ");
      }
      writer.write(getCaseInsensitiveRegex(TLD));
      writer.write(NL);
    }
    writer.write("  ) \".\"?   // Accept trailing root (empty) domain");
    writer.write(NL);
    writer.write(NL);
  }

  /**
   * Returns a regex that will accept the given ASCII TLD case-insensitively.
   *
   * @param ASCIITLD The ASCII TLD to generate a regex for
   * @return a regex that will accept the given ASCII TLD case-insensitively
   */
  private String getCaseInsensitiveRegex(String ASCIITLD) {
    StringBuilder builder = new StringBuilder();
    for (int pos = 0; pos < ASCIITLD.length(); ++pos) {
      char ch = ASCIITLD.charAt(pos);
      if (Character.isDigit(ch) || ch == '-') {
        builder.append(ch);
      } else {
        builder.append("[").append(ch).append(Character.toUpperCase(ch)).append("]");
      }
    }
    return builder.toString();
  }
}
