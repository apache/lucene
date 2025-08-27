package org.apache.lucene.gradle.plugins.licenses;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.rat.Defaults;
import org.apache.rat.ReportConfiguration;
import org.apache.rat.analysis.IHeaderMatcher;
import org.apache.rat.analysis.util.HeaderMatcherMultiplexer;
import org.apache.rat.anttasks.SubstringLicenseMatcher;
import org.apache.rat.api.RatException;
import org.apache.rat.document.impl.FileDocument;
import org.apache.rat.license.SimpleLicenseFamily;
import org.apache.rat.report.RatReport;
import org.apache.rat.report.claim.ClaimStatistic;
import org.apache.rat.report.xml.XmlReportFactory;
import org.apache.rat.report.xml.writer.impl.base.XmlWriter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Java port of the Groovy/Ant-based RAT task using Apache RAT Core API (0.16.1). */
@CacheableTask
public abstract class LicenceCheckTask extends DefaultTask {
  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  @IgnoreEmptyDirectories
  public abstract ConfigurableFileCollection getInputFileTrees();

  @OutputFile
  public abstract RegularFileProperty getXmlReport();

  @Inject
  public LicenceCheckTask(ProjectLayout layout) {
    getXmlReport().convention(layout.getBuildDirectory().file("rat/rat-report.xml"));
    setGroup("Verification");
    setDescription(
        "Runs Apache RAT on the configured sources and fails on unknown/unapproved licenses.");
  }

  @TaskAction
  public void run() {
    final String origEncoding = System.getProperty("file.encoding");
    File reportFile = getXmlReport().get().getAsFile();
    try {
      generateReport(reportFile);
      printUnknownFiles(reportFile);
    } finally {
      if (!Objects.equals(System.getProperty("file.encoding"), origEncoding)) {
        throw new GradleException(
            "Something is wrong: Apache RAT changed file.encoding to "
                + System.getProperty("file.encoding")
                + "?");
      }
    }
  }

  private void generateReport(File reportFile) {
    try {
      Files.createDirectories(reportFile.getParentFile().toPath());
      // Write the input file list for debugging
      String inputFileList =
          getInputFileTrees().getFiles().stream()
              .map(File::getPath)
              .sorted()
              .collect(Collectors.joining("\n"));
      File listFile = new File(reportFile.getPath().replaceAll("\\.xml$", "-filelist.txt"));
      try (Writer w =
          new OutputStreamWriter(new FileOutputStream(listFile), StandardCharsets.UTF_8)) {
        w.write(inputFileList);
      }

      ReportConfiguration config = new ReportConfiguration();

      List<IHeaderMatcher> matchers = new ArrayList<>();
      matchers.add(Defaults.createDefaultMatcher());

      matchers.add(
          subStringMatcher(
              "BSD4 ",
              "Original BSD License (with advertising clause)",
              "All advertising materials"));
      matchers.add(
          subStringMatcher(
              "BSD  ", "Modified BSD License", "Copyright (c) 2001-2009 Anders Moeller"));
      matchers.add(
          subStringMatcher(
              "BSD  ", "Modified BSD License", "Copyright (c) 2001, Dr Martin Porter"));
      matchers.add(
          subStringMatcher(
              "BSD  ",
              "Modified BSD License",
              "THIS SOFTWARE IS PROVIDED BY UNIVERSITY OF MASSACHUSETTS AND OTHER CONTRIBUTORS"));
      matchers.add(
          subStringMatcher(
              "BSD  ", "Modified BSD License", "Egothor Software License version 1.00"));
      matchers.add(
          subStringMatcher("BSD  ", "Modified BSD License", "Copyright (c) 2005 Bruno Martins"));
      matchers.add(
          subStringMatcher(
              "BSD  ",
              "Modified BSD License",
              "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS"));
      matchers.add(
          subStringMatcher(
              "BSD  ",
              "Modified BSD License",
              "THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS"));

      matchers.add(
          subStringMatcher(
              "MIT  ",
              "Modified BSD License",
              "Permission is hereby granted, free of charge, to any person obtaining a copy"));
      matchers.add(subStringMatcher("MIT  ", "Modified BSD License", " ; License: MIT"));

      matchers.add(
          subStringMatcher(
              "AL   ", "Apache", "Licensed to the Apache Software Foundation (ASF) under"));
      matchers.add(
          subStringMatcher(
              "AL   ",
              "Apache",
              "Licensed under the Apache License, Version 2.0 (the \"License\")"));

      matchers.add(subStringMatcher("GEN  ", "Generated", "Produced by GNUPLOT"));
      matchers.add(subStringMatcher("GEN  ", "Generated", "Generated by Snowball"));

      config.setHeaderMatcher(new HeaderMatcherMultiplexer(matchers));

      config.setApprovedLicenseNames(
          new SimpleLicenseFamily[] {
            simpleFamily("Apache"),
            simpleFamily("The MIT License"),
            simpleFamily("Modified BSD License"),
            simpleFamily("Generated")
          });

      Files.deleteIfExists(reportFile.toPath());
      try (Writer writer =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(reportFile), StandardCharsets.UTF_8))) {
        toXmlReportFile(config, writer);
      }
    } catch (IOException | RatException e) {
      throw new GradleException("Cannot generate RAT report", e);
    }
  }

  private static SimpleLicenseFamily simpleFamily(String name) {
    SimpleLicenseFamily fam = new SimpleLicenseFamily();
    fam.setFamilyName(name);
    return fam;
  }

  private static IHeaderMatcher subStringMatcher(
      String licenseFamilyCategory, String licenseFamilyName, String substringPattern) {
    SubstringLicenseMatcher substringLicenseMatcher = new SubstringLicenseMatcher();
    substringLicenseMatcher.setLicenseFamilyCategory(licenseFamilyCategory);
    substringLicenseMatcher.setLicenseFamilyName(licenseFamilyName);
    SubstringLicenseMatcher.Pattern p = new SubstringLicenseMatcher.Pattern();
    p.setSubstring(substringPattern);
    substringLicenseMatcher.addConfiguredPattern(p);
    return substringLicenseMatcher;
  }

  private void toXmlReportFile(ReportConfiguration config, Writer writer)
      throws RatException, IOException {
    ClaimStatistic stats = new ClaimStatistic();
    RatReport standardReport =
        XmlReportFactory.createStandardReport(new XmlWriter(writer), stats, config);
    standardReport.startReport();
    for (File f : getInputFileTrees().getFiles()) {
      standardReport.report(new FileDocument(f));
    }
    standardReport.endReport();
    writer.flush();
  }

  private void printUnknownFiles(File reportFile) {
    List<String> errors = parseUnknowns(reportFile);
    if (!errors.isEmpty()) {
      String msg =
          "Found "
              + errors.size()
              + " file(s) with errors:\n"
              + errors.stream().map(e -> "  - " + e).collect(Collectors.joining("\n"));
      throw new GradleException(msg);
    }
  }

  private static List<String> parseUnknowns(File reportFile) {
    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setXIncludeAware(false);
      dbf.setIgnoringComments(true);
      dbf.setExpandEntityReferences(false);
      dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
      dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
      dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

      Document doc = dbf.newDocumentBuilder().parse(reportFile);
      NodeList resources = doc.getElementsByTagName("resource");
      List<String> bad = new ArrayList<>();
      for (int i = 0; i < resources.getLength(); i++) {
        Element res = (Element) resources.item(i);
        NodeList children = res.getChildNodes();
        for (int j = 0; j < children.getLength(); j++) {
          if (children.item(j) instanceof Element el) {
            if ("license-approval".equals(el.getTagName())
                && "false".equals(el.getAttribute("name"))) {
              bad.add("Unknown license: " + res.getAttribute("name"));
              break;
            }
          }
        }
      }
      Collections.sort(bad);
      return bad;
    } catch (Exception e) {
      throw new GradleException("Error parsing RAT XML report: " + reportFile.getAbsolutePath(), e);
    }
  }
}
