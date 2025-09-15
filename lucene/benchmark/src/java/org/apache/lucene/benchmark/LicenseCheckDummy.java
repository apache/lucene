package org.apache.lucene.benchmark;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.rat.*;
import org.apache.rat.analysis.IHeaderMatcher;
import org.apache.rat.analysis.matchers.CopyrightMatcher;
import org.apache.rat.analysis.matchers.OrMatcher;
import org.apache.rat.analysis.matchers.SimpleTextMatcher;
import org.apache.rat.api.RatException;
import org.apache.rat.document.impl.FileDocument;
import org.apache.rat.license.ILicense;
import org.apache.rat.license.ILicenseFamily;
import org.apache.rat.report.IReportable;
import org.apache.rat.report.RatReport;
import org.apache.rat.report.claim.ClaimStatistic;
import org.apache.rat.report.claim.impl.ClaimAggregator;
import org.apache.rat.utils.DefaultLog;

public class LicenseCheckDummy {
  public static void main(String[] args) throws Exception {
    final ReportConfiguration configuration = new ReportConfiguration(DefaultLog.INSTANCE);

    Defaults.Builder defaultBuilder = Defaults.builder();
    Defaults defaults = defaultBuilder.build();
    configuration.setFrom(defaults);

    SortedSet<ILicenseFamily> family =
        new TreeSet<>(
            Set.of(
                ILicenseFamily.builder()
                    .setLicenseFamilyCategory("BSD")
                    .setLicenseFamilyName("BSD license")
                    .build()));
    configuration.addLicense(
        ILicense.builder()
            .setId("xyz")
            .setName("Name xyz")
            .setLicenseFamilyCategory("BSD")
            .setMatcher(IHeaderMatcher.Builder.text().setText("license xyz").build())
            .build(family));

    configuration.setStyleReport(false);

    List<Path> paths;
    try (var s = Files.walk(Paths.get("/home/dweiss/tmp/rat/"))) {
      paths = s.filter(p -> Files.isRegularFile(p)).toList();
    }

    configuration.setReportable(
        new IReportable() {
          @Override
          public void run(RatReport report) throws RatException {
            for (var path : paths) {
              report.report(new FileDocument(path.toFile()));
            }
          }
        });

    final IHeaderMatcher asf1Matcher =
        new SimpleTextMatcher("http://www.apache.org/licenses/LICENSE-2.0");
    final IHeaderMatcher asf2Matcher = new SimpleTextMatcher("https://www.apache.org/licenses/LICENSE-2.0.txt");
    final IHeaderMatcher asfMatcher = new OrMatcher(Arrays.asList(asf1Matcher, asf2Matcher));

    final IHeaderMatcher qosMatcher = new CopyrightMatcher("2004", "2011", "QOS.ch");
    final ILicense qosLic = new TestingLicense("QOS", qosMatcher);

    IDocumentAnalyser analyser = DefaultAnalyserFactory.createDefaultAnalyser(DefaultLog.INSTANCE,Arrays.asList(asfLic, qosLic));
    final List<AbstractReport> reporters = new ArrayList<>();
    reporters.add(reporter);
    report = new ClaimReporterMultiplexer(analyser, reporters);

    configuration.setOut(OutputStream::nullOutputStream);
    ClaimStatistic statistic = new ClaimStatistic();
    var report = new ClaimAggregator(statistic);
    report.startReport();
    configuration.getReportable().run(report);
    report.endReport();

    System.out.println("Approved: " + statistic.getNumApproved());
    System.out.println("Unapproved: " + statistic.getNumUnApproved());
    System.out.println("Unknown: " + statistic.getNumUnknown());

    System.out.println("Document categories:");
    System.out.println(
        statistic.getDocumentCategoryMap().entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining("\n")));

    System.out.println();
    System.out.println("License file names:");
    System.out.println(
        statistic.getLicenseFileNameMap().entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining("\n")));

    System.out.println();
    System.out.println("License codes:");
    System.out.println(
        statistic.getLicenseFileCodeMap().entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining("\n")));

    System.out.println();
    System.out.println("Doc categories:");
    System.out.println(
        statistic.getDocumentCategoryMap().entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining("\n")));
  }
}
