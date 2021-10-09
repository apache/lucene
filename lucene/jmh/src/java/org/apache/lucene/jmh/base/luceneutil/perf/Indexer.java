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
package org.apache.lucene.jmh.base.luceneutil.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.jmh.base.luceneutil.perf.IndexThreads.Mode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;

// javac -Xlint:deprecation -cp
// ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test-framework:build/classes/test:build/contrib/misc/classes/java perf/Indexer.java perf/LineFileDocs.java

/** The type Indexer. */
public final class Indexer {

  /** Instantiates a new Indexer. */
  public Indexer() {}

  /**
   * The entry point of application.
   *
   * @param clArgs the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] clArgs) throws Exception {

    StatisticsHelper stats = new StatisticsHelper();
    stats.startStatistics();
    try {
      _main(clArgs);
    } finally {
      stats.stopStatistics();
    }
  }

  private static MergeScheduler getMergeScheduler(
      AtomicBoolean indexingFailed,
      boolean useCMS,
      int maxConcurrentMerges,
      boolean disableIOThrottle) {
    if (useCMS) {
      ConcurrentMergeScheduler cms =
          new ConcurrentMergeScheduler() {
            @Override
            protected void handleMergeException(Throwable exc) {
              System.out.println("ERROR: CMS hit exception during merging; aborting...");
              indexingFailed.set(true);
              exc.printStackTrace(System.out);
              super.handleMergeException(exc);
            }
          };
      cms.setMaxMergesAndThreads(maxConcurrentMerges + 4, maxConcurrentMerges);
      if (disableIOThrottle) {
        cms.disableAutoIOThrottle();
      }
      return cms;
    } else {
      // Gives better repeatability because if you use CMS, the order in which the merges complete
      // can impact how the merge policy later
      // picks merges so you can easily get a very different index structure when you are comparing
      // two indices:
      return new SerialMergeScheduler();
    }
  }

  private static MergePolicy getMergePolicy(String mergePolicy, boolean useCFS) {

    MergePolicy mp;
    if (mergePolicy.equals("LogDocMergePolicy")) {
      mp = new LogDocMergePolicy();
      mp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    } else if (mergePolicy.equals("LogByteSizeMergePolicy")) {
      mp = new LogByteSizeMergePolicy();
      mp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    } else if (mergePolicy.equals("NoMergePolicy")) {
      mp = NoMergePolicy.INSTANCE;
    } else if (mergePolicy.equals("TieredMergePolicy")) {
      final TieredMergePolicy tmp = new TieredMergePolicy();
      // tmp.setMaxMergedSegmentMB(1000000.0);
      tmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
      mp = tmp;
    } else {
      throw new RuntimeException("unknown MergePolicy " + mergePolicy);
    }

    return mp;
  }

  private static void _main(String[] clArgs) throws Exception {

    Args args = new Args(clArgs);

    // EG: -facets taxonomy;Date -facets taxonomy;Month -facets sortedset:facetGroupField;Month
    FacetsConfig facetsConfig = new FacetsConfig();
    facetsConfig.setHierarchical("Date.taxonomy", true);

    // all unique facet group fields ($facet alone, by default):
    final Set<String> facetFields = new HashSet<>();

    // facet dim name -> facet method flag
    final Map<String, Integer> facetDimMethods = new HashMap<>();
    if (args.hasArg("-facets")) {
      for (String arg : args.getStrings("-facets")) {
        String[] dims = arg.split(";");
        String facetGroupField;
        String facetMethod;
        if (dims[0].equals("taxonomy") || dims[0].equals("sortedset")) {
          // method --> use the default facet field for this group
          facetGroupField = FacetsConfig.DEFAULT_INDEX_FIELD_NAME;
          facetMethod = dims[0];
        } else {
          // method:indexFieldName --> use a custom facet field for this group
          int i = dims[0].indexOf(":");
          if (i == -1) {
            throw new IllegalArgumentException(
                "-facets: expected (taxonomy|sortedset):fieldName but got " + dims[0]);
          }
          facetMethod = dims[0].substring(0, i);
          if (facetMethod.equals("taxonomy") == false && facetMethod.equals("sortedset") == false) {
            throw new IllegalArgumentException(
                "-facets: expected (taxonomy|sortedset):fieldName but got " + dims[0]);
          }
          facetGroupField = dims[0].substring(i + 1);
        }
        facetFields.add(facetGroupField);
        for (int i = 1; i < dims.length; i++) {
          int flag;
          if (facetDimMethods.containsKey(dims[i])) {
            flag = facetDimMethods.get(dims[i]);
          } else {
            flag = 0;
          }
          if (facetMethod.equals("taxonomy")) {
            flag |= 1;
            facetsConfig.setIndexFieldName(dims[i] + ".taxonomy", facetGroupField + ".taxonomy");
          } else {
            flag |= 2;
            facetsConfig.setIndexFieldName(dims[i] + ".sortedset", facetGroupField + ".sortedset");
          }
          facetDimMethods.put(dims[i], flag);
        }
      }
    }

    final String dirImpl = args.getString("-dirImpl");
    final String dirPath = args.getString("-indexPath") + "/index";

    final Directory dir;
    OpenDirectory od = OpenDirectory.get(dirImpl);

    dir = od.open(Paths.get(dirPath));

    final String analyzer = args.getString("-analyzer");
    final Analyzer a;
    if (analyzer.equals("EnglishAnalyzer")) {
      a = new EnglishAnalyzer();
    } else if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer();
    } else if (analyzer.equals("StandardAnalyzerNoStopWords")) {
      a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    } else if (analyzer.equals("ShingleStandardAnalyzer")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(), 2, 2);
    } else if (analyzer.equals("ShingleStandardAnalyzerNoStopWords")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(CharArraySet.EMPTY_SET), 2, 2);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    }

    final String lineFile = args.getString("-lineDocsFile");
    String vectorFile;
    int vectorDimension;
    if (args.hasArg("-vectorFile")) {
      vectorFile = args.getString("-vectorFile");
      vectorDimension = args.getInt("-vectorDimension");
    } else {
      vectorFile = null;
      vectorDimension = 0;
    }

    // -1 means all docs in the line file:
    final int docCountLimit = args.getInt("-docCountLimit");
    final int numThreads = args.getInt("-threadCount");

    final boolean doForceMerge = args.getFlag("-forceMerge");
    final boolean verbose = args.getFlag("-verbose");

    int arrangement = 0;
    if (args.hasArg("-rearrange")) {
      arrangement = args.getInt("-rearrange");
      if (doForceMerge && arrangement > 0) {
        throw new IllegalArgumentException("Force merge not compatible with rearrange!");
      }
      if (arrangement < 0) {
        throw new IllegalArgumentException("Illegal arrangement!");
      }
    }

    String indexSortField = null;
    SortField.Type indexSortType = null;

    if (args.hasArg("-indexSort")) {
      indexSortField = args.getString("-indexSort");

      int i = indexSortField.indexOf(':');
      if (i == -1) {
        throw new IllegalArgumentException(
            "-indexSort should have form field:type; got: " + indexSortField);
      }
      String typeString = indexSortField.substring(i + 1);
      if (typeString.equals("long")) {
        indexSortType = SortField.Type.LONG;
      } else if (typeString.equals("string")) {
        indexSortType = SortField.Type.STRING;
      } else if (typeString.equals("int")) {
        indexSortType = SortField.Type.INT;
      } else {
        throw new IllegalArgumentException(
            "-indexSort can only handle {long,int,string} sort; got: " + typeString);
      }
      indexSortField = indexSortField.substring(0, i);
    }

    final double ramBufferSizeMB = args.getDouble("-ramBufferMB");
    final int maxBufferedDocs = args.getInt("-maxBufferedDocs");

    final String defaultPostingsFormat = args.getString("-postingsFormat");
    final boolean doDeletions = args.getFlag("-deletions");
    final boolean printDPS = args.getFlag("-printDPS");
    final boolean waitForMerges = args.getFlag("-waitForMerges");
    final boolean waitForCommit = args.getFlag("-waitForCommit");
    final String mergePolicy = args.getString("-mergePolicy");
    final Mode mode;
    final boolean doUpdate = args.getFlag("-update");
    if (doUpdate) {
      mode = Mode.UPDATE;
    } else {
      mode = Mode.valueOf(args.getString("-mode", "add").toUpperCase(Locale.ROOT));
    }
    int randomDocIDMax;
    if (mode == Mode.UPDATE) {
      randomDocIDMax = args.getInt("-randomDocIDMax");
    } else {
      randomDocIDMax = -1;
    }
    final String idFieldPostingsFormat = args.getString("-idFieldPostingsFormat");
    final boolean addGroupingFields = args.getFlag("-grouping");
    final boolean useCFS = args.getFlag("-cfs");
    final boolean storeBody = args.getFlag("-store");
    final boolean tvsBody = args.getFlag("-tvs");
    final boolean bodyPostingsOffsets = args.getFlag("-bodyPostingsOffsets");
    final int maxConcurrentMerges = args.getInt("-maxConcurrentMerges");
    final boolean addDVFields = args.getFlag("-dvfields");
    final boolean doRandomCommit = args.getFlag("-randomCommit");
    final boolean useCMS = args.getFlag("-useCMS");
    final boolean disableIOThrottle = args.getFlag("-disableIOThrottle");

    if (waitForCommit == false && waitForMerges) {
      throw new RuntimeException("pass -waitForCommit if you pass -waitForMerges");
    }

    if (waitForCommit == false && doForceMerge) {
      throw new RuntimeException("pass -waitForCommit if you pass -forceMerge");
    }

    if (waitForCommit == false && doDeletions) {
      throw new RuntimeException("pass -waitForCommit if you pass -deletions");
    }

    if (useCMS == false && disableIOThrottle) {
      throw new RuntimeException("-disableIOThrottle only makes sense with -useCMS");
    }

    final double nrtEverySec;
    if (args.hasArg("-nrtEverySec")) {
      nrtEverySec = args.getDouble("-nrtEverySec");
    } else {
      nrtEverySec = -1.0;
    }

    // True to start back at the beginning if we run out of
    // docs from the line file source:
    final boolean repeatDocs = args.getFlag("-repeatDocs");

    final String facetDVFormatName;
    if (facetFields.isEmpty()) {
      facetDVFormatName = "Lucene90";
    } else {
      facetDVFormatName = args.getString("-facetDVFormat");
    }

    if (addGroupingFields && docCountLimit == -1) {
      a.close();
      throw new RuntimeException("cannot add grouping fields unless docCount is set");
    }

    args.check();

    System.out.println("Dir: " + dirImpl);
    System.out.println("Index path: " + dirPath);
    System.out.println("Analyzer: " + analyzer);
    System.out.println("Line file: " + lineFile);
    System.out.println("Vector file: " + vectorFile + ", dim=" + vectorDimension);
    System.out.println(
        "Doc count limit: " + (docCountLimit == -1 ? "all docs" : "" + docCountLimit));
    System.out.println("Threads: " + numThreads);
    System.out.println("Force merge: " + (doForceMerge ? "yes" : "no"));
    System.out.println("Rearrange to (0 for no rearrange): " + arrangement);
    System.out.println("Verbose: " + (verbose ? "yes" : "no"));
    System.out.println("RAM Buffer MB: " + ramBufferSizeMB);
    System.out.println("Max buffered docs: " + maxBufferedDocs);
    System.out.println("Default postings format: " + defaultPostingsFormat);
    System.out.println("Do deletions: " + (doDeletions ? "yes" : "no"));
    System.out.println("Wait for merges: " + (waitForMerges ? "yes" : "no"));
    System.out.println("Wait for commit: " + (waitForCommit ? "yes" : "no"));
    System.out.println("IO throttle: " + (disableIOThrottle ? "no" : "yes"));
    System.out.println("Merge policy: " + mergePolicy);
    System.out.println("Mode: " + mode);
    if (mode == Mode.UPDATE) {
      System.out.println("DocIDMax: " + randomDocIDMax);
    }
    System.out.println("ID field postings format: " + idFieldPostingsFormat);
    System.out.println("Add grouping fields: " + (addGroupingFields ? "yes" : "no"));
    System.out.println("Compound file format: " + (useCFS ? "yes" : "no"));
    System.out.println("Store body field: " + (storeBody ? "yes" : "no"));
    System.out.println("Term vectors for body field: " + (tvsBody ? "yes" : "no"));
    System.out.println("Facet DV Format: " + facetDVFormatName);
    System.out.println("Facet dimension methods: " + facetDimMethods);
    System.out.println("Facet fields: " + facetFields);
    System.out.println("Body postings offsets: " + (bodyPostingsOffsets ? "yes" : "no"));
    System.out.println("Max concurrent merges: " + maxConcurrentMerges);
    System.out.println("Add DocValues fields: " + addDVFields);
    System.out.println("Use ConcurrentMergeScheduler: " + useCMS);
    if (nrtEverySec > 0.0) {
      System.out.println("Open & close NRT reader every: " + nrtEverySec + " sec");
    } else {
      System.out.println("Open & close NRT reader every: never");
    }
    System.out.println("Repeat docs: " + repeatDocs);

    if (verbose) {
      InfoStream.setDefault(new PrintStreamInfoStream(System.out));
    }

    // Use codec at defaults:
    final Codec codec =
        new Lucene90Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return PostingsFormat.forName(
                field.equals("id") ? idFieldPostingsFormat : defaultPostingsFormat);
          }

          // Use doc values format at defaults:
          private final DocValuesFormat facetsDVFormat = new Lucene90DocValuesFormat();

          @Override
          public DocValuesFormat getDocValuesFormatForField(String field) {
            if (facetFields.contains(field)) {
              return facetsDVFormat;
            } else {
              // Use default DVFormat for all else:
              return super.getDocValuesFormatForField(field);
            }
          }
        };

    final AtomicBoolean indexingFailed = new AtomicBoolean();

    final String finalIndexSortField = indexSortField;
    final SortField.Type finalIndexSortType = indexSortType;

    Callable<IndexWriterConfig> getIWC =
        () -> {
          final IndexWriterConfig iwc = new IndexWriterConfig(a);

          if (finalIndexSortField != null) {
            iwc.setIndexSort(new Sort(new SortField(finalIndexSortField, finalIndexSortType)));
          }

          if (mode == Mode.UPDATE) {
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
          } else {
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
          }

          iwc.setMaxBufferedDocs(maxBufferedDocs);
          iwc.setRAMBufferSizeMB(ramBufferSizeMB);

          // So flushed segments do/don't use CFS:
          iwc.setUseCompoundFile(useCFS);

          iwc.setMergeScheduler(
              getMergeScheduler(indexingFailed, useCMS, maxConcurrentMerges, disableIOThrottle));
          iwc.setMergePolicy(getMergePolicy(mergePolicy, useCFS));

          // Keep all commit points:
          if (doDeletions || doForceMerge) {
            iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
          }
          iwc.setCodec(codec);
          return iwc;
        };

    final IndexWriterConfig iwc = getIWC.call();

    System.out.println("IW config=" + iwc);

    IndexWriter w = new IndexWriter(dir, iwc);

    System.out.println("Index has " + w.getDocStats().maxDoc + " docs");

    final TaxonomyWriter taxoWriter;
    if (facetFields.isEmpty() == false) {
      taxoWriter =
          new DirectoryTaxonomyWriter(
              od.open(Paths.get(args.getString("-indexPath"), "facets")),
              IndexWriterConfig.OpenMode.CREATE);
    } else {
      taxoWriter = null;
    }

    // Fixed seed so group field values are always consistent:
    final Random random = new Random(17);

    LineFileDocs lineFileDocs =
        new LineFileDocs(
            lineFile,
            repeatDocs,
            storeBody,
            tvsBody,
            bodyPostingsOffsets,
            false,
            taxoWriter,
            facetDimMethods,
            facetsConfig,
            addDVFields,
            vectorFile,
            vectorDimension);

    float docsPerSecPerThread = -1f;
    // float docsPerSecPerThread = 100f;

    IndexThreads threads =
        new IndexThreads(
            random,
            w,
            indexingFailed,
            lineFileDocs,
            numThreads,
            docCountLimit,
            addGroupingFields,
            printDPS,
            mode,
            docsPerSecPerThread,
            null,
            nrtEverySec,
            randomDocIDMax);

    System.out.println("\nIndexer: start");
    final long t0 = System.currentTimeMillis();

    threads.start();

    while (!threads.done() && indexingFailed.get() == false) {
      Thread.sleep(100);

      // Commits once per minute on average:
      if (doRandomCommit && random.nextInt(600) == 17) {
        System.out.println("Indexer: now commit");
        long commitStartNS = System.nanoTime();
        w.commit();
        System.out.println(
            String.format(
                Locale.ROOT,
                "Indexer: commit took %.1f msec",
                (System.nanoTime() - commitStartNS) / 1000000.));
      }
    }

    threads.stop();

    final long t1 = System.currentTimeMillis();
    System.out.println(
        "\nIndexer: indexing done ("
            + (t1 - t0)
            + " msec); total "
            + w.getDocStats().maxDoc
            + " docs");
    // if we update we can not tell how many docs
    if (threads.failed.get()) {
      throw new RuntimeException("exceptions during indexing");
    }

    // Very tricky: if the line file docs source is binary, and you use multiple threads, and you
    // use grouping fields, then the doc count
    // may not match:
    boolean countShouldMatch;

    if (docCountLimit == -1) {
      countShouldMatch = false;
    } else if (mode == Mode.UPDATE) {
      countShouldMatch = false;
    } else countShouldMatch = !lineFileDocs.isBinary || numThreads <= 1 || !addGroupingFields;

    if (countShouldMatch) {
      if (w.getDocStats().maxDoc != docCountLimit) {
        throw new RuntimeException(
            "w.maxDoc()="
                + w.getDocStats().maxDoc
                + " but expected "
                + docCountLimit
                + " (off by "
                + (docCountLimit - w.getDocStats().maxDoc)
                + ")");
      }
      if (w.getDocStats().maxDoc != countUniqueTerms(w, "id")) {
        throw new RuntimeException(
            "w.maxDoc()="
                + w.getDocStats().maxDoc
                + " but countUniqueIds="
                + countUniqueTerms(w, "id"));
      }
    }

    final Map<String, String> commitData = new HashMap<String, String>();

    if (waitForMerges) {
      w.close();
      IndexWriterConfig iwc2 = getIWC.call();
      iwc2.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
      w = new IndexWriter(dir, iwc2);
      long t2 = System.currentTimeMillis();
      System.out.println("\nIndexer: waitForMerges done (" + (t2 - t1) + " msec)");
    }

    if (waitForCommit) {
      commitData.put("userData", "multi");
      w.setLiveCommitData(commitData.entrySet());
      long t2 = System.currentTimeMillis();
      w.commit();
      long t3 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit multi (took " + (t3 - t2) + " msec)");
    } else {
      w.rollback();
      w = null;
    }

    if (doForceMerge) {
      long forceMergeStartMSec = System.currentTimeMillis();
      w.forceMerge(1);
      long forceMergeEndMSec = System.currentTimeMillis();
      System.out.println(
          "\nIndexer: force merge done (took "
              + (forceMergeEndMSec - forceMergeStartMSec)
              + " msec)");

      commitData.put("userData", "single");
      w.setLiveCommitData(commitData.entrySet());
      w.commit();
      final long t5 = System.currentTimeMillis();
      System.out.println(
          "\nIndexer: commit single done (took " + (t5 - forceMergeEndMSec) + " msec)");
    }

    if (doDeletions) {
      final long t5 = System.currentTimeMillis();
      // Randomly delete 5% of the docs
      final Set<Integer> deleted = new HashSet<Integer>();
      IndexWriter.DocStats docStats = w.getDocStats();
      final int maxDoc = docStats.maxDoc;
      final int numDocs = docStats.numDocs;
      final int toDeleteCount = (int) (maxDoc * 0.05);
      System.out.println("\nIndexer: delete " + toDeleteCount + " docs");
      while (deleted.size() < toDeleteCount) {
        final int id = random.nextInt(maxDoc);
        if (!deleted.contains(id)) {
          deleted.add(id);
          w.deleteDocuments(new Term("id", LineFileDocs.intToID(id)));
        }
      }
      final long t6 = System.currentTimeMillis();
      System.out.println("\nIndexer: deletes done (took " + (t6 - t5) + " msec)");

      commitData.put("userData", doForceMerge ? "delsingle" : "delmulti");
      w.setLiveCommitData(commitData.entrySet());
      w.commit();
      final long t7 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit delmulti done (took " + (t7 - t6) + " msec)");

      if (doUpdate || numDocs != maxDoc - toDeleteCount) {
        throw new RuntimeException(
            "count mismatch: w.numDocs()=" + numDocs + " but expected " + (maxDoc - toDeleteCount));
      }
    }

    if (taxoWriter != null) {
      System.out.println("Taxonomy has " + taxoWriter.getSize() + " ords");
      taxoWriter.commit();
      taxoWriter.close();
    }

    final long tCloseStart = System.currentTimeMillis();
    if (w != null) {
      w.close();
    }
    if (waitForCommit) {
      System.out.println("\nIndexer: at close: " + SegmentInfos.readLatestCommit(dir));
      System.out.println(
          "\nIndexer: close took " + (System.currentTimeMillis() - tCloseStart) + " msec");
    }

    if (arrangement != 0) {
      // rearrange after normal indexing routine is completed
      System.out.println("\nIndexer: rearrange start");
      long rearrangeStartMSec = System.currentTimeMillis();
      Path tmpDirPath = Files.createTempDirectory("rearrange");
      System.out.println("Created tmp dir for rearranging: " + tmpDirPath);
      try (Directory tmpDir = od.open(tmpDirPath)) {
        BenchRearranger.rearrange(dir, tmpDir, getIWC.call(), arrangement);
        PerfUtils.clearDir(dir);
        PerfUtils.copyDir(tmpDir, dir);
      } finally {
        Files.walk(tmpDirPath)
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
        Files.delete(tmpDirPath);
        System.out.println("Deleted tmp dir: " + tmpDirPath);
      }
      long rearrangeEndMSec = System.currentTimeMillis();
      //      IndexReader reader = DirectoryReader.open(dir);
      //      for (LeafReaderContext context: reader.leaves()) {
      //        System.out.println(context.reader().numDocs());
      //      }
      //      reader.close();
      /* code above will print '1810' once, '1800' 4 times, '180' 5 times, '18' 5 times
      on a 10000 doc, 555 arrangement */
      System.out.println(
          "\nIndexer: rearrange done (took " + (rearrangeEndMSec - rearrangeStartMSec) + " msec)");
    }

    dir.close();
    final long tFinal = System.currentTimeMillis();
    System.out.println("\nIndexer: net bytes indexed " + threads.getBytesIndexed());

    final long indexingTime;
    if (waitForCommit) {
      indexingTime = tFinal - t0;
      System.out.println("\nIndexer: finished (" + indexingTime + " msec)");
    } else {
      indexingTime = t1 - t0;
      System.out.println("\nIndexer: finished (" + indexingTime + " msec), excluding commit");
    }
    System.out.println(
        "\nIndexer: "
            + (threads.getBytesIndexed() / 1024. / 1024. / 1024. / (indexingTime / 3600000.))
            + " GB/hour plain text");
  }

  private static long countUniqueTerms(IndexWriter iw, String fld) throws IOException {
    long total = 0;
    try (IndexReader reader = DirectoryReader.open(iw)) {
      for (LeafReaderContext ctx : reader.leaves()) {
        long size = ctx.reader().terms(fld).size();
        if (size > 0) {
          total += size;
        }
      }
    }
    return total;
  }
}
