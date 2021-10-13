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
package org.apache.lucene.jmh.benchmarks.search;

import static org.apache.lucene.jmh.base.BaseBenchState.log;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.classic.ClassicAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.jmh.base.BaseBenchState;
import org.apache.lucene.jmh.base.luceneutil.perf.IndexState;
import org.apache.lucene.jmh.base.luceneutil.perf.IndexThreads;
import org.apache.lucene.jmh.base.luceneutil.perf.LineFileDocs;
import org.apache.lucene.jmh.base.luceneutil.perf.LocalTaskSource;
import org.apache.lucene.jmh.base.luceneutil.perf.OpenDirectory;
import org.apache.lucene.jmh.base.luceneutil.perf.PerfUtils;
import org.apache.lucene.jmh.base.luceneutil.perf.Task;
import org.apache.lucene.jmh.base.luceneutil.perf.TaskParser;
import org.apache.lucene.jmh.base.luceneutil.perf.TaskSource;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SuppressForbidden;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

/** The type. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Warmup(time = 15, iterations = 5)
@Measurement(time = 20, iterations = 5)
@Fork(value = 1)
@Timeout(time = 600)
public class SearchPerf {

  private static final boolean VERBOSE = false;

  /** Instantiates a new Search perf. */
  public SearchPerf() {}

  private static IndexSearcher createIndexSearcher(
      IndexReader reader, ExecutorService executorService) {
    return new IndexSearcher(reader, executorService);
  }

  /** The type Single index searcher. */
  // ReferenceManager that never changes its searcher:
  public static class SingleIndexSearcher extends ReferenceManager<IndexSearcher> {

    /**
     * Instantiates a new Single index searcher.
     *
     * @param s the s
     */
    public SingleIndexSearcher(IndexSearcher s) {
      this.current = s;
    }

    @Override
    public void decRef(IndexSearcher ref) throws IOException {
      ref.getIndexReader().decRef();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher ref) {
      return null;
    }

    @Override
    protected boolean tryIncRef(IndexSearcher ref) {
      return ref.getIndexReader().tryIncRef();
    }

    @Override
    protected int getRefCount(IndexSearcher ref) {
      return ref.getIndexReader().getRefCount();
    }
  }

  /** The type Bench state. */
  @State(Scope.Benchmark)
  public static class BenchState {
    // Benchmark (analyzer) (cloneDocs)  (commit) (dirImpl)  (dpspt)  (facets)  (fn) (hiliteimpl)
    // (idpf) (index)  (ithreads) (ldfile)  (mode)  (nrt)  (pss)  (pklu)  (pf)  (reopens)
    // (sthreads) (sim)  (storebdy)  (sloads) (tasks)  (topN)  (tvsBody)  (useCFS)  (vectorFile)
    /** The constant TAXONOMY. */
    public static final String TAXONOMY = "taxonomy";

    /** The Dir. */
    @Param({"MMapDirectory"})
    String dirimpl;
    /** The Field name. */
    @Param({"body"})
    String fld;
    /** The Analyzer. */
    @Param({"StandardAnalyzer"})
    String analyzer;
    /** The Search thread count. */
    @Param({"2"})
    int sthrds;
    /** The Do pk lookup. */
    @Param({"false"})
    boolean pklu;
    /** The Do concurrent searches. */
    @Param({"true"})
    boolean pss;
    /** The Top n. */
    @Param({"10"})
    int topn;
    /** The Do stored loads. */
    @Param({"false"})
    boolean storelds;
    /** The Similarity. */
    // TODO: this could be way better.
    @Param({"BM25Similarity"})
    String sim;
    /** The Commit. */
    @Param({"multi"})
    String commit;
    /** The Hilite. */
    @Param({"FastVectorHighlighter"})
    String hlimpl;
    /** The Vector file. */
    @Param({""})
    String vecfile;
    /** The Index thread count. */
    @Param({"1"})
    int ithrds;
    /** The Docs per sec per thread. */
    @Param({"10"})
    float dpspt;
    /** The Reopen every sec. */
    @Param({"5"})
    float reopnsec;
    /** The Store body. */
    @Param({"false"})
    boolean storebdy;
    /** The Tvs body. */
    @Param({"false"})
    boolean tvsbdy;
    /** The Use cfs. */
    @Param({"false"})
    boolean cfs;
    /** The Default postings format. */
    @Param({"Lucene90"})
    String postfmt;
    /** The Id field postings format. */
    @Param({"Lucene90"})
    String idpostfmt;
    /** The Clone docs. */
    @Param({"false"})
    boolean clonedocs;
    /** The Mode. */
    @Param({"update"})
    String mode;
    /** The Facets. */
    @Param({TAXONOMY})
    String facets;
    /** The Nrt. */
    @Param({"true"})
    boolean nrt;

    /** The Executor service. */
    ExecutorService executorService;
    /** The Mgr. */
    ReferenceManager<IndexSearcher> mgr;
    /** The Reopen thread. */
    Thread reopenThread;
    /** The Shutdown. */
    volatile boolean shutdown;
    /** The Threads. */
    IndexThreads threads;
    /** The Tasks. */
    TaskSource tasks;

    private IndexState indexState;

    /** Instantiates a new Bench state. */
    public BenchState() {}

    /**
     * Sets .
     *
     * @param benchmarkParams the benchmark params
     * @param benchState the bench state
     * @throws Exception the exception
     */
    @Setup(Level.Trial)
    @SuppressForbidden(reason = "benchmark")
    public void setup(BenchmarkParams benchmarkParams, BaseBenchState benchState) throws Exception {

      Directory dir0;

      OpenDirectory od = OpenDirectory.get(dirimpl);

      String index = System.getProperty("index", "work/index");

      String ldfile = System.getProperty("ldfile", "work/lines.txt");

      String tasksFile = System.getProperty("tasksFile", "work/tasks.txt");

      dir0 = od.open(Paths.get(index));

      // TODO: NativeUnixDir?

      int cores = Runtime.getRuntime().availableProcessors();

      if (pss) {
        executorService =
            new ThreadPoolExecutor(
                cores,
                cores,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                  Thread thread = new Thread(r);
                  thread.setName("ConcurrentSearches");
                  thread.setDaemon(true);
                  return thread;
                });
      } else {
        executorService = null;
      }

      // now reflect
      final Class<? extends Similarity> simClazz =
          Class.forName("org.apache.lucene.search.similarities." + sim)
              .asSubclass(Similarity.class);

      final Similarity similarity = simClazz.newInstance();

      log("Using dir impl " + dir0.getClass().getName());
      log("Analyzer " + analyzer);
      log("Similarity " + this.sim);
      log("Search thread count " + sthrds);
      log("topN " + topn);
      log("JVM " + (Constants.JRE_IS_64BIT ? "is" : "is not") + " 64bit");
      log("Pointer is " + RamUsageEstimator.NUM_BYTES_OBJECT_REF + " bytes");
      log("Concurrent segment reads is " + pss);

      final Analyzer a;
      switch (analyzer) {
        case "EnglishAnalyzer":
          a = new EnglishAnalyzer();
          break;
        case "ClassicAnalyzer":
          a = new ClassicAnalyzer();
          break;
        case "StandardAnalyzer":
          a = new StandardAnalyzer();
          break;
        case "StandardAnalyzerNoStopWords":
          a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
          break;
        case "ShingleStandardAnalyzer":
          a =
              new ShingleAnalyzerWrapper(
                  new StandardAnalyzer(CharArraySet.EMPTY_SET),
                  2,
                  2,
                  ShingleFilter.DEFAULT_TOKEN_SEPARATOR,
                  true,
                  true,
                  ShingleFilter.DEFAULT_FILLER_TOKEN);
          break;
        default:
          throw new RuntimeException("unknown analyzer " + analyzer);
      }

      final IndexWriter writer;
      final Directory dir;

      final long tSearcherStart = System.currentTimeMillis();

      if (vecfile.isEmpty()) {
        vecfile = null;
      }

      long randomSeed = benchState.getRandomSeed();

      @SuppressWarnings("ReplacePseudorandomGenerator")
      final Random random = new Random(randomSeed);

      if (nrt) {
        // TODO: get taxoReader working here too

        final IndexThreads.Mode threadsMode =
            IndexThreads.Mode.valueOf(mode.toUpperCase(Locale.ROOT));

        final long reopenEveryMS = (long) (1000 * reopnsec);

        if (VERBOSE) {
          InfoStream.setDefault(new PrintStreamInfoStream(System.out));
        }

        if (!dirimpl.equals("RAMExceptDirectPostingsDirectory")) {
          log("Wrap NRTCachingDirectory");
          dir0 = new NRTCachingDirectory(dir0, 20, 400.0);
        }

        dir = dir0;

        final IndexWriterConfig iwc = new IndexWriterConfig(a);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setRAMBufferSizeMB(256.0);
        iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

        // TODO: also RAMDirExceptDirect...?  need to
        // ... block deletes against wrapped FSDir?

        if (commit != null && commit.length() > 0) {
          log("Opening writer on commit=" + commit);
          iwc.setIndexCommit(PerfUtils.findCommitPoint(commit, dir));
        }

        ((TieredMergePolicy) iwc.getMergePolicy()).setNoCFSRatio(cfs ? 1.0 : 0.0);
        // ((TieredMergePolicy) iwc.getMergePolicy()).setMaxMergedSegmentMB(1024);
        // ((TieredMergePolicy) iwc.getMergePolicy()).setReclaimDeletesWeight(3.0);
        // ((TieredMergePolicy) iwc.getMergePolicy()).setMaxMergeAtOnce(4);

        final Codec codec =
            new Lucene90Codec() {
              @Override
              public PostingsFormat getPostingsFormatForField(String field) {
                return PostingsFormat.forName(field.equals("id") ? idpostfmt : postfmt);
              }
            };
        iwc.setCodec(codec);

        final ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
        // Only let one merge run at a time...
        // ... but queue up up to 4, before index thread is stalled:
        cms.setMaxMergesAndThreads(4, 1);

        iwc.setMergedSegmentWarmer(
            new IndexWriter.IndexReaderWarmer() {
              @Override
              public void warm(LeafReader reader) throws IOException {
                final long t0 = System.currentTimeMillis();
                // System.out.println("DO WARM: " + reader);
                IndexSearcher s = createIndexSearcher(reader, executorService);
                s.setQueryCache(null); // don't bench the cache
                s.search(new TermQuery(new Term(fld, "united")), 10);
                final long t1 = System.currentTimeMillis();
                log(
                    "warm segment="
                        + reader
                        + " numDocs="
                        + reader.numDocs()
                        + ": took "
                        + (t1 - t0)
                        + " msec");
              }
            });

        writer = new IndexWriter(dir, iwc);
        log("Initial writer.maxDoc()=" + writer.getDocStats().maxDoc);

        // TODO: add -nrtBodyPostingsOffsets instead of
        // hardwired false:
        boolean addDVFields =
            threadsMode == IndexThreads.Mode.BDV_UPDATE
                || threadsMode == IndexThreads.Mode.NDV_UPDATE;

        LineFileDocs lineFileDocs =
            new LineFileDocs(
                ldfile,
                false,
                storebdy,
                tvsbdy,
                false,
                clonedocs,
                null,
                null,
                null,
                addDVFields,
                null,
                0);
        threads =
            new IndexThreads(
                random,
                writer,
                new AtomicBoolean(false),
                lineFileDocs,
                ithrds,
                -1,
                false,
                false,
                threadsMode,
                dpspt,
                null,
                -1.0,
                -1);
        threads.start();

        mgr =
            new SearcherManager(
                writer,
                new SearcherFactory() {
                  @Override
                  public IndexSearcher newSearcher(IndexReader reader, IndexReader previous) {
                    IndexSearcher s = createIndexSearcher(reader, executorService);
                    s.setQueryCache(null); // don't bench the cache
                    s.setSimilarity(similarity);
                    return s;
                  }
                });

        log("reopen every " + reopnsec);

        reopenThread =
            new Thread() {
              @Override
              public void run() {
                try {
                  final long startMS = System.currentTimeMillis();

                  int reopenCount = 1;
                  while (!shutdown) {
                    final long sleepMS =
                        startMS + (reopenCount * reopenEveryMS) - System.currentTimeMillis();
                    if (sleepMS < 0) {
                      log("WARNING: reopen fell behind by " + Math.abs(sleepMS) + " ms");
                    } else {
                      Thread.sleep(sleepMS);
                    }

                    mgr.maybeRefresh();
                    reopenCount++;
                    IndexSearcher s = mgr.acquire();
                    try {
                      log(
                          String.format(
                              Locale.ENGLISH,
                              "%.1fs: done reopen; writer.maxDoc()=%d; searcher.maxDoc()=%d; searcher.numDocs()=%d",
                              (System.currentTimeMillis() - startMS) / 1000.0,
                              writer.getDocStats().maxDoc,
                              s.getIndexReader().maxDoc(),
                              s.getIndexReader().numDocs()));
                    } finally {
                      mgr.release(s);
                    }
                  }
                } catch (InterruptedException e) {
                  log(e.getMessage());
                  Thread.currentThread().interrupt();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            };
        reopenThread.setName("ReopenThread");
        reopenThread.setPriority(4 + Thread.currentThread().getPriority());
        reopenThread.start();

      } else {
        dir = dir0;
        writer = null;
        final DirectoryReader reader;
        if (commit != null && commit.length() > 0) {
          log("Opening searcher on commit=" + commit);
          reader = DirectoryReader.open(PerfUtils.findCommitPoint(commit, dir));
        } else {
          // open last commit
          reader = DirectoryReader.open(dir);
        }

        IndexSearcher s = createIndexSearcher(reader, executorService);
        s.setQueryCache(null); // don't bench the cache
        s.setSimilarity(similarity);
        log(
            "maxDoc="
                + reader.maxDoc()
                + " numDocs="
                + reader.numDocs()
                + " %tg live docs="
                + (100. * reader.maxDoc() / reader.numDocs()));

        mgr = new SingleIndexSearcher(s);
      }

      log((System.currentTimeMillis() - tSearcherStart) + " msec to init searcher/NRT");

      {
        IndexSearcher s = mgr.acquire();
        try {
          log(
              "Searcher: numDocs="
                  + s.getIndexReader().numDocs()
                  + " maxDoc="
                  + s.getIndexReader().maxDoc()
                  + ": "
                  + s);
        } finally {
          mgr.release(s);
        }
      }

      // System.out.println("searcher=" + searcher);

      FacetsConfig facetsConfig = new FacetsConfig();
      facetsConfig.setHierarchical("Date.taxonomy", true);

      // all unique facet group fields ($facet alone, by default):
      final Set<String> facetFields = new HashSet<>();

      // facet dim name -> facet method
      final Map<String, Integer> facetDimMethods = new HashMap<>();
      if (facets != null) {
        for (String arg : facets.split(",")) {
          String[] dims = arg.split(";");
          String facetGroupField;
          String facetMethod;
          if (dims[0].equals(TAXONOMY) || dims[0].equals("sortedset")) {
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
            if (!facetMethod.equals(TAXONOMY) && !facetMethod.equals("sortedset")) {
              throw new IllegalArgumentException(
                  "-facets: expected (taxonomy|sortedset):fieldName but got " + dims[0]);
            }
            facetGroupField = dims[0].substring(i + 1);
          }
          facetFields.add(facetGroupField);
          for (int i = 1; i < dims.length; i++) {
            int flag;
            flag = facetDimMethods.getOrDefault(dims[i], 0);
            if (facetMethod.equals(TAXONOMY)) {
              flag |= 1;
              facetsConfig.setIndexFieldName(dims[i] + ".taxonomy", facetGroupField + ".taxonomy");
            } else {
              flag |= 2;
              facetsConfig.setIndexFieldName(
                  dims[i] + ".sortedset", facetGroupField + ".sortedset");
            }
            facetDimMethods.put(dims[i], flag);
          }
        }
      }

      TaxonomyReader taxoReader;
      Path taxoPath = Paths.get(index, "facets");
      Directory taxoDir = od.open(taxoPath);
      if (DirectoryReader.indexExists(taxoDir)) {
        taxoReader = new DirectoryTaxonomyReader(taxoDir);
        log("Taxonomy has " + taxoReader.getSize() + " ords");
      } else {
        taxoReader = null;
      }

      final DirectSpellChecker spellChecker = new DirectSpellChecker();
      indexState =
          new IndexState(mgr, taxoReader, fld, spellChecker, hlimpl, facetsConfig, facetDimMethods);

      final QueryParser queryParser = new QueryParser("body", a);
      TaskParser taskParser =
          new TaskParser(indexState, queryParser, fld, topn, random, vecfile, storelds);

      // Load the tasks from a file:

      tasks = new LocalTaskSource(indexState, taskParser, tasksFile, random, pklu);
      log("Tasks file " + tasksFile);
    }

    /**
     * Teardown.
     *
     * @param benchmarkParams the benchmark params
     * @throws Exception the exception
     */
    @TearDown(Level.Trial)
    public void teardown(BenchmarkParams benchmarkParams) throws Exception {
      executorService.shutdown();
      shutdown = true;
      reopenThread.interrupt();
      threads.stop();
      reopenThread.join();
      mgr.close();
    }
  }

  /**
   * search
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object orHighHigh(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("OrHighHigh");
    task.go(state.indexState);
    return task;
  }

  /**
   * search
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object respell(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("Respell");
    task.go(state.indexState);
    return task;
  }

  /**
   * search
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object fuzzy1(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("Fuzzy1");
    task.go(state.indexState);
    return task;
  }

  /**
   * search
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object fuzzy2(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("Fuzzy2");
    task.go(state.indexState);
    return task;
  }

  /**
   * And high med object.
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object andHighMed(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("AndHighMed");
    task.go(state.indexState);
    return task;
  }

  /**
   * Or high med object.
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object orHighMed(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("OrHighMed");
    task.go(state.indexState);
    return task;
  }

  /**
   * Prefix 3 object.
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object prefix3(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("Prefix3");
    task.go(state.indexState);
    return task;
  }

  /**
   * Term object.
   *
   * @param state the state
   * @return the object
   * @throws Exception the exception
   */
  @Benchmark
  public Object term(BenchState state) throws Exception {
    Task task = state.tasks.nextTask("Term");
    task.go(state.indexState);
    return task;
  }
}
