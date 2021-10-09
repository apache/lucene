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
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

/** The type Index threads. */
public class IndexThreads {

  /** The enum Mode. */
  public enum Mode {
    /** Update mode. */
    UPDATE,
    /** Add mode. */
    ADD,
    /** Ndv update mode. */
    NDV_UPDATE,
    /** Bdv update mode. */
    BDV_UPDATE
  }

  /** The Printer. */
  final IngestRatePrinter printer;
  /** The Start latch. */
  final CountDownLatch startLatch = new CountDownLatch(1);
  /** The Stop. */
  final AtomicBoolean stop;
  /** The Failed. */
  final AtomicBoolean failed;
  /** The Docs. */
  final LineFileDocs docs;
  /** The Threads. */
  final Thread[] threads;
  /** The Refreshing. */
  final AtomicBoolean refreshing;
  /** The Last refresh ns. */
  final AtomicLong lastRefreshNS;

  /**
   * Instantiates a new Index threads.
   *
   * @param random the random
   * @param w the w
   * @param indexingFailed the indexing failed
   * @param lineFileDocs the line file docs
   * @param numThreads the num threads
   * @param docCountLimit the doc count limit
   * @param addGroupingFields the add grouping fields
   * @param printDPS the print dps
   * @param mode the mode
   * @param docsPerSecPerThread the docs per sec per thread
   * @param updatesListener the updates listener
   * @param nrtEverySec the nrt every sec
   * @param randomDocIDMax the random doc id max
   * @throws IOException the io exception
   * @throws InterruptedException the interrupted exception
   */
  public IndexThreads(
      Random random,
      IndexWriter w,
      AtomicBoolean indexingFailed,
      LineFileDocs lineFileDocs,
      int numThreads,
      int docCountLimit,
      boolean addGroupingFields,
      boolean printDPS,
      Mode mode,
      float docsPerSecPerThread,
      UpdatesListener updatesListener,
      double nrtEverySec,
      int randomDocIDMax)
      throws IOException, InterruptedException {
    final AtomicInteger groupBlockIndex;

    this.docs = lineFileDocs;
    if (addGroupingFields) {
      IndexThread.group100 = randomStrings(100, random);
      IndexThread.group10K = randomStrings(10000, random);
      IndexThread.group100K = randomStrings(100000, random);
      IndexThread.group1M = randomStrings(1000000, random);
      groupBlockIndex = new AtomicInteger();
    } else {
      groupBlockIndex = null;
    }

    threads = new Thread[numThreads];

    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final AtomicInteger count = new AtomicInteger();
    stop = new AtomicBoolean(false);
    failed = indexingFailed;
    refreshing = new AtomicBoolean(false);
    lastRefreshNS = new AtomicLong(System.nanoTime());

    for (int thread = 0; thread < numThreads; thread++) {
      threads[thread] =
          new IndexThread(
              random,
              startLatch,
              stopLatch,
              w,
              docs,
              docCountLimit,
              count,
              mode,
              groupBlockIndex,
              stop,
              refreshing,
              lastRefreshNS,
              docsPerSecPerThread,
              failed,
              updatesListener,
              nrtEverySec,
              randomDocIDMax);
      threads[thread].setName("Index #" + thread);
      threads[thread].start();
    }

    Thread.sleep(10);

    if (printDPS) {
      printer = new IngestRatePrinter(count, stop);
      printer.start();
    } else {
      printer = null;
    }
  }

  /** Start. */
  public void start() {
    startLatch.countDown();
  }

  /**
   * Gets bytes indexed.
   *
   * @return the bytes indexed
   */
  public long getBytesIndexed() {
    return docs.getBytesIndexed();
  }

  /**
   * Stop.
   *
   * @throws InterruptedException the interrupted exception
   * @throws IOException the io exception
   */
  public void stop() throws InterruptedException, IOException {
    stop.getAndSet(true);
    for (Thread t : threads) {
      t.join();
    }
    if (printer != null) {
      printer.join();
    }
    docs.close();
  }

  /**
   * Done boolean.
   *
   * @return the boolean
   */
  public boolean done() {
    for (Thread t : threads) {
      if (t.isAlive()) {
        return false;
      }
    }

    return true;
  }

  /** The interface Updates listener. */
  public interface UpdatesListener {

    /** Before update. */
    void beforeUpdate();

    /** After update. */
    void afterUpdate();
  }

  private static class IndexThread extends Thread {

    /** The Group 100. */
    public static BytesRef[] group100;
    /** The Group 100 k. */
    public static BytesRef[] group100K;
    /** The Group 10 k. */
    public static BytesRef[] group10K;
    /** The Group 1 m. */
    public static BytesRef[] group1M;

    private final LineFileDocs docs;
    private final int numTotalDocs;
    private final IndexWriter w;
    private final AtomicBoolean stop;
    private final AtomicInteger count;
    private final AtomicInteger groupBlockIndex;
    private final Mode mode;
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final float docsPerSec;
    private final Random random;
    private final AtomicBoolean failed;
    private final UpdatesListener updatesListener;
    private final AtomicBoolean refreshing;
    private final AtomicLong lastRefreshNS;
    private final double nrtEverySec;
    /** The Random doc id max. */
    final int randomDocIDMax;

    /**
     * Instantiates a new Index thread.
     *
     * @param random the random
     * @param startLatch the start latch
     * @param stopLatch the stop latch
     * @param w the w
     * @param docs the docs
     * @param numTotalDocs the num total docs
     * @param count the count
     * @param mode the mode
     * @param groupBlockIndex the group block index
     * @param stop the stop
     * @param refreshing the refreshing
     * @param lastRefreshNS the last refresh ns
     * @param docsPerSec the docs per sec
     * @param failed the failed
     * @param updatesListener the updates listener
     * @param nrtEverySec the nrt every sec
     * @param randomDocIDMax the random doc id max
     */
    public IndexThread(
        Random random,
        CountDownLatch startLatch,
        CountDownLatch stopLatch,
        IndexWriter w,
        LineFileDocs docs,
        int numTotalDocs,
        AtomicInteger count,
        Mode mode,
        AtomicInteger groupBlockIndex,
        AtomicBoolean stop,
        AtomicBoolean refreshing,
        AtomicLong lastRefreshNS,
        float docsPerSec,
        AtomicBoolean failed,
        UpdatesListener updatesListener,
        double nrtEverySec,
        int randomDocIDMax) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.w = w;
      this.docs = docs;
      this.numTotalDocs = numTotalDocs;
      this.count = count;
      this.mode = mode;
      this.groupBlockIndex = groupBlockIndex;
      this.stop = stop;
      this.docsPerSec = docsPerSec;
      this.random = random;
      this.failed = failed;
      this.updatesListener = updatesListener;
      this.refreshing = refreshing;
      this.lastRefreshNS = lastRefreshNS;
      this.nrtEverySec = nrtEverySec;
      this.randomDocIDMax = randomDocIDMax;
    }

    @Override
    public void run() {
      try {
        final LineFileDocs.DocState docState = docs.newDocState();
        final Field idField = docState.id;
        final long tStart = System.currentTimeMillis();
        final Field group100Field;
        final Field group100KField;
        final Field group10KField;
        final Field group1MField;
        final Field groupBlockField;
        final Field groupEndField;
        if (group100 != null) {
          group100Field = new SortedDocValuesField("group100", new BytesRef());
          docState.doc.add(group100Field);
          group10KField = new SortedDocValuesField("group10K", new BytesRef());
          docState.doc.add(group10KField);
          group100KField = new SortedDocValuesField("group100K", new BytesRef());
          docState.doc.add(group100KField);
          group1MField = new SortedDocValuesField("group1M", new BytesRef());
          docState.doc.add(group1MField);
          groupBlockField = new SortedDocValuesField("groupblock", new BytesRef());
          docState.doc.add(groupBlockField);
          // Binary marker field:
          groupEndField = new StringField("groupend", "x", Field.Store.NO);
        } else {
          group100Field = null;
          group100KField = null;
          group10KField = null;
          group1MField = null;
          groupBlockField = null;
          groupEndField = null;
        }

        try {
          startLatch.await();
        } catch (InterruptedException ie) {
          System.out.println(ie.getMessage());
          Thread.currentThread().interrupt();
          return;
        }

        if (group100 != null) {

          if (numTotalDocs == -1) {
            throw new IllegalStateException(
                "must specify numTotalDocs when indexing doc blocks for grouping");
          }

          // Add docs in blocks:

          final BytesRef[] groupBlocks;
          if (numTotalDocs >= 5000000) {
            groupBlocks = group1M;
          } else if (numTotalDocs >= 500000) {
            groupBlocks = group100K;
          } else {
            groupBlocks = group10K;
          }
          final double docsPerGroupBlock = numTotalDocs / (double) groupBlocks.length;

          while (!stop.get()) {
            final int groupCounter = groupBlockIndex.getAndIncrement();
            if (groupCounter >= groupBlocks.length) {
              break;
            }
            final int numDocs;
            // This will toggle between X and X+1 docs,
            // converging over time on average to the
            // floating point docsPerGroupBlock:
            if (groupCounter == groupBlocks.length - 1) {
              numDocs = numTotalDocs - ((int) (groupCounter * docsPerGroupBlock));
            } else {
              numDocs =
                  ((int) ((1 + groupCounter) * docsPerGroupBlock))
                      - ((int) (groupCounter * docsPerGroupBlock));
            }
            groupBlockField.setBytesValue(groupBlocks[groupCounter]);

            w.addDocuments(
                new Iterable<Document>() {
                  @Override
                  public Iterator<Document> iterator() {
                    return new Iterator<Document>() {
                      int upto;
                      Document doc;

                      @SuppressWarnings("synthetic-access")
                      @Override
                      public boolean hasNext() {
                        if (upto < numDocs) {
                          upto++;

                          try {
                            doc = docs.nextDoc(docState);
                          } catch (IOException ioe) {
                            throw new RuntimeException(ioe);
                          }
                          if (doc == null) {
                            return false;
                          }

                          if (upto == numDocs) {
                            // Sneaky: we remove it down below, so that in the not-cloned case we
                            // don't accumulate this field:
                            doc.add(groupEndField);
                          }

                          final int id = LineFileDocs.idToInt(idField.stringValue());
                          if (id >= numTotalDocs) {
                            throw new IllegalStateException();
                          }
                          if (((1 + id) % 10000) == 0) {
                            System.out.println(
                                "Indexer: "
                                    + (1 + id)
                                    + " docs... ("
                                    + (System.currentTimeMillis() - tStart)
                                    + " msec)");
                          }
                          group100Field.setBytesValue(group100[id % 100]);
                          group10KField.setBytesValue(group10K[id % 10000]);
                          group100KField.setBytesValue(group100K[id % 100000]);
                          group1MField.setBytesValue(group1M[id % 1000000]);
                          count.incrementAndGet();
                          return true;
                        } else {
                          doc = null;
                          return false;
                        }
                      }

                      @Override
                      public Document next() {
                        return doc;
                      }

                      @Override
                      public void remove() {
                        throw new UnsupportedOperationException();
                      }
                    };
                  }
                });

            docState.doc.removeField("groupend");
          }
        } else if (docsPerSec > 0 && mode != null) {
          final long startNS = System.nanoTime();
          int threadCount = 0;
          while (!stop.get()) {
            final Document doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            final int id = LineFileDocs.idToInt(idField.stringValue());
            if (numTotalDocs != -1 && id >= numTotalDocs) {
              break;
            }

            if (((1 + id) % 10000) == 0) {
              System.out.println(
                  "Indexer: "
                      + (1 + id)
                      + " docs... ("
                      + (System.currentTimeMillis() - tStart)
                      + " msec)");
            }
            // TODO have a 'sometimesAdd' mode where 25%
            // of the time we add a new doc
            final String updateID =
                LineFileDocs.intToID(
                    random.nextInt(randomDocIDMax == -1 ? Integer.MAX_VALUE : randomDocIDMax));
            if (updatesListener != null) {
              updatesListener.beforeUpdate();
            }
            switch (mode) {
              case UPDATE:
                // NOTE: can't use docState.id in case doClone
                // was true
                ((Field) doc.getField("id")).setStringValue(updateID);
                w.updateDocument(new Term("id", updateID), doc);
                break;
              case NDV_UPDATE:
                w.updateNumericDocValue(
                    new Term("id", updateID), "lastModNDV", System.currentTimeMillis());
                break;
              case BDV_UPDATE:
                throw new IllegalArgumentException("not implemented!");
                // w.updateBinaryDocValue(new Term("id", updateID), "titleBDV",
                // docState.titleBDV.binaryValue());
                // break;
              case ADD:
                w.addDocument(doc);
                break;
              default:
                throw new IllegalArgumentException("unknown mode " + mode);
            }
            if (updatesListener != null) {
              updatesListener.afterUpdate();
            }
            int docCount = count.incrementAndGet();
            threadCount++;

            if ((docCount % 10000) == 0) {
              System.out.println(
                  "Indexer: "
                      + docCount
                      + " docs... ("
                      + (System.currentTimeMillis() - tStart)
                      + " msec)");
            }

            final long sleepNS =
                startNS + (long) (1000000000 * (threadCount / docsPerSec)) - System.nanoTime();
            if (sleepNS > 0) {
              final long sleepMS = sleepNS / 1000000;
              final int sleepNS2 = (int) (sleepNS - sleepMS * 1000000);
              Thread.sleep(sleepMS, sleepNS2);
            }

            maybeOpenReader(tStart);
          }
        } else {
          while (!stop.get()) {
            final Document doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            int docCount = count.incrementAndGet();
            if (numTotalDocs != -1 && docCount > numTotalDocs) {
              break;
            }

            if ((docCount % 10000) == 0) {
              long nowMS = System.currentTimeMillis();
              double dps = docCount / ((nowMS - tStart) / 1000.0);
              System.out.println(
                  String.format(
                      Locale.ROOT,
                      "Indexer: %d docs (%.1f sec); %.1f docs/sec",
                      docCount,
                      (nowMS - tStart) / 1000.0,
                      dps));
            }

            if (mode == Mode.UPDATE) {
              final String updateID = LineFileDocs.intToID(random.nextInt(randomDocIDMax));
              // NOTE: can't use docState.id in case doClone
              // was true
              ((Field) doc.getField("id")).setStringValue(updateID);
              w.updateDocument(new Term("id", updateID), doc);
            } else {
              w.addDocument(doc);
            }

            maybeOpenReader(tStart);
          }
        }
      } catch (Exception e) {
        failed.set(true);
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }

    private void maybeOpenReader(long tStart) throws IOException {
      if (nrtEverySec > 0.0) {
        long ns = System.nanoTime();
        if (ns - lastRefreshNS.get() > nrtEverySec * 1000000000
            && refreshing.compareAndSet(false, true)) {
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "%.2f sec: now getReader",
                  (System.currentTimeMillis() - tStart) / 1000.0));
          DirectoryReader r = DirectoryReader.open(w);
          int irNumDocs = r.numDocs();
          int irMaxDoc = r.maxDoc();
          System.out.println(
              String.format(
                  Locale.ROOT,
                  "%.2f sec: %d numDocs, %d deletions (%.2f%%)",
                  (System.currentTimeMillis() - tStart) / 1000.0,
                  irNumDocs,
                  irMaxDoc - irNumDocs,
                  100. * (irMaxDoc - irNumDocs) / irNumDocs));
          r.close();
          lastRefreshNS.set(ns);
          refreshing.set(false);
        }
      }
    }
  }

  private static class IngestRatePrinter extends Thread {

    private final AtomicInteger count;
    private final AtomicBoolean stop;

    /**
     * Instantiates a new Ingest rate printer.
     *
     * @param count the count
     * @param stop the stop
     */
    public IngestRatePrinter(AtomicInteger count, AtomicBoolean stop) {
      this.count = count;
      this.stop = stop;
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      System.out.println("startIngest: " + time);
      final long start = time;
      int lastCount = count.get();
      while (!stop.get()) {
        try {
          Thread.sleep(200);
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
        }
        int numDocs = count.get();

        double current = numDocs - lastCount;
        long now = System.currentTimeMillis();
        double seconds = (now - time) / 1000.0d;
        System.out.println("ingest: " + (current / seconds) + " " + (now - start));
        time = now;
        lastCount = numDocs;
      }
    }
  }

  // NOTE: returned array might have dups
  private static BytesRef[] randomStrings(int count, Random random) {
    final BytesRef[] strings = new BytesRef[count];
    int i = 0;
    while (i < count) {
      final String s = randomRealisticUnicodeString(random);
      if (s.length() >= 7) {
        strings[i++] = new BytesRef(s);
      }
    }

    return strings;
  }

  // NOTE: copied from Lucene's _TestUtil, so we don't have
  // a [dangerous] dep on test-framework:

  private static final int[] blockStarts = {
    0x0000, 0x0080, 0x0100, 0x0180, 0x0250, 0x02B0, 0x0300, 0x0370, 0x0400, 0x0500, 0x0530, 0x0590,
    0x0600, 0x0700, 0x0750, 0x0780, 0x07C0, 0x0800, 0x0900, 0x0980, 0x0A00, 0x0A80, 0x0B00, 0x0B80,
    0x0C00, 0x0C80, 0x0D00, 0x0D80, 0x0E00, 0x0E80, 0x0F00, 0x1000, 0x10A0, 0x1100, 0x1200, 0x1380,
    0x13A0, 0x1400, 0x1680, 0x16A0, 0x1700, 0x1720, 0x1740, 0x1760, 0x1780, 0x1800, 0x18B0, 0x1900,
    0x1950, 0x1980, 0x19E0, 0x1A00, 0x1A20, 0x1B00, 0x1B80, 0x1C00, 0x1C50, 0x1CD0, 0x1D00, 0x1D80,
    0x1DC0, 0x1E00, 0x1F00, 0x2000, 0x2070, 0x20A0, 0x20D0, 0x2100, 0x2150, 0x2190, 0x2200, 0x2300,
    0x2400, 0x2440, 0x2460, 0x2500, 0x2580, 0x25A0, 0x2600, 0x2700, 0x27C0, 0x27F0, 0x2800, 0x2900,
    0x2980, 0x2A00, 0x2B00, 0x2C00, 0x2C60, 0x2C80, 0x2D00, 0x2D30, 0x2D80, 0x2DE0, 0x2E00, 0x2E80,
    0x2F00, 0x2FF0, 0x3000, 0x3040, 0x30A0, 0x3100, 0x3130, 0x3190, 0x31A0, 0x31C0, 0x31F0, 0x3200,
    0x3300, 0x3400, 0x4DC0, 0x4E00, 0xA000, 0xA490, 0xA4D0, 0xA500, 0xA640, 0xA6A0, 0xA700, 0xA720,
    0xA800, 0xA830, 0xA840, 0xA880, 0xA8E0, 0xA900, 0xA930, 0xA960, 0xA980, 0xAA00, 0xAA60, 0xAA80,
    0xABC0, 0xAC00, 0xD7B0, 0xE000, 0xF900, 0xFB00, 0xFB50, 0xFE00, 0xFE10, 0xFE20, 0xFE30, 0xFE50,
    0xFE70, 0xFF00, 0xFFF0, 0x10000, 0x10080, 0x10100, 0x10140, 0x10190, 0x101D0, 0x10280, 0x102A0,
    0x10300, 0x10330, 0x10380, 0x103A0, 0x10400, 0x10450, 0x10480, 0x10800, 0x10840, 0x10900,
    0x10920, 0x10A00, 0x10A60, 0x10B00, 0x10B40, 0x10B60, 0x10C00, 0x10E60, 0x11080, 0x12000,
    0x12400, 0x13000, 0x1D000, 0x1D100, 0x1D200, 0x1D300, 0x1D360, 0x1D400, 0x1F000, 0x1F030,
    0x1F100, 0x1F200, 0x20000, 0x2A700, 0x2F800, 0xE0000, 0xE0100, 0xF0000, 0x100000
  };

  private static final int[] blockEnds = {
    0x007F, 0x00FF, 0x017F, 0x024F, 0x02AF, 0x02FF, 0x036F, 0x03FF, 0x04FF, 0x052F, 0x058F, 0x05FF,
    0x06FF, 0x074F, 0x077F, 0x07BF, 0x07FF, 0x083F, 0x097F, 0x09FF, 0x0A7F, 0x0AFF, 0x0B7F, 0x0BFF,
    0x0C7F, 0x0CFF, 0x0D7F, 0x0DFF, 0x0E7F, 0x0EFF, 0x0FFF, 0x109F, 0x10FF, 0x11FF, 0x137F, 0x139F,
    0x13FF, 0x167F, 0x169F, 0x16FF, 0x171F, 0x173F, 0x175F, 0x177F, 0x17FF, 0x18AF, 0x18FF, 0x194F,
    0x197F, 0x19DF, 0x19FF, 0x1A1F, 0x1AAF, 0x1B7F, 0x1BBF, 0x1C4F, 0x1C7F, 0x1CFF, 0x1D7F, 0x1DBF,
    0x1DFF, 0x1EFF, 0x1FFF, 0x206F, 0x209F, 0x20CF, 0x20FF, 0x214F, 0x218F, 0x21FF, 0x22FF, 0x23FF,
    0x243F, 0x245F, 0x24FF, 0x257F, 0x259F, 0x25FF, 0x26FF, 0x27BF, 0x27EF, 0x27FF, 0x28FF, 0x297F,
    0x29FF, 0x2AFF, 0x2BFF, 0x2C5F, 0x2C7F, 0x2CFF, 0x2D2F, 0x2D7F, 0x2DDF, 0x2DFF, 0x2E7F, 0x2EFF,
    0x2FDF, 0x2FFF, 0x303F, 0x309F, 0x30FF, 0x312F, 0x318F, 0x319F, 0x31BF, 0x31EF, 0x31FF, 0x32FF,
    0x33FF, 0x4DBF, 0x4DFF, 0x9FFF, 0xA48F, 0xA4CF, 0xA4FF, 0xA63F, 0xA69F, 0xA6FF, 0xA71F, 0xA7FF,
    0xA82F, 0xA83F, 0xA87F, 0xA8DF, 0xA8FF, 0xA92F, 0xA95F, 0xA97F, 0xA9DF, 0xAA5F, 0xAA7F, 0xAADF,
    0xABFF, 0xD7AF, 0xD7FF, 0xF8FF, 0xFAFF, 0xFB4F, 0xFDFF, 0xFE0F, 0xFE1F, 0xFE2F, 0xFE4F, 0xFE6F,
    0xFEFF, 0xFFEF, 0xFFFF, 0x1007F, 0x100FF, 0x1013F, 0x1018F, 0x101CF, 0x101FF, 0x1029F, 0x102DF,
    0x1032F, 0x1034F, 0x1039F, 0x103DF, 0x1044F, 0x1047F, 0x104AF, 0x1083F, 0x1085F, 0x1091F,
    0x1093F, 0x10A5F, 0x10A7F, 0x10B3F, 0x10B5F, 0x10B7F, 0x10C4F, 0x10E7F, 0x110CF, 0x123FF,
    0x1247F, 0x1342F, 0x1D0FF, 0x1D1FF, 0x1D24F, 0x1D35F, 0x1D37F, 0x1D7FF, 0x1F02F, 0x1F09F,
    0x1F1FF, 0x1F2FF, 0x2A6DF, 0x2B73F, 0x2FA1F, 0xE007F, 0xE01EF, 0xFFFFF, 0x10FFFF
  };

  /**
   * Returns random string of length between 0-20 codepoints, all codepoints within the same unicode
   * block. @param r the r
   *
   * @param r the r
   * @return the string
   */
  static String randomRealisticUnicodeString(Random r) {
    return randomRealisticUnicodeString(r, 20);
  }

  /**
   * Returns random string of length up to maxLength codepoints , all codepoints within the same
   * unicode block. @param r the r
   *
   * @param r the r
   * @param maxLength the max length
   * @return the string
   */
  static String randomRealisticUnicodeString(Random r, int maxLength) {
    return randomRealisticUnicodeString(r, 0, maxLength);
  }

  /**
   * Returns random string of length between min and max codepoints, all codepoints within the same
   * unicode block. @param r the r
   *
   * @param r the r
   * @param minLength the min length
   * @param maxLength the max length
   * @return the string
   */
  static String randomRealisticUnicodeString(Random r, int minLength, int maxLength) {
    final int end = nextInt(r, minLength, maxLength);
    final int block = r.nextInt(blockStarts.length);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < end; i++) {
      sb.appendCodePoint(nextInt(r, blockStarts[block], blockEnds[block]));
    }
    return sb.toString();
  }

  /**
   * Next int int.
   *
   * @param r the r
   * @param start the start
   * @param end the end
   * @return the int
   */
  static int nextInt(Random r, int start, int end) {
    if (end == start) {
      return start;
    } else {
      return start + r.nextInt(end - start + 1);
    }
  }
}
