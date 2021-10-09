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

import static org.apache.lucene.util.IOUtils.UTF_8;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PrintStreamInfoStream;

// First: cd to /foo/bar/baz/lucene-solr-clone/lucene

// Then: ant clean jar"

// Then: javac -cp build/core/classes/java /l/util/src/main/perf/IndexTaxis.java

// rm -rf /b/taxis.sparse; javac -cp build/core/classes/java /l/util/src/main/perf/IndexTaxis.java;
// java -cp build/core/classes/java:/l/util/src/main perf.IndexTaxis /b/taxis.nonsparse 4
// /l/data/mergedSubset.sparse.csv.blocks

/** The type Index taxis. */
public class IndexTaxis {

  private static final int NEWLINE = (byte) '\n';
  private static final int COMMA = (byte) ',';
  private static final byte[] header = new byte[Integer.BYTES];

  /** The Start ns. */
  static long startNS;

  /** Instantiates a new Index taxis. */
  public IndexTaxis() {}

  private static synchronized byte[] readChunk(BufferedInputStream docs) throws IOException {
    int count = docs.read(header, 0, header.length);
    if (count == -1) {
      // end
      return null;
    }

    int byteCount =
        (header[0] & 0xff)
            + ((header[1] & 0xff) << 8)
            + ((header[2] & 0xff) << 16)
            + ((header[3] & 0xff) << 24);
    byte[] chunk = new byte[byteCount];
    count = docs.read(chunk, 0, byteCount);
    if (count != byteCount) {
      throw new AssertionError();
    }
    return chunk;
  }

  /**
   * Add one field.
   *
   * @param doc the doc
   * @param dateParser the date parser
   * @param parsePosition the parse position
   * @param reuseField the reuse field
   * @param reuseField2 the reuse field 2
   * @param rawValue the raw value
   */
  static void addOneField(
      Document doc,
      SimpleDateFormat dateParser,
      ParsePosition parsePosition,
      Field reuseField,
      Field reuseField2,
      String rawValue) {
    switch (reuseField.name()) {
      case "vendor_id":
      case "green_vendor_id":
      case "yellow_vendor_id":
      case "payment_type":
      case "green_payment_type":
      case "yellow_payment_type":
      case "trip_type":
      case "green_trip_type":
      case "yellow_trip_type":
      case "rate_code_id":
      case "green_rate_code_id":
      case "yellow_rate_code_id":
      case "store_and_fwd_flag":
      case "green_store_and_fwd_flag":
      case "yellow_store_and_fwd_flag":
        {
          reuseField.setStringValue(rawValue);
          doc.add(reuseField);
          reuseField2.setBytesValue(new BytesRef(rawValue));
          doc.add(reuseField2);
          break;
        }
      case "pickup_datetime":
      case "green_pickup_datetime":
      case "yellow_pickup_datetime":
      case "dropoff_datetime":
      case "green_dropoff_datetime":
      case "yellow_dropoff_datetime":
        {
          parsePosition.setIndex(0);
          Date date = dateParser.parse(rawValue, parsePosition);
          if (parsePosition.getErrorIndex() != -1) {
            throw new IllegalArgumentException(
                "could not parse field \""
                    + reuseField.name()
                    + "\" as date: rawValue="
                    + rawValue);
          }
          if (parsePosition.getIndex() != rawValue.length()) {
            throw new IllegalArgumentException(
                "could not parse field \""
                    + reuseField.name()
                    + "\" as date: rawValue="
                    + rawValue);
          }
          long v = date.getTime();
          reuseField.setLongValue(v);
          doc.add(reuseField);
          reuseField2.setLongValue(v);
          doc.add(reuseField2);
          break;
        }
      case "passenger_count":
      case "green_passenger_count":
      case "yellow_passenger_count":
        {
          int v = Integer.parseInt(rawValue);
          reuseField.setIntValue(v);
          doc.add(reuseField);
          reuseField2.setLongValue(v);
          doc.add(reuseField2);
          break;
        }
      case "trip_distance":
      case "green_trip_distance":
      case "yellow_trip_distance":
      case "pickup_latitude":
      case "green_pickup_latitude":
      case "yellow_pickup_latitude":
      case "pickup_longitude":
      case "green_pickup_longitude":
      case "yellow_pickup_longitude":
      case "dropoff_latitude":
      case "green_dropoff_latitude":
      case "yellow_dropoff_latitude":
      case "dropoff_longitude":
      case "green_dropoff_longitude":
      case "yellow_dropoff_longitude":
      case "fare_amount":
      case "green_fare_amount":
      case "yellow_fare_amount":
      case "surcharge":
      case "green_surcharge":
      case "yellow_surcharge":
      case "mta_tax":
      case "green_mta_tax":
      case "yellow_mta_tax":
      case "extra":
      case "green_extra":
      case "yellow_extra":
      case "ehail_fee":
      case "green_ehail_fee":
      case "yellow_ehail_fee":
      case "improvement_surcharge":
      case "green_improvement_surcharge":
      case "yellow_improvement_surcharge":
      case "tip_amount":
      case "green_tip_amount":
      case "yellow_tip_amount":
      case "tolls_amount":
      case "green_tolls_amount":
      case "yellow_tolls_amount":
      case "total_amount":
      case "green_total_amount":
      case "yellow_total_amount":
        {
          double v = Double.parseDouble(rawValue);
          reuseField.setDoubleValue(v);
          doc.add(reuseField);
          reuseField2.setLongValue(Double.doubleToRawLongBits(v));
          doc.add(reuseField2);
          break;
        }
      default:
        throw new AssertionError("failed to handle field \"" + reuseField.name() + "\"");
    }
  }

  /**
   * Index all documents contained in one chunk @param dateParser the date parser
   *
   * @param dateParser the date parser
   * @param parsePosition the parse position
   * @param reuseFields the reuse fields
   * @param reuseFields2 the reuse fields 2
   * @param reuseDoc the reuse doc
   * @param cabColorField the cab color field
   * @param cabColorDVField the cab color dv field
   * @param chunk the chunk
   * @param w the w
   * @param docCounter the doc counter
   * @param bytesCounter the bytes counter
   * @throws IOException the io exception
   */
  static void indexOneChunk(
      SimpleDateFormat dateParser,
      ParsePosition parsePosition,
      Field[][] reuseFields,
      Field[][] reuseFields2,
      Document reuseDoc,
      Field cabColorField,
      Field cabColorDVField,
      byte[] chunk,
      IndexWriter w,
      AtomicInteger docCounter,
      AtomicLong bytesCounter)
      throws IOException {
    // System.out.println("CHUNK: " + chunk.length + " bytes");
    String s = new String(chunk, 0, chunk.length, UTF_8);
    if (s.charAt(s.length() - 1) != '\n') {
      throw new AssertionError();
    }

    w.addDocuments(
        new Iterable<Document>() {
          @Override
          public Iterator<Document> iterator() {
            return new Iterator<Document>() {
              private int i;
              private Document nextDoc;
              private boolean nextSet;
              private int lastLineStart;
              // private int chunkDocCount;
              private final BytesRef colorBytesRef = new BytesRef(new byte[1]);

              @Override
              public boolean hasNext() {
                if (!nextSet) {
                  setNextDoc();
                  nextSet = true;
                }

                return nextDoc != null;
              }

              @Override
              public Document next() {
                assert nextSet;
                nextSet = false;
                Document result = nextDoc;
                nextDoc = null;
                return result;
              }

              private void setNextDoc() {
                reuseDoc.clear();
                if (i == s.length()) {
                  return;
                }
                int fieldUpto = 0;
                char color = s.charAt(i++);
                if (s.charAt(i++) != ':') {
                  throw new IllegalArgumentException("expected ':' but saw '" + s.charAt(i - 1));
                }
                cabColorField.setStringValue(Character.toString(color));
                reuseDoc.add(cabColorField);
                colorBytesRef.bytes[0] = (byte) color;
                cabColorDVField.setBytesValue(colorBytesRef);
                reuseDoc.add(cabColorDVField);
                int colorFieldIndex;
                if (color == 'g') {
                  colorFieldIndex = 0;
                } else if (color == 'y') {
                  colorFieldIndex = 1;
                } else {
                  throw new IllegalArgumentException(
                      "expected color 'g' or 'y' but got '" + color + "'");
                }
                Field[] colorReuseFields = reuseFields[colorFieldIndex];
                Field[] colorReuseFields2 = reuseFields2[colorFieldIndex];
                int lastFieldStart = i;
                while (true) {
                  char c = s.charAt(i);
                  if (c == '\n' || c == ',') {
                    if (i > lastFieldStart) {
                      addOneField(
                          reuseDoc,
                          dateParser,
                          parsePosition,
                          colorReuseFields[fieldUpto],
                          colorReuseFields2[fieldUpto],
                          s.substring(lastFieldStart, i));
                    }
                    if (c == '\n') {
                      if (fieldUpto != colorReuseFields.length - 1) {
                        throw new AssertionError(
                            "fieldUpto="
                                + fieldUpto
                                + " vs fields.length-1="
                                + (colorReuseFields.length - 1));
                      }
                      // chunkDocCount++;
                      this.nextDoc = reuseDoc;
                      int x = docCounter.incrementAndGet();
                      long y = bytesCounter.addAndGet((i + 1) - lastLineStart);
                      if (x % 100000 == 0) {
                        double sec = (System.nanoTime() - startNS) / 1000000000.0;
                        System.out.println(
                            String.format(
                                Locale.ROOT,
                                "%.1f sec: %d docs; %.1f docs/sec; %.1f MB/sec",
                                sec,
                                x,
                                x / sec,
                                (y / 1024. / 1024.) / sec));
                      }
                      i++;
                      lastLineStart = i;
                      return;
                    } else {
                      fieldUpto++;
                    }
                    lastFieldStart = i + 1;
                  }
                  i++;
                }
                // System.out.println("chunk doc count: " + chunkDocCount);
              }
            };
          }
        });
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get(args[0]);
    Directory dir = FSDirectory.open(indexPath);
    int threadCount = Integer.parseInt(args[1]);
    Path docsPath = Paths.get(args[2]);
    String sparseOrNot = args[3];
    String sortOrNot = args[4];

    final boolean sparse;
    if (sparseOrNot.equals("sparse")) {
      sparse = true;
    } else if (sparseOrNot.equals("nonsparse")) {
      sparse = false;
    } else {
      throw new IllegalArgumentException(
          "sparseOrNot: expected 'sparse' or 'nonsparse' but saw " + sparseOrNot);
    }

    final boolean sorted;
    if (sortOrNot.equals("True")) {
      sorted = true;
    } else if (sortOrNot.equals("False")) {
      sorted = false;
    } else {
      throw new IllegalArgumentException(
          "sortOrNot: expected 'true' or 'false' but saw " + sortOrNot);
    }

    // Pass analyzer explicitly, even though we've switched to StandardAnalyzer by default, so when
    // we back-test to before that default
    // change, this compiles:
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    // System.out.println("NOW SET INFO STREAM");

    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    if (threadCount == 1) {
      // 555 segment structure for 20M docs:
      iwc.setMaxBufferedDocs(36036);
      iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      iwc.setMergeScheduler(new SerialMergeScheduler());
      iwc.setMergePolicy(new LogDocMergePolicy());
      // so we see details about flush times, merge times:
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    } else {
      iwc.setRAMBufferSizeMB(1024.);
    }

    if (sorted) {
      iwc.setIndexSort(new Sort(new SortField("cab_color", SortField.Type.STRING)));
    }

    final IndexWriter w = new IndexWriter(dir, iwc);

    BufferedInputStream docs =
        new BufferedInputStream(Files.newInputStream(docsPath, StandardOpenOption.READ));

    // parse the header fields
    List<String> fieldsList = new ArrayList<>();
    StringBuilder builder = new StringBuilder();
    while (true) {
      int x = docs.read();
      if (x == -1) {
        throw new IllegalArgumentException(
            "hit EOF while trying to read CSV header; are you sure you have the right CSV file!");
      }
      byte b = (byte) x;
      if (b == NEWLINE) {
        fieldsList.add(builder.toString());
        break;
      } else if (b == COMMA) {
        fieldsList.add(builder.toString());
        builder.setLength(0);
      } else {
        // this is OK because headers are all ascii:
        builder.append((char) b);
      }
    }

    final String[] fields = fieldsList.toArray(new String[fieldsList.size()]);

    Thread[] threads = new Thread[threadCount];

    final AtomicInteger docCounter = new AtomicInteger();
    final AtomicLong bytesCounter = new AtomicLong();
    final AtomicBoolean failed = new AtomicBoolean();

    startNS = System.nanoTime();

    for (int i = 0; i < threadCount; i++) {

      threads[i] =
          new Thread() {
            @Override
            public void run() {
              try {
                _run();
              } catch (Exception e) {
                failed.set(true);
                throw new RuntimeException(e);
              }
            }

            private void _run() throws IOException {

              SimpleDateFormat dateParser =
                  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
              dateParser.setTimeZone(TimeZone.getTimeZone("UTC"));
              ParsePosition parsePosition = new ParsePosition(0);

              // Setup fields & document to reuse
              final Field[][] reuseFields = new Field[2][];
              final Field[][] reuseFields2 = new Field[2][];

              // green's fields:
              reuseFields[0] = new Field[fields.length];
              reuseFields2[0] = new Field[fields.length];

              // yellow's fields:
              reuseFields[1] = new Field[fields.length];
              reuseFields2[1] = new Field[fields.length];

              for (int i = 0; i < fields.length; i++) {
                String fieldName = fields[i];
                switch (fieldName) {
                  case "vendor_id":
                  case "payment_type":
                  case "trip_type":
                  case "rate_code_id":
                  case "store_and_fwd_flag":
                    {
                      if (sparse) {
                        reuseFields[0][i] =
                            new StringField("green_" + fieldName, "", Field.Store.YES);
                        reuseFields2[0][i] =
                            new SortedDocValuesField("green_" + fieldName, new BytesRef());
                        reuseFields[1][i] =
                            new StringField("yellow_" + fieldName, "", Field.Store.YES);
                        reuseFields2[1][i] =
                            new SortedDocValuesField("yellow_" + fieldName, new BytesRef());
                      } else {
                        reuseFields[0][i] = new StringField(fieldName, "", Field.Store.YES);
                        reuseFields2[0][i] = new SortedDocValuesField(fieldName, new BytesRef());
                        reuseFields[1][i] = reuseFields[0][i];
                        reuseFields2[1][i] = reuseFields2[0][i];
                      }
                      break;
                    }
                  case "pickup_datetime":
                  case "dropoff_datetime":
                    {
                      if (sparse) {
                        reuseFields[0][i] = new LongPoint("green_" + fieldName, 0);
                        reuseFields2[0][i] = new NumericDocValuesField("green_" + fieldName, 0);
                        reuseFields[1][i] = new LongPoint("yellow_" + fieldName, 0);
                        reuseFields2[1][i] = new NumericDocValuesField("yellow_" + fieldName, 0);
                      } else {
                        reuseFields[0][i] = new LongPoint(fieldName, 0);
                        reuseFields2[0][i] = new NumericDocValuesField(fieldName, 0);
                        reuseFields[1][i] = reuseFields[0][i];
                        reuseFields2[1][i] = reuseFields2[0][i];
                      }
                      break;
                    }
                  case "passenger_count":
                    {
                      if (sparse) {
                        reuseFields[0][i] = new IntPoint("green_" + fieldName, 0);
                        reuseFields2[0][i] = new NumericDocValuesField("green_" + fieldName, 0);
                        reuseFields[1][i] = new IntPoint("yellow_" + fieldName, 0);
                        reuseFields2[1][i] = new NumericDocValuesField("yellow_" + fieldName, 0);
                      } else {
                        reuseFields[0][i] = new IntPoint(fieldName, 0);
                        reuseFields2[0][i] = new NumericDocValuesField(fieldName, 0);
                        reuseFields[1][i] = reuseFields[0][i];
                        reuseFields2[1][i] = reuseFields2[0][i];
                      }
                      break;
                    }
                  case "trip_distance":
                  case "pickup_latitude":
                  case "pickup_longitude":
                  case "dropoff_latitude":
                  case "dropoff_longitude":
                  case "fare_amount":
                  case "surcharge":
                  case "mta_tax":
                  case "extra":
                  case "ehail_fee":
                  case "improvement_surcharge":
                  case "tip_amount":
                  case "tolls_amount":
                  case "total_amount":
                    {
                      if (sparse) {
                        reuseFields[0][i] = new DoublePoint("green_" + fieldName, 0.0);
                        reuseFields2[0][i] = new NumericDocValuesField("green_" + fieldName, 0);
                        reuseFields[1][i] = new DoublePoint("yellow_" + fieldName, 0.0);
                        reuseFields2[1][i] = new NumericDocValuesField("yellow_" + fieldName, 0);
                      } else {
                        reuseFields[0][i] = new DoublePoint(fieldName, 0.0);
                        reuseFields2[0][i] = new NumericDocValuesField(fieldName, 0);
                        reuseFields[1][i] = reuseFields[0][i];
                        reuseFields2[1][i] = reuseFields2[0][i];
                      }
                      break;
                    }
                  default:
                    throw new AssertionError("failed to handle field \"" + fieldName + "\"");
                }
              }

              Field cabColorField = new StringField("cab_color", "", Field.Store.NO);
              Field cabColorDVField = new SortedDocValuesField("cab_color", new BytesRef());
              Document reuseDoc = new Document();

              while (true) {
                byte[] chunk = readChunk(docs);
                if (chunk == null) {
                  break;
                }
                indexOneChunk(
                    dateParser,
                    parsePosition,
                    reuseFields,
                    reuseFields2,
                    reuseDoc,
                    cabColorField,
                    cabColorDVField,
                    chunk,
                    w,
                    docCounter,
                    bytesCounter);
              }
            }
          };
      threads[i].start();
    }

    for (int i = 0; i < threadCount; i++) {
      threads[i].join();
    }

    if (threadCount == 1) {
      System.out.println("Indexing done; now close");
      w.close();
    } else {
      System.out.println("Indexing done; now rollback");
      w.rollback();
    }
    docs.close();

    if (failed.get()) {
      throw new RuntimeException("indexing failed");
    }
  }
}
