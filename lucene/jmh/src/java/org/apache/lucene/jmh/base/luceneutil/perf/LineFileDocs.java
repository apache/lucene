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

// FIELDS_HEADER_INDICATOR###   title   timestamp   text    username    characterCount  categories
// imageCount  sectionCount    subSectionCount subSubSectionCount  refCount

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormatSymbols;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

/** The type Line file docs. */
public class LineFileDocs implements Closeable {

  // sentinel:
  private static final LineFileDoc END = new LineFileDoc("END", null);

  private final AtomicInteger nextID = new AtomicInteger();

  private BufferedReader reader;
  private SeekableByteChannel channel;
  private static final int BUFFER_SIZE = 1 << 16; // 64K
  private final boolean doRepeat;
  private final String path;
  private final boolean storeBody;
  private final boolean tvsBody;
  private final boolean bodyPostingsOffsets;
  private final AtomicLong bytesIndexed = new AtomicLong();
  private final boolean doClone;
  private final TaxonomyWriter taxoWriter;
  // maps field name to 1 (taxonomy) | 2 (sorted set)
  private final Map<String, Integer> facetFields;
  private final FacetsConfig facetsConfig;
  private String[] extraFacetFields;
  private final boolean addDVFields;
  private final BlockingQueue<LineFileDoc> queue = new ArrayBlockingQueue<>(1024);
  private final Thread readerThread;
  /** The Is binary. */
  final boolean isBinary;

  private final ThreadLocal<LineFileDoc> nextDocs = new ThreadLocal<>();
  private final String[] months = DateFormatSymbols.getInstance(Locale.ROOT).getMonths();
  private final String vectorFile;
  private final int vectorDimension;
  private SeekableByteChannel vectorChannel;

  /**
   * Instantiates a new Line file docs.
   *
   * @param path the path
   * @param doRepeat the do repeat
   * @param storeBody the store body
   * @param tvsBody the tvs body
   * @param bodyPostingsOffsets the body postings offsets
   * @param doClone the do clone
   * @param taxoWriter the taxo writer
   * @param facetFields the facet fields
   * @param facetsConfig the facets config
   * @param addDVFields the add dv fields
   * @param vectorFile the vector file
   * @param vectorDimension the vector dimension
   * @throws IOException the io exception
   */
  public LineFileDocs(
      String path,
      boolean doRepeat,
      boolean storeBody,
      boolean tvsBody,
      boolean bodyPostingsOffsets,
      boolean doClone,
      TaxonomyWriter taxoWriter,
      Map<String, Integer> facetFields,
      FacetsConfig facetsConfig,
      boolean addDVFields,
      String vectorFile,
      int vectorDimension)
      throws IOException {
    this.path = path;
    this.isBinary = path.endsWith(".bin");
    this.storeBody = storeBody;
    this.tvsBody = tvsBody;
    this.bodyPostingsOffsets = bodyPostingsOffsets;
    this.doClone = doClone;
    this.doRepeat = doRepeat;
    this.taxoWriter = taxoWriter;
    this.facetFields = facetFields;
    this.facetsConfig = facetsConfig;
    this.addDVFields = addDVFields;
    this.vectorFile = vectorFile;
    this.vectorDimension = vectorDimension;

    open();
    readerThread =
        new Thread() {
          @Override
          public void run() {
            try {
              readDocs();
            } catch (Throwable t) {
              throw new RuntimeException(t);
            }
          }
        };
    readerThread.setName("LineFileDocs reader");
    readerThread.setDaemon(true);
    readerThread.start();
  }

  private void readDocs() throws Exception {
    if (isBinary) {
      byte[] headerBytes = new byte[8];
      ByteBuffer header = ByteBuffer.wrap(headerBytes);
      header.order(ByteOrder.LITTLE_ENDIAN);
      while (true) {
        header.position(0);
        int x = channel.read(header);
        if (x == -1) {
          if (doRepeat) {
            close();
            open();
            x = channel.read(header);
          } else {
            break;
          }
        }

        if (x != 8) {
          throw new RuntimeException("expected 8 header bytes but read " + x);
        }
        int count = header.getInt(0);
        int length = header.getInt(4);
        // System.out.println("count= " + count + " len=" + length);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[length]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        x = channel.read(buffer);
        if (x != length) {
          throw new RuntimeException("expected " + length + " document bytes but read " + x);
        }
        buffer.position(0);
        queue.put(new LineFileDoc(buffer, readVector(count)));
      }
    } else {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          if (doRepeat) {
            close();
            open();
            line = reader.readLine();
          } else {
            break;
          }
        }
        queue.put(new LineFileDoc(line, readVector(1)));
      }
    }
    for (int i = 0; i < 128; i++) {
      queue.put(END);
    }
  }

  private float[] readVector(int count) throws IOException {
    if (vectorChannel == null) {
      return null;
    }
    float[] vector = new float[count * vectorDimension];
    ByteBuffer buffer =
        ByteBuffer.allocate(count * vectorDimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    int n = vectorChannel.read(buffer);
    if (n != count * vectorDimension * Float.BYTES) {
      throw new RuntimeException(
          "expected " + count * vectorDimension * Float.BYTES + " vector bytes but read " + n);
    }
    buffer.position(0);
    buffer.asFloatBuffer().get(vector);
    return vector;
  }

  /**
   * Gets bytes indexed.
   *
   * @return the bytes indexed
   */
  public long getBytesIndexed() {
    return bytesIndexed.get();
  }

  private void open() throws IOException {
    if (isBinary) {
      channel = Files.newByteChannel(Paths.get(path), StandardOpenOption.READ);
    } else {
      InputStream is = Files.newInputStream(Paths.get(path));
      reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), BUFFER_SIZE);
      String firstLine = reader.readLine();
      if (firstLine.startsWith("FIELDS_HEADER_INDICATOR")) {
        if (!firstLine.startsWith("FIELDS_HEADER_INDICATOR###\tdoctitle\tdocdate\tbody")
            && !firstLine.startsWith("FIELDS_HEADER_INDICATOR###\ttitle\ttimestamp\ttext")) {
          throw new IllegalArgumentException(
              "unrecognized header in line docs file: " + firstLine.trim());
        }
        if (facetFields != null && facetFields.isEmpty() == false) {
          String[] fields = firstLine.split("\t");
          if (fields.length > 4) {
            extraFacetFields = ArrayUtil.copyOfSubArray(fields, 4, fields.length);
            System.out.println("Additional facet fields: " + Arrays.toString(extraFacetFields));

            List<String> extraFacetFieldsList = Arrays.asList(extraFacetFields);

            // Verify facet fields now:
            for (String field : facetFields.keySet()) {
              if (field.equals("Date") == false
                  && field.equals("Month") == false
                  && field.equals("DayOfYear") == false
                  && !extraFacetFieldsList.contains(field)) {
                throw new IllegalArgumentException(
                    "facet field \"" + field + "\" is not recognized");
              }
            }
          } else {
            // Verify facet fields now:
            for (String field : facetFields.keySet()) {
              if (field.equals("Date") == false
                  && field.equals("Month") == false
                  && field.equals("DayOfYear") == false) {
                throw new IllegalArgumentException(
                    "facet field \"" + field + "\" is not recognized");
              }
            }
          }
        }
        // Skip header
      } else {
        // Old format: no header
        reader.close();
        is = Files.newInputStream(Paths.get(path));
        reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), BUFFER_SIZE);
      }
    }
    if (vectorFile != null) {
      vectorChannel = Files.newByteChannel(Paths.get(vectorFile), StandardOpenOption.READ);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (vectorChannel != null) {
      vectorChannel.close();
      vectorChannel = null;
    }
  }

  /**
   * Int to id string.
   *
   * @param id the id
   * @return the string
   */
  public static String intToID(int id) {
    // Base 36, prefixed with 0s to be length 6 (= 2.2 B)
    final String s =
        String.format(Locale.US, "%6s", Integer.toString(id, Character.MAX_RADIX))
            .replace(' ', '0');
    // System.out.println("fromint: " + id + " -> " + s);
    return s;
  }

  // --Commented out by Inspection START (10/7/21, 12:37 AM):
  //  /**
  //   * Id to int int.
  //   *
  //   * @param id the id
  //   * @return the int
  //   */
  //  public static int idToInt(BytesRef id) {
  //    // Decode base 36
  //    int accum = 0;
  //    int downTo = id.length + id.offset - 1;
  //    int multiplier = 1;
  //    while (downTo >= id.offset) {
  //      final char ch = (char) (id.bytes[downTo--] & 0xff);
  //      final int digit;
  //      if (ch >= '0' && ch <= '9') {
  //        digit = ch - '0';
  //      } else if (ch >= 'a' && ch <= 'z') {
  //        digit = 10 + (ch - 'a');
  //      } else {
  //        assert false;
  //        digit = -1;
  //      }
  //      accum += multiplier * digit;
  //      multiplier *= 36;
  //    }
  //
  //    // System.out.println("toint: " + id.utf8ToString() + " -> " + accum);
  //    return accum;
  //  }
  // --Commented out by Inspection STOP (10/7/21, 12:37 AM)

  /**
   * Id to int int.
   *
   * @param id the id
   * @return the int
   */
  public static int idToInt(String id) {
    // Decode base 36
    int accum = 0;
    int downTo = id.length() - 1;
    int multiplier = 1;
    while (downTo >= 0) {
      final char ch = id.charAt(downTo--);
      final int digit;
      if (ch >= '0' && ch <= '9') {
        digit = ch - '0';
      } else if (ch >= 'a' && ch <= 'z') {
        digit = 10 + (ch - 'a');
      } else {
        assert false;
        digit = -1;
      }
      accum += multiplier * digit;
      multiplier *= 36;
    }

    // System.out.println("toint: " + id + " -> " + accum);
    return accum;
  }

  private static final char SEP = '\t';

  /** The type Doc state. */
  public static final class DocState {

    /** The Doc. */
    final Document doc;
    /** The Title tokenized. */
    final Field titleTokenized;
    /** The Title. */
    final Field title;
    /** The Title dv. */
    final Field titleDV;
    /** The Month dv. */
    final Field monthDV;
    /** The Day of year dv. */
    final Field dayOfYearDV;
    /** The Day of year ip. */
    final IntPoint dayOfYearIP;
    /** The Title bdv. */
    final BinaryDocValuesField titleBDV;
    /** The Last mod ndv. */
    final NumericDocValuesField lastModNDV;
    /** The Last mod lp. */
    final LongPoint lastModLP;
    /** The Body. */
    final Field body;
    /** The Id. */
    final Field id;
    /** The Id point. */
    final Field idPoint;
    /** The Date. */
    final Field date;
    /** The Time sec. */
    // final NumericDocValuesField dateMSec;
    // final LongField rand;
    final Field timeSec;
    /** The Date parser. */
    // Necessary for "old style" wiki line files:
    final SimpleDateFormat dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);
    /** The Vector. */
    final KnnVectorField vector;

    // For just y/m/day:
    // final SimpleDateFormat dateParser = new SimpleDateFormat("y/M/d", Locale.US);

    /** The Date cal. */
    // final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'",
    // Locale.US);
    final Calendar dateCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    /** The Date pos. */
    final ParsePosition datePos = new ParsePosition(0);

    /**
     * Instantiates a new Doc state.
     *
     * @param storeBody the store body
     * @param tvsBody the tvs body
     * @param bodyPostingsOffsets the body postings offsets
     * @param addDVFields the add dv fields
     * @param vectorDimension the vector dimension
     */
    DocState(
        boolean storeBody,
        boolean tvsBody,
        boolean bodyPostingsOffsets,
        boolean addDVFields,
        int vectorDimension) {
      doc = new Document();

      title = new StringField("title", "", Field.Store.NO);
      doc.add(title);

      if (addDVFields) {
        titleDV = new SortedDocValuesField("titleDV", new BytesRef(""));
        doc.add(titleDV);

        titleBDV = new BinaryDocValuesField("titleBDV", new BytesRef(""));
        doc.add(titleBDV);

        lastModNDV = new NumericDocValuesField("lastModNDV", -1);
        doc.add(lastModNDV);
        lastModLP =
            new LongPoint(
                "lastModNDV", -1); // points field must have the same name and value as DV field
        doc.add(lastModLP);

        monthDV = new SortedDocValuesField("monthSortedDV", new BytesRef(""));
        doc.add(monthDV);

        dayOfYearDV = new NumericDocValuesField("dayOfYearNumericDV", 0);
        doc.add(dayOfYearDV);
        dayOfYearIP =
            new IntPoint(
                "dayOfYearNumericDV",
                0); // points field must have the same name and value as DV field
        doc.add(dayOfYearIP);
      } else {
        titleDV = null;
        titleBDV = null;
        lastModNDV = null;
        lastModLP = null;
        monthDV = null;
        dayOfYearDV = null;
        dayOfYearIP = null;
      }

      titleTokenized = new Field("titleTokenized", "", TextField.TYPE_STORED);
      doc.add(titleTokenized);

      FieldType bodyFieldType = new FieldType(TextField.TYPE_NOT_STORED);
      if (storeBody) {
        bodyFieldType.setStored(true);
      }

      if (tvsBody) {
        bodyFieldType.setStoreTermVectors(true);
        bodyFieldType.setStoreTermVectorOffsets(true);
        bodyFieldType.setStoreTermVectorPositions(true);
      }

      if (bodyPostingsOffsets) {
        bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      }

      body = new Field("body", "", bodyFieldType);
      doc.add(body);

      id = new Field("id", "", StringField.TYPE_STORED);
      doc.add(id);

      idPoint = new IntPoint("id", 0);
      doc.add(idPoint);

      date = new Field("date", "", StringField.TYPE_STORED);
      doc.add(date);

      // dateMSec = new NumericDocValuesField("datenum", 0L);
      // doc.add(dateMSec);

      // rand = new LongField("rand", 0L, Field.Store.NO);
      // doc.add(rand);

      timeSec = new IntPoint("timesecnum", 0);
      doc.add(timeSec);

      if (vectorDimension > 0) {
        // create a throwaway vector so the field's type gets the proper dimension
        vector =
            new KnnVectorField(
                "vector", new float[vectorDimension], VectorSimilarityFunction.DOT_PRODUCT);
        doc.add(vector);
      } else {
        vector = null;
      }
    }
  }

  /**
   * New doc state doc state.
   *
   * @return the doc state
   */
  public DocState newDocState() {
    return new DocState(storeBody, tvsBody, bodyPostingsOffsets, addDVFields, vectorDimension);
  }

  /**
   * Clone doc document.
   *
   * @param doc1 the doc 1
   * @return the document
   */
  // TODO: is there a pre-existing way to do this!!!
  static Document cloneDoc(Document doc1) {
    final Document doc2 = new Document();

    for (IndexableField f0 : doc1.getFields()) {
      Field f = (Field) f0;
      if (f instanceof LongPoint) {
        doc2.add(new LongPoint(f.name(), ((LongPoint) f).numericValue().longValue()));
      } else if (f instanceof IntPoint) {
        doc2.add(new IntPoint(f.name(), ((IntPoint) f).numericValue().intValue()));
      } else if (f instanceof SortedDocValuesField) {
        doc2.add(new SortedDocValuesField(f.name(), f.binaryValue()));
      } else if (f instanceof NumericDocValuesField) {
        doc2.add(new NumericDocValuesField(f.name(), f.numericValue().longValue()));
      } else if (f instanceof BinaryDocValuesField) {
        doc2.add(new BinaryDocValuesField(f.name(), f.binaryValue()));
      } else if (f instanceof KnnVectorField) {
        doc2.add(
            new KnnVectorField(
                f.name(),
                ((KnnVectorField) f).vectorValue(),
                f.fieldType().vectorSimilarityFunction()));
      } else {
        Field field2 = new Field(f.name(), f.stringValue(), f.fieldType());
        doc2.add(field2);
      }
    }

    return doc2;
  }

  /**
   * Next doc document.
   *
   * @param doc the doc
   * @return the document
   * @throws IOException the io exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Document nextDoc(DocState doc) throws IOException {

    long msecSinceEpoch;
    int timeSec;
    int spot3;
    String line;
    String title;
    String body;

    if (isBinary) {

      //      float[] vector = new float[vectorDimension];

      LineFileDoc lfd = nextDocs.get();
      if (lfd == null || lfd.byteText.hasRemaining() == false) {
        /*
        System.out.println("  prev buffer=" + buffer);
        if (buffer != null) {
          System.out.println("    pos=" + buffer.position() + " vs limit=" + buffer.limit());
        }
        */

        try {
          lfd = queue.take();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
        if (lfd == END) {
          return null;
        }
        nextDocs.set(lfd);
        // System.out.println("    got new buffer=" + buffer + " pos=" + buffer.position() + "
        // limit=" + buffer.limit());
      }
      ByteBuffer buffer = lfd.byteText;
      int titleLenBytes = buffer.getInt();
      int bodyLenBytes = buffer.getInt();
      // System.out.println("    titleLen=" + titleLenBytes + " bodyLenBytes=" + bodyLenBytes);
      msecSinceEpoch = buffer.getLong();
      timeSec = buffer.getInt();
      byte[] bytes = buffer.array();

      char[] titleChars = new char[titleLenBytes];
      int titleLenChars =
          UnicodeUtil.UTF8toUTF16(bytes, buffer.position(), titleLenBytes, titleChars);
      title = new String(titleChars, 0, titleLenChars);
      // System.out.println("title: " + title);

      char[] bodyChars = new char[bodyLenBytes];
      int bodyLenChars =
          UnicodeUtil.UTF8toUTF16(
              bytes, buffer.position() + titleLenBytes, bodyLenBytes, bodyChars);
      body = new String(bodyChars, 0, bodyLenChars);
      buffer.position(buffer.position() + titleLenBytes + bodyLenBytes);

      doc.dateCal.setTimeInMillis(msecSinceEpoch);

      spot3 = 0;
      line = null;

      if (lfd.vector != null) {
        lfd.vector.get(doc.vector.vectorValue());
      }
    } else {
      LineFileDoc lfd;
      try {
        lfd = queue.take();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
      if (lfd == END) {
        return null;
      }
      line = lfd.stringText;

      int spot = line.indexOf(SEP);
      if (spot == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      int spot2 = line.indexOf(SEP, 1 + spot);
      if (spot2 == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      spot3 = line.indexOf(SEP, 1 + spot2);
      if (spot3 == -1) {
        spot3 = line.length();
      }

      body = line.substring(1 + spot2, spot3);

      title = line.substring(0, spot);

      final String dateString = line.substring(1 + spot, spot2);
      doc.date.setStringValue(dateString);
      doc.datePos.setIndex(0);
      final Date date = doc.dateParser.parse(dateString, doc.datePos);
      if (date == null) {
        System.out.println("FAILED: " + dateString);
      }
      // doc.dateMSec.setLongValue(date.getTime());

      // doc.rand.setLongValue(rand.nextInt(10000));
      // System.out.println("DATE: " + date);
      doc.dateCal.setTime(date);
      msecSinceEpoch = doc.dateCal.getTimeInMillis();
      timeSec =
          doc.dateCal.get(Calendar.HOUR_OF_DAY) * 3600
              + doc.dateCal.get(Calendar.MINUTE) * 60
              + doc.dateCal.get(Calendar.SECOND);
      if (doc.vector != null) {
        doc.vector.setVectorValue(lfd.vector.array());
      }
    }

    final int myID = nextID.getAndIncrement();

    bytesIndexed.addAndGet(body.length() + title.length());
    doc.body.setStringValue(body);
    doc.title.setStringValue(title);
    if (addDVFields) {
      doc.titleBDV.setBytesValue(new BytesRef(title));
      doc.titleDV.setBytesValue(new BytesRef(title));
      doc.monthDV.setBytesValue(new BytesRef(months[doc.dateCal.get(Calendar.MONTH)]));
      doc.dayOfYearDV.setLongValue(doc.dateCal.get(Calendar.DAY_OF_YEAR));
      doc.dayOfYearIP.setIntValue(doc.dateCal.get(Calendar.DAY_OF_YEAR));
    }
    doc.titleTokenized.setStringValue(title);
    doc.id.setStringValue(intToID(myID));

    doc.idPoint.setIntValue(myID);

    if (addDVFields) {
      doc.lastModNDV.setLongValue(msecSinceEpoch);
      doc.lastModLP.setLongValue(msecSinceEpoch);
    }

    doc.timeSec.setIntValue(timeSec);

    if (facetFields != null && facetFields.isEmpty() == false) {
      Document doc2 = cloneDoc(doc.doc);

      if (facetFields.containsKey("Date")) {
        int flag = facetFields.get("Date");
        if ((flag & 1) != 0) {
          doc2.add(
              new FacetField(
                  "Date.taxonomy",
                  "" + doc.dateCal.get(Calendar.YEAR),
                  "" + doc.dateCal.get(Calendar.MONTH),
                  "" + doc.dateCal.get(Calendar.DAY_OF_MONTH)));
        }
        if ((flag & 2) != 0) {
          throw new IllegalArgumentException(
              "Date field can only be indexed as taxonomy facets, not sortedset, because it is hierarchical");
        }
      }

      if (facetFields.containsKey("Month")) {
        int flag = facetFields.get("Month");
        if ((flag & 1) != 0) {
          doc2.add(new FacetField("Month.taxonomy", months[doc.dateCal.get(Calendar.MONTH)]));
        }
        if ((flag & 2) != 0) {
          doc2.add(
              new SortedSetDocValuesFacetField(
                  "Month.sortedset", months[doc.dateCal.get(Calendar.MONTH)]));
        }
      }

      if (facetFields.containsKey("DayOfYear")) {
        int flag = facetFields.get("DayOfYear");
        if ((flag & 1) != 0) {
          doc2.add(
              new FacetField(
                  "DayOfYear.taxonomy", Integer.toString(doc.dateCal.get(Calendar.DAY_OF_YEAR))));
        }
        if ((flag & 2) != 0) {
          doc2.add(
              new SortedSetDocValuesFacetField(
                  "DayOfYear.sortedset", Integer.toString(doc.dateCal.get(Calendar.DAY_OF_YEAR))));
        }
      }

      if (extraFacetFields != null) {
        String[] extraValues = line.substring(spot3 + 1).split("\t");

        for (int i = 0; i < extraFacetFields.length; i++) {
          String extraFieldName = extraFacetFields[i];
          if (facetFields.containsKey(extraFieldName)) {
            if (extraFieldName.equals("categories")) {
              for (String cat : extraValues[i].split("\\|")) {
                // TODO: scary how taxo writer writes a
                // second /categories ord for this case ...
                if (cat.length() == 0) {
                  continue;
                }
                doc2.add(new FacetField("categories", cat));
              }
            } else if (extraFieldName.equals("characterCount")) {

              // Make number drilldown hierarchy, so eg 1877
              // characters is under
              // 0-1M/0-100K/0-10K/1-2K/1800-1900:
              List<String> nodes = new ArrayList<String>();
              int value = Integer.parseInt(extraValues[i]);
              int accum = 0;
              int base = 1000000;
              while (base > 100) {
                int factor = (value - accum) / base;
                nodes.add(
                    String.format(
                        Locale.ENGLISH,
                        "%d - %d",
                        accum + factor * base,
                        accum + (factor + 1) * base));
                accum += factor * base;
                base /= 10;
              }
              doc2.add(new FacetField(extraFieldName, nodes.toArray(new String[nodes.size()])));
            } else {
              doc2.add(new FacetField(extraFieldName, extraValues[i]));
            }
          }
        }

        /*
        String dvFieldName = "$facets_sorted_doc_values";
        doc.doc.removeFields(dvFieldName);
        for(CategoryPath path : paths) {
          //System.out.println("ADD: " + path.toString());
          doc.doc.add(new SortedSetDocValuesField(dvFieldName, new BytesRef(path.toString(FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR))));
        }
        */
      }
      return facetsConfig.build(taxoWriter, doc2);
    } else if (doClone) {
      return cloneDoc(doc.doc);
    } else {
      return doc.doc;
    }
  }

  private static class LineFileDoc {

    /** The Vector. */
    final FloatBuffer vector;
    /** The String text. */
    final String stringText;
    /** The Byte text. */
    final ByteBuffer byteText;

    /**
     * Instantiates a new Line file doc.
     *
     * @param text the text
     * @param vector the vector
     */
    LineFileDoc(String text, float[] vector) {
      stringText = text;
      byteText = null;
      if (vector == null) {
        this.vector = null;
      } else {
        this.vector = FloatBuffer.wrap(vector);
      }
    }

    /**
     * Instantiates a new Line file doc.
     *
     * @param bytes the bytes
     * @param vector the vector
     */
    LineFileDoc(ByteBuffer bytes, float[] vector) {
      stringText = null;
      byteText = bytes;
      if (vector == null) {
        this.vector = null;
      } else {
        this.vector = FloatBuffer.wrap(vector);
      }
    }
  }
}
