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
package org.apache.lucene.benchmark.jmh;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
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
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 8)
@Fork(value = 1)
public class DocIdEncodingBenchmark {

  private static List<int[]> DOC_ID_SEQUENCES = new ArrayList<>();

  private static int INPUT_SCALE_FACTOR;

  static {
    parseInput();
  }

  @Param({"Bit21With3StepsEncoder", "Bit21With2StepsEncoder", "Bit24Encoder", "Bit32Encoder"})
  String encoderName;

  @Param({"encode", "decode"})
  String methodName;

  private DocIdEncoder docIdEncoder;

  private Path tmpDir;

  private IndexInput in;

  private final int[] scratch = new int[512];

  private String decoderInputFile;

  @Setup(Level.Trial)
  public void init() throws IOException {
    tmpDir = Files.createTempDirectory("docIdJmh");
    docIdEncoder = DocIdEncoder.SingletonFactory.fromName(encoderName);
    decoderInputFile =
        String.join("_", "docIdJmhData", docIdEncoder.getClass().getSimpleName(), "DecoderInput");
    // Create a file for decoders ( once per trial ) to read in every JMH iteration
    if (methodName.equalsIgnoreCase("decode")) {
      try (Directory dir = FSDirectory.open(tmpDir);
          IndexOutput out = dir.createOutput(decoderInputFile, IOContext.DEFAULT)) {
        encode(out, docIdEncoder, DOC_ID_SEQUENCES, INPUT_SCALE_FACTOR);
      }
    }
  }

  @TearDown(Level.Trial)
  public void finish() throws IOException {
    if (methodName.equalsIgnoreCase("decode")) {
      Files.delete(tmpDir.resolve(decoderInputFile));
    }
    Files.delete(tmpDir);
  }

  @Benchmark
  public void executeEncodeOrDecode() throws IOException {
    if (methodName.equalsIgnoreCase("encode")) {
      String outputFile =
          String.join(
              "_",
              "docIdJmhData",
              docIdEncoder.getClass().getSimpleName(),
              String.valueOf(System.nanoTime()));
      try (Directory dir = FSDirectory.open(tmpDir);
          IndexOutput out = dir.createOutput(outputFile, IOContext.DEFAULT)) {
        encode(out, docIdEncoder, DOC_ID_SEQUENCES, INPUT_SCALE_FACTOR);
      } finally {
        Files.delete(tmpDir.resolve(outputFile));
      }
    } else if (methodName.equalsIgnoreCase("decode")) {
      try (Directory dir = FSDirectory.open(tmpDir)) {
        in = dir.openInput(decoderInputFile, IOContext.DEFAULT);
        decode(in, docIdEncoder, DOC_ID_SEQUENCES, INPUT_SCALE_FACTOR, scratch);
      }
    } else {
      throw new IllegalArgumentException("Unknown method: " + methodName);
    }
  }

  public void encode(
      IndexOutput out, DocIdEncoder docIdEncoder, List<int[]> docIdSequences, int inputScaleFactor)
      throws IOException {
    for (int[] docIdSequence : docIdSequences) {
      for (int i = 1; i <= inputScaleFactor; i++) {
        docIdEncoder.encode(out, 0, docIdSequence.length, docIdSequence);
      }
    }
  }

  public void decode(
      IndexInput in,
      DocIdEncoder docIdEncoder,
      List<int[]> docIdSequences,
      int inputScaleFactor,
      int[] scratch)
      throws IOException {
    for (int[] docIdSequence : docIdSequences) {
      for (int i = 1; i <= inputScaleFactor; i++) {
        docIdEncoder.decode(in, 0, docIdSequence.length, scratch);
        // TODO Use a unit test with a DocIdProvider that generates a few random sequences based on
        // given BPV.
        // Uncomment to test the output of Encoder
        //            if (!Arrays.equals(
        //                docIdSequence, Arrays.copyOfRange(scratch, 0, docIdSequence.length)))
        // {
        //              throw new RuntimeException(
        //                  String.format(
        //                      "Error for Encoder %s with sequence Expected %s Got %s",
        //                      encoderName, Arrays.toString(docIdSequence),
        // Arrays.toString(scratch)));
        //            }
      }
    }
  }

  /**
   * Extend this interface to add a new implementation used for DocId Encoding and Decoding. These
   * are taken from org.apache.lucene.util.bkd.DocIdsWriter.
   */
  public interface DocIdEncoder {

    void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException;

    void decode(IndexInput in, int start, int count, int[] docIds) throws IOException;

    class SingletonFactory {

      static final Map<String, DocIdEncoder> ENCODER_NAME_TO_INSTANCE_MAPPING = new HashMap<>();

      static {
        Class<?>[] allImplementations = DocIdEncoder.class.getDeclaredClasses();
        for (Class<?> clazz : allImplementations) {
          boolean isADocIdEncoder =
              Arrays.asList(clazz.getInterfaces()).contains(DocIdEncoder.class);
          if (isADocIdEncoder) {
            try {
              ENCODER_NAME_TO_INSTANCE_MAPPING.put(
                  clazz.getSimpleName().toLowerCase(Locale.ROOT),
                  (DocIdEncoder) clazz.getConstructor().newInstance());
            } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }

      public static DocIdEncoder fromName(String encoderName) {
        String parsedEncoderName = encoderName.trim().toLowerCase(Locale.ROOT);
        if (ENCODER_NAME_TO_INSTANCE_MAPPING.containsKey(parsedEncoderName)) {
          return ENCODER_NAME_TO_INSTANCE_MAPPING.get(parsedEncoderName);
        } else {
          throw new IllegalArgumentException("Unknown DocIdEncoder " + encoderName);
        }
      }
    }

    class Bit24Encoder implements DocIdEncoder {
      @Override
      public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
          int doc1 = docIds[i];
          int doc2 = docIds[i + 1];
          int doc3 = docIds[i + 2];
          int doc4 = docIds[i + 3];
          int doc5 = docIds[i + 4];
          int doc6 = docIds[i + 5];
          int doc7 = docIds[i + 6];
          int doc8 = docIds[i + 7];
          long l1 = (doc1 & 0xffffffL) << 40 | (doc2 & 0xffffffL) << 16 | ((doc3 >>> 8) & 0xffffL);
          long l2 =
              (doc3 & 0xffL) << 56
                  | (doc4 & 0xffffffL) << 32
                  | (doc5 & 0xffffffL) << 8
                  | ((doc6 >> 16) & 0xffL);
          long l3 = (doc6 & 0xffffL) << 48 | (doc7 & 0xffffffL) << 24 | (doc8 & 0xffffffL);
          out.writeLong(l1);
          out.writeLong(l2);
          out.writeLong(l3);
        }
        for (; i < count; ++i) {
          out.writeShort((short) (docIds[i] >>> 8));
          out.writeByte((byte) docIds[i]);
        }
      }

      @Override
      public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i;
        for (i = 0; i < count - 7; i += 8) {
          long l1 = in.readLong();
          long l2 = in.readLong();
          long l3 = in.readLong();
          docIDs[i] = (int) (l1 >>> 40);
          docIDs[i + 1] = (int) (l1 >>> 16) & 0xffffff;
          docIDs[i + 2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
          docIDs[i + 3] = (int) (l2 >>> 32) & 0xffffff;
          docIDs[i + 4] = (int) (l2 >>> 8) & 0xffffff;
          docIDs[i + 5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
          docIDs[i + 6] = (int) (l3 >>> 24) & 0xffffff;
          docIDs[i + 7] = (int) l3 & 0xffffff;
        }
        for (; i < count; ++i) {
          docIDs[i] =
              (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
        }
      }
    }

    class Bit21With2StepsEncoder implements DocIdEncoder {
      @Override
      public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i = 0;
        for (; i < count - 2; i += 3) {
          long packedLong =
              ((docIds[i] & 0x001FFFFFL) << 42)
                  | ((docIds[i + 1] & 0x001FFFFFL) << 21)
                  | (docIds[i + 2] & 0x001FFFFFL);
          out.writeLong(packedLong);
        }
        for (; i < count; i++) {
          out.writeInt(docIds[i]);
        }
      }

      @Override
      public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i = 0;
        for (; i < count - 2; i += 3) {
          long packedLong = in.readLong();
          docIDs[i] = (int) (packedLong >>> 42);
          docIDs[i + 1] = (int) ((packedLong & 0x000003FFFFE00000L) >>> 21);
          docIDs[i + 2] = (int) (packedLong & 0x001FFFFFL);
        }
        for (; i < count; i++) {
          docIDs[i] = in.readInt();
        }
      }
    }

    /**
     * Variation of @{@link Bit21With2StepsEncoder} but uses 3 loops to decode the array of DocIds.
     * Comparatively better than @{@link Bit21With2StepsEncoder} on aarch64 with JDK 22
     */
    class Bit21With3StepsEncoder implements DocIdEncoder {

      @Override
      public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        int i = 0;
        for (; i < count - 8; i += 9) {
          long l1 =
              ((docIds[i] & 0x001FFFFFL) << 42)
                  | ((docIds[i + 1] & 0x001FFFFFL) << 21)
                  | (docIds[i + 2] & 0x001FFFFFL);
          long l2 =
              ((docIds[i + 3] & 0x001FFFFFL) << 42)
                  | ((docIds[i + 4] & 0x001FFFFFL) << 21)
                  | (docIds[i + 5] & 0x001FFFFFL);
          long l3 =
              ((docIds[i + 6] & 0x001FFFFFL) << 42)
                  | ((docIds[i + 7] & 0x001FFFFFL) << 21)
                  | (docIds[i + 8] & 0x001FFFFFL);
          out.writeLong(l1);
          out.writeLong(l2);
          out.writeLong(l3);
        }
        for (; i < count - 2; i += 3) {
          long packedLong =
              ((docIds[i] & 0x001FFFFFL) << 42)
                  | ((docIds[i + 1] & 0x001FFFFFL) << 21)
                  | (docIds[i + 2] & 0x001FFFFFL);
          out.writeLong(packedLong);
        }
        for (; i < count; i++) {
          out.writeInt(docIds[i]);
        }
      }

      @Override
      public void decode(IndexInput in, int start, int count, int[] docIDs) throws IOException {
        int i = 0;
        for (; i < count - 8; i += 9) {
          long l1 = in.readLong();
          long l2 = in.readLong();
          long l3 = in.readLong();
          docIDs[i] = (int) (l1 >>> 42);
          docIDs[i + 1] = (int) ((l1 & 0x000003FFFFE00000L) >>> 21);
          docIDs[i + 2] = (int) (l1 & 0x001FFFFFL);
          docIDs[i + 3] = (int) (l2 >>> 42);
          docIDs[i + 4] = (int) ((l2 & 0x000003FFFFE00000L) >>> 21);
          docIDs[i + 5] = (int) (l2 & 0x001FFFFFL);
          docIDs[i + 6] = (int) (l3 >>> 42);
          docIDs[i + 7] = (int) ((l3 & 0x000003FFFFE00000L) >>> 21);
          docIDs[i + 8] = (int) (l3 & 0x001FFFFFL);
        }
        for (; i < count - 2; i += 3) {
          long packedLong = in.readLong();
          docIDs[i] = (int) (packedLong >>> 42);
          docIDs[i + 1] = (int) ((packedLong & 0x000003FFFFE00000L) >>> 21);
          docIDs[i + 2] = (int) (packedLong & 0x001FFFFFL);
        }
        for (; i < count; i++) {
          docIDs[i] = in.readInt();
        }
      }
    }

    class Bit32Encoder implements DocIdEncoder {

      @Override
      public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException {
        for (int i = 0; i < count; i++) {
          out.writeInt(docIds[i]);
        }
      }

      @Override
      public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException {
        for (int i = 0; i < count; i++) {
          docIds[i] = in.readInt();
        }
      }
    }
  }

  interface DocIdProvider {
    /**
     * We want to load all the docId sequences completely in memory to avoid including the time
     * spent in fetching from disk. <br>
     *
     * @return: All the docId sequences or empty list.
     */
    List<int[]> getDocIds(Object... args);
  }

  static class DocIdsFromLocalFS implements DocIdProvider {

    @Override
    public List<int[]> getDocIds(Object... args) {
      List<int[]> docIds = new ArrayList<>();
      InputStream fileContents = (InputStream) args[0];
      try (Scanner fileReader = new Scanner(fileContents, Charset.defaultCharset())) {
        while (fileReader.hasNextLine()) {
          String sequence = fileReader.nextLine().trim();
          if (!sequence.startsWith("#") && !sequence.isEmpty()) {
            docIds.add(
                Arrays.stream(sequence.split(","))
                    .map(String::trim)
                    .mapToInt(Integer::parseInt)
                    .toArray());
          }
        }
      }
      return docIds;
    }
  }

  private static void parseInput() {

    String inputScaleFactor = System.getProperty("docIdEncoding.inputScaleFactor");

    if (inputScaleFactor != null && !inputScaleFactor.isEmpty()) {
      INPUT_SCALE_FACTOR = Integer.parseInt(inputScaleFactor);
    } else {
      INPUT_SCALE_FACTOR = 2_00_000;
    }

    String docProviderFQDN = System.getProperty("docIdEncoding.docIdProviderFQDN");

    DocIdProvider docIdProvider = new DocIdsFromLocalFS();

    if (docProviderFQDN != null && !docProviderFQDN.isEmpty()) {
      try {
        docIdProvider =
            (DocIdProvider) Class.forName(docProviderFQDN).getConstructor().newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException
          | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    if (docIdProvider instanceof DocIdsFromLocalFS) {
      String inputFilePath = System.getProperty("docIdEncoding.inputFile");
      try {

        if (inputFilePath != null && !inputFilePath.isEmpty()) {
          DOC_ID_SEQUENCES = docIdProvider.getDocIds(new FileInputStream(inputFilePath));
        } else {
          DOC_ID_SEQUENCES =
              docIdProvider.getDocIds(
                  DocIdEncodingBenchmark.class.getResourceAsStream(
                      "/org.apache.lucene.benchmark.jmh/docIds_bpv21.txt"));
        }
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
