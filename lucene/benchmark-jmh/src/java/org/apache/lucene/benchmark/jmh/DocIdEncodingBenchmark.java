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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
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
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 5, time = 8)
@Fork(value = 1)
public class DocIdEncodingBenchmark {

  /**
   * Taken from multiple leaf blocks of BKD Tree in NYC Taxi dataset which fit under the condition
   * required for BPV_21 as the condition <i>max <= 0x001FFFFF</i> is met with this array.
   */
  private static final List<int[]> docIdSequences = new ArrayList<>();

  static {
    try (Scanner fileReader =
        new Scanner(
            Objects.requireNonNull(
                DocIdEncodingBenchmark.class.getResourceAsStream("/docIds_bpv21.txt")))) {
      while (fileReader.hasNextLine()) {
        String sequence = fileReader.nextLine().trim();
        if (!sequence.startsWith("#") && !sequence.isEmpty()) {
          docIdSequences.add(
              Arrays.stream(sequence.split(",")).mapToInt(Integer::parseInt).toArray());
        }
      }
    }
  }

  @Param({"Bit24Encoder", "Bit21With2StepsAddEncoder", "Bit21With3StepsAddEncoder"})
  String encoderName;

  private static final int INPUT_SCALE_FACTOR = 50_000;

  private DocIdEncoder docIdEncoder;

  private Path tmpDir;

  private final int[] scratch = new int[512];

  @Setup(Level.Trial)
  public void init() throws IOException {
    tmpDir = Files.createTempDirectory("docIdJmh");
    docIdEncoder = DocIdEncoder.Factory.fromName(encoderName);
  }

  @TearDown(Level.Trial)
  public void finish() throws IOException {
    Files.delete(tmpDir);
  }

  @Benchmark
  public void performEncodeDecode() throws IOException {
    String dataFile =
        String.join(
            "_",
            "docIdJmhData_",
            docIdEncoder.getClass().getSimpleName(),
            String.valueOf(System.nanoTime()));
    for (int[] docIdSequence : docIdSequences) {
      try (Directory dir = new NIOFSDirectory(tmpDir)) {
        try (IndexOutput out = dir.createOutput(dataFile, IOContext.DEFAULT)) {
          for (int i = 1; i <= INPUT_SCALE_FACTOR; i++) {
            docIdEncoder.encode(out, 0, docIdSequence.length, docIdSequence);
          }
        }
        try (IndexInput in = dir.openInput(dataFile, IOContext.DEFAULT)) {
          for (int i = 1; i <= INPUT_SCALE_FACTOR; i++) {
            docIdEncoder.decode(in, 0, docIdSequence.length, scratch);
          }
        }
      } finally {
        Files.delete(tmpDir.resolve(dataFile));
      }
    }
  }

  /**
   * Extend this interface to add a new implementation used for DocId Encoding and Decoding. These
   * are taken from org.apache.lucene.util.bkd.DocIdsWriter.
   */
  public interface DocIdEncoder {
    public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException;

    public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException;

    static class Factory {

      public static DocIdEncoder fromName(String encoderName) {
        String parsedEncoderName = encoderName.trim();
        if (parsedEncoderName.equalsIgnoreCase(Bit24Encoder.class.getSimpleName())) {
          return new Bit24Encoder();
        } else if (parsedEncoderName.equalsIgnoreCase(
            Bit21With2StepsAddEncoder.class.getSimpleName())) {
          return new Bit21With2StepsAddEncoder();
        } else if (parsedEncoderName.equalsIgnoreCase(
            Bit21With3StepsAddEncoder.class.getSimpleName())) {
          return new Bit21With3StepsAddEncoder();
        } else {
          throw new IllegalArgumentException("Unknown DocIdEncoder " + encoderName);
        }
      }
    }
  }

  static class Bit24Encoder implements DocIdEncoder {
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
        docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
      }
    }
  }

  static class Bit21With2StepsAddEncoder implements DocIdEncoder {
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
   * Variation of @{@link Bit21With2StepsAddEncoder} but uses 3 loops to decode the array of DocIds.
   */
  static class Bit21With3StepsAddEncoder implements DocIdEncoder {

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
}
