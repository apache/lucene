package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDocIdEncoding extends LuceneTestCase {

  private static final Map<Class<? extends DocIdEncodingBenchmark.DocIdEncoder>, Integer>
      ENCODER_TO_BPV_MAPPING =
          Map.of(
              DocIdEncodingBenchmark.DocIdEncoder.Bit21With2StepsEncoder.class, 21,
              DocIdEncodingBenchmark.DocIdEncoder.Bit21With3StepsEncoder.class, 21,
              DocIdEncodingBenchmark.DocIdEncoder.Bit21HybridEncoder.class, 21,
              DocIdEncodingBenchmark.DocIdEncoder.Bit24Encoder.class, 24,
              DocIdEncodingBenchmark.DocIdEncoder.Bit32Encoder.class, 32);

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  static class FixedBPVRandomDocIdProvider implements DocIdEncodingBenchmark.DocIdProvider {

    @Override
    public List<int[]> getDocIds(Object... args) {
      DocIdEncodingBenchmark.DocIdEncoder encoder = (DocIdEncodingBenchmark.DocIdEncoder) args[0];
      int capacity = (int) args[1];
      int low = (int) args[2];
      int high = (int) args[3];
      List<int[]> docIdSequences = new ArrayList<>(capacity);

      for (int i = 1; i <= capacity; i++) {
        docIdSequences.add(
            random()
                .ints(0, (int) Math.pow(2, ENCODER_TO_BPV_MAPPING.get(encoder.getClass())) - 1)
                .distinct()
                .limit(random().nextInt(low, high))
                .toArray());
      }
      return docIdSequences;
    }
  }

  public void testBPV21AndAbove() {

    List<DocIdEncodingBenchmark.DocIdEncoder> encoders =
        DocIdEncodingBenchmark.DocIdEncoder.SingletonFactory.getAllExcept(Collections.emptyList());

    final int[] scratch = new int[512];

    DocIdEncodingBenchmark.DocIdProvider docIdProvider = new FixedBPVRandomDocIdProvider();

    try {

      Path tempDir = Files.createTempDirectory("DocIdEncoding_testBPV21AndAbove_");

      for (DocIdEncodingBenchmark.DocIdEncoder encoder : encoders) {

        List<int[]> docIdSequences = docIdProvider.getDocIds(encoder, 50, 100, 512);

        String encoderFileName = "Encoder_" + encoder.getClass().getSimpleName();

        try (Directory outDir = FSDirectory.open(tempDir);
            IndexOutput out = outDir.createOutput(encoderFileName, IOContext.DEFAULT)) {
          for (int[] sequence : docIdSequences) {
            encoder.encode(out, 0, sequence.length, sequence);
          }
        }

        try (Directory inDir = FSDirectory.open(tempDir);
            IndexInput in = inDir.openInput(encoderFileName, IOContext.DEFAULT)) {
          for (int[] sequence : docIdSequences) {
            encoder.decode(in, 0, sequence.length, scratch);
            assertArrayEquals(sequence, Arrays.copyOf(scratch, sequence.length));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
