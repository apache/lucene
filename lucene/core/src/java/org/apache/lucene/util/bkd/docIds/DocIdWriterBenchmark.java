package org.apache.lucene.util.bkd.docIds;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class DocIdWriterBenchmark {

    private static final int[] scratch = new int[525];

    static class BenchmarkInput {

        private static final String USAGE = "\n USAGE " +
                "\n <1> Input Data File Path " +
                "\n<2> Output Directory Path " +
                "\n<3> Number of Iterations " +
                "\n<4> Encoder name \n" +
                "\n<5> Input Scale Factor";

        private Path inputDataFile;

        private Path outputDir;

        private int totalIterations;

        private DocIdEncoder encoder;

        private int inputScaleFactor;

        public BenchmarkInput(String[] args) {
            try {
                inputDataFile = Paths.get(args[0]);
                outputDir = Paths.get(args[1]);
                encoder = DocIdEncoder.Factory.fromName(args[2]);
                totalIterations = Integer.parseInt(args[3]);
                inputScaleFactor = Integer.parseInt(args[4]);
            } catch (RuntimeException e) {
                e.printStackTrace();
                System.out.println(USAGE);
            }
        }

        public Path getInputDataFile() {
            return inputDataFile;
        }

        public Path getOutputDir() {
            return outputDir;
        }

        public int getTotalIterations() {
            return totalIterations;
        }
    }

    public static void main(String[] args) throws IOException {

        BenchmarkInput benchmarkInput = new BenchmarkInput(args);
        System.out.println(Arrays.toString(args));

        List<int[]> allDocIds = readAllDocIds(benchmarkInput.getInputDataFile());

//        allDocIds = allDocIds.stream().filter(x -> x.length == 512).map(x -> {
//            int[] newArr = new int[x.length + 12]; // Did this for one pass reading without 2 for loops to avoid ArrayIndexOOB
//            System.arraycopy(x, 0, newArr, 0, x.length);
//            return newArr;
//        }).collect(Collectors.toList());

        System.out.println("Found " + allDocIds.size() + " docId sequences.");

        List<int[]> finalDocIds = Collections.nCopies(benchmarkInput.inputScaleFactor, allDocIds).stream().flatMap(List::stream).collect(Collectors.toList());

        System.out.printf("\nFinal size of docIds sequences is %s \n", finalDocIds.size());
//
//        System.out.println(allDocIds.stream().filter(x -> x.length < 510).count());
//        System.out.println(allDocIds.stream().filter(x -> x.length == 510).count());

        runBenchmark(finalDocIds, true, benchmarkInput);
        System.out.println("Warmup complete!!");

        runBenchmark(finalDocIds, false, benchmarkInput);
    }

    private static void runBenchmark(List<int[]> allDocIds, boolean isWarmup, BenchmarkInput benchmarkInput) throws IOException {

        try (Directory dir = FSDirectory.open(benchmarkInput.getOutputDir())) {

            int totalIterations = isWarmup ? 1 : benchmarkInput.getTotalIterations();
            //int arrayLen = 512; // Hardcoded as we are filtering for docId sequences of 512

            DocIdEncoder docIdEncoder = benchmarkInput.encoder;

            List<Integer> docIdLengths = allDocIds.stream().map(x -> x.length).toList();

            long totalEncodeTime = 0L, totalDecodeTime = 0L;

            for (int i = 1; i <= totalIterations; i++) {

                if (!isWarmup) {
                    System.out.printf("\nRunning iteration %s/%s\n", i, benchmarkInput.getTotalIterations());
                }

                long encodeTime = 0L, decodeTime = 0L;

                if (!isWarmup) {
                    System.out.printf("\nRunning benchmark for DocIdEncoder %s\n", docIdEncoder.getClass().getSimpleName());
                }

                // Writing all DocId Arrays in the file using the encoder
                long encodeStart = System.nanoTime();
                IndexOutput out = dir.createOutput("tmp", IOContext.DEFAULT);
                for (int[] docIds : allDocIds) {
                    docIdEncoder.encode(out, 0, docIds.length, docIds);
                }
                out.close();
                encodeTime += (System.nanoTime() - encodeStart);
                totalEncodeTime += encodeTime;

                // Reading all the written DocId Arrays in the file using the decoder
                long decodeStart = System.nanoTime();
                IndexInput in = dir.openInput("tmp", IOContext.DEFAULT);
                for (int docIdLength : docIdLengths) {
                    docIdEncoder.decode(in, 0, docIdLength, scratch);
                }
                in.close();
                decodeTime += (System.nanoTime() - decodeStart);
                totalDecodeTime += decodeTime;

                // Kind of a black hole to ensure compiler doesn't optimise away decoding.
                if (Arrays.stream(scratch).filter(x -> x == Integer.MAX_VALUE).count() > 0) {
                    // Should not be reachable
                    System.out.printf("\nSomething went Wrong\nScratch is %s for Encoder %s \n", Arrays.toString(scratch), docIdEncoder.getClass().getSimpleName());
                }

                dir.deleteFile("tmp");

                if (!isWarmup) {
                    System.out.printf("\nFor Encoder %s Total Encode time is %s ms and Total Decode time is %s ms\n",
                            docIdEncoder.getClass().getSimpleName(), encodeTime / 1_000_000, decodeTime / 1_000_000);
                }

            }

            if (!isWarmup) {
                System.out.printf("\nBenchmark completed for Encoder %s with average encode time as %s ms and average decode time as %s ms\n",
                        docIdEncoder.getClass().getSimpleName(), (totalEncodeTime / totalIterations) / 1_000_000, (totalDecodeTime / totalIterations) / 1_000_000);
            }

        }

    }

    private static List<int[]> readAllDocIds(Path inputFile) {
        List<int[]> allDocIds = new ArrayList<>();
        try (Scanner reader = new Scanner(inputFile)) {
            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                try {
                    allDocIds.add(Arrays.stream(line.split(",")).mapToInt(x -> Integer.parseInt(x.trim())).toArray());
                } catch (NumberFormatException nfe) {
                    System.out.println("Number Format Exception for line " + line);
                    throw nfe;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return allDocIds;
    }


}
