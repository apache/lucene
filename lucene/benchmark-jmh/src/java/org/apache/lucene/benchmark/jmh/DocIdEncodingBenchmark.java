package org.apache.lucene.benchmark.jmh;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 10, time = 8)
@Fork(value = 1)
public class DocIdEncodingBenchmark {

    private static final int[] DOC_IDS = new int[]{270868, 1354402, 1001357, 1188141, 614345, 823249, 955812, 524727, 33848, 920354, 912964, 27329, 659459, 938249, 1094207, 1119475, 335026, 828262, 1137164, 1200383, 535709, 917083, 272762, 98216, 865827, 1320140, 86065, 285026, 1300025, 867222, 115295, 543105, 729239, 1074088, 1190650, 1206580, 1241860, 1343448, 1007165, 796400, 166067, 824724, 1190755, 40370, 47587, 69165, 71539, 75497, 97056, 98534, 111989, 125772, 127017, 130221, 130867, 146356, 169507, 202284, 204382, 210889, 214257, 222024, 240243, 245495, 263500, 298028, 338265, 341071, 351839, 377703, 398449, 401454, 422750, 437016, 440081, 449048, 457054, 480446, 482000, 506291, 515137, 525216, 529295, 532078, 548882, 554806, 561107, 575685, 575940, 583209, 590657, 595610, 602444, 613369, 615987, 618498, 618549, 625679, 644023, 648630, 650515, 660371, 665393, 674824, 717089, 718255, 730123, 753681, 762335, 767541, 772169, 830406, 832355, 838246, 843573, 844978, 848910, 850949, 868434, 889963, 918089, 971559, 979233, 1019604, 1038100, 1052977, 1056285, 1078119, 1095552, 1101935, 1109608, 1187018, 1207479, 1218674, 1231614, 1244877, 1254300, 1269298, 1271640, 1284238, 1317059, 1318537, 1324592, 1336627, 1337717, 1342370, 1349469, 53995, 162496, 278702, 794069, 1004390, 1069339, 26927, 102928, 141672, 144991, 203390, 588398, 892482, 71823, 1208477, 1111385, 1154044, 1196631, 581099, 728969, 984399, 1183707, 885502, 410436, 481976, 523715, 543117, 743257, 1087872, 1243653, 1268960, 1061500, 545236, 548262, 692764, 1104894, 1316940, 424397, 981354, 996772, 381666, 548830, 894825, 397856, 15073, 49877, 99132, 237910, 305680, 394957, 406129, 409820, 410402, 554710, 589680, 619735, 717215, 835463, 842990, 852837, 881664, 932203, 1097771, 1099550, 1108559, 1108573, 1238791, 1241513, 1254184, 1305826, 1320107, 447526, 1240738, 218875, 1302762, 589024, 66720, 705418, 30243, 68439, 257453, 543023, 545768, 794973, 943660, 1093665, 1137144, 1137155, 161039, 375532, 375614, 631638, 378676, 968307, 1279411, 200732, 1302423, 78863, 459829, 1085773, 172650, 261116, 18323, 126884, 249690, 296774, 495032, 548631, 672344, 933950, 1006265, 1158863, 121485, 1037749, 114168, 505759, 79827, 656742, 699990, 804813, 964578, 991414, 1005102, 1058218, 90676, 141485, 286093, 413223, 423879, 426709, 474171, 532623, 567289, 606457, 668266, 703627, 708341, 716115, 826851, 992480, 994509, 1085724, 1178728, 1341924, 1351371, 53549, 61888, 92199, 273868, 375330, 447062, 459696, 483358, 564821, 577424, 638524, 683678, 703841, 704870, 841234, 895065, 909173, 969243, 997606, 1228439, 1236367, 1249241, 1287369, 1300986, 2731, 56796, 83624, 195949, 210371, 221112, 244612, 398038, 424234, 444566, 445443, 510300, 513326, 522469, 627609, 700787, 703297, 706838, 867636, 874946, 895111, 935145, 1005915, 1031599, 1035988, 1092180, 1129961, 1138729, 1163447, 1313435, 1348623, 677297, 3683, 21930, 31361, 58012, 80413, 90236, 93334, 95938, 108650, 110480, 153740, 160754, 161530, 180089, 201552, 204127, 218347, 223282, 233582, 274509, 305715, 330779, 345811, 363922, 380514, 408389, 413092, 439515, 464255, 482446, 508774, 535377, 538848, 585885, 589766, 611894, 623557, 632046, 664148, 709237, 726627, 790630, 826400, 827832, 873629, 927290, 977594, 1024146, 1028941, 1110613, 1113501, 1116453, 1117223, 1129591, 1139102, 1151598, 1179220, 1225066, 1239028, 1241338, 1258446, 1308188, 1338899, 1347143, 1062678, 13682, 20848, 32490, 42603, 54220, 54839, 70582, 79870, 101916, 107072, 145421, 160645, 181788, 205826, 210489, 223441, 235372, 244013, 245507, 259167, 262314, 264137, 269681, 302847, 304093, 305896, 314909, 317681, 334100, 334137, 341225, 344088, 361014, 373084, 379755, 381971, 420012, 447338, 468889, 470850, 478342, 482330, 503664, 540547, 548290, 551615, 553397, 581630, 597393, 597448, 612979, 618450, 621708, 680435, 681975, 699087, 755501, 770569, 791096, 794610, 887149, 891532, 892642, 896578, 899463, 905894, 914556, 939056, 941892, 1005237, 1005508, 1019257, 1045047, 1061345, 1087990, 1091402, 1113898, 1116195, 1131149, 1150913, 1164752, 1175740, 1187408, 1194255, 1208301, 1208961, 1210179, 1221615, 1227887, 1269492, 1289182, 1320129, 1328172, 1336702, 1340338, 1347051, 1351492, 10068, 17246};

    @Param({"Bit24Encoder", "Bit21With2StepsAddEncoder"})
    String encoderName;

    private static final int INPUT_SCALE_FACTOR = 5_00_000;

    private DocIdEncoder docIdEncoder;

    private static final Path TMP_DIR;

    static {
        try {
            TMP_DIR = Files.createTempDirectory("docIdJmh");
            System.out.println(TMP_DIR.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final int[] scratch = new int[512];

    @Setup(Level.Iteration)
    public void init() throws IOException {
        docIdEncoder = DocIdEncoder.Factory.fromName(encoderName);
    }

    @Benchmark
    public void performEncodeDecode(Blackhole blackhole) throws IOException {
        try (Directory dir = new NIOFSDirectory(TMP_DIR)) {
            String innerDirName = String.join("_", "docIdJmhData_", docIdEncoder.getClass().getSimpleName(), String.valueOf(System.nanoTime()));
            try (IndexOutput out = dir.createOutput(innerDirName, IOContext.DEFAULT)) {
                for (int i = 1; i <= INPUT_SCALE_FACTOR; i++) {
                    docIdEncoder.encode(out, 0, DOC_IDS.length, DOC_IDS);
                }
            }
            try (IndexInput in = dir.openInput(innerDirName, IOContext.DEFAULT)) {
                for (int i = 1; i <= INPUT_SCALE_FACTOR; i++) {
                    docIdEncoder.decode(in, 0, DOC_IDS.length, scratch);
                }
            }
        } finally {
            Files.walkFileTree(TMP_DIR, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    public interface DocIdEncoder {
        public void encode(IndexOutput out, int start, int count, int[] docIds) throws IOException;

        public void decode(IndexInput in, int start, int count, int[] docIds) throws IOException;

        static class Factory {

            public static DocIdEncoder fromName(String encoderName) {
                String parsedEncoderName = encoderName.trim();
                if (parsedEncoderName.equalsIgnoreCase(Bit24Encoder.class.getSimpleName())) {
                    return new Bit24Encoder();
                } else if (parsedEncoderName.equalsIgnoreCase(Bit21With2StepsAddEncoder.class.getSimpleName())) {
                    return new Bit21With2StepsAddEncoder();
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
                long packedLong = ((docIds[i] & 0x001FFFFFL) << 42) |
                        ((docIds[i + 1] & 0x001FFFFFL) << 21) |
                        (docIds[i + 2] & 0x001FFFFFL);
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


}
