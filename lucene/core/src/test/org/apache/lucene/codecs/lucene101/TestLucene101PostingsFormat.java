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
package org.apache.lucene.codecs.lucene101;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.BinMapReader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsReader.MutableImpactList;
import org.apache.lucene.codecs.lucene90.blocktree.FieldReader;
import org.apache.lucene.codecs.lucene90.blocktree.Stats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.BinScoreUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

public class TestLucene101PostingsFormat extends BasePostingsFormatTestCase {

    public static SegmentReadState fakeSegmentReadState(Directory dir, String segmentName, int maxDoc)
            throws IOException {
        SegmentInfo info =
                new SegmentInfo(
                        dir,
                        Version.LATEST,
                        Version.LATEST,
                        segmentName,
                        maxDoc,
                        false,
                        false,
                        Codec.getDefault(),
                        Collections.emptyMap(),
                        StringHelper.randomId(),
                        Collections.emptyMap(),
                        null);

        return new SegmentReadState(dir, info, null, IOContext.READONCE);
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysPostingsFormat(new Lucene101PostingsFormat());
    }

    public void testVInt15() throws IOException {
        byte[] bytes = new byte[5];
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        ByteArrayDataInput in = new ByteArrayDataInput();
        for (int i : new int[]{0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE}) {
            out.reset(bytes);
            Lucene101PostingsWriter.writeVInt15(out, i);
            in.reset(bytes, 0, out.getPosition());
            assertEquals(i, Lucene101PostingsReader.readVInt15(in));
            assertEquals(out.getPosition(), in.getPosition());
        }
    }

    public void testVLong15() throws IOException {
        byte[] bytes = new byte[9];
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        ByteArrayDataInput in = new ByteArrayDataInput();
        for (long i : new long[]{0, 1, 127, 128, 32767, 32768, Integer.MAX_VALUE, Long.MAX_VALUE}) {
            out.reset(bytes);
            Lucene101PostingsWriter.writeVLong15(out, i);
            in.reset(bytes, 0, out.getPosition());
            assertEquals(i, Lucene101PostingsReader.readVLong15(in));
            assertEquals(out.getPosition(), in.getPosition());
        }
    }

    /**
     * Make sure the final sub-block(s) are not skipped.
     */
    public void testFinalBlock() throws Exception {
        Directory d = newDirectory();
        IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
        for (int i = 0; i < 25; i++) {
            Document doc = new Document();
            doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
            doc.add(newStringField("field", "z" + Character.toString((char) (97 + i)), Field.Store.NO));
            w.addDocument(doc);
        }
        w.forceMerge(1);

        DirectoryReader r = DirectoryReader.open(w);
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        // We should see exactly two blocks: one root block (prefix empty string) and one block for z*
        // terms (prefix z):
        Stats stats = field.getStats();
        assertEquals(0, stats.floorBlockCount);
        assertEquals(2, stats.nonFloorBlockCount);
        r.close();
        w.close();
        d.close();
    }

    public void testImpactSerialization() throws IOException {
        // omit norms and omit freqs
        doTestImpactSerialization(Collections.singletonList(new Impact(1, 1L)));

        // omit freqs
        doTestImpactSerialization(Collections.singletonList(new Impact(1, 42L)));
        // omit freqs with very large norms
        doTestImpactSerialization(Collections.singletonList(new Impact(1, -100L)));

        // omit norms
        doTestImpactSerialization(Collections.singletonList(new Impact(30, 1L)));
        // omit norms with large freq
        doTestImpactSerialization(Collections.singletonList(new Impact(500, 1L)));

        // freqs and norms, basic
        doTestImpactSerialization(
                Arrays.asList(
                        new Impact(1, 7L),
                        new Impact(3, 9L),
                        new Impact(7, 10L),
                        new Impact(15, 11L),
                        new Impact(20, 13L),
                        new Impact(28, 14L)));

        // freqs and norms, high values
        doTestImpactSerialization(
                Arrays.asList(
                        new Impact(2, 2L),
                        new Impact(10, 10L),
                        new Impact(12, 50L),
                        new Impact(50, -100L),
                        new Impact(1000, -80L),
                        new Impact(1005, -3L)));
    }

    private void doTestImpactSerialization(List<Impact> impacts) throws IOException {
        CompetitiveImpactAccumulator acc = new CompetitiveImpactAccumulator();
        for (Impact impact : impacts) {
            acc.add(impact.freq, impact.norm);
        }
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
                Lucene101PostingsWriter.writeImpacts(acc.getCompetitiveFreqNormPairs(), out);
            }
            try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
                byte[] b = new byte[Math.toIntExact(in.length())];
                in.readBytes(b, 0, b.length);
                List<Impact> impacts2 =
                        Lucene101PostingsReader.readImpacts(
                                new ByteArrayDataInput(b),
                                new MutableImpactList(impacts.size() + random().nextInt(3)));
                assertEquals(impacts, impacts2);
            }
        }
    }

    public void testBinMapIsWrittenOnFlush() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setCodec(getCodec());
            iwc.setUseCompoundFile(false);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 3; i++) {
                    Document doc = new Document();
                    FieldType ft = new FieldType(TextField.TYPE_STORED);
                    ft.setStoreTermVectors(true);
                    ft.putAttribute("doBinning", "true");
                    Field field = new Field("field", "value" + i, ft);
                    doc.add(field);
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            SegmentCommitInfo info = infos.info(0);
            String segmentName = info.info.name;
            String segmentSuffix = null;
            String binmapFile = null;

            for (String file : dir.listAll()) {
                if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
                    binmapFile = file;
                    segmentSuffix =
                            file.substring(segmentName.length() + 1, file.length() - ".binmap".length());
                    break;
                }
            }

            assertNotNull("binmap file should exist", binmapFile);

            SegmentReadState readState =
                    new SegmentReadState(dir, info.info, null, newIOContext(random()), segmentSuffix);

            try (BinMapReader binMap = new BinMapReader(dir, readState)) {
                assertEquals(3, binMap.getBinArrayCopy().length);
                for (int i = 0; i < 3; i++) {
                    int bin = binMap.getBin(i);
                    assertTrue("Bin should be non-negative", bin >= 0);
                }
            }
        }
    }

    public void testBinMapWithCompoundFileFormat() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setCodec(getCodec());
            iwc.setUseCompoundFile(true);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 4; i++) {
                    Document doc = new Document();
                    FieldType ft = new FieldType(TextField.TYPE_STORED);
                    ft.setStoreTermVectors(true);
                    ft.putAttribute("doBinning", "true");
                    Field field = new Field("field", "term" + i, ft);
                    doc.add(field);
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            SegmentCommitInfo info = infos.info(0);
            String segmentName = info.info.name;

            try (Directory cfsDir =
                         info.info.getCodec().compoundFormat().getCompoundReader(dir, info.info)) {
                String binmapFile = null;
                String segmentSuffix = null;
                for (String file : cfsDir.listAll()) {
                    if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
                        binmapFile = file;
                        segmentSuffix =
                                file.substring(segmentName.length() + 1, file.length() - ".binmap".length());
                        break;
                    }
                }

                assertNotNull("compound file should contain binmap", binmapFile);

                SegmentReadState readState =
                        new SegmentReadState(cfsDir, info.info, null, newIOContext(random()), segmentSuffix);

                try (BinMapReader binMap = new BinMapReader(cfsDir, readState)) {
                    assertEquals(4, binMap.getBinArrayCopy().length);
                    for (int i = 0; i < 4; i++) {
                        int bin = binMap.getBin(i);
                        assertTrue("Bin should be non-negative", bin >= 0);
                    }
                }
            }
        }
    }

    public void testBinMapBinningDisabled() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setCodec(getCodec());
            iwc.setUseCompoundFile(false); // standalone files

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                doc.add(newTextField("field", "term value relevance", Field.Store.NO)); // no binning
                writer.addDocument(doc);
                writer.commit();
            }

            for (String file : dir.listAll()) {
                assertFalse(
                        "binmap should not exist when binning is not enabled", file.endsWith(".binmap"));
            }
        }
    }

    public void testExplicitBinCountIsHonored() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setCodec(getCodec());
            iwc.setUseCompoundFile(false);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 12; i++) {
                    Document doc = new Document();
                    FieldType ft = new FieldType(TextField.TYPE_STORED);
                    ft.setStoreTermVectors(true);
                    ft.putAttribute("doBinning", "true");
                    ft.putAttribute("bin.count", "4");
                    Field field = new Field("field", "lucene term doc", ft);
                    doc.add(field);
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
            SegmentCommitInfo info = infos.info(0);

            String segmentName = info.info.name;
            String segmentSuffix = null;
            String binmapFile = null;

            for (String file : dir.listAll()) {
                if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
                    binmapFile = file;
                    segmentSuffix =
                            file.substring(segmentName.length() + 1, file.length() - ".binmap".length());
                    break;
                }
            }

            assertNotNull("binmap file should exist", binmapFile);

            SegmentReadState readState =
                    new SegmentReadState(dir, info.info, null, newIOContext(random()), segmentSuffix);

            try (BinMapReader binMap = new BinMapReader(dir, readState)) {
                assertEquals("Expected 4 bins", 4, binMap.getBinCount());
                int[] bins = binMap.getBinArrayCopy();
                assertEquals(12, bins.length);
                for (int bin : bins) {
                    assertTrue("bin id should be in [0, 3]", bin >= 0 && bin < 4);
                }
            }
        }
    }

    public void testBinMapIsPreservedAndUsableAfterMerge() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setCodec(getCodec());
            iwc.setUseCompoundFile(random().nextBoolean());
            iwc.setMaxBufferedDocs(2);

            FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
            ft.setStoreTermVectors(false);
            ft.putAttribute("postingsFormat", "Lucene101");
            ft.putAttribute("doBinning", "true");
            ft.putAttribute("bin.count", "4");
            ft.freeze();

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 24; i++) {
                    Document doc = new Document();
                    doc.add(new Field("field", "lucene merge binning test", ft));
                    writer.addDocument(doc);
                }
                writer.commit();
                writer.forceMerge(1);
            }

            // Confirm single merged segment exists
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("Expected single merged segment", 1, reader.leaves().size());

                LeafReader leaf = reader.leaves().get(0).reader();
                SegmentReader segmentReader = (SegmentReader) FilterLeafReader.unwrap(leaf);
                SegmentCommitInfo info = segmentReader.getSegmentInfo();
                String segmentName = info.info.name;

                // Extract actual binmap file from Directory
                String binmapFile = null;
                for (String file : dir.listAll()) {
                    if (file.startsWith(segmentName + "_") && file.endsWith(".binmap")) {
                        binmapFile = file;
                        break;
                    }
                }

                assertNotNull("Expected .binmap file after merge", binmapFile);

                String suffix = binmapFile.substring(
                        segmentName.length() + 1,
                        binmapFile.length() - ".binmap".length());

                SegmentReadState readState = new SegmentReadState(
                        dir,
                        info.info,
                        segmentReader.getFieldInfos(),
                        newIOContext(random()),
                        suffix);

                try (BinMapReader binMap = new BinMapReader(dir, readState)) {
                    assertEquals("Expected 4 bins", 4, binMap.getBinCount());
                    int[] bins = binMap.getBinArrayCopy();
                    assertEquals("Unexpected number of docs", 24, bins.length);
                    for (int bin : bins) {
                        assertTrue("Invalid bin ID", bin >= 0 && bin < 4);
                    }
                }
            }
        }
    }
}
