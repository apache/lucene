package org.apache.lucene.util.packed;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class PackedMacroTest {
    public static void main(String[] args) throws Exception {
        String log = "/tmp/merge.log";
        Directory dir = FSDirectory.open(Paths.get("/tmp/temp-index-dir"));
        IndexWriterConfig config = new IndexWriterConfig()
                //.setInfoStream(new PrintStream(System.out))
                .setInfoStream(new PrintStream(Files.newOutputStream(Paths.get(log))))
                .setMergeScheduler(new SerialMergeScheduler())
                .setMergePolicy(new LogDocMergePolicy())
                .setMaxBufferedDocs(65536);
        IndexWriter w = new IndexWriter(dir, config);
        Document doc = new Document();
        SortedDocValuesField field = new SortedDocValuesField("foo", new BytesRef());
        doc.add(field);
        for (int i = 0; i < 100_000_000; ++i) {
            field.setBytesValue(new BytesRef(Integer.toString(i)));
            w.addDocument(doc);
        }
    }
}
