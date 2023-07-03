import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.FileVisitResult;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.sandbox.pim.PimIndexWriter;
import org.apache.lucene.sandbox.pim.PimConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Indexing program based on Lucene's demo example
 * Creates a PIM index
 */
public class IndexRAMDPU {

    public static void main(String[] args) throws Exception {

        String usage =
                "Usage:\tjava IndexRAMDPU.java [-index string] [-dataset dir] [-nbdpus num]\n\n.";
        if (args.length > 0 && ("-h".equals(args[0]) || "-help".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        String index = null;
        String dataset = null;
        int nbDpus = 0;

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                index = args[i + 1];
                i++;
            } else if ("-dataset".equals(args[i])) {
                dataset = args[i + 1];
                i++;
            } else if ("-nbdpus".equals(args[i])) {
                try {
                    nbDpus = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.out.println("Error: wrong number of dpus.");
                    break;
                }
                i++;
            }
        }

        if (index == null || dataset == null || nbDpus == 0) {
            System.out.println(usage);
            System.exit(0);
        }

        createIndex(index, dataset, nbDpus);
    }

    public static void createIndex(String index, String dataset, int nbDpus) throws CorruptIndexException, LockObtainFailedException, IOException {
        Analyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = new MMapDirectory(Paths.get(index));
        Directory pimIndexDirectory = new MMapDirectory(Paths.get(index + "/dpu"));
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        // provide a directory for pim index and a pim config to the PimIndexWriter constructor
        IndexWriter writer = new PimIndexWriter(indexDirectory, pimIndexDirectory, iwc, new PimConfig(nbDpus));
        Path path = Paths.get(dataset);
        final AtomicReference<Integer> fileCount = new AtomicReference<Integer>(0);
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        try (InputStream stream = Files.newInputStream(file)) {
                            // make a new, empty document
                            Document doc = new Document();

                            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                            doc.add(pathField);

                            doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

                            fileCount.set(fileCount.get() + 1);
                            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                                System.out.println("adding " + file);
                                writer.addDocument(doc);
                            } else {
                                System.out.println("#" + fileCount + " updating " + file);
                                writer.updateDocument(new Term("path", file.toString()), doc);
                            }
                        }
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        System.out.println("#" + fileCount + " finished");
        writer.close();
    }
}
