package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AnytimeRankingSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class AnytimeSLARelevanceBenchmark {

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher baselineSearcher;
  private AnytimeRankingSearcher anytimeSearcher;
  private TermQuery query;

  @Param({"1000", "10000"})
  private int docCount;

  @Param({"1", "5", "10"})
  private int slaScoreBudget;

  @Setup
  public void setup() throws Exception {
    Path tempDir = Files.createTempDirectory("sla-relevance-benchmark");
    directory = new MMapDirectory(tempDir);
    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setCodec(new Lucene101Codec());
    config.setUseCompoundFile(false);

    FieldType fieldType = new FieldType();
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    fieldType.setTokenized(true);
    fieldType.setStored(false);
    fieldType.putAttribute("postingsFormat", "Lucene101");
    fieldType.putAttribute("doBinning", "true");
    fieldType.putAttribute("bin.count", "4");
    fieldType.freeze();

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        // Every 200th document is a high-quality relevant one
        String content = (i % 200 == 0) ? "lucene high quality content" : "filler junk text";
        doc.add(new Field("field", content, fieldType));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(directory);
    baselineSearcher = new IndexSearcher(reader);
    baselineSearcher.setSimilarity(new BM25Similarity());

    anytimeSearcher = new AnytimeRankingSearcher(baselineSearcher, 10, slaScoreBudget, "field");
    query = new TermQuery(new Term("field", "lucene"));
  }

  @Benchmark
  public int baselineRecallUnderSLA() throws IOException {
    TopDocs topDocs = baselineSearcher.search(query, 10);
    return countRelevant(topDocs);
  }

  @Benchmark
  public int anytimeRecallUnderSLA() throws IOException {
    TopDocs topDocs = anytimeSearcher.search(query);
    return countRelevant(topDocs);
  }

  private int countRelevant(TopDocs topDocs) {
    int relevant = 0;
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      if (topDocs.scoreDocs[i].doc % 200 == 0) {
        relevant++;
      }
    }
    return relevant;
  }

  @TearDown
  public void tearDown() throws IOException {
    reader.close();
    directory.close();
  }

  /** Analyzer that emits a single fixed token to eliminate dependency on analyzer modules. */
  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer =
          new Tokenizer() {
            private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
            private boolean emitted = false;

            @Override
            public boolean incrementToken() {
              if (emitted) return false;
              clearAttributes();
              termAttr.append("lucene");
              emitted = true;
              return true;
            }

            @Override
            public void reset() throws IOException {
              super.reset();
              emitted = false;
            }
          };
      return new TokenStreamComponents(tokenizer);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      return reader;
    }
  }
}
