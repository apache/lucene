package org.apache.lucene.benchmark.jmh;

import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
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
import org.apache.lucene.codecs.lucene101.Lucene101Codec;

import org.openjdk.jmh.annotations.*;

import java.nio.file.Files;
import java.nio.file.Path;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Fork(1)
public class AnytimeRankingBenchmark {

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher baselineSearcher;
  private AnytimeRankingSearcher anytimeSearcher;
  private TermQuery query;

  @Param({"1000", "10000"})
  private int docCount;

  @Param({"true", "false"})
  private boolean binningEnabled;

  @Setup
  public void setup() throws Exception {
    Path tempDir = Files.createTempDirectory("anytime-benchmark");
    directory = new MMapDirectory(tempDir);
    IndexWriterConfig config = new IndexWriterConfig(new SingleTokenAnalyzer());
    config.setCodec(new Lucene101Codec());
    config.setUseCompoundFile(false);

    FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
    fieldType.setTokenized(true);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    if (binningEnabled) {
      fieldType.putAttribute("postingsFormat", "Lucene101");
      fieldType.putAttribute("doBinning", "true");
      fieldType.putAttribute("bin.count", "4");
    }
    fieldType.freeze();

    try (IndexWriter writer = new IndexWriter(directory, config)) {
      for (int i = 0; i < docCount; i++) {
        Document doc = new Document();
        String content = (i % 5 == 0) ? "lucene search fast scoring" : "noise filler";
        doc.add(new Field("field", content, fieldType));
        writer.addDocument(doc);
      }
      writer.commit();
      writer.forceMerge(1);
    }

    reader = DirectoryReader.open(directory);
    baselineSearcher = new IndexSearcher(reader);
    baselineSearcher.setSimilarity(new BM25Similarity());
    anytimeSearcher = new AnytimeRankingSearcher(baselineSearcher, 10, 5, "field");
    query = new TermQuery(new Term("field", "lucene"));
  }

  @Benchmark
  public TopDocs baselineLuceneSearch() throws Exception {
    return baselineSearcher.search(query, 10);
  }

  @Benchmark
  public TopDocs anytimeRankingSearch() throws Exception {
    return anytimeSearcher.search(query);
  }

  @TearDown
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
  }

  /**
   * Minimal analyzer that emits a single token — avoids dependency on any Lucene analyzer modules.
   */
  private static final class SingleTokenAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new Tokenizer() {
        private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
        private boolean done = false;

        @Override
        public boolean incrementToken() {
          if (done) return false;
          clearAttributes();
          termAttr.append("lucene"); // emit single fixed token
          done = true;
          return true;
        }

        @Override
        public void reset() throws IOException {
          super.reset();
          done = false;
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