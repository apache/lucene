package org.apache.lucene.document;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Monster;
import org.apache.lucene.tests.util.TestUtil;

@TimeoutSuite(millis = 86_400_000) // 24 hour timeout
@Monster("takes ~4 hours and needs extra heap, disk space, file handles")
public class TestManyKnnDocs extends LuceneTestCase {
  // gradlew -p lucene/core test --tests TestManyKnnDocs -Ptests.heapsize=16g -Dtests.monster=true

  public void testGraphOffsetOverflowProtection() throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(TestUtil.getDefaultCodec());
    iwc.setRAMBufferSizeMB(256);
    TieredMergePolicy mp = new TieredMergePolicy();
    mp.setMaxMergeAtOnce(512);
    mp.setSegmentsPerTier(512);
    iwc.setMergePolicy(mp);
    String fieldName = "field";
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;

    try (Directory dir = FSDirectory.open(createTempDir("ManyKnnVectorDocs"));
        IndexWriter iw = new IndexWriter(dir, iwc)) {

      int numVectors = 16268816;
      float[] vector = new float[2];
      Document doc = new Document();
      doc.add(new KnnVectorField(fieldName, vector, similarityFunction));
      for (int i = 0; i < numVectors; i++) {
        vector[0] = (i % 256);
        vector[1] = (i + 1 % 256);
        iw.addDocument(doc);
      }

      // merge to single segment and then verify
      iw.forceMerge(1);
      iw.commit();
      IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));
      TopDocs docs = searcher.search(new KnnVectorQuery("field", new float[] {120, 123}, 10), 5);
      assertEquals(5, docs.scoreDocs.length);
    }
  }
}
