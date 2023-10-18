// nocommit temporary tool

import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

/*
2
4
8
16
32
64
128
256
512
1024
2048
4096
8192
16384
32768
65536
131072
262144
524288
1048576
2097152
4194304
8388608
16777216
33554432
67108864
134217728
268435456
536870912
1073741824
2147483648
4294967296
*/  

// javac -cp lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST.java; java -cp .:lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar IndexToFST /l/indices/wikimediumall.trunk.facets.taxonomy:Date.taxonomy:Month.taxonomy:DayOfYear.taxonomy:RandomLabel.taxonomy.sortedset:Date.sortedset:Month.sortedset:DayOfYear.sortedset:RandomLabel.sortedset.Lucene90.Lucene90.dvfields.nd33.3326M/index

public class IndexToFST {

  private static double nsToSec(long ns) {
    return ns / (double) TimeUnit.SECONDS.toNanos(1);
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: IndexToFST <path-to-index-dir> <size-of-suffix-hash>");
      System.exit(1);
    }

    double ramLimitMB;
    
    if (args[1].equals("inf")) {
      ramLimitMB = Double.POSITIVE_INFINITY;
    } else {
     ramLimitMB = Double.parseDouble(args[1]);
    }
    
    try (Directory dir = FSDirectory.open(Paths.get(args[0]));
         IndexReader r = DirectoryReader.open(dir)) {

      IntsRefBuilder scratch = new IntsRefBuilder();
      Terms terms = MultiTerms.getTerms(r, "body");
      TermsEnum termsEnum = terms.iterator();

      Outputs<Long> outputs = PositiveIntOutputs.getSingleton();
      FSTCompiler.Builder<Long> builder = new FSTCompiler.Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
      builder.suffixRAMLimitMB(ramLimitMB);

      FSTCompiler<Long> fstBuilder = builder.build();

      long t0NS = System.nanoTime();
      while (true) {
        BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        fstBuilder.add(Util.toIntsRef(term, scratch), termsEnum.totalTermFreq());
      }

      FST<Long> fst = fstBuilder.compile();
      long t1NS = System.nanoTime();
      fst.save(Paths.get("fst.bin"));
      System.out.println("saved FST to \"fst.bin\": " + fst.ramBytesUsed() + " bytes; " + String.format(Locale.ROOT, "%.3f sec", nsToSec(t1NS - t0NS)));

      if (false) {
        // again, verifying iteration
        termsEnum = terms.iterator();

        BytesRefFSTEnum<Long> fstEnum = new BytesRefFSTEnum(fst);
        System.out.println("now test identical iteration...");
        long termCount = 0;
        while (true) {
          BytesRef indexTerm = termsEnum.next();
          BytesRefFSTEnum.InputOutput<Long> fstTermPair = fstEnum.next();

          if (indexTerm == null) {
            if (fstTermPair != null) {
              throw new RuntimeException("term iteration: index has no more terms, yet FST does");
            }
            break;
          } else {
            if (fstTermPair == null) {
              throw new RuntimeException("term iteration: FST has no more terms, yet index does");
            }
            if (indexTerm.equals(fstTermPair.input) == false) {
              throw new RuntimeException("term iteration: index term is " + indexTerm.utf8ToString() + " but FST term is " + fstTermPair.input.utf8ToString());
            }
            if (termsEnum.totalTermFreq() != fstTermPair.output) {
              throw new RuntimeException("term iteration: index term's totalTermFreq is " + termsEnum.totalTermFreq() + " but FST's is " + fstTermPair.output);
            }

            // Also verify direct get:
            Long v = Util.get(fst, indexTerm);
            if (v == null) {
              throw new RuntimeException("Util.get(\"" + indexTerm.utf8ToString() + "\") returned null");
            }
            if (v != termsEnum.totalTermFreq()) {
              throw new RuntimeException("Util.get(\"" + indexTerm.utf8ToString() + "\") returned wong value (" + v + "); expected " + termsEnum.totalTermFreq());
            }

            if (termCount % 100000 == 0) {
              System.out.println(termCount + "...");
            }

            termCount++;
          }
        }
      }
    }
  }
}
