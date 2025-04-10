package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Identifies clusters of near-duplicate documents based on term overlap using Jaccard similarity.
 */
public final class NearDuplicateFinder {

  private final String field;
  private final float threshold;

  /**
   * Create a new finder.
   *
   * @param field the field to analyze
   * @param threshold similarity threshold between 0 and 1
   */
  public NearDuplicateFinder(String field, float threshold) {
    this.field = field;
    this.threshold = threshold;
  }

  /**
   * Computes near-duplicate clusters across all segments using global docIDs.
   *
   * @param reader top-level reader
   * @return list of duplicate clusters
   */
  public List<DuplicateCluster> findNearDuplicates(DirectoryReader reader) throws IOException {
    final int maxDoc = reader.maxDoc();
    final Map<Integer, Set<String>> docToTerms = new HashMap<>();
    final Map<String, Set<Integer>> termToDocs = new HashMap<>();

    int docBase = 0;
    for (LeafReaderContext ctx : reader.leaves()) {
      Terms terms = ctx.reader().terms(field);
      if (terms == null) {
        docBase += ctx.reader().maxDoc();
        continue;
      }

      TermsEnum termsEnum = terms.iterator();
      BytesRef term;
      while ((term = termsEnum.next()) != null) {
        PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
        if (postings == null) {
          continue;
        }

        String termStr = term.utf8ToString();
        while (postings.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
          int globalDocID = docBase + postings.docID();
          docToTerms.computeIfAbsent(globalDocID, k -> new HashSet<>()).add(termStr);
          termToDocs.computeIfAbsent(termStr, k -> new HashSet<>()).add(globalDocID);
        }
      }

      docBase += ctx.reader().maxDoc();
    }

    final BitSet visited = new BitSet(maxDoc);
    final List<DuplicateCluster> clusters = new ArrayList<>();

    for (Map.Entry<Integer, Set<String>> entry : docToTerms.entrySet()) {
      final int docID = entry.getKey();
      if (visited.get(docID)) {
        continue;
      }

      final Set<Integer> cluster = new HashSet<>();
      final Set<String> termsA = entry.getValue();
      cluster.add(docID);
      visited.set(docID);

      // Include all documents with no terms as potential duplicates of this one
      for (Map.Entry<Integer, Set<String>> otherEntry : docToTerms.entrySet()) {
        final int candidate = otherEntry.getKey();
        if (candidate == docID || visited.get(candidate)) {
          continue;
        }

        final Set<String> termsB = otherEntry.getValue();

        if (termsA.isEmpty() && termsB.isEmpty()) {
          cluster.add(candidate);
          visited.set(candidate);
          continue;
        }

        float score = jaccard(termsA, termsB);
        if (score >= threshold) {
          cluster.add(candidate);
          visited.set(candidate);
        }
      }

      if (cluster.size() > 1) {
        clusters.add(new DuplicateCluster(cluster));
      }
    }

    return clusters;
  }

  private static float jaccard(Set<String> a, Set<String> b) {
    int intersection = 0;
    for (String s : a) {
      if (b.contains(s)) {
        intersection++;
      }
    }
    int union = a.size() + b.size() - intersection;
    return union == 0 ? 0.0f : (float) intersection / union;
  }
}
