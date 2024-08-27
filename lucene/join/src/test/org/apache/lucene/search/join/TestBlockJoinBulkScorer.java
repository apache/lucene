package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestBlockJoinBulkScorer extends LuceneTestCase {
  private static final String TYPE_FIELD_NAME = "type";
  private static final String VALUE_FIELD_NAME = "value";
  private static final String PARENT_FILTER_VALUE = "parent";
  private static final String CHILD_FILTER_VALUE = "child";

  private enum MatchValue {
    MATCH_A("A", 1),
    MATCH_B("B", 2),
    MATCH_C("C", 3);

    private static final List<MatchValue> VALUES = List.of(values());
    private static final Random RANDOM = new Random();

    private final String text;
    private final int score;

    MatchValue(String text, int score) {
      this.text = text;
      this.score = score;
    }

    public String getText() {
      return text;
    }

    public int getScore() {
      return score;
    }

    @Override
    public String toString() {
      return text;
    }

    public static MatchValue random() {
      return VALUES.get(RANDOM.nextInt(VALUES.size()));
    }
  }

  private record ChildDocMatch(int docId, List<MatchValue> matches) {
    public ChildDocMatch(int docId, List<MatchValue> matches) {
      this.docId = docId;
      this.matches = Collections.unmodifiableList(matches);
    }
  }

  private static Map<Integer, List<ChildDocMatch>> populateIndex(
      RandomIndexWriter writer, int maxParentDocCount, int maxChildDocCount, int maxChildDocMatches)
      throws IOException {
    Map<Integer, List<ChildDocMatch>> expectedMatches = new HashMap<>();

    final int parentDocCount = random().nextInt(maxParentDocCount + 1);
    int currentDocId = 0;
    for (int i = 0; i < parentDocCount; i++) {
      final int childDocCount = random().nextInt(maxChildDocCount + 1);
      List<Document> docs = new ArrayList<>(childDocCount);
      List<ChildDocMatch> childDocMatches = new ArrayList<>(childDocCount);

      // TODO: Need special handling for 0 child docs?
      for (int j = 0; j < childDocCount; j++) {
        // Build a child doc
        Document childDoc = new Document();
        childDoc.add(newStringField(TYPE_FIELD_NAME, CHILD_FILTER_VALUE, Field.Store.NO));

        final int matchCount = random().nextInt(maxChildDocMatches + 1);
        List<MatchValue> matchValues = new ArrayList<>(matchCount);
        for (int k = 0; k < matchCount; k++) {
          // Add a match to the child doc
          MatchValue matchValue = MatchValue.random();
          matchValues.add(matchValue);
          childDoc.add(newStringField(VALUE_FIELD_NAME, matchValue.getText(), Field.Store.NO));
        }

        docs.add(childDoc);
        childDocMatches.add(new ChildDocMatch(currentDocId++, matchValues));
      }

      // Build a parent doc
      Document parentDoc = new Document();
      parentDoc.add(newStringField(TYPE_FIELD_NAME, PARENT_FILTER_VALUE, Field.Store.NO));
      docs.add(parentDoc);
      expectedMatches.put(currentDocId++, childDocMatches);

      writer.addDocuments(docs);
    }

    return expectedMatches;
  }
}
