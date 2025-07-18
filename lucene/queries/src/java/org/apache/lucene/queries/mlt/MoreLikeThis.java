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
package org.apache.lucene.queries.mlt;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FloatComparator;
import org.apache.lucene.util.PriorityQueue;

/**
 * Generate "more like this" similarity queries. Based on this mail:
 *
 * <pre><code>
 * Lucene does let you access the document frequency of terms, with IndexReader.docFreq().
 * Term frequencies can be computed by re-tokenizing the text, which, for a single document,
 * is usually fast enough.  But looking up the docFreq() of every term in the document is
 * probably too slow.
 *
 * You can use some heuristics to prune the set of terms, to avoid calling docFreq() too much,
 * or at all.  Since you're trying to maximize a tf*idf score, you're probably most interested
 * in terms with a high tf. Choosing a tf threshold even as low as two or three will radically
 * reduce the number of terms under consideration.  Another heuristic is that terms with a
 * high idf (i.e., a low df) tend to be longer.  So you could threshold the terms by the
 * number of characters, not selecting anything less than, e.g., six or seven characters.
 * With these sorts of heuristics you can usually find small set of, e.g., ten or fewer terms
 * that do a pretty good job of characterizing a document.
 *
 * It all depends on what you're trying to do.  If you're trying to eek out that last percent
 * of precision and recall regardless of computational difficulty so that you can win a TREC
 * competition, then the techniques I mention above are useless.  But if you're trying to
 * provide a "more like this" button on a search results page that does a decent job and has
 * good performance, such techniques might be useful.
 *
 * An efficient, effective "more-like-this" query generator would be a great contribution, if
 * anyone's interested.  I'd imagine that it would take a Reader or a String (the document's
 * text), analyzer Analyzer, and return a set of representative terms using heuristics like those
 * above.  The frequency and length thresholds could be parameters, etc.
 *
 * Doug
 * </code></pre>
 *
 * <h2>Initial Usage</h2>
 *
 * <p>This class has lots of options to try to make it efficient and flexible. The simplest possible
 * usage is as follows. The bold fragment is specific to this class. <br>
 *
 * <pre class="prettyprint">
 * IndexReader ir = ...
 * IndexSearcher is = ...
 *
 * MoreLikeThis mlt = new MoreLikeThis(ir);
 * Reader target = ... // orig source of doc you want to find similarities to
 * Query query = mlt.like( target);
 *
 * TopDocs topDocs = is.search(query, 10);
 * // now the usual iteration thru 'topDocs' - the only thing to watch for is to make sure
 * // you ignore the doc if it matches your 'target' document, as it should be similar to itself
 *
 * </pre>
 *
 * <p>Thus you:
 *
 * <ol>
 *   <li>do your normal, Lucene setup for searching,
 *   <li>create a MoreLikeThis,
 *   <li>get the text of the doc you want to find similarities to
 *   <li>then call one of the like() calls to generate a similarity query
 *   <li>call the searcher to find the similar docs
 * </ol>
 *
 * <br>
 *
 * <h2>More Advanced Usage</h2>
 *
 * <p>You may want to use {@link #setFieldNames setFieldNames(...)} so you can examine multiple
 * fields (e.g. body and title) for similarity.
 *
 * <p>Depending on the size of your index and the size and makeup of your documents you may want to
 * call the other set methods to control how the similarity queries are generated:
 *
 * <ul>
 *   <li>{@link #setMinTermFreq setMinTermFreq(...)}
 *   <li>{@link #setMinDocFreq setMinDocFreq(...)}
 *   <li>{@link #setMaxDocFreq setMaxDocFreq(...)}
 *   <li>{@link #setMaxDocFreqPct setMaxDocFreqPct(...)}
 *   <li>{@link #setMinWordLen setMinWordLen(...)}
 *   <li>{@link #setMaxWordLen setMaxWordLen(...)}
 *   <li>{@link #setMaxQueryTerms setMaxQueryTerms(...)}
 *   <li>{@link #setMaxNumTokensParsed setMaxNumTokensParsed(...)}
 *   <li>{@link #setStopWords setStopWord(...)}
 * </ul>
 *
 * <br>
 * <hr>
 *
 * <pre>
 * Changes: Mark Harwood 29/02/04
 * Some bugfixing, some refactoring, some optimisation.
 * - bugfix: retrieveTerms(int docNum) was not working for indexes without a termvector -added missing code
 * - bugfix: No significant terms being created for fields with a termvector - because
 * was only counting one occurrence per term/field pair in calculations(ie not including frequency info from TermVector)
 * - refactor: moved common code into isNoiseWord()
 * - optimise: when no termvector support available - used maxNumTermsParsed to limit amount of tokenization
 * </pre>
 */
public final class MoreLikeThis {

  /**
   * Default maximum number of tokens to parse in each example doc field that is not stored with
   * TermVector support.
   *
   * @see #getMaxNumTokensParsed
   */
  public static final int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;

  /**
   * Ignore terms with less than this frequency in the source doc.
   *
   * @see #getMinTermFreq
   * @see #setMinTermFreq
   */
  public static final int DEFAULT_MIN_TERM_FREQ = 2;

  /**
   * Ignore words which do not occur in at least this many docs.
   *
   * @see #getMinDocFreq
   * @see #setMinDocFreq
   */
  public static final int DEFAULT_MIN_DOC_FREQ = 5;

  /**
   * Ignore words which occur in more than this many docs.
   *
   * @see #getMaxDocFreq
   * @see #setMaxDocFreq
   * @see #setMaxDocFreqPct
   */
  public static final int DEFAULT_MAX_DOC_FREQ = Integer.MAX_VALUE;

  /**
   * Boost terms in query based on score.
   *
   * @see #isBoost
   * @see #setBoost
   */
  public static final boolean DEFAULT_BOOST = false;

  /**
   * Default field names. Null is used to specify that the field names should be looked up at
   * runtime from the provided reader.
   */
  public static final String[] DEFAULT_FIELD_NAMES = new String[] {"contents"};

  /**
   * Ignore words less than this length or if 0 then this has no effect.
   *
   * @see #getMinWordLen
   * @see #setMinWordLen
   */
  public static final int DEFAULT_MIN_WORD_LENGTH = 0;

  /**
   * Ignore words greater than this length or if 0 then this has no effect.
   *
   * @see #getMaxWordLen
   * @see #setMaxWordLen
   */
  public static final int DEFAULT_MAX_WORD_LENGTH = 0;

  /**
   * Default set of stopwords. If null means to allow stop words.
   *
   * @see #setStopWords
   * @see #getStopWords
   */
  public static final Set<?> DEFAULT_STOP_WORDS = null;

  /** Current set of stop words. */
  private Set<?> stopWords = DEFAULT_STOP_WORDS;

  /**
   * Return a Query with no more than this many terms.
   *
   * @see IndexSearcher#getMaxClauseCount
   * @see #getMaxQueryTerms
   * @see #setMaxQueryTerms
   */
  public static final int DEFAULT_MAX_QUERY_TERMS = 25;

  /** Analyzer that will be used to parse the doc. */
  private Analyzer analyzer = null;

  /** Ignore words less frequent that this. */
  private int minTermFreq = DEFAULT_MIN_TERM_FREQ;

  /** Ignore words which do not occur in at least this many docs. */
  private int minDocFreq = DEFAULT_MIN_DOC_FREQ;

  /** Ignore words which occur in more than this many docs. */
  private int maxDocFreq = DEFAULT_MAX_DOC_FREQ;

  /** Should we apply a boost to the Query based on the scores? */
  private boolean boost = DEFAULT_BOOST;

  /** Field name we'll analyze. */
  private String[] fieldNames = DEFAULT_FIELD_NAMES;

  /**
   * The maximum number of tokens to parse in each example doc field that is not stored with
   * TermVector support
   */
  private int maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;

  /** Ignore words if less than this len. */
  private int minWordLen = DEFAULT_MIN_WORD_LENGTH;

  /** Ignore words if greater than this len. */
  private int maxWordLen = DEFAULT_MAX_WORD_LENGTH;

  /** Don't return a query longer than this. */
  private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

  /** For idf() calculations. */
  private TFIDFSimilarity similarity; // = new DefaultSimilarity();

  /** IndexReader to use */
  private final IndexReader ir;

  /** Boost factor to use when boosting the terms */
  private float boostFactor = 1;

  /**
   * Returns the boost factor used when boosting terms
   *
   * @return the boost factor used when boosting terms
   * @see #setBoostFactor(float)
   */
  public float getBoostFactor() {
    return boostFactor;
  }

  /**
   * Sets the boost factor to use when boosting terms
   *
   * @see #getBoostFactor()
   */
  public void setBoostFactor(float boostFactor) {
    this.boostFactor = boostFactor;
  }

  /** Constructor requiring an IndexReader. */
  public MoreLikeThis(IndexReader ir) throws IOException {
    this(ir, new ClassicSimilarity());
  }

  public MoreLikeThis(IndexReader ir, TFIDFSimilarity sim) throws IOException {
    this.ir = ir;
    this.similarity = sim;
  }

  public TFIDFSimilarity getSimilarity() {
    return similarity;
  }

  public void setSimilarity(TFIDFSimilarity similarity) {
    this.similarity = similarity;
  }

  /**
   * Returns an analyzer that will be used to parse source doc with. The default analyzer is not
   * set.
   *
   * @return the analyzer that will be used to parse source doc with.
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Sets the analyzer to use. An analyzer is not required for generating a query with the {@link
   * #like(int)} method, all other 'like' methods require an analyzer.
   *
   * @param analyzer the analyzer to use to tokenize text.
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  /**
   * Returns the frequency below which terms will be ignored in the source doc. The default
   * frequency is the {@link #DEFAULT_MIN_TERM_FREQ}.
   *
   * @return the frequency below which terms will be ignored in the source doc.
   */
  public int getMinTermFreq() {
    return minTermFreq;
  }

  /**
   * Sets the frequency below which terms will be ignored in the source doc.
   *
   * @param minTermFreq the frequency below which terms will be ignored in the source doc.
   */
  public void setMinTermFreq(int minTermFreq) {
    this.minTermFreq = minTermFreq;
  }

  /**
   * Returns the frequency at which words will be ignored which do not occur in at least this many
   * docs. The default frequency is {@link #DEFAULT_MIN_DOC_FREQ}.
   *
   * @return the frequency at which words will be ignored which do not occur in at least this many
   *     docs.
   */
  public int getMinDocFreq() {
    return minDocFreq;
  }

  /**
   * Sets the frequency at which words will be ignored which do not occur in at least this many
   * docs.
   *
   * @param minDocFreq the frequency at which words will be ignored which do not occur in at least
   *     this many docs.
   */
  public void setMinDocFreq(int minDocFreq) {
    this.minDocFreq = minDocFreq;
  }

  /**
   * Returns the maximum frequency in which words may still appear. Words that appear in more than
   * this many docs will be ignored. The default frequency is {@link #DEFAULT_MAX_DOC_FREQ}.
   *
   * @return get the maximum frequency at which words are still allowed, words which occur in more
   *     docs than this are ignored.
   */
  public int getMaxDocFreq() {
    return maxDocFreq;
  }

  /**
   * Set the maximum frequency in which words may still appear. Words that appear in more than this
   * many docs will be ignored.
   *
   * @param maxFreq the maximum count of documents that a term may appear in to be still considered
   *     relevant
   */
  public void setMaxDocFreq(int maxFreq) {
    this.maxDocFreq = maxFreq;
  }

  /**
   * Set the maximum percentage in which words may still appear. Words that appear in more than this
   * many percent of all docs will be ignored.
   *
   * <p>This method calls {@link #setMaxDocFreq(int)} internally (both conditions cannot be used at
   * the same time).
   *
   * @param maxPercentage the maximum percentage of documents (0-100) that a term may appear in to
   *     be still considered relevant.
   */
  public void setMaxDocFreqPct(int maxPercentage) {
    setMaxDocFreq(Math.toIntExact((long) maxPercentage * ir.maxDoc() / 100));
  }

  /**
   * Returns whether to boost terms in query based on "score" or not. The default is {@link
   * #DEFAULT_BOOST}.
   *
   * @return whether to boost terms in query based on "score" or not.
   * @see #setBoost
   */
  public boolean isBoost() {
    return boost;
  }

  /**
   * Sets whether to boost terms in query based on "score" or not.
   *
   * @param boost true to boost terms in query based on "score", false otherwise.
   * @see #isBoost
   */
  public void setBoost(boolean boost) {
    this.boost = boost;
  }

  /**
   * Returns the field names that will be used when generating the 'More Like This' query. The
   * default field names that will be used is {@link #DEFAULT_FIELD_NAMES}.
   *
   * @return the field names that will be used when generating the 'More Like This' query.
   */
  public String[] getFieldNames() {
    return fieldNames;
  }

  /**
   * Sets the field names that will be used when generating the 'More Like This' query. Set this to
   * null for the field names to be determined at runtime from the IndexReader provided in the
   * constructor.
   *
   * @param fieldNames the field names that will be used when generating the 'More Like This' query.
   */
  public void setFieldNames(String[] fieldNames) {
    this.fieldNames = fieldNames;
  }

  /**
   * Returns the minimum word length below which words will be ignored. Set this to 0 for no minimum
   * word length. The default is {@link #DEFAULT_MIN_WORD_LENGTH}.
   *
   * @return the minimum word length below which words will be ignored.
   */
  public int getMinWordLen() {
    return minWordLen;
  }

  /**
   * Sets the minimum word length below which words will be ignored.
   *
   * @param minWordLen the minimum word length below which words will be ignored.
   */
  public void setMinWordLen(int minWordLen) {
    this.minWordLen = minWordLen;
  }

  /**
   * Returns the maximum word length above which words will be ignored. Set this to 0 for no maximum
   * word length. The default is {@link #DEFAULT_MAX_WORD_LENGTH}.
   *
   * @return the maximum word length above which words will be ignored.
   */
  public int getMaxWordLen() {
    return maxWordLen;
  }

  /**
   * Sets the maximum word length above which words will be ignored.
   *
   * @param maxWordLen the maximum word length above which words will be ignored.
   */
  public void setMaxWordLen(int maxWordLen) {
    this.maxWordLen = maxWordLen;
  }

  /**
   * Set the set of stopwords. Any word in this set is considered "uninteresting" and ignored. Even
   * if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them,
   * as for the purposes of document similarity it seems reasonable to assume that "a stop word is
   * never interesting".
   *
   * @param stopWords set of stopwords, if null it means to allow stop words
   * @see #getStopWords
   */
  public void setStopWords(Set<?> stopWords) {
    this.stopWords = stopWords;
  }

  /**
   * Get the current stop words being used.
   *
   * @see #setStopWords
   */
  public Set<?> getStopWords() {
    return stopWords;
  }

  /**
   * Returns the maximum number of query terms that will be included in any generated query. The
   * default is {@link #DEFAULT_MAX_QUERY_TERMS}.
   *
   * @return the maximum number of query terms that will be included in any generated query.
   */
  public int getMaxQueryTerms() {
    return maxQueryTerms;
  }

  /**
   * Sets the maximum number of query terms that will be included in any generated query.
   *
   * @param maxQueryTerms the maximum number of query terms that will be included in any generated
   *     query.
   */
  public void setMaxQueryTerms(int maxQueryTerms) {
    this.maxQueryTerms = maxQueryTerms;
  }

  /**
   * @return The maximum number of tokens to parse in each example doc field that is not stored with
   *     TermVector support
   * @see #DEFAULT_MAX_NUM_TOKENS_PARSED
   */
  public int getMaxNumTokensParsed() {
    return maxNumTokensParsed;
  }

  /**
   * @param i The maximum number of tokens to parse in each example doc field that is not stored
   *     with TermVector support
   */
  public void setMaxNumTokensParsed(int i) {
    maxNumTokensParsed = i;
  }

  /**
   * Return a query that will return docs like the passed lucene document ID.
   *
   * @param docNum the documentID of the lucene doc to generate the 'More Like This" query for.
   * @return a query that will return docs like the passed lucene document ID.
   */
  public Query like(int docNum) throws IOException {
    if (fieldNames == null) {
      // gather list of valid fields from lucene
      Collection<String> fields = FieldInfos.getIndexedFields(ir);
      fieldNames = fields.toArray(new String[fields.size()]);
    }

    return createQuery(retrieveTerms(docNum));
  }

  /**
   * @param filteredDocument Document with field values extracted for selected fields.
   * @return More Like This query for the passed document.
   */
  public Query like(Map<String, Collection<Object>> filteredDocument) throws IOException {
    if (fieldNames == null) {
      // gather list of valid fields from lucene
      Collection<String> fields = FieldInfos.getIndexedFields(ir);
      fieldNames = fields.toArray(new String[fields.size()]);
    }
    return createQuery(retrieveTerms(filteredDocument));
  }

  /**
   * Return a query that will return docs like the passed Readers. This was added in order to treat
   * multi-value fields.
   *
   * @return a query that will return docs like the passed Readers.
   */
  public Query like(String fieldName, Reader... readers) throws IOException {
    Map<String, Map<String, Int>> perFieldTermFrequencies = new HashMap<>();
    for (Reader r : readers) {
      addTermFrequencies(r, perFieldTermFrequencies, fieldName);
    }
    return createQuery(createQueue(perFieldTermFrequencies));
  }

  /** Create the More like query from a PriorityQueue */
  private Query createQuery(PriorityQueue<ScoreTerm> q) {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    ScoreTerm scoreTerm;
    float bestScore = -1;

    while ((scoreTerm = q.pop()) != null) {
      Query tq = new TermQuery(new Term(scoreTerm.topField, scoreTerm.word));

      if (boost) {
        if (bestScore == -1) {
          bestScore = (scoreTerm.score);
        }
        float myScore = (scoreTerm.score);
        tq = new BoostQuery(tq, boostFactor * myScore / bestScore);
      }

      try {
        query.add(tq, BooleanClause.Occur.SHOULD);
      } catch (
          @SuppressWarnings("unused")
          IndexSearcher.TooManyClauses ignore) {
        break;
      }
    }
    return query.build();
  }

  /**
   * Create a PriorityQueue from a word-&gt;tf map.
   *
   * @param perFieldTermFrequencies a per field map of words keyed on the word(String) with Int
   *     objects as the values.
   */
  private PriorityQueue<ScoreTerm> createQueue(
      Map<String, Map<String, Int>> perFieldTermFrequencies) throws IOException {
    // have collected all words in doc and their freqs
    final int limit = Math.min(maxQueryTerms, this.getTermsCount(perFieldTermFrequencies));
    PriorityQueue<ScoreTerm> queue =
        PriorityQueue.usingComparator(limit, FloatComparator.comparing(st -> st.score));
    for (Map.Entry<String, Map<String, Int>> entry : perFieldTermFrequencies.entrySet()) {
      Map<String, Int> perWordTermFrequencies = entry.getValue();
      String fieldName = entry.getKey();

      long numDocs = ir.getDocCount(fieldName);
      if (numDocs == -1) {
        numDocs = ir.numDocs();
      }

      for (Map.Entry<String, Int> tfEntry : perWordTermFrequencies.entrySet()) { // for every word
        String word = tfEntry.getKey();
        int tf = tfEntry.getValue().x; // term freq in the source doc
        if (minTermFreq > 0 && tf < minTermFreq) {
          continue; // filter out words that don't occur enough times in the source
        }

        int docFreq = ir.docFreq(new Term(fieldName, word));

        if (minDocFreq > 0 && docFreq < minDocFreq) {
          continue; // filter out words that don't occur in enough docs
        }

        if (docFreq > maxDocFreq) {
          continue; // filter out words that occur in too many docs
        }

        if (docFreq == 0) {
          continue; // index update problem?
        }

        float idf = similarity.idf(docFreq, numDocs);
        float score = tf * idf;

        if (queue.size() < limit) {
          // there is still space in the queue
          queue.add(new ScoreTerm(word, fieldName, score));
        } else {
          ScoreTerm term = queue.top();
          // update the smallest in the queue in place and update the queue.
          if (term.score < score) {
            term.update(word, fieldName, score);
            queue.updateTop();
          }
        }
      }
    }
    return queue;
  }

  private int getTermsCount(Map<String, Map<String, Int>> perFieldTermFrequencies) {
    int totalTermsCount = 0;
    Collection<Map<String, Int>> values = perFieldTermFrequencies.values();
    for (Map<String, Int> perWordTermFrequencies : values) {
      totalTermsCount += perWordTermFrequencies.size();
    }
    return totalTermsCount;
  }

  /** Describe the parameters that control how the "more like this" query is formed. */
  public String describeParams() {
    StringBuilder sb = new StringBuilder();
    sb.append("\t").append("maxQueryTerms  : ").append(maxQueryTerms).append("\n");
    sb.append("\t").append("minWordLen     : ").append(minWordLen).append("\n");
    sb.append("\t").append("maxWordLen     : ").append(maxWordLen).append("\n");
    sb.append("\t").append("fieldNames     : ");
    String delim = "";
    for (String fieldName : fieldNames) {
      sb.append(delim).append(fieldName);
      delim = ", ";
    }
    sb.append("\n");
    sb.append("\t").append("boost          : ").append(boost).append("\n");
    sb.append("\t").append("minTermFreq    : ").append(minTermFreq).append("\n");
    sb.append("\t").append("minDocFreq     : ").append(minDocFreq).append("\n");
    return sb.toString();
  }

  /**
   * Find words for a more-like-this query former.
   *
   * @param docNum the id of the lucene document from which to find terms
   */
  private PriorityQueue<ScoreTerm> retrieveTerms(int docNum) throws IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    TermVectors termVectors = ir.termVectors();
    for (String fieldName : fieldNames) {
      final Fields vectors = termVectors.get(docNum);
      final Terms vector;
      if (vectors != null) {
        vector = vectors.terms(fieldName);
      } else {
        vector = null;
      }

      // field does not store term vector info
      if (vector == null) {
        Document d = ir.storedFields().document(docNum);
        IndexableField[] fields = d.getFields(fieldName);
        for (IndexableField field : fields) {
          final String stringValue = field.stringValue();
          if (stringValue != null) {
            addTermFrequencies(new StringReader(stringValue), field2termFreqMap, fieldName);
          }
        }
      } else {
        addTermFrequencies(field2termFreqMap, vector, fieldName);
      }
    }

    return createQueue(field2termFreqMap);
  }

  private PriorityQueue<ScoreTerm> retrieveTerms(Map<String, Collection<Object>> field2fieldValues)
      throws IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    for (String fieldName : fieldNames) {
      Collection<Object> fieldValues = field2fieldValues.get(fieldName);
      if (fieldValues == null) {
        continue;
      }
      for (Object fieldValue : fieldValues) {
        if (fieldValue != null) {
          addTermFrequencies(
              new StringReader(String.valueOf(fieldValue)), field2termFreqMap, fieldName);
        }
      }
    }
    return createQueue(field2termFreqMap);
  }

  /**
   * Adds terms and frequencies found in vector into the Map termFreqMap
   *
   * @param field2termFreqMap a Map of terms and their frequencies per field
   * @param vector List of terms and their frequencies for a doc/field
   */
  private void addTermFrequencies(
      Map<String, Map<String, Int>> field2termFreqMap, Terms vector, String fieldName)
      throws IOException {
    Map<String, Int> termFreqMap =
        field2termFreqMap.computeIfAbsent(fieldName, _ -> new HashMap<>());
    final TermsEnum termsEnum = vector.iterator();
    final CharsRefBuilder spare = new CharsRefBuilder();
    BytesRef text;
    while ((text = termsEnum.next()) != null) {
      spare.copyUTF8Bytes(text);
      final String term = spare.toString();
      if (isNoiseWord(term)) {
        continue;
      }
      final int freq = (int) termsEnum.totalTermFreq();

      // increment frequency
      Int cnt = termFreqMap.get(term);
      if (cnt == null) {
        cnt = new Int();
        termFreqMap.put(term, cnt);
        cnt.x = freq;
      } else {
        cnt.x += freq;
      }
    }
  }

  /**
   * Adds term frequencies found by tokenizing text from reader into the Map words
   *
   * @param r a source of text to be tokenized
   * @param perFieldTermFrequencies a Map of terms and their frequencies per field
   * @param fieldName Used by analyzer for any special per-field analysis
   */
  private void addTermFrequencies(
      Reader r, Map<String, Map<String, Int>> perFieldTermFrequencies, String fieldName)
      throws IOException {
    if (analyzer == null) {
      throw new UnsupportedOperationException(
          "To use MoreLikeThis without " + "term vectors, you must provide an Analyzer");
    }
    Map<String, Int> termFreqMap =
        perFieldTermFrequencies.computeIfAbsent(fieldName, _ -> new HashMap<>());
    try (TokenStream ts = analyzer.tokenStream(fieldName, r)) {
      int tokenCount = 0;
      // for every token
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      TermFrequencyAttribute tfAtt = ts.addAttribute(TermFrequencyAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        String word = termAtt.toString();
        tokenCount++;
        if (tokenCount > maxNumTokensParsed) {
          break;
        }
        if (isNoiseWord(word)) {
          continue;
        }

        // increment frequency
        Int cnt = termFreqMap.get(word);
        if (cnt == null) {
          termFreqMap.put(word, new Int(tfAtt.getTermFrequency()));
        } else {
          cnt.x += tfAtt.getTermFrequency();
        }
      }
      ts.end();
    }
  }

  /**
   * determines if the passed term is likely to be of interest in "more like" comparisons
   *
   * @param term The word being considered
   * @return true if should be ignored, false if should be used in further analysis
   */
  private boolean isNoiseWord(String term) {
    int len = term.length();
    if (minWordLen > 0 && len < minWordLen) {
      return true;
    }
    if (maxWordLen > 0 && len > maxWordLen) {
      return true;
    }
    return stopWords != null && stopWords.contains(term);
  }

  /**
   * Find words for a more-like-this query former. The result is a priority queue of arrays with one
   * entry for <b>every word</b> in the document. Each array has 6 elements. The elements are:
   *
   * <ol>
   *   <li>The word (String)
   *   <li>The top field that this word comes from (String)
   *   <li>The score for this word (Float)
   *   <li>The IDF value (Float)
   *   <li>The frequency of this word in the index (Integer)
   *   <li>The frequency of this word in the source document (Integer)
   * </ol>
   *
   * This is a somewhat "advanced" routine, and in general only the 1st entry in the array is of
   * interest. This method is exposed so that you can identify the "interesting words" in a
   * document. For an easier method to call see {@link #retrieveInterestingTerms
   * retrieveInterestingTerms()}.
   *
   * @param r the reader that has the content of the document
   * @param fieldName field passed to the analyzer to use when analyzing the content
   * @return the most interesting words in the document ordered by score, with the highest scoring,
   *     or best entry, first
   * @see #retrieveInterestingTerms
   */
  private PriorityQueue<ScoreTerm> retrieveTerms(Reader r, String fieldName) throws IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    addTermFrequencies(r, field2termFreqMap, fieldName);
    return createQueue(field2termFreqMap);
  }

  /**
   * @see #retrieveInterestingTerms(java.io.Reader, String)
   */
  public String[] retrieveInterestingTerms(int docNum) throws IOException {
    ArrayList<String> al = new ArrayList<>(maxQueryTerms);
    PriorityQueue<ScoreTerm> pq = retrieveTerms(docNum);
    ScoreTerm scoreTerm;
    // have to be careful, retrieveTerms returns all words but that's probably not useful to our
    // caller...
    int lim = maxQueryTerms;
    // we just want to return the top words
    while (((scoreTerm = pq.pop()) != null) && lim-- > 0) {
      al.add(scoreTerm.word); // the 1st entry is the interesting word
    }
    String[] res = new String[al.size()];
    return al.toArray(res);
  }

  /**
   * Convenience routine to make it easy to return the most interesting words in a document. More
   * advanced users will call {@link #retrieveTerms(Reader, String) retrieveTerms()} directly.
   *
   * @param r the source document
   * @param fieldName field passed to analyzer to use when analyzing the content
   * @return the most interesting words in the document
   * @see #retrieveTerms(java.io.Reader, String)
   * @see #setMaxQueryTerms
   */
  public String[] retrieveInterestingTerms(Reader r, String fieldName) throws IOException {
    ArrayList<String> al = new ArrayList<>(maxQueryTerms);
    PriorityQueue<ScoreTerm> pq = retrieveTerms(r, fieldName);
    ScoreTerm scoreTerm;
    // have to be careful, retrieveTerms returns all words but that's probably not useful to our
    // caller...
    int lim = maxQueryTerms;
    // we just want to return the top words
    while (((scoreTerm = pq.pop()) != null) && lim-- > 0) {
      al.add(scoreTerm.word); // the 1st entry is the interesting word
    }
    String[] res = new String[al.size()];
    return al.toArray(res);
  }

  private static class ScoreTerm {
    // only really need 1st 3 entries, other ones are for troubleshooting
    String word;
    String topField;
    float score;

    ScoreTerm(String word, String topField, float score) {
      this.word = word;
      this.topField = topField;
      this.score = score;
    }

    void update(String word, String topField, float score) {
      this.word = word;
      this.topField = topField;
      this.score = score;
    }
  }

  /** Use for frequencies and to avoid renewing Integers. */
  private static class Int {
    int x;

    Int() {
      this(1);
    }

    Int(int initialValue) {
      x = initialValue;
    }
  }
}
