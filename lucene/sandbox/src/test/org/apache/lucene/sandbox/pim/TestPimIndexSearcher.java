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

package org.apache.lucene.sandbox.pim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.sandbox.sdk.DpuException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestRuleLimitSysouts;
import org.apache.lucene.util.BytesRef;
import org.junit.AfterClass;

@TestRuleLimitSysouts.Limit(bytes = 1 << 14, hardLimit = 1 << 14)
public class TestPimIndexSearcher extends LuceneTestCase {

  private static Directory directory;
  private static Directory pimDirectory;

  public static void initDirectories() {

    directory = newDirectory();
    pimDirectory = newDirectory();
  }

  public static void closeDirectories() throws Exception {
    directory.close();
    directory = null;
    pimDirectory.close();
    pimDirectory = null;
  }

  @AfterClass
  public static void afterClass() throws Exception {

    // Need an explicit PimSystemManager shutdown here
    // The managing thread is normally killed with a hook at JVM shutdown
    // But the test system verifies that threads are not leaked before JVM shutdowm
    PimSystemManager.get().shutDown();
  }

  public void testTermBasic() throws Exception {
    initDirectories();
    PimConfig pimConfig = new PimConfig();
    IndexWriterConfig indexWriterConfig =
        new IndexWriterConfig(getAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
    PimIndexWriter writer =
        new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

    Document doc = new Document();
    doc.add(newTextField("id", "AAA", Field.Store.YES));
    doc.add(newTextField("field1", "red black yellow", Field.Store.YES));
    doc.add(newTextField("field2", "red orange white orange red", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("id", "AAB", Field.Store.YES));
    doc.add(newTextField("field1", "yellow green blue", Field.Store.YES));
    doc.add(newTextField("field2", "green red", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("id", "AAC", Field.Store.YES));
    doc.add(newTextField("field1", "black blue pink", Field.Store.YES));
    doc.add(newTextField("field2", "white brown", Field.Store.YES));
    writer.addDocument(doc);

    System.out.println("-- CLOSE -------------------------------");
    writer.close();

    System.out.println("\nTEST PIM INDEX SEARCH (BASIC)");
    PimIndexSearcher pimSearcher = new PimIndexSearcher(writer.getPimIndexInfo(), true);

    var matches = pimSearcher.searchTerm(new BytesRef("field1"), new BytesRef("yellow"));
    System.out.println("\nSearching for field1:yellow: found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    ArrayList<PimMatch> expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 1));
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("field1"), new BytesRef("green"));
    System.out.println("\nSearching for field1:green found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("field2"), new BytesRef("green"));
    System.out.println("\nSearching for field2:green found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("field2"), new BytesRef("orange"));
    System.out.println("\nSearching for field2:orange found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 2));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("field2"), new BytesRef("yellow"));
    System.out.println("\nSearching for field2:yellow found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("id"), new BytesRef("AAC"));
    System.out.println("\nSearching for id:AAC found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(2, 1));
    assert matches.equals(expectedMatches);
    System.out.println("");

    pimSearcher.close();
    closeDirectories();
  }

  public void testTermMoreText() throws Exception {

    initDirectories();
    PimConfig pimConfig = new PimConfig(2, 2);
    PimIndexInfo pimIndexInfo = writeFewWikiText(pimConfig);

    System.out.println("\nTEST PIM INDEX SEARCH (MORE TEXT)");
    PimIndexSearcher pimSearcher = new PimIndexSearcher(pimIndexInfo, true);

    var matches = pimSearcher.searchTerm(new BytesRef("title"), new BytesRef("Apache"));
    System.out.println("\nSearching for title:Apache: found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    ArrayList<PimMatch> expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("title"), new BytesRef("München"));
    System.out.println("\nSearching for title:München found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("body"), new BytesRef("manuscrit"));
    System.out.println("\nSearching for body:manuscrit found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(2, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("body"), new BytesRef("copie"));
    System.out.println("\nSearching for body:copie found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(2, 2));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("body"), new BytesRef("wird"));
    System.out.println("\nSearching for body:wird found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 2));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("body"), new BytesRef("Dativ"));
    System.out.println("\nSearching for body:Dativ found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchTerm(new BytesRef("body"), new BytesRef("conservé"));
    System.out.println("\nSearching for body:conservé found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("title", "Apache", "Lucene"));
    System.out.println(
        "\nSearching for title:[Apache Lucene] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    System.out.println("");
    pimSearcher.close();
    closeDirectories();
  }

  public void testPhraseMoreText() throws Exception {

    initDirectories();
    PimConfig pimConfig = new PimConfig(2, 2);
    PimIndexInfo pimIndexInfo = writeFewWikiText(pimConfig);

    System.out.println("\nTEST PIM INDEX SEARCH (PHRASE MORE TEXT)");
    PimIndexSearcher pimSearcher = new PimIndexSearcher(pimIndexInfo, true);

    var matches = pimSearcher.searchPhrase(new PimPhraseQuery("title", "Apache", "Lucene"));
    System.out.println(
        "\nSearching for title:[Apache Lucene] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    var expectedMatches = new ArrayList<PimMatch>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "recette", "secrète"));
    System.out.println(
        "\nSearching for body:[recette secrète] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(2, 1));
    assert matches.equals(expectedMatches);

    matches =
        pimSearcher.searchPhrase(
            new PimPhraseQuery("body", "dem", "Vorläufer", "von", "neuhochdeutsch"));
    System.out.println(
        "\nSearching for body:[dem Vorläufer von neuhochdeutsch] found "
            + matches.size()
            + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "fuzzy", "search"));
    System.out.println("\nSearching for body:[fuzzy search] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    expectedMatches.add(new PimMatch(3, 1));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "edit", "distance."));
    System.out.println("\nSearching for body:[edit distance] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    expectedMatches.add(new PimMatch(3, 1));
    assert matches.equals(expectedMatches);

    matches =
        pimSearcher.searchPhrase(
            new PimPhraseQuery("body", "fuzzy", "search", "based", "on", "edit", "distance."));
    System.out.println(
        "\nSearching for body:[fuzzy search based on edit distance.] found "
            + matches.size()
            + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    // assert matches.equals(expectedMatches);

    matches =
        pimSearcher.searchPhrase(
            new PimPhraseQuery(
                "body", "fuzzy", "search", "based", "on", "Levenshtein", "distance."));
    System.out.println(
        "\nSearching for body:[fuzzy search based on Levenshtein distance.] found "
            + matches.size()
            + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    assert matches.equals(expectedMatches);

    matches =
        pimSearcher.searchPhrase(
            new PimPhraseQuery(
                "body",
                "Lucene",
                "is",
                "recognized",
                "for",
                "its",
                "utility",
                "in",
                "the",
                "implementation",
                "of",
                "Internet",
                "search",
                "engines"));
    System.out.println(
        "\nSearching for body:[Lucene is recognized for its utility in the"
            + " implementation of Internet search engines] found "
            + matches.size()
            + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(1, 1));
    assert matches.equals(expectedMatches);

    System.out.println("");
    pimSearcher.close();
    closeDirectories();
  }

  public void testPimPhraseQuery() throws Exception {

    initDirectories();
    PimConfig pimConfig = new PimConfig(2, 4);
    writeFewWikiText(pimConfig);

    // load the index to PIM system
    PimSystemManager pimSystemManager = PimSystemManager.get();
    pimSystemManager.loadPimIndex(pimDirectory);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    System.out.println("\nTEST PIM PHRASE QUERY (PHRASE MORE TEXT)");

    checkPhraseQuery(searcher, 10, "title", "Apache", "Lucene");
    checkPhraseQuery(searcher, 10, "body", "recette", "secrète");
    checkPhraseQuery(searcher, 10, "body", "dem", "Vorläufer", "von", "neuhochdeutsch");
    checkPhraseQuery(searcher, 10, "body", "fuzzy", "search");
    checkPhraseQuery(searcher, 10, "body", "edit", "distance.");
    checkPhraseQuery(searcher, 10, "body", "fuzzy", "search", "based", "on", "edit", "distance.");
    checkPhraseQuery(
        searcher, 10, "body", "fuzzy", "search", "based", "on", "Levenshtein", "distance.");
    checkPhraseQuery(
        searcher, 10,
        "body",
        "Lucene",
        "is",
        "recognized",
        "for",
        "its",
        "utility",
        "in",
        "the",
        "implementation",
        "of",
        "Internet",
        "search",
        "engines");

    System.out.println("");
    pimSystemManager.unloadPimIndex();
    reader.close();
    closeDirectories();
  }

  public void testPimPhraseQueryNoPimLoad() throws Exception {

    initDirectories();
    PimConfig pimConfig = new PimConfig(2, 4);
    writeFewWikiText(pimConfig);

    // no load of the index to PIM system
    // the PimPhraseQuery queries should be rewritten as PhraseQuery
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    System.out.println("\nTEST PIM PHRASE QUERY NO LOAD (PHRASE MORE TEXT)");

    checkPhraseQuery(searcher, 10, "title", "Apache", "Lucene");
    checkPhraseQuery(searcher, 10, "body", "recette", "secrète");
    checkPhraseQuery(searcher, 10, "body", "dem", "Vorläufer", "von", "neuhochdeutsch");
    checkPhraseQuery(
        searcher, 10, "body", "fuzzy", "search", "based", "on", "Levenshtein", "distance.");

    System.out.println("");
    reader.close();
    closeDirectories();
  }

  public void testPimPhraseQueryLowerBound() throws Exception {
    testPimPhraseQueryLowerBoundWithParams(1, 1, 1);
    testPimPhraseQueryLowerBoundWithParams(2, 1, 1);
    testPimPhraseQueryLowerBoundWithParams(4, 1, 1);
    testPimPhraseQueryLowerBoundWithParams(1, 1, 2);
    testPimPhraseQueryLowerBoundWithParams(2, 1, 2);
    testPimPhraseQueryLowerBoundWithParams(4, 1, 2);
    testPimPhraseQueryLowerBoundWithParams(4, 1, 20);
    testPimPhraseQueryLowerBoundWithParams(4, 2, 1);
    testPimPhraseQueryLowerBoundWithParams(4, 2, 2);
  }

  private void testPimPhraseQueryLowerBoundWithParams(int nbDpus, int nbSegments, int nbTopDocs)
          throws Exception {

    initDirectories();
    PimConfig pimConfig = new PimConfig(nbDpus, nbSegments);
    writeLowerBoundTestText(pimConfig);

    // load the index to PIM system
    PimSystemManager pimSystemManager = PimSystemManager.get();
    pimSystemManager.loadPimIndex(pimDirectory);
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);

    System.out.println("\nTEST PIM PHRASE QUERY (LOWER BOUND)");

    checkPhraseQuery(searcher, nbTopDocs, "body", "Une", "phrase", "a", "trouver");

    System.out.println("");
    pimSystemManager.unloadPimIndex();
    reader.close();
    closeDirectories();
  }

  public void testPhraseCornerCases() throws Exception {

    initDirectories();
    // note pim config is using 2 DPUs unless one won't have any files
    // and thus an empty index
    PimConfig pimConfig = new PimConfig(2);
    IndexWriterConfig indexWriterConfig =
        new IndexWriterConfig(getAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
    PimIndexWriter writer =
        new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

    Document doc = new Document();
    doc.add(newTextField("title", "blah", Field.Store.YES));
    doc.add(newTextField("body", "blah blah blah blah blah youpi blah blah blah", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    System.out.println("\nTEST PIM INDEX SEARCH (PHRASE CORNER CASES)");
    PimIndexSearcher pimSearcher = new PimIndexSearcher(writer.getPimIndexInfo());

    var matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "blah", "blah"));
    System.out.println("\nSearching for body:[blah blah] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    var expectedMatches = new ArrayList<PimMatch>();
    expectedMatches.add(new PimMatch(0, 6));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "blah", "blah", "blah"));
    System.out.println(
        "\nSearching for body:[blah blah blah] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 4));
    assert matches.equals(expectedMatches);

    matches = pimSearcher.searchPhrase(new PimPhraseQuery("body", "blah", "youpi", "blah"));
    System.out.println(
        "\nSearching for body:[blah youpi blah] found " + matches.size() + " results");
    matches.forEach(
        (m) -> {
          System.out.println("Doc:" + m.docId + " freq:" + m.score);
        });
    expectedMatches = new ArrayList<>();
    expectedMatches.add(new PimMatch(0, 1));
    assert matches.equals(expectedMatches);

    System.out.println("");
    pimSearcher.close();
    closeDirectories();
  }

  PimIndexInfo writeFewWikiText(PimConfig pimConfig) throws IOException {

    IndexWriterConfig indexWriterConfig =
        new IndexWriterConfig(getAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
    PimIndexWriter writer =
        new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

    Document doc = new Document();
    doc.add(newTextField("title", "München", Field.Store.YES));
    doc.add(
        newTextField(
            "body",
            "Der Name München wird üblicherweise als "
                + "„bei den Mönchen“ gedeutet. Erstmals erwähnt wird der Name als forum apud "
                + "Munichen im Augsburger Schied vom 14. Juni 1158 von Kaiser Friedrich I.[15][16] "
                + "Munichen ist der Dativ Plural von althochdeutsch munih bzw. mittelhochdeutsch mün(e)ch, "
                + "dem Vorläufer von neuhochdeutsch Mönch",
            Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "Apache Lucene", Field.Store.YES));
    doc.add(
        newTextField(
            "body",
            "While suitable for any application that requires full "
                + "text indexing and searching capability, Lucene is recognized for its utility in the "
                + "implementation of Internet search engines and local, single-site searching.[10][11]. "
                + "Lucene includes a feature to perform a fuzzy search based on edit distance.",
            Field.Store.YES));
    writer.addDocument(doc);

    writer.commit();

    doc = new Document();
    doc.add(newTextField("title", "Chartreuse", Field.Store.YES));
    doc.add(
        newTextField(
            "body",
            "Poursuivis pendant la Révolution française, les moines "
                + "sont dispersés en 1793. La distillation de la chartreuse s'interrompt alors, mais les "
                + "chartreux réussissent à conserver la recette secrète : le manuscrit est emporté par un des "
                + "pères et une copie est conservée par le moine autorisé à garder le monastère ; lors de son "
                + "incarcération à Bordeaux, ce dernier remet sa copie à un confrère qui finit par la céder à "
                + "un pharmacien de Grenoble, un certain Liotard. ",
            Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "FuzzyQuery", Field.Store.YES));
    doc.add(
        newTextField(
            "body",
            "Implements the fuzzy search query. The similarity "
                + "measurement is based on the Damerau-Levenshtein (optimal string alignment) algorithm, though"
                + " you can explicitly choose classic Levenshtein by passing false to the transpositions parameter.\n"
                + "This query uses MultiTermQuery.TopTermsBlendedFreqScoringRewrite as default. So terms will "
                + "be collected and scored according to their edit distance. Only the top terms are used for building "
                + "the BooleanQuery. It is not recommended to change the rewrite mode for fuzzy queries.",
            Field.Store.YES));
    writer.addDocument(doc);

    System.out.println("-- CLOSE -------------------------------");
    writer.close();
    return writer.getPimIndexInfo();
  }

  PimIndexInfo writeLowerBoundTestText(PimConfig pimConfig) throws IOException {

    IndexWriterConfig indexWriterConfig =
            new IndexWriterConfig(getAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
    PimIndexWriter writer =
            new PimIndexWriter(directory, pimDirectory, indexWriterConfig, pimConfig);

    Document doc = new Document();
    doc.add(newTextField("title", "doc0", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Il joue avec le vent, cause avec le nuage,\n" +
                            "Et s’enivre en chantant du chemin de la croix ;\n" +
                            "Et l’Esprit qui le suit dans son pèlerinage\n" +
                            "Pleure de le voir gai comme un oiseau des bois." +
                            "Une phrase a trouver . Tous ceux qu’il veut aimer l’observent avec crainte,\n" +
                            "Ou bien, s’enhardissant de sa tranquillité,\n" +
                            "Cherchent à qui saura lui tirer une plainte,\n" +
                            "Et font sur lui l’essai de leur férocité.",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc1", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    " Voici venir les temps où vibrant sur sa tige\n" +
                            "Chaque fleur s’évapore ainsi qu’un encensoir ;\n" +
                            "Les sons et les parfums tournent dans l’air du soir ;\n" +
                            "Valse mélancolique et langoureux vertige ! Une phrase a trouver . " +
                            " Une phrase a trouver . Chaque fleur s’évapore ainsi qu’un encensoir ;\n" +
                            "Le violon frémit comme un cœur qu’on afflige ;\n" +
                            "Valse mélancolique et langoureux vertige !\n" +
                            "Le ciel est triste et beau comme un grand reposoir. Une phrase a trouver . " +
                            " Une phrase a trouver . Le violon frémit comme un cœur qu’on afflige,\n" +
                            "Un cœur tendre, qui hait le néant vaste et noir ! Une phrase a trouver . " +
                            "Le ciel est triste et beau comme un grand reposoir ;\n" +
                            "Le soleil s’est noyé dans son sang qui se fige. Une phrase a trouver . " +
                            " Un cœur tendre, qui hait le néant vaste et noir,\n" +
                            "Du passé lumineux recueille tout vestige ! Une phrase a trouver . " +
                            " Le soleil s’est noyé dans son sang qui se fige……\n" +
                            "Ton souvenir en moi luit comme un ostensoir ! Une phrase a trouver . ",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc2", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "La Nature est un temple où de vivants piliers\n" +
                            "Laissent parfois sortir de confuses paroles ;\n" +
                            "L’homme y passe à travers des forêts de symboles\n" +
                            "Qui l’observent avec des regards familiers. Une phrase a ne pas trouver .",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc3", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Rubens, fleuve d’oubli, jardin de la paresse,\n" +
                            "Oreiller de chair fraîche où l’on ne peut aimer,\n" +
                            "Mais où la vie afflue et s’agite sans cesse,\n" +
                            "Comme l’air dans le ciel et la mer dans la mer ; Une phrase a ne pas trouver. " +
                            "Léonard de Vinci, miroir profond et sombre,\n" +
                            "Où des anges charmants, avec un doux souris\n" +
                            "Tout chargé de mystère, apparaissent à l’ombre\n" +
                            "Des glaciers et des pins qui ferment leur pays ;" +
                            " Une phrase a trouver . " +
                            " Rembrandt, triste hôpital tout rempli de murmures,\n" +
                            "Et d’un grand crucifix décoré seulement,\n" +
                            "Où la prière en pleurs s’exhale des ordures,\n" +
                            "Et d’un rayon d’hiver traversé brusquement ; Une phrase a trouver" +
                            " Michel-Ange, lieu vague où l’on voit des Hercules\n" +
                            "Se mêler à des Christs, et se lever tout droits\n" +
                            "Des fantômes puissants qui dans les crépuscules\n" +
                            "Déchirent leur suaire en étirant leurs doigts ;",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc4", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Une phrase a trouver . La tribu prophétique aux prunelles ardentes\n" +
                            "Hier s’est mise en route, emportant ses petits\n" +
                            "Sur son dos, ou livrant à leurs fiers appétits\n" +
                            "Le trésor toujours prêt des mamelles pendantes." +
                            " Une phrase a trouver . " +
                            "Les hommes vont à pied sous leurs armes luisantes\n" +
                            "Le long des chariots où les leurs sont blottis,\n" +
                            "Promenant sur le ciel des yeux appesantis\n" +
                            "Par le morne regret des chimères absentes." +
                            "Une phrase a trouver . Du fond de son réduit sablonneux, le grillon,\n" +
                            "Les regardant passer, redouble sa chanson ;\n" +
                            "Cybèle, qui les aime, augmente ses verdures,",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc5", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Quand Don Juan descendit vers l’onde souterraine\n" +
                            "Et lorsqu’il eut donné son obole à Charon,\n" +
                            "Un sombre mendiant, l’œil fier comme Antisthène,\n" +
                            "D’un bras vengeur et fort saisit chaque aviron." +
                            "Une phrase a ne pas trouver. Une phrase a trouver . Une phrase a ne pas trouver. " +
                            "Montrant leurs seins pendants et leurs robes ouvertes,\n" +
                            "Des femmes se tordaient sous le noir firmament,\n" +
                            "Et, comme un grand troupeau de victimes offertes,\n" +
                            "Derrière lui traînaient un long mugissement.",
                    Field.Store.YES));
    writer.addDocument(doc);

    writer.commit();

    doc = new Document();
    doc.add(newTextField("title", "doc6", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Dans les caveaux d’insondable tristesse\n" +
                            "Où le Destin m’a déjà relégué ;\n" +
                            "Où jamais n’entre un rayon rose et gai ;\n" +
                            "Où, seul avec la Nuit, maussade hôtesse, Une phrase a trouver . " +
                            " Une phrase a trouver . Je suis comme un peintre qu’un Dieu moqueur\n" +
                            "Condamne à peindre, hélas ! sur les ténèbres ;\n" +
                            "Où, cuisinier aux appétits funèbres,\n" +
                            "Je fais bouillir et je mange mon cœur,\n " +
                            " Une phrase a ne pas trouver. " +
                            " Par instants brille, et s’allonge, et s’étale\n" +
                            "Un spectre fait de grâce et de splendeur.\n" +
                            "À sa rêveuse allure orientale, Une phrase a trouver . Quand il atteint sa totale grandeur,\n" +
                            "Je reconnais ma belle visiteuse :\n" +
                            "C’est Elle ! noire et pourtant lumineuse.\n" +
                            "Une phrase a trouver . ",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc7", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Lecteur, as-tu quelquefois respiré\n" +
                            "Avec ivresse et lente gourmandise\n" +
                            "Ce grain d’encens qui remplit une église,\n" +
                            "Ou d’un sachet le musc invétéré ?",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc8", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Une phrase a ne pas trouver. Une phrase a ne pas trouver. Une phrase a ne pas trouver. " +
                            " Une phrase qu'il ne faut pas trouver. Une phrase. Une suite de mots a trouver . " +
                            " Un mot a trouver . " +
                            " Une belle phrase. ",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc9", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Je suis comme le roi d’un pays pluvieux,\n" +
                            "Riche, mais impuissant, jeune et pourtant très-vieux,\n" +
                            "Qui, de ses précepteurs méprisant les courbettes, " +
                            "S’ennuie avec ses chiens comme avec d’autres bêtes. Une phrase.",
                    Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("title", "doc10", Field.Store.YES));
    doc.add(
            newTextField(
                    "body",
                    "Quand le ciel bas et lourd pèse comme un couvercle\n" +
                            "Sur l’esprit gémissant en proie aux longs ennuis,\n" +
                            "Et que de l’horizon embrassant tout le cercle\n" +
                            "Il nous verse un jour noir plus triste que les nuits ; a trouver . Une phrase",
                    Field.Store.YES));
    writer.addDocument(doc);

    System.out.println("-- CLOSE -------------------------------");
    writer.close();
    return writer.getPimIndexInfo();
  }

  private void checkPhraseQuery(IndexSearcher searcher, int ntopdocs, String field, String... terms)
      throws IOException {

    var matchesRef = searcher.search(new PhraseQuery(field, terms), ntopdocs);
    System.out.println("\nRef Searching for " + field + ":" + Arrays.toString(terms));
    System.out.println("Found " + matchesRef.totalHits + " results:");
    for (ScoreDoc m : matchesRef.scoreDocs) {
      System.out.println("Doc:" + m.doc + " score:" + m.score);
    }
    ;
    System.out.println("\nPIM Searching for " + field + ":" + Arrays.toString(terms));
    var matchesPim = searcher.search(new PimPhraseQuery(field, terms).setMaxNumHitsFromDpuSystem(ntopdocs), ntopdocs);
    System.out.println("Found " + matchesPim.totalHits + " results:");
    for (ScoreDoc m : matchesPim.scoreDocs) {
      System.out.println("Doc:" + m.doc + " score:" + m.score);
    }
    ;
    assert compareScoreDocs(matchesPim.scoreDocs, matchesRef.scoreDocs);
  }

  private boolean compareScoreDocs(ScoreDoc[] s1, ScoreDoc[] s2) {

    if (s1.length != s2.length) {
      System.out.println("Different number of matches ! PIM:" + s1.length + " Ref:" + s2.length);
      return false;
    }
    for (int i = 0; i < s1.length; ++i) {
      if (s1[i].doc != s2[i].doc) {
        System.out.println(
            "Different doc ID for match " + i + " ! PIM:" + s1[i].doc + " Ref:" + s2[i].doc);
        return false;
      }
      if (Float.compare(s1[i].score, s2[i].score) != 0) {
        System.out.println(
            "Different scores for match " + i + "! PIM:" + s1[i].score + " Ref:" + s2[i].score);
        return false;
      }
    }
    System.out.println("PIM and Ref MATCH !");
    return true;
  }

  private Analyzer getAnalyzer() {
    return new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }
    };
  }
}
