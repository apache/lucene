# Exact Phrase Search

This directory contains benchmarks for the exact phrase search using standard Lucene 
and PIM-Lucene.

These benchmarks run on an UPMEM server with PIM memory.

They use the wikipedia dataset and a set of queries extracted from the luceneutil repo (see queries/requests_phrase_nl.txt).
There are also two small compressed datasets present in the datasets directory.

Please reach us at contact@upmem.com if you would like to get access to UPMEM's cloud to 
execute those benchmarks.


# Setup

Open the Makefile and set appropriately the following variables

INDEX_DIR : where to store the index

DATASET_DIR : where to find the directory containing text files to index

PIM_LUCENE_DIR : path to the PIM Lucene root directory


# Creating the PIM Index

```
make index_dpu
```

Creates the standard Lucene index augmented with the PIM index to be loaded in PIM memory.
The PIM index is splitted in a number of sub-indexes equal to variable NB_DPUS.
Each part will be loaded in a different PIM core so that the search happens in parallel.
The Java code for indexing is very similar to the indexing with standard Lucene (see src/IndexRAMDPU.java).
It uses a PimIndexWriter object instead of the regular IndexWriter object:

```java
        Analyzer analyzer = new StandardAnalyzer();
        Directory indexDirectory = new MMapDirectory(Paths.get(index));
        Directory pimIndexDirectory = new MMapDirectory(Paths.get(index + "/dpu"));
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE);
        // provide a directory for pim index and a pim config to the PimIndexWriter constructor
        PimIndexWriter writer = new PimIndexWriter(indexDirectory, pimIndexDirectory, iwc, new PimConfig(nbDpus, 16));
```

Note: indexing takes long time. If you are using this program in UPMEM's cloud, it is not necessary to perform 
indexing of the wikipedia dataset as the index is already available in a
shared area '/home/shared/usecases/lucene/index_full_wiki_dpu'.

However, it is important to copy the index in the machine's local NVME disk (/scratch area) before running the benchmarks.


# Running Exact Phrase Search using PIM Index

```
make run_dpu_parallel
```

Runs the exact phrase search using the PIM index.
A number of search threads are created based on the variable NB_THREADS.
Each thread executes in parallel a subset of the queries from the query input file (specified through variable QUERIES).
The number of top docs to be reported is set using variable NB_TOPDOCS.

For instance the following command

```
make run_dpu_parallel NB_THREADS=64 NB_TOPDOCS=100 QUERIES=./queries/requests_phrase_nl.txt
```

runs the exact phrase search using PIM index with 64 searcher threads and with 100 top docs.

The code for search is very similar to the code with standard Lucene (see src/SearchWikiDPU.java).

A PimPhraseQuery object is created using a PimPhraseQueryBuilder (instead of using a PhraseQueryBuilder).

The PIM index also needs to be explicitely loaded into PIM memory, as shown in the following code snippet:

```java
    IndexReader reader = DirectoryReader.open(MMapDirectory.open(Paths.get(index)));
    IndexSearcher searcher = new IndexSearcher(reader);
    // load PIM index to PIM directory
    PimSystemManager.setNumAllocDpus(2048);
    PimSystemManager.get().loadPimIndex(MMapDirectory.open(Paths.get(index + "/dpu")));
```

