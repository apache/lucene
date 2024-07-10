<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# PIM Lucene

![UPMEM Logo](https://sdk.upmem.com/2023.2.0/_static/UPMEM.png)
![Lucene Logo](https://lucene.apache.org/theme/images/lucene/lucene_logo_green_300.png?v=0e493d7a)

Apache Lucene is a high-performance, full-featured text search engine library
written in Java.

PIM-lucene is a project to create an extension of Lucene to offload specific queries to UPMEM’s PIM (Processing In Memory) hardware.

UPMEM is a French company proposing a PIM product which can accelerate data-intensive applications.
The PIM hardware is a DIMM module in which each memory chip embed small processors with fast access to the memory bank.
More information about UPMEM is available on the [company website](https://www.upmem.com/) and in the [UPMEM's SDK documentation](https://sdk.upmem.com/2023.2.0/).

Our goal is to create a non-intrusive extension of the Lucene code base, providing an option
to use PIM for specific queries (or part of queries) without impacting Lucene's performance or functionality.
When using the PIM extension, the standard Lucene index is created but a new index specific to PIM is also created and stored in the PIM system. 
A PimIndexWriter object is the new interface for writing the Lucene index augmented with the PIM index.

The first query being ported to PIM is the phrase query. 
A PimPhraseQuery object can be used in place of a PhraseQuery object
in order to use PIM to execute the query. When using a PimPhraseQuery, the system may or may not execute the query using PIM (e.g., depending on the PIM system availability, the PIM load vs CPU load).

## Project Status

This project is currently under development. 
The implementation of the PimPhraseQuery is functional and the current performance (QPS) when compared to standard Lucene is reported in the benchmarks' section.
The next step is to improve the score's lower bound computation to reduce the work imbalance between the PIM cores.

## Building

### Basic steps:
  
1. Install OpenJDK 17 or 18.
2. Clone PIM Lucene's git repository.
3. Run git submodule update --init.
4. Make sure cunit is installed on your system (sudo apt install libcunit1-dev).
5. Run gradle launcher script (`gradlew`).

We'll assume that you know how to get and set up the JDK - if you don't, then we suggest starting at https://jdk.java.net/ and learning more about Java, before returning to this README.

## Benchmarks

### Benchmarking Setup

The machine used has the following characteristics:

<img src="./pictures/bsetup.png" alt="drawing" width="600"/>

The dataset is the english wikipedia dataset, and the set of queries consist in 1036 phrase queries extracted from the luceneutil repository.
The setup and details of the benchmarks are found [here](https://github.com/upmem/pim-lucene-private/tree/main/lucene/pim-benchmarks/exact_phrase).
Both standard Lucene and PIM-Lucene are run on the same server.

### Results

The speedup in throughput (QPS) for various number of search threads and top docs is as follows:

<img src="./pictures/bresults.png" alt="drawing" width="600"/>
