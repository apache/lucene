<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Designing file formats

## Use little JVM heap

Lucene generally prefers to avoid loading gigabytes of data into the JVM heap.
Could this data be stored in a file and accessed using a
`org.apache.lucene.store.RandomAccessInput` instead?

## Avoid options

One of the hardest problems with file formats is maintaining backward
compatibility. Avoid giving options to the user, and instead let the file
format make decisions based on the information it has. If an expert user wants
to optimize for a specific case, they can write a custom codec and maintain it
on their own.

## How to split the data into files?

Most file formats split the data into 3 files:
 - metadata,
 - index data,
 - raw data.

The metadata file contains all the data that is read once at open time. This
helps on several fronts:
 - One can validate the checksums of this data at open time without significant
   overhead since all data needs to be read anyway, this helps detect
   corruptions early.
 - No need to perform expensive seeks into the index/raw data files at open
   time, one can create slices into these files from offsets that have been
   written into the metadata file.

The index file contains data-structures that help search the raw data. For KD
trees, this would be the inner nodes, for doc values this would be jump tables,
for KNN vectors this would be the HNSW graph structure, for terms this would be
the FST that stores term prefixes, etc. Having it in a separate file from the
data file enables users to do things like `MMapDirectory#setPreload(boolean)`
on these files which are generally rather small and accessed randomly. It is
also convenient at times so that index and raw data can be written on the fly
without buffering all index data into memory.

The raw file contains the data that needs to be retrieved.

Some file formats are simpler, e.g. the compound file format's index is so
small that it can be loaded fully into memory at open time. So it becomes
read-once and can be stored in the same file as metadata.

Some file formats are more complex, e.g. postings have multiple types of data
(docs, freqs, positions, offsets, payloads) that are optionally retrieved, so
they use multiple data files in order not to have to read lots of useless data.

## Don't use too many files

The maximum number of file descriptors is usually not infinite. It's ok to use
multiple files per segment as described above, but this number should always be
small. For instance, it would be a bad practice to use a different file per
field.

## Add codec headers and footers to all files

Use `CodecUtil` to add headers and footers to all files of the index. This
helps make sure that we are opening the right file and differenciate Lucene
bugs from file corruptions.

## Validate checksums of the metadata file when opening a segment

If data has been organized in such a way that the metadata file only contains
read-once data then verifying checksums is very cheap to do and can help detect
corruptions early and in a way that we can give users a meaningful error
message that tells users that their index is corrupt, rather than a confusing
exception that tells them that Lucene tried to read data beyond the end of the
file or anything like that.

## Validate structures of other files when opening a segment

One of the most frequent case of index corruption that we have observed over
the years is file truncation. Verifying that index files have the expected
codec header and a correct structure for the codec footer when opening a
segment helps detect a significant spectrum of cases of corruption.

## Do as many consistency checks as reasonable

It is common for some data to be redundant, e.g. data from the metadata file
might be redundant with information from `FieldInfos`, or all files from the
same file format should have the same version in their codec header. Checking
that these redundant pieces of information are consistent is always a good
idea, as it would make cases of corruption much easier to debug.

## Make sure to not leak files

Be paranoid regarding where exceptions might be thrown and make sure that files
would be closed on all paths. E.g. imagine that opening the data file fails
while the index file is already open, make sure that the index file would also
get closed in that case. Lucene has tests that randomly throw exceptions when
interacting with the `Directory` in order to detect some bugs, but it might
take many runs before randomization triggers the exact case that triggers a
bug.

## Verify checksums upon merges

Merges need to read most if not all input data anyway, so make sure to verify
checksums before starting a merge by calling `checkIntegrity()` on the file
format reader in order to make sure that file corruptions don't get propagated
by merges. All default implementations do this.

## How to make backward-compatible changes to file formats?

See [here](../lucene/backward-codecs/README.md).
