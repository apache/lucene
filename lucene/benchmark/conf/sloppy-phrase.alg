#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.

max.buffered=100
merge.factor=10
compound=true

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory
#directory=ByteBuffersDirectory

doc.stored=false
doc.tokenized=true
doc.term.vector=false
log.step=500

work.dir=data
docs.dir=reuters21578
#docs.dir=reuters-111

content.source=org.apache.lucene.benchmark.byTask.feeds.SingleDocSource
#content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleSloppyPhraseQueryMaker
#query.maker=org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker
#query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=1

log.queries=false
# -------------------------------------------------------------------------------------


ResetSystemErase

{ "Populate"
    CreateIndex
    { "MAddDocs" AddDoc(2000) > : 20000
    ForceMerge(1)
    CloseIndex
}


{ "Round"

  OpenReader
  { "SearchSameRdr" Search > : 6000
  CloseReader

  ResetInputs
  RepSumByName
  NewRound
} : 4

RepSumByPrefRound MAddDocs

RepSumByName
RepSumByPrefRound Search
