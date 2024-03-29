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

ram.flush.mb=flush:32:32
compound=cmpnd:true:false

analyzer=org.apache.lucene.analysis.standard.StandardAnalyzer
directory=FSDirectory

doc.stored=true
doc.tokenized=true
doc.term.vector=false
doc.term.vector.offsets=false
doc.term.vector.positions=false
log.step=2000

work.dir=data
docs.dir=reuters21578

content.source=org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource

query.maker=org.apache.lucene.benchmark.byTask.feeds.ReutersQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

log.queries=true
# -------------------------------------------------------------------------------------
{ "Populate"
        CreateIndex
        { "MAddDocs" AddDoc } : 20000
        ForceMerge(1)
        CloseIndex
}
{ "Rounds"

    ResetSystemSoft
    OpenReader
      { "SrchTrvRetNewRdr" SearchTravRet(10) > : 1000
    CloseReader
    OpenReader
      { "SearchHlgtSameRdr" SearchTravRetHighlight(type[UH]) > : 1000

    CloseReader

    RepSumByPref SearchHlgtSameRdr

    NewRound

} : 2

RepSumByNameRound
RepSumByName
RepSumByPrefRound MAddDocs
