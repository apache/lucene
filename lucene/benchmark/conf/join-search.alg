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
# Compares 1:M skus->products join algorithms. Build indices with join-index.alg first.
#   term      - JoinUtil, SortedDocValues FK -> PK terms           (products, skus)
#   numeric   - JoinUtil, NumericDocValues FK -> PK LongPoint      (products, skus)
#   globalOrd - JoinUtil, global ordinals over SortedDocValues FK  (mixed)
#   block     - ToParentBlockJoinQuery                             (block)
# Run: ./gradlew -p lucene/benchmark run -PtaskAlg=conf/join-search.alg -PmaxHeapSize=4G

join.dir=work/join
join.scoreMode=None
# segment parallel search threads, 0 - sequential; term and numeric collect the from side
# sequentially anyway (JoinUtil)
join.searchThreads=4
# -------------------------------------------------------------------------------------

{ "Warmup"
    { JoinSearch(term) JoinSearch(numeric) JoinSearch(globalOrd) JoinSearch(block) } : 100
}
ClearStats

{ "Rounds"
    { "term" JoinSearch(term) > : 500
    { "numeric" JoinSearch(numeric) > : 500
    { "globalOrd" JoinSearch(globalOrd) > : 500
    { "block" JoinSearch(block) > : 500
    NewRound
} : 3

RepSumByNameRound
RepSumByName
