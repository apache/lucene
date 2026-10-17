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
# Incremental updates interleaved with join searches. Build indices with join-index.alg first.
# Each round overrides existing products, skus or both (join.update.side), then searches;
# block rewrites the whole block on every update (update amplification).
# Updates are never committed, so the indices stay intact for reruns.
# Run: ./gradlew -p lucene/benchmark run -PtaskAlg=conf/join-update.alg -PmaxHeapSize=4G

# must match join-index.alg
doc.maker=org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker
join.products=1000000
join.skusPerProduct=10

join.dir=work/join
join.scoreMode=None
join.update.side=side:products:skus:both
join.update.batch=100
# -------------------------------------------------------------------------------------

{ "Warmup"
    { JoinUpdate(split) JoinSearch(term) JoinSearch(numeric) } : 10
    { JoinUpdate(mixed) JoinSearch(globalOrd) } : 10
    { JoinUpdate(block) JoinSearch(block) } : 10
}
ClearStats

# cold-* is the first search after each update batch, i.e. it pays for per-searcher caches
# (global ordinals, block join parent bitsets); warm-* are the next ones
{ "Rounds"
    { "split"
        { "update-split" JoinUpdate(split) >
        { "cold-term" JoinSearch(term) >
        { "cold-numeric" JoinSearch(numeric) >
        { "warm-term" JoinSearch(term) > : 2
        { "warm-numeric" JoinSearch(numeric) > : 2
    } : 50
    { "mixed"
        { "update-mixed" JoinUpdate(mixed) >
        { "cold-globalOrd" JoinSearch(globalOrd) >
        { "warm-globalOrd" JoinSearch(globalOrd) > : 2
    } : 50
    { "block"
        { "update-block" JoinUpdate(block) >
        { "cold-block" JoinSearch(block) >
        { "warm-block" JoinSearch(block) > : 2
    } : 50
    NewRound
} : 3

RepSumByNameRound
RepSumByName
RepSumByPrefRound update-
RepSumByPrefRound cold-
RepSumByPrefRound warm-
