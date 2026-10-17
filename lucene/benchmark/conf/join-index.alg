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
# Builds the 1:M products/skus indices for join-search.alg and join-update.alg, one layout per round:
#   products, skus - one side per index;   mixed - both sides in one index;   block - skus+product blocks
# Run: ./gradlew -p lucene/benchmark run -PtaskAlg=conf/join-index.alg -PmaxHeapSize=4G

doc.maker=org.apache.lucene.benchmark.byTask.feeds.JoinDocMaker
# 1/100 scale for a quick run; for the full scale (1M products, 10M skus) set
# join.products=1000000 here and in join-update.alg, then re-run this alg
join.products=10000
join.skusPerProduct=10
join.layout=layout:products:skus:mixed:block
work.dir=dir:work/join/products:work/join/skus:work/join/mixed:work/join/block

directory=FSDirectory
compound=false
ram.flush.mb=64
log.step=100000
# -------------------------------------------------------------------------------------

{ "Rounds"
    ResetSystemErase
    { "Populate"
        CreateIndex
        [{ "AddDocs" AddJoinDocs > : * ] : 4
        CloseIndex
    }
    NewRound
} : 4

RepSumByPrefRound AddDocs
