# 
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 

#dpu-clang -I ../inc/ test_index_basic.c ../src/decoder.c ../src/matcher.c ../src/parser.c ../src/query_parser.c ../src/term_lookup.c -o test_index_basic
#dpu-clang -I ../inc/ test_index_moretext.c ../src/decoder.c ../src/matcher.c ../src/parser.c ../src/query_parser.c ../src/term_lookup.c -o test_index_moretext
dpu-clang -g -DTEST1 -DSTACK_SIZE_DEFAULT=512 -DNR_TASKLETS=16 -I ../inc/ ../src/dpu.c ../src/decoder.c ../src/matcher.c ../src/parser.c ../src/query_parser.c ../src/term_lookup.c ../src/postings_util.c -o test_phrase
clang -O3 test.c -o test -I/usr/include/dpu -ldpu
./test
