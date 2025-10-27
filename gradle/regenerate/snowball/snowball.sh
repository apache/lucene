#!/usr/bin/env bash

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

# remove this script when problems are fixed
SRCDIR=$1
WWWSRCDIR=$2
PROJECTDIR=$3
DESTDIR="${PROJECTDIR}/src/java/org/tartarus/snowball"
WWWDSTDIR="${PROJECTDIR}/src/resources/org/apache/lucene/analysis/snowball"
TESTDSTDIR="${PROJECTDIR}/src/test/org/apache/lucene/analysis/snowball"

trap 'echo "usage: ./snowball.sh <snowball> <snowball-website> <analysis-common>" && exit 2' ERR
test $# -eq 3

trap 'echo "*** BUILD FAILED ***" $BASH_SOURCE:$LINENO: error: "$BASH_COMMAND" returned $?' ERR
set -eEuo pipefail

# generate stuff with existing makefile, just 'make' will try to do crazy stuff with e.g. python
# and likely fail. so only ask for our specific target.
(cd ${SRCDIR} && make dist_libstemmer_java)

for file in "SnowballStemmer.java" "Among.java" "SnowballProgram.java"; do
  # add license header to files since they have none, otherwise rat will flip the fuck out
  echo "/*" > ${DESTDIR}/${file}
  cat ${SRCDIR}/COPYING >> ${DESTDIR}/${file}
  echo "*/" >> ${DESTDIR}/${file}
  cat ${SRCDIR}/java/org/tartarus/snowball/${file} >> ${DESTDIR}/${file}
done

rm ${DESTDIR}/ext/*Stemmer.java
rm -f ${TESTDSTDIR}/languages.txt
for file in ${SRCDIR}/java/org/tartarus/snowball/ext/*.java; do
  # title-case the classes (fooStemmer -> FooStemmer) so they obey normal java conventions
  base=$(basename $file)
  oldclazz="${base%.*}"
  newclazz=$(echo "$oldclazz" | perl -pe 's/^(.)/\U$1/')
  echo ${newclazz} | sed -e 's/Stemmer//' >> ${TESTDSTDIR}/languages.txt
  cat $file | sed "s/${oldclazz}/${newclazz}/g" > ${DESTDIR}/ext/${newclazz}.java
done

# regenerate stopwords data
rm -f ${WWWDSTDIR}/*_stop.txt
for file in ${WWWSRCDIR}/algorithms/*/stop.txt; do
  language=$(basename $(dirname ${file}))
  cat > ${WWWDSTDIR}/${language}_stop.txt << EOF
| From https://snowballstem.org/algorithms/${language}/stop.txt
| This file is distributed under the BSD License.
| See https://snowballstem.org/license.html
| Also see https://opensource.org/licenses/bsd-license.html
|  - Encoding was converted to UTF-8.
|  - This notice was added.
|
| NOTE: To use this file with StopFilterFactory, you must specify format="snowball"
EOF
# try to confirm its really UTF-8
iconv -f UTF-8 -t UTF-8 $file >> ${WWWDSTDIR}/${language}_stop.txt
done
