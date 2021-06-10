#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

LUKE_HOME=$(cd $(dirname $0) && pwd)
cd ${LUKE_HOME}

JAVA_OPTIONS="${JAVA_OPTIONS} -Xmx1024m -Xms512m -XX:MaxMetaspaceSize=256m"

CLASSPATHS="./*:./lib/*:../core/*:../codecs/*:../backward-codecs/*:../queries/*:../queryparser/*:../suggest/*:../misc/*"
for dir in `ls ../analysis`; do
  CLASSPATHS="${CLASSPATHS}:../analysis/${dir}/*:../analysis/${dir}/lib/*"
done

LOG_DIR=${HOME}/.luke.d/
 if [[ ! -d ${LOG_DIR} ]]; then
   mkdir ${LOG_DIR}
 fi

nohup java -cp ${CLASSPATHS} ${JAVA_OPTIONS} org.apache.lucene.luke.app.desktop.LukeMain > ${LOG_DIR}/luke_out.log 2>&1 &
