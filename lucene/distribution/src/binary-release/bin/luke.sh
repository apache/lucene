#!/bin/sh

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

MODULES=`dirname "$0"`/..
MODULES=`cd "$MODULES" && pwd`

# check for overridden launch command (for use in integration tests), otherwise
# use the default.
if [ -z "$LAUNCH_CMD" ]; then
  LAUNCH_CMD=java
  LAUNCH_OPTS=
else
  # We are integration-testing. Force UTF-8 as the encoding.
  LAUNCH_OPTS=-Dfile.encoding=UTF-8
  # check if Xvfb is available
  if command -v xvfb-run > /dev/null 2>&1; then
    LAUNCH_OPTS="$LAUNCH_CMD $LAUNCH_OPTS"
    LAUNCH_CMD="xvfb-run"
  fi
fi

"$LAUNCH_CMD" $LAUNCH_OPTS --module-path "$MODULES/modules:$MODULES/modules-thirdparty" --module org.apache.lucene.luke "$@"
exit $?
