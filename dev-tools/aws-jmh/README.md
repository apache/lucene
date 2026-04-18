<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# EC2 Microbenchmarks

Runs lucene microbenchmarks across a variety of CPUs in EC2.

Example:

    export AWS_ACCESS_KEY_ID=xxxxx
    export AWS_SECRET_ACCESS_KEY=yyyy
    make PATCH_BRANCH=rmuir:some-speedup

Results file will be in build/report.txt

You can also pass additional JMH args if you want:

    make PATCH_BRANCH=rmuir:some-speedup JMH_ARGS='float -p size=756'

Prerequisites:

1. It is expected that you have an ed25519 ssh key, use `ssh-keygen -t ed25519` to make one.
2. AWS key's IAM user needs `AmazonEC2FullAccess` and `AWSCloudFormationFullAccess` permissions at a minimum.
