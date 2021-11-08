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

# JRE Version Migration Guide

If possible, use the same JRE major version at both index and search time.
When upgrading to a different JRE major version, consider re-indexing. 

Different Java versions may implement different versions of Unicode,
which will change the way some parts of Lucene treat your text.

An (outdated) example: with Java 1.4, `LetterTokenizer` will split around the 
character U+02C6, but with Java 5 it will not. This is because Java 1.4 
implements Unicode 3, but Java 5 implements Unicode 4.

The version of Unicode supported by Java is listed in the documentation
of java.lang.Character class. For reference, Java versions after Java 11
support the following Unicode versions:

 * Java 11, Unicode 10.0
 * Java 12, Unicode 11.0
 * Java 13, Unicode 12.1
 * Java 15, Unicode 13.0
 * Java 16, Unicode 13.0
 * Java 17, Unicode 13.0

In general, whether you need to re-index largely depends upon the data that
you are searching, and what was changed in any given Unicode version. For example, 
if you are completely sure your content is limited to the "Basic Latin" range
of Unicode, you can safely ignore this. 
