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

This is a record of changes to the modernized gradle build.

### ```buildOptions``` plugin instead of ```propertyOrDefault``` method

All "propertyOrDefault*" methods have been removed and an external
plugin for configuring various build options is now used. This is
the plugin (blame me for any shortcomings):

https://github.com/carrotsearch/gradle-build-infra#plugin-comcarrotsearchgradlebuildinfrabuildoptionsbuildoptionsplugin

this plugin will source the value of a given build option from
several places, in order:
- system property (```-Dfoo=value```),
- gradle property (```-Pfoo=value```),
- environment variable (```foo=value ./gradlew ...```)
- a *non-versioned*, ```build-options.local.properties``` property file
  (for your personal, local tweaks),
- a *versioned* ```build-options.properties``` property file.

It works much like before - you can override any build option
temporarily by passing, for example, ```-Ptests.verbose=true```. But you
can also make such changes locally persistent by placing them
in your ```build-options.local.properties``` file. Alternatively,
the generated ```gradle.properties``` is also supported since it declares
project properties (equivalent to ```-Pkey=value```). At some point
in the future, we may want to keep gradle.properties versioned though,
so it's probably a good idea to switch over to 
```build-options.local.properties```.

The biggest gain from using build options is that you can now see
all declared options and their values, including their source (where their
value comes from). To see what I mean, try running these:
```
./gradlew :buildOptions
./gradlew -p lucene/core buildOptions
```

### Lucene 'base version' string moved

* Various Lucene's "version" properties are now full build options (as described above).
Their default values are in ```build-options.properties```. You can override
any of ```version.suffix```, ```version.base``` or ```version.release```
using any of the build option plugin's methods. If you'd like to bump
Lucene's base version, it now lives in the versioned ```build-options.properties```.

### Tweaks to properties and options

* ```runtime.java.home``` is a build option now but the support for
```RUNTIME_JAVA_HOME``` env. variable has been implemented for backward
compatibility.

* ```tests.neverUpToDate``` option is renamed ```tests.rerun```.
The value of ```true``` indicates test tasks will re-run even if 
nothing has changed.

### Changes to project structure

* ```build-tools/missing-doclet``` is now a regular project within Lucene (not
an included composite project). This simplifies the build and ensures we apply
the same checks to this subproject as we do to everything else.

* all gradle build scripts are converted to convention plugins (in groovy) and
live under ```dev-tools/build-infra/src/main/groovy```. The long-term
plan is to move some or all of these groovy plugins to Java (so that the code is
easier to read and debug for non-groovians).

* ```tidy``` is now applied to groovy/gradle scripts (formatting is enforced). 

### Security manager support

* parts of the build supporting security manager settings have been just
removed, without any replacement.

### Other notable changes

* The legacy ```precommit``` task has been removed; use gradle's ```check```.

* Removed dependency on jgit entirely. This is replaced by forking the system's git
in porcelain mode (which should be stable and portable). This logic is implemented
in [this plugin](https://github.com/carrotsearch/gradle-build-infra/?tab=readme-ov-file#plugin-comcarrotsearchgradlebuildinfraenvironmentgitinfoplugin).
An additional benefit is that all features of git should now work (including worktrees).

* Added support for owasp API keys in the form of ```validation.owasp.apikey``` build option. Owasp check is
still very, very slow. We should probably just drop it.

* I've changed the default on ```gradle.ge``` (Gradle Enterprise, develocity) to ```false```
on non-CI builds.

* I've tried to clean up lots and lots of gradle/groovy code related to eager initialization of
tasks as well as update deprecated APIs. This isn't always straightforward and I might have broken
some things...

### Fixes to existing issues

* ```gradlew clean check``` will work now (https://github.com/apache/lucene/issues/13567) 
