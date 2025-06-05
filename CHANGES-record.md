This is a record of changes to the modernized gradle build.

### ```buildOptions``` plugin instead of ```propertyOrDefault``` method

The family of "propertyOrDefault*" methods has been removed and an external
plugin for configuring various build options is now used. This is
the plugin (blame me for any shortcomings):

https://github.com/carrotsearch/gradle-build-infra#plugin-comcarrotsearchgradlebuildinfrabuildoptionsbuildoptionsplugin

this plugin will source the given build option from several places, in
order:
- system property (```-Dfoo=value```),
- gradle property (```-Pfoo=value```),
- environment variable (```foo=value ./gradlew ...```)
- a *not versioned*, ```build-options.local.properties``` property file
  (for your personal, local tweaks),
- a *versioned* ```build-options.properties``` property file.

It works much like before - you can override any build option
temporarily by passing, for example, ```-Ptests.verbose=true```. But you
can also make such changes locally persistent by placing them
in your ```build-options.local.properties``` file. Alternatively,
the generated ```gradle.properties``` is also supported since it can declare
project properties (like ```-Pkey=value```).


The biggest gain from using this plugin is that you can now see
all options and their values, including their source (where their
value is defined). For example, try this:
```
./gradlew :buildOptions
./gradlew -p lucene/core buildOptions
```

### Lucene base version string moved

* Various Lucene's version properties are now full build options.
Their default values are in ```build-options.properties```. You can override
any of ```version.suffix```, ```version.base``` or ```version.release```
using any of the build option plugin's methods (see above).

### Tweaks to properties and options

* ```runtime.java.home``` is a build option now but the support for
```RUNTIME_JAVA_HOME``` env. variable has been implemented for backward
compatibility.

* ```tests.neverUpToDate``` option is renamed ```tests.rerun```; value of
  ```true``` indicates test tasks will re-run even if nothing has changed.

### Changes to project structure

* ```build-tools/missing-doclet``` is now a regular project within Lucene (not
an included composite project). This simplifies the build and ensures we apply
the same checks to this subproject as we do to everything else.

* all gradle build scripts are converted to convention plugins (in groovy) and
live under ```dev-tools/build-infra/src/main/groovy```. The long-term
plan is to move some of these groovy plugins to Java (so that the code is
easier to read for non-groovians).

* ```tidy``` is now applied to groovy/gradle scripts (formatting is
enforced). 

### Security manager support

* parts of the build supporting security manager settings have been just
removed, without any replacement.

### Other notable changes

* The legacy ```precommit``` task has been removed; use gradle's ```check```.

* Removed dependency on jgit entirely. This is replaced by forking the system's git
in porcelain mode (which should be stable and portable). This logic is implemented
in [this plugin](https://github.com/carrotsearch/gradle-build-infra/?tab=readme-ov-file#plugin-comcarrotsearchgradlebuildinfraenvironmentgitinfoplugin).
An additional benefit is that all features of git should now work (including worktrees).

### Fixes to existing issues

* ```gradlew clean check``` will work now (https://github.com/apache/lucene/issues/13567) 