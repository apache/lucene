# Typical workflow and tasks

This shows some typical workflow gradle commands.

Ensure your changes are correctly formatted (run `gradlew :helpFormatting` for more):

```shell
gradlew tidy
```

Run tests on a module:

```shell
gradlew -p lucene/core test
```

Run test of a single-class (run `gradlew :helpTests` for more):

```shell
gradlew -p lucene/core test --tests "*Demo*"
```

Run all tests and validation checks on a module:

```shell
gradlew -p lucene/core check
```

Run all tests and validation checks on everything:

```shell
gradlew check
```

Run all validation checks but skip all tests:

```shell
gradlew check -x test
```

Assemble a single module's JAR (here for lucene-core):

```shell
gradlew -p lucene/core assemble
ls lucene/core/build/libs
```

Assemble all Lucene artifacts (JARs, and so on):

```shell
gradlew assemble
```

Create all distributable packages, POMs, etc. and create a local maven repository for inspection:

```shell
gradlew mavenLocal
ls -R build/maven-local/
```

Assemble Javadocs on a module:

```shell
gradlew -p lucene/core javadoc
ls lucene/core/build/docs
```

Assemble entire documentation (including javadocs):

```shell
gradlew documentation
ls lucene/documentation/build/site
```
