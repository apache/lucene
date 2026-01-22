/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.gradle.plugins.java;

import com.google.errorprone.scanner.BuiltInCheckerSuppliers;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.ltgt.gradle.errorprone.CheckSeverity;
import net.ltgt.gradle.errorprone.ErrorProneOptions;
import org.apache.lucene.gradle.plugins.LuceneGradlePlugin;
import org.apache.lucene.gradle.plugins.misc.CheckEnvironmentPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.ExtensionAware;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.compile.JavaCompile;

/**
 * Applies Google's error-prone project for additional linting.
 *
 * @see "https://github.com/google/error-prone"
 */
public class ErrorPronePlugin extends LuceneGradlePlugin {
  private static final String TASK_ERROR_PRONE_SKIPPED = "errorProneSkipped";
  public static final String OPT_VALIDATION_ERRORPRONE = "validation.errorprone";

  public static class ErrorPronePluginRootExtPlugin extends LuceneGradlePlugin {

    public abstract static class ErrorProneStatus {
      public abstract Property<Boolean> getEnabled();
    }

    @Override
    public void apply(Project project) {
      applicableToRootProjectOnly(project);

      // Ensure internal consistency with error-prone.
      var verifyErrorProneCheckListSanity =
          project
              .getTasks()
              .register(
                  "verifyErrorProneCheckListSanity",
                  t -> {
                    t.doFirst(
                        _ -> {
                          ensureCheckListIsComplete();
                        });
                  });
      project
          .getTasks()
          .named("check")
          .configure(t -> t.dependsOn(verifyErrorProneCheckListSanity));

      // check if error-prone should be hooked up to javac or not.
      var buildGlobals = getLuceneBuildGlobals(project);
      Provider<Boolean> errorproneOption =
          getBuildOptions(project)
              .addBooleanOption(
                  OPT_VALIDATION_ERRORPRONE,
                  "Applies error-prone for additional source code linting.",
                  buildGlobals.isCIBuild);

      var status = project.getExtensions().create("errorProneStatus", ErrorProneStatus.class);

      String skipReason;
      if (project
          .getExtensions()
          .getByType(AlternativeJdkSupportPlugin.AltJvmExtension.class)
          .getAltJvmUsed()
          .get()) {
        // LUCENE-9650: Errorprone does not work when running as a plugin inside a forked Javac
        // process.
        skipReason = "won't work with alternative java toolchain";
      } else {
        if (errorproneOption.get()) {
          // Enabled and fine to run.
          skipReason = null;
        } else {
          if (buildGlobals.isCIBuild) {
            skipReason = "skipped explicitly on a CI build it seems";
          } else {
            skipReason =
                "skipped on non-CI environments, pass "
                    + ("-P" + OPT_VALIDATION_ERRORPRONE + "=true")
                    + " to enable";
          }
        }
      }

      if (skipReason != null) {
        project
            .getTasks()
            .register(
                TASK_ERROR_PRONE_SKIPPED,
                task -> {
                  boolean hasCheckTask =
                      project.getGradle().getStartParameter().getTaskNames().contains("check");

                  task.onlyIf(
                      _ -> {
                        // Only complain if we're running a 'check'.
                        return hasCheckTask;
                      });

                  task.doFirst(
                      t -> {
                        t.getLogger().warn("Errorprone linting turned off ({})", skipReason);
                      });
                });
      }

      status.getEnabled().set(skipReason == null && errorproneOption.get());
    }

    // double check we have all the checks from the current version of errorprone covered.
    private void ensureCheckListIsComplete() {
      Set<String> allCanonicalNames = new TreeSet<>();
      Map<String, String> altToCanonical = new HashMap<>();

      Stream.of(
              BuiltInCheckerSuppliers.ENABLED_ERRORS,
              BuiltInCheckerSuppliers.ENABLED_WARNINGS,
              BuiltInCheckerSuppliers.DISABLED_CHECKS)
          .flatMap(Collection::stream)
          .forEach(
              bugCheckerInfo -> {
                var canonicalName = bugCheckerInfo.canonicalName();
                allCanonicalNames.add(canonicalName);
                bugCheckerInfo
                    .allNames()
                    .forEach(
                        name -> {
                          if (!name.equals(canonicalName)) {
                            altToCanonical.put(name, canonicalName);
                          }
                        });
              });

      var currentChecks = ALL_CHECKS_PARSED.keySet();

      Set<String> missingChecks = new TreeSet<>(currentChecks);
      missingChecks.removeAll(allCanonicalNames);
      String pluginClassName = ErrorPronePlugin.class.getName();

      if (!missingChecks.isEmpty()) {
        throw new GradleException(
            String.format(
                Locale.ROOT,
                "Verify the list of error-prone checks in '%s', there are unknown checks declared there:\n%s",
                pluginClassName,
                missingChecks.stream()
                    .map(
                        name ->
                            "  - "
                                + (altToCanonical.containsKey(name)
                                    ? name
                                        + " (canonical check name is '"
                                        + altToCanonical.get(name)
                                        + "')"
                                    : name))
                    .collect(Collectors.joining("\n"))));
      }

      Set<String> newChecks = new TreeSet<>(allCanonicalNames);
      newChecks.removeAll(currentChecks);
      if (!newChecks.isEmpty()) {
        throw new GradleException(
            String.format(
                Locale.ROOT,
                "Verify the list of error-prone checks in '%s', there are new checks in "
                    + "error-prone that are not declared there:\n%s",
                pluginClassName,
                newChecks.stream()
                    .map(
                        name ->
                            "  - "
                                + name
                                + ", docs at: "
                                + "https://errorprone.info/bugpattern/"
                                + name)
                    .collect(Collectors.joining("\n"))));
      }
    }
  }

  @Override
  public void apply(Project project) {
    requiresAppliedPlugin(project, JavaPlugin.class);

    if (project != project.getRootProject()) {
      project.getRootProject().getPlugins().apply(ErrorPronePluginRootExtPlugin.class);
    }

    boolean errorProneEnabled =
        project
            .getRootProject()
            .getExtensions()
            .getByType(ErrorPronePluginRootExtPlugin.ErrorProneStatus.class)
            .getEnabled()
            .get();

    if (errorProneEnabled) {
      configureErrorProne(project);
    } else {
      project
          .getTasks()
          .withType(JavaCompile.class)
          .configureEach(
              t -> {
                t.dependsOn(":" + TASK_ERROR_PRONE_SKIPPED);
              });

      // The errorprone plugin automatically adds test classpath entries.
      // We need to add it here manually so that versions.lock is consistent with or without
      // the errorprone plugin.
      var errorproneConfiguration = project.getConfigurations().create("errorprone");
      project
          .getConfigurations()
          .getByName("annotationProcessor")
          .extendsFrom(errorproneConfiguration);

      applyLocalDependency(project);
    }
  }

  /**
   * List of enabled/disabled checks.
   *
   * <p>Please keep this synced with <a href="https://errorprone.info/bugpatterns">bugpatterns</a>
   * when upgrading. Do <em>NOT</em> enable checks based on their name or description. Read the
   * source code and make sure they are useful! Most error-prone checks are not useful for
   * non-google software.
   *
   * @see "https://errorprone.info/bugpatterns"
   */
  private static final String[] ALL_CHECKS = {
    "ASTHelpersSuggestions:OFF", // we don't use ASTHelpers
    "AddNullMarkedToClass:OFF", // we don't use jspecify
    "AddNullMarkedToPackageInfo:OFF", // we don't use jspecify
    "AddressSelection:OFF", // TODO: new, not checked if applicable to Lucene
    "AlmostJavadoc:OFF", // noisy (e.g. commented-out code misinterpreted as javadocs)
    "AlreadyChecked:OFF", // TODO: there are problems
    "AlwaysThrows:OFF", // we don't use guava
    "AmbiguousMethodReference:OFF",
    "AndroidInjectionBeforeSuper:OFF", // we don't use android
    "AnnotateFormatMethod:OFF", // we don't use this annotation
    "AnnotationMirrorToString:OFF", // TODO: new, not checked if applicable to Lucene
    "AnnotationPosition:OFF", // TODO: new, not checked if applicable to Lucene
    "AnnotationValueToString:OFF", // TODO: new, not checked if applicable to Lucene
    "ArgumentSelectionDefectChecker:OFF", // noisy
    "ArrayAsKeyOfSetOrMap:OFF", // TODO: there are problems
    "ArrayEquals:ERROR",
    "ArrayFillIncompatibleType:ERROR",
    "ArrayHashCode:ERROR",
    "ArrayRecordComponent:OFF", // TODO: there are problems
    "ArrayToString:ERROR",
    "ArraysAsListPrimitiveArray:OFF", // we don't use guava
    "AssertEqualsArgumentOrderChecker:WARN",
    "AssertFalse:OFF", // TODO: new, not checked if applicable to Lucene
    "AssertThrowsMultipleStatements:WARN",
    "AssertionFailureIgnored:OFF", // TODO: there are problems
    "AssertSameIncompatible:WARN",
    "AssignmentExpression:OFF", // TODO: there are problems
    "AssistedInjectAndInjectOnConstructors:OFF", // TODO: new, not checked if applicable to Lucene
    "AssistedInjectAndInjectOnSameConstructor:OFF", // we don't use this annotation
    "AsyncCallableReturnsNull:OFF", // we don't use guava
    "AsyncFunctionReturnsNull:OFF", // we don't use guava
    "AttemptedNegativeZero:WARN",
    "AutoFactoryAtInject:OFF", // TODO: new, not checked if applicable to Lucene
    "AutoValueBoxedValues:OFF", // we don't use autovalue
    "AutoValueBuilderDefaultsInConstructor:OFF", // we don't use autovalue
    "AutoValueConstructorOrderChecker:OFF", // we don't use autovalue
    "AutoValueFinalMethods:OFF", // we don't use autovalue
    "AutoValueImmutableFields:OFF", // we don't use autovalue
    "AutoValueSubclassLeaked:OFF", // we don't use autovalue
    "AvoidObjectArrays:OFF", // TODO: new, not checked if applicable to Lucene
    "BadAnnotationImplementation:ERROR",
    "BadComparable:WARN",
    "BadImport:OFF", // TODO: there are problems
    "BadInstanceof:OFF", // TODO: there are problems
    "BadShiftAmount:ERROR",
    "BanClassLoader:OFF", // implemented with forbidden APIs instead
    "BanJNDI:OFF", // implemented with forbidden APIs instead
    "BanSerializableRead:OFF", // TODO: new, not checked if applicable to Lucene
    "BareDotMetacharacter:WARN",
    "BigDecimalEquals:OFF", // BigDecimal barely used, use forbidden-apis for this
    "BigDecimalLiteralDouble:OFF", // BigDecimal barely used, use forbidden-apis for this
    "BinderIdentityRestoredDangerously:OFF", // TODO: new, not checked if applicable to Lucene
    "BindingToUnqualifiedCommonType:OFF", // TODO: new, not checked if applicable to Lucene
    "BooleanLiteral:ERROR",
    "BooleanParameter:OFF", // TODO: new, not checked if applicable to Lucene
    "BoxedPrimitiveConstructor:OFF", // we have forbiddenapis for that
    "BoxedPrimitiveEquality:OFF", // TODO: there are problems
    "BugPatternNaming:OFF", // we don't use this annotation
    "BuilderReturnThis:OFF", // TODO: new, not checked if applicable to Lucene
    "BundleDeserializationCast:OFF", // we don't use android
    "ByteBufferBackingArray:OFF",
    "CacheLoaderNull:OFF", // we don't use guava
    "CanIgnoreReturnValueSuggester:OFF", // TODO: new, not checked if applicable to Lucene
    "CannotMockFinalClass:OFF", // TODO: new, not checked if applicable to Lucene
    "CannotMockMethod:OFF", // TODO: new, not checked if applicable to Lucene
    "CanonicalDuration:OFF", // barely use Duration.of (one test), just a style thing
    "CatchAndPrintStackTrace:OFF", // noisy
    "CatchFail:OFF", // TODO: there are problems
    "CatchingUnchecked:OFF", // TODO: new, not checked if applicable to Lucene
    "ChainedAssertionLosesContext:OFF", // we don't use truth
    "ChainingConstructorIgnoresParameter:ERROR",
    "CharacterGetNumericValue:OFF", // noisy
    "CheckNotNullMultipleTimes:OFF", // we don't use guava
    "CheckReturnValue:OFF", // we don't use these annotations
    "CheckedExceptionNotThrown:OFF", // TODO: new, not checked if applicable to Lucene
    "ClassCanBeStatic:OFF", // noisy
    "ClassInitializationDeadlock:ERROR",
    "ClassName:OFF", // TODO: new, not checked if applicable to Lucene
    "ClassNamedLikeTypeParameter:OFF", // TODO: new, not checked if applicable to Lucene
    "ClassNewInstance:WARN",
    "CloseableProvides:OFF", // we don't use this annotation
    "ClosingStandardOutputStreams:WARN",
    "CollectionIncompatibleType:OFF", // TODO: new, not checked if applicable to Lucene
    "CollectionToArraySafeParameter:ERROR",
    "CollectionUndefinedEquality:OFF", // TODO: there are problems
    "CollectorShouldNotUseState:WARN",
    "ComparableAndComparator:WARN",
    "ComparableType:OFF",
    "CompareToZero:WARN",
    "ComparingThisWithNull:OFF", // we use ast-grep for this check
    "ComparisonContractViolated:OFF", // TODO: new, not checked if applicable to Lucene
    "ComparisonOutOfRange:ERROR",
    "CompatibleWithAnnotationMisuse:OFF", // we don't use this annotation
    "CompileTimeConstant:OFF", // we don't use this annotation
    "ComplexBooleanConstant:OFF", // TODO: there are problems
    "ComputeIfAbsentAmbiguousReference:ERROR",
    "ConditionalExpressionNumericPromotion:ERROR",
    "ConstantField:OFF", // TODO: new, not checked if applicable to Lucene
    "ConstantOverflow:ERROR",
    "ConstantPatternCompile:OFF", // TODO: new, not checked if applicable to Lucene
    "DaggerProvidesNull:OFF", // we don't use dagger
    "DangerousLiteralNull:ERROR",
    "DateChecker:OFF", // we don't use these Date setters/ctors
    "DateFormatConstant:OFF", // we don't use Date setters
    "DeadException:ERROR",
    "DeadThread:ERROR",
    "DeduplicateConstants:OFF", // TODO: new, not checked if applicable to Lucene
    "DeeplyNested:WARN",
    "DefaultCharset:OFF", // we have forbiddenapis for that
    "DefaultLocale:OFF", // we have forbiddenapis for that
    "DefaultPackage:OFF",
    "DepAnn:OFF", // TODO: new, not checked if applicable to Lucene
    "DeprecatedVariable:WARN",
    "DereferenceWithNullBranch:ERROR",
    "DifferentNameButSame:OFF", // TODO: new, not checked if applicable to Lucene
    "DirectInvocationOnMock:OFF", // we don't use mocking libraries
    "DiscardedPostfixExpression:ERROR",
    "DistinctVarargsChecker:OFF", // we don't use google collections
    "DoNotCall:OFF", // we don't use this annotation
    "DoNotCallSuggester:OFF", // we don't use this annotation
    "DoNotClaimAnnotations:OFF", // we don't use annotation processors
    "DoNotMock:OFF", // we don't use mocking libraries
    "DoNotMockAutoValue:OFF", // we don't use autovalue
    "DoubleBraceInitialization:OFF", // we don't use guava
    "DoubleCheckedLocking:OFF", // TODO: there are problems
    "DuplicateAssertion:ERROR",
    "DuplicateBranches:ERROR",
    "DuplicateDateFormatField:WARN",
    "DuplicateMapKeys:ERROR",
    "DurationFrom:OFF", // we don't use Duration.from()
    "DurationGetTemporalUnit:OFF", // we don't use Duration.get()
    "DurationTemporalUnit:OFF", // we don't use Duration.of() etc
    "DurationToLongTimeUnit:OFF", // we don't use TimeUnit.convert Duration, etc
    "EffectivelyPrivate:OFF", // TODO: many violations of this.
    "EmptyBlockTag:OFF", // ECJ takes care
    "EmptyCatch:OFF", // ECJ takes care
    "EmptyIf:OFF", // TODO: new, not checked if applicable to Lucene
    "EmptySetMultibindingContributions:OFF", // we don't use this annotation
    "EmptyTopLevelDeclaration:OFF", // noisy
    "EnumOrdinal:OFF", // noisy
    "EqualsBrokenForNull:OFF", // TODO: new, not checked if applicable to Lucene
    "EqualsGetClass:OFF", // noisy
    "EqualsHashCode:ERROR",
    "EqualsIncompatibleType:OFF",
    "EqualsMissingNullable:OFF", // TODO: new, not checked if applicable to Lucene
    "EqualsNaN:ERROR",
    "EqualsNull:ERROR",
    "EqualsReference:ERROR",
    "EqualsUnsafeCast:OFF", // noisy
    "EqualsUsingHashCode:WARN",
    "EqualsWrongThing:ERROR",
    "ErroneousBitwiseExpression:WARN",
    "ErroneousThreadPoolConstructorChecker:WARN",
    "EscapedEntity:OFF",
    "ExpectedExceptionChecker:OFF", // TODO: new, not checked if applicable to Lucene
    "ExpensiveLenientFormatString:OFF", // we use astgrep for this check.
    "ExplicitArrayForVarargs:OFF", // TODO: new, not checked if applicable to Lucene
    "ExtendingJUnitAssert:OFF", // noisy
    "ExtendsAutoValue:OFF", // TODO: new, not checked if applicable to Lucene
    "ExtendsObject:OFF", // TODO: there are problems
    "FallThrough:OFF", // TODO: there are problems
    "FieldCanBeFinal:OFF", // TODO: new, not checked if applicable to Lucene
    "FieldCanBeLocal:OFF", // TODO: new, not checked if applicable to Lucene
    "FieldCanBeStatic:OFF", // TODO: new, not checked if applicable to Lucene
    "FieldMissingNullable:OFF", // TODO: new, not checked if applicable to Lucene
    "Finalize:WARN",
    "Finally:OFF", // TODO: there are problems
    "FloatCast:WARN",
    "FloatingPointAssertionWithinEpsilon:WARN",
    "FloatingPointLiteralPrecision:OFF", // TODO: there are problems
    "FloggerArgumentToString:OFF", // we don't use flogger
    "FloggerFormatString:OFF", // we don't use flogger
    "FloggerLogString:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerLogVarargs:OFF", // we don't use flogger
    "FloggerLogWithCause:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerMessageFormat:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerPerWithoutRateLimit:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerRedundantIsEnabled:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerRequiredModifiers:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerSplitLogStatement:OFF", // we don't use flogger
    "FloggerStringConcatenation:OFF", // we don't use flogger
    "FloggerWithCause:OFF", // TODO: new, not checked if applicable to Lucene
    "FloggerWithoutCause:OFF", // TODO: new, not checked if applicable to Lucene
    "ForEachIterable:OFF", // TODO: new, not checked if applicable to Lucene
    "ForOverride:OFF", // we don't use this annotation
    "FormatString:ERROR",
    "FormatStringShouldUsePlaceholders:ERROR",
    "FormatStringAnnotation:OFF", // we don't use this annotation
    "FragmentInjection:OFF", // we don't use android
    "FragmentNotInstantiable:OFF", // we don't use android
    "FromTemporalAccessor:OFF", // we don't use .from(LocalDate) etc
    "FunctionalInterfaceClash:OFF", // TODO: new, not checked if applicable to Lucene
    "FunctionalInterfaceMethodChanged:ERROR",
    "FutureReturnValueIgnored:OFF", // TODO: there are problems
    "FutureTransformAsync:OFF", // we don't use these google libraries
    "FuturesGetCheckedIllegalExceptionType:OFF", // we don't use guava
    "FuzzyEqualsShouldNotBeUsedInEqualsMethod:OFF", // we don't use guava
    "GetClassOnAnnotation:ERROR",
    "GetClassOnClass:ERROR",
    "GetClassOnEnum:WARN",
    "GuardedBy:OFF", // we don't use this annotation
    "GuiceAssistedInjectScoping:OFF", // we don't use guice
    "GuiceAssistedParameters:OFF", // we don't use guice
    "GuiceInjectOnFinalField:OFF", // we don't use guice
    "GuiceNestedCombine:OFF", // we don't use guice
    "HardCodedSdCardPath:OFF", // TODO: new, not checked if applicable to Lucene
    "HashtableContains:ERROR",
    "HidingField:OFF", // noisy
    "ICCProfileGetInstance:OFF", // we use forbidden-apis for this
    "IdentifierName:OFF", // TODO: new, not checked if applicable to Lucene
    "IdentityBinaryExpression:OFF",
    "IdentityHashMapBoxing:ERROR",
    "IdentityHashMapUsage:OFF", // noisy
    "IfChainToSwitch:WARN",
    "IgnoredPureGetter:OFF", // we don't use these annotations
    "Immutable:OFF", // we don't use this annotation
    "ImmutableAnnotationChecker:OFF", // we don't use this annotation
    "ImmutableEnumChecker:OFF", // noisy
    "ImmutableMemberCollection:OFF", // TODO: new, not checked if applicable to Lucene
    "ImmutableRefactoring:OFF", // TODO: new, not checked if applicable to Lucene
    "ImmutableSetForContains:OFF", // TODO: new, not checked if applicable to Lucene
    "ImplementAssertionWithChaining:OFF", // TODO: new, not checked if applicable to Lucene
    "ImpossibleNullComparison:OFF", // we don't use protobuf
    "Incomparable:ERROR",
    "IncompatibleArgumentType:OFF", // we don't use this annotation
    "IncompatibleModifiers:OFF", // we don't use this annotation
    "InconsistentCapitalization:OFF", // TODO: there are problems
    "InconsistentHashCode:OFF", // noisy
    "InconsistentOverloads:OFF", // TODO: new, not checked if applicable to Lucene
    "IncorrectMainMethod:OFF", // we use ast-grep for this
    "IncrementInForLoopAndHeader:WARN",
    "IndexOfChar:ERROR",
    "InexactVarargsConditional:ERROR",
    "InfiniteRecursion:OFF",
    "InheritDoc:WARN",
    "InitializeInline:OFF", // TODO: new, not checked if applicable to Lucene
    "InjectInvalidTargetingOnScopingAnnotation:OFF", // we don't use this annotation
    "InjectMoreThanOneQualifier:OFF", // TODO: new, not checked if applicable to Lucene
    "InjectMoreThanOneScopeAnnotationOnClass:OFF", // we don't use this annotation
    "InjectOnBugCheckers:OFF", // we don't use this annotation
    "InjectOnConstructorOfAbstractClass:OFF", // we don't use this annotation
    "InjectOnMemberAndConstructor:OFF", // we don't use this annotation
    "InjectScopeAnnotationOnInterfaceOrAbstractClass:OFF", // we don't use this annotation
    "InjectedConstructorAnnotations:OFF", // we don't use this annotation
    "InlineFormatString:OFF", // noisy
    "InlineMeInliner:OFF", // we don't use this annotation
    "InlineMeSuggester:OFF", // we don't use this annotation
    "InlineMeValidator:OFF", // we don't use this annotation
    "InlineTrivialConstant:OFF", // stylistic
    "InputStreamSlowMultibyteRead:OFF",
    "InsecureCryptoUsage:OFF", // TODO: new, not checked if applicable to Lucene
    "InstanceOfAndCastMatchWrongType:WARN",
    "InstantTemporalUnit:OFF", // we don't use Instant apis with strange temporal units
    "IntFloatConversion:ERROR",
    "IntLiteralCast:OFF", // TODO: there are problems
    "IntLongMath:OFF", // noisy
    "InterfaceWithOnlyStatics:OFF", // TODO: new, not checked if applicable to Lucene
    "InterruptedExceptionSwallowed:OFF", // TODO: new, not checked if applicable to Lucene
    "Interruption:OFF", // TODO: new, not checked if applicable to Lucene
    "InvalidBlockTag:OFF", // noisy (e.g. lucene.experimental)
    "InvalidInlineTag:OFF", // TODO: there are problems
    "InvalidJavaTimeConstant:OFF", // we don't use impacted java.time classes
    "InvalidLink:WARN",
    "InvalidParam:OFF", // TODO: there are problems
    "InvalidPatternSyntax:OFF",
    "InvalidSnippet:ERROR",
    "InvalidThrows:WARN",
    "InvalidThrowsLink:WARN",
    "InvalidTimeZoneID:OFF", // we don't use getTimeZone with constant IDs
    "InvalidZoneId:OFF", // we don't use ZoneId.of
    "IsInstanceIncompatibleType:ERROR",
    "IsInstanceOfClass:ERROR",
    "IsLoggableTagLength:OFF", // we don't use android
    "IterableAndIterator:WARN",
    "IterablePathParameter:OFF", // TODO: new, not checked if applicable to Lucene
    "JUnit3FloatingPointComparisonWithoutDelta:OFF", // we don't use junit3
    "JUnit3TestNotRun:OFF", // we don't use junit3
    "JUnit4ClassAnnotationNonStatic:OFF", // we use ast-grep for this check
    "JUnit4ClassUsedInJUnit3:OFF", // we don't use junit3
    "JUnit4EmptyMethods:OFF", // we use ast-grep for this check
    "JUnit4SetUpNotRun:OFF", // LuceneTestCase takes care
    "JUnit4TearDownNotRun:OFF", // LuceneTestCase takes care
    "JUnit4TestNotRun:OFF", // noisy
    "JUnit4TestsNotRunWithinEnclosed:ERROR",
    "JUnitAmbiguousTestClass:OFF", // we don't use junit3
    "JUnitAssertSameCheck:ERROR",
    "JUnitIncompatibleType:WARN",
    "JUnitParameterMethodNotFound:ERROR",
    "Java8ApiChecker:OFF", // TODO: new, not checked if applicable to Lucene
    "JavaDurationGetSecondsGetNano:OFF", // we don't use these Duration methods
    "JavaDurationGetSecondsToToSeconds:OFF", // we don't use these Duration methods
    "JavaDurationWithNanos:OFF", // we don't use these Duration methods
    "JavaDurationWithSeconds:OFF", // we don't use these Duration methods
    "JavaInstantGetSecondsGetNano:OFF", // we don't use these Instant methods
    "JavaLangClash:OFF", // TODO: there are problems
    "JavaLocalDateTimeGetNano:OFF", // we don't use LocalDateTime
    "JavaLocalTimeGetNano:OFF", // we don't use LocalTime
    "JavaPeriodGetDays:OFF", // we don't use Period
    "JavaTimeDefaultTimeZone:OFF", // forbidden-apis checks this
    "JavaUtilDate:OFF", // noisy
    "JavaxInjectOnAbstractMethod:OFF", // we don't this annotation
    "JavaxInjectOnFinalField:OFF", // we don't use this annotation
    "JdkObsolete:OFF", // noisy
    "JodaConstructors:OFF", // we don't use joda-time
    "JodaDateTimeConstants:OFF", // we don't use joda-time
    "JodaDurationWithMillis:OFF", // we don't use joda-time
    "JodaInstantWithMillis:OFF", // we don't use joda-time
    "JodaNewPeriod:OFF", // we don't use joda-time
    "JodaPlusMinusLong:OFF", // we don't use joda-time
    "JodaTimeConverterManager:OFF", // we don't use joda-time
    "JodaToSelf:OFF", // we don't use joda-time
    "JodaWithDurationAddedLong:OFF", // we don't use joda-time
    "LabelledBreakTarget:OFF", // stylistic
    "LambdaFunctionalInterface:OFF", // TODO: new, not checked if applicable to Lucene
    "LenientFormatStringValidation:OFF", // we don't use these google libraries
    "LiteByteStringUtf8:OFF", // we don't use protobuf
    "LiteEnumValueOf:OFF", // we don't use protobuf
    "LiteProtoToString:OFF", // we don't use protobuf
    "LocalDateTemporalAmount:OFF", // we don't use LocalDate math
    "LockNotBeforeTry:OFF", // TODO: there are problems
    "LockOnBoxedPrimitive:ERROR",
    "LockOnNonEnclosingClassLiteral:OFF", // TODO: there are problems
    "LogicalAssignment:WARN",
    "LongDoubleConversion:WARN",
    "LongFloatConversion:WARN",
    "LongLiteralLowerCaseSuffix:OFF", // TODO: new, not checked if applicable to Lucene
    "LoopConditionChecker:ERROR",
    "LoopOverCharArray:WARN",
    "LossyPrimitiveCompare:ERROR",
    "MalformedInlineTag:WARN",
    "MathAbsoluteNegative:OFF", // TODO: there are problems
    "MathRoundIntLong:ERROR",
    "MemoizeConstantVisitorStateLookups:OFF", // we don't use this class
    "MethodCanBeStatic:OFF", // TODO: new, not checked if applicable to Lucene
    "MisformattedTestData:OFF", // stylistic
    "MislabeledAndroidString:OFF", // we don't use android
    "MisleadingEmptyVarargs:ERROR",
    "MisleadingEscapedSpace:ERROR",
    "MisplacedScopeAnnotations:OFF", // we don't use this annotation
    "MissingBraces:OFF", // TODO: new, not checked if applicable to Lucene
    "MissingCasesInEnumSwitch:OFF", // redundant (ECJ has the same)
    "MissingDefault:OFF", // TODO: new, not checked if applicable to Lucene
    "MissingFail:OFF", // TODO: there are problems
    "MissingImplementsComparable:WARN",
    "MissingOverride:OFF", // ECJ takes care of this
    "MissingRefasterAnnotation:OFF", // we don't use this annotation
    "MissingRuntimeRetention:OFF", // we don't use this annotation
    "MissingSummary:OFF", // TODO: there are problems
    "MissingSuperCall:OFF", // we don't use this annotation
    "MissingTestCall:OFF", // we don't use guava
    "MisusedDayOfYear:OFF", // we don't use date patterns
    "MisusedWeekYear:OFF", // we don't use date patterns
    "MixedArrayDimensions:OFF", // TODO: new, not checked if applicable to Lucene
    "MixedDescriptors:OFF", // we don't use protobuf
    "MixedMutabilityReturnType:OFF", // noisy
    "MockIllegalThrows:OFF", // we don't use mockito
    "MockNotUsedInProduction:OFF", // we don't use mocking libraries
    "MockitoDoSetup:OFF", // we don't use mocking libraries
    "MockitoUsage:OFF", // we don't use mockito
    "ModifiedButNotUsed:OFF", // TODO: there are problems
    "ModifyCollectionInEnhancedForLoop:WARN",
    "ModifySourceCollectionInStream:WARN",
    "ModifyingCollectionWithItself:ERROR",
    "MoreThanOneInjectableConstructor:OFF", // we don't use this annotation
    "MultiVariableDeclaration:OFF", // TODO: new, not checked if applicable to Lucene
    "MultimapKeys:WARN",
    "MultipleNullnessAnnotations:OFF", // we don't use these annotations
    "MultipleParallelOrSequentialCalls:WARN",
    "MultipleTopLevelClasses:OFF", // TODO: new, not checked if applicable to Lucene
    "MultipleUnaryOperatorsInMethodCall:WARN",
    "MustBeClosedChecker:OFF", // we don't use this annotation
    "MutableGuiceModule:OFF", // we don't use guice
    "MutablePublicArray:OFF", // TODO: there are problems
    "NCopiesOfChar:ERROR",
    "NamedLikeContextualKeyword:WARN",
    "NarrowCalculation:WARN",
    "NarrowingCompoundAssignment:OFF", // noisy
    "NegativeBoolean:OFF", // TODO: there are problems
    "NegativeCharLiteral:WARN",
    "NestedInstanceOfConditions:WARN",
    "NewFileSystem:OFF", // we don't create new filesystems
    "NoAllocation:OFF", // TODO: new, not checked if applicable to Lucene
    "NoCanIgnoreReturnValueOnClasses:OFF", // we don't use this annotation
    "NonApiType:OFF", // noisy
    "NonAtomicVolatileUpdate:OFF", // TODO: there are problems
    "NonCanonicalStaticImport:ERROR",
    "NonCanonicalStaticMemberImport:OFF", // TODO: new, not checked if applicable to Lucene
    "NonCanonicalType:OFF", // noisy
    "NonFinalCompileTimeConstant:OFF", // we don't use this annotation
    "NonFinalStaticField:WARN",
    "NonOverridingEquals:WARN",
    "NonRuntimeAnnotation:ERROR",
    "NotJavadoc:WARN",
    "NullArgumentForNonNullParameter:OFF", // we don't use this annotation
    "NullNeedsCastForVarargs:ERROR",
    "NullOptional:WARN",
    "NullTernary:ERROR",
    "NullableConstructor:OFF", // we don't use this annotation
    "NullableOnContainingClass:OFF", // we don't use this annotation
    "NullableOptional:WARN",
    "NullablePrimitive:OFF", // we don't use this annotation
    "NullablePrimitiveArray:OFF", // we don't use this annotation
    "NullableTypeParameter:OFF", // we don't use this annotation
    "NullableVoid:OFF", // we don't use this annotation
    "NullableWildcard:OFF", // we don't use this annotation
    "ObjectEqualsForPrimitives:WARN",
    "ObjectToString:OFF", // TODO: there are problems
    "ObjectsHashCodePrimitive:OFF", // TODO: there are problems
    "OperatorPrecedence:OFF", // noisy
    "OptionalEquality:ERROR",
    "OptionalMapToOptional:WARN",
    "OptionalMapUnusedValue:ERROR",
    "OptionalNotPresent:WARN",
    "OptionalOfRedundantMethod:ERROR",
    "OrphanedFormatString:WARN",
    "OutlineNone:OFF", // we don't use gwt
    "OverlappingQualifierAndScopeAnnotation:OFF", // we don't use this annotation
    "OverrideThrowableToString:WARN",
    "Overrides:WARN",
    "OverridesGuiceInjectableMethod:OFF", // we don't use guice
    "OverridesJavaxInjectableMethod:OFF", // we don't use this annotation
    "OverridingMethodInconsistentArgumentNamesChecker:WARN",
    "PackageInfo:OFF", // we use ast-grep for this check
    "PackageLocation:OFF", // TODO: new, not checked if applicable to Lucene
    "ParameterComment:OFF", // TODO: new, not checked if applicable to Lucene
    "ParameterMissingNullable:OFF", // TODO: new, not checked if applicable to Lucene
    "ParameterName:OFF", // we don't pass parameters with comments in this way
    "ParametersButNotParameterized:ERROR",
    "ParcelableCreator:OFF", // we don't use android
    "PatternMatchingInstanceof:OFF", // there are problems and check seems not great
    "PeriodFrom:OFF", // we don't use Period
    "PeriodGetTemporalUnit:OFF", // we don't use Period
    "PeriodTimeMath:OFF", // we don't use Period
    "PreconditionsCheckNotNullRepeated:OFF", // we don't use guava
    "PreconditionsInvalidPlaceholder:OFF", // we don't use guava
    "PreferInstanceofOverGetKind:OFF", // TODO: new, not checked if applicable to Lucene
    "PreferJavaTimeOverload:OFF", // TODO: new, not checked if applicable to Lucene
    "PreferredInterfaceType:OFF", // TODO: new, not checked if applicable to Lucene
    "PrimitiveArrayPassedToVarargsMethod:OFF", // TODO: new, not checked if applicable to Lucene
    "PrimitiveAtomicReference:WARN",
    "PrivateConstructorForNoninstantiableModule:OFF", // TODO: new, not checked if applicable to
    // Lucene
    "PrivateConstructorForUtilityClass:OFF", // TODO: new, not checked if applicable to Lucene
    "PrivateSecurityContractProtoAccess:OFF", // we don't use protobuf
    "ProtectedMembersInFinalClass:OFF", // we don't use protobuf
    "ProtoBuilderReturnValueIgnored:OFF", // we don't use protobuf
    "ProtoDurationGetSecondsGetNano:OFF", // we don't use protobuf
    "ProtoStringFieldReferenceEquality:OFF", // we don't use protobuf
    "ProtoTimestampGetSecondsGetNano:OFF", // we don't use protobuf
    "ProtoTruthMixedDescriptors:OFF", // we don't use protobuf
    "ProtocolBufferOrdinal:OFF", // we don't use protobuf
    "ProvidesMethodOutsideOfModule:OFF", // we don't use guice
    "PublicApiNamedStreamShouldReturnStream:OFF", // TODO: new, not checked if applicable to Lucene
    "QualifierOrScopeOnInjectMethod:OFF", // we don't use this annotation
    "QualifierWithTypeUse:OFF", // TODO: new, not checked if applicable to Lucene
    "RandomCast:ERROR",
    "RandomModInteger:ERROR",
    "ReachabilityFenceUsage:WARN",
    "RectIntersectReturnValueIgnored:OFF", // we don't use android
    "RedundantControlFlow:OFF", // stylistic
    "RedundantNullCheck:ERROR",
    "RedundantOverride:OFF", // TODO: new, not checked if applicable to Lucene
    "RedundantSetterCall:OFF", // we don't use protobuf
    "RedundantThrows:OFF", // TODO: new, not checked if applicable to Lucene
    "ReferenceEquality:OFF", // noisy
    "RefersToDaggerCodegen:OFF", // TODO: new, not checked if applicable to Lucene
    "RemoveUnusedImports:OFF", // TODO: new, not checked if applicable to Lucene
    "RequiredModifiers:OFF", // we don't use this annotation
    "RestrictedApi:OFF", // we don't use this annotation
    "RethrowReflectiveOperationExceptionAsLinkageError:WARN",
    "ReturnAtTheEndOfVoidFunction:OFF", // stylistic
    "ReturnFromVoid:WARN",
    "ReturnMissingNullable:OFF", // TODO: new, not checked if applicable to Lucene
    "ReturnValueIgnored:OFF", // noisy
    "ReturnsNullCollection:OFF", // TODO: new, not checked if applicable to Lucene
    "RobolectricShadowDirectlyOn:OFF", // we don't use robolectric
    "RuleNotRun:ERROR",
    "RxReturnValueIgnored:OFF", // we don't use rxjava
    "SameNameButDifferent:OFF", // TODO: there are problems
    "ScopeOnModule:OFF", // TODO: new, not checked if applicable to Lucene
    "SelfAlwaysReturnsThis:OFF", // we don't use self() methods, this isn't python.
    "SelfAssertion:ERROR",
    "SelfAssignment:ERROR",
    "SelfComparison:ERROR",
    "SelfEquals:ERROR",
    "SelfSet:ERROR",
    "SetUnrecognized:OFF", // we don't use protobuf
    "ScannerUseDelimiter:OFF", // not very useful
    "ShortCircuitBoolean:OFF", // TODO: there are problems
    "ShouldHaveEvenArgs:OFF", // we don't use truth
    "SizeGreaterThanOrEqualsZero:ERROR",
    "StatementSwitchToExpressionSwitch:OFF", // TODO: there are problems
    "StaticAssignmentInConstructor:OFF",
    "StaticAssignmentOfThrowable:OFF", // noisy
    "StaticGuardedByInstance:OFF",
    "StaticMockMember:OFF", // we don't use mock libraries
    "StaticOrDefaultInterfaceMethod:OFF", // TODO: new, not checked if applicable to Lucene
    "StaticQualifiedUsingExpression:OFF", // TODO: new, not checked if applicable to Lucene
    "StreamResourceLeak:OFF", // TODO: there are problems
    "StreamToIterable:WARN",
    "StreamToString:ERROR",
    "StringBuilderInitWithChar:ERROR",
    "StringCaseLocaleUsage:OFF", // noisy, can use forbidden-apis for this
    "StringCharset:OFF", // we use ast-grep for this
    "StringConcatToTextBlock:OFF", // TODO: there are problems
    "StringFormatWithLiteral:WARN",
    "StringJoin:OFF", // TODO: new, not checked if applicable to Lucene
    "StringSplitter:OFF", // noisy, can use forbidden-apis for this
    "StronglyTypeByteString:OFF", // TODO: new, not checked if applicable to Lucene
    "StronglyTypeTime:OFF", // TODO: new, not checked if applicable to Lucene
    "SubstringOfZero:OFF", // we use ast-grep for this check
    "SunApi:OFF", // we use forbidden-apis for this
    "SuperCallToObjectMethod:WARN",
    "SuppressWarningsDeprecated:ERROR",
    "SuppressWarningsWithoutExplanation:OFF", // TODO: new, not checked if applicable to Lucene
    "SwigMemoryLeak:OFF", // we don't use swig
    "SwitchDefault:OFF", // TODO: new, not checked if applicable to Lucene
    "SymbolToString:OFF", // TODO: new, not checked if applicable to Lucene
    "SynchronizeOnNonFinalField:OFF", // noisy
    "SystemConsoleNull:WARN",
    "SystemExitOutsideMain:OFF", // TODO: new, not checked if applicable to Lucene
    "SystemOut:OFF", // TODO: new, not checked if applicable to Lucene
    "TemporalAccessorGetChronoField:OFF", // we don't use TemporalAccessor.get
    "TestExceptionChecker:OFF", // TODO: new, not checked if applicable to Lucene
    "TestParametersNotInitialized:OFF", // we don't use this annotation
    "TheoryButNoTheories:OFF", // we don't use junit theory apis/runner
    "ThreadBuilderNameWithPlaceholder:OFF", // TODO: new, not checked if applicable to Lucene
    "ThreadJoinLoop:OFF",
    "ThreadLocalUsage:OFF", // noisy
    "ThreadPriorityCheck:OFF", // noisy, forbidden APIs can do this
    "ThreeLetterTimeZoneID:OFF", // we use ast-grep for this
    "ThrowIfUncheckedKnownChecked:OFF", // we don't use this annotation
    "ThrowIfUncheckedKnownUnchecked:OFF", // we don't use these google libraries
    "ThrowNull:OFF", // noisy (LuceneTestCase)
    "ThrowSpecificExceptions:OFF", // TODO: new, not checked if applicable to Lucene
    "ThrowsUncheckedException:OFF", // TODO: new, not checked if applicable to Lucene
    "TimeInStaticInitializer:ERROR",
    "TimeUnitConversionChecker:WARN",
    "TimeUnitMismatch:OFF", // TODO: new, not checked if applicable to Lucene
    "ToStringReturnsNull:OFF", // TODO: there are problems
    "TooManyParameters:OFF", // TODO: new, not checked if applicable to Lucene
    "TraditionalSwitchExpression:OFF", // TODO: new, not checked if applicable to Lucene
    "TransientMisuse:OFF", // TODO: new, not checked if applicable to Lucene
    "TreeToString:OFF", // we don't use javac API
    "TruthAssertExpected:OFF", // we don't use truth
    "TruthConstantAsserts:OFF", // we don't use truth
    "TruthContainsExactlyElementsInUsage:OFF", // we don't use truth
    "TruthGetOrDefault:OFF", // we don't use truth
    "TruthIncompatibleType:OFF", // we don't use truth
    "TryFailRefactoring:OFF", // TODO: new, not checked if applicable to Lucene
    "TryFailThrowable:OFF",
    "TryWithResourcesVariable:OFF", // TODO: new, not checked if applicable to Lucene
    "TypeEquals:OFF", // we don't use this internal javac api
    "TypeNameShadowing:WARN",
    "TypeParameterNaming:OFF", // TODO: new, not checked if applicable to Lucene
    "TypeParameterQualifier:ERROR",
    "TypeParameterShadowing:OFF",
    "TypeParameterUnusedInFormals:OFF",
    "TypeToString:OFF", // TODO: new, not checked if applicable to Lucene
    "URLEqualsHashCode:WARN",
    "UndefinedEquals:OFF", // TODO: there are problems
    "UnescapedEntity:OFF", // TODO: there are problems
    "UngroupedOverloads:OFF", // TODO: new, not checked if applicable to Lucene
    "UnicodeDirectionalityCharacters:ERROR",
    "UnicodeEscape:OFF", // noisy
    "UnicodeInCode:ERROR",
    "UnnecessarilyFullyQualified:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessarilyUsedValue:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessarilyVisible:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryAnonymousClass:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryAssignment:OFF", // we don't use these annotations
    "UnnecessaryAsync:WARN",
    "UnnecessaryBoxedAssignment:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryBoxedVariable:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryBreakInSwitch:WARN",
    "UnnecessaryCheckNotNull:ERROR",
    "UnnecessaryCopy:OFF", // we don't use google collections
    "UnnecessaryDefaultInEnumSwitch:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryFinal:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryLambda:OFF", // TODO: there are problems
    "UnnecessaryLongToIntConversion:OFF", // TODO: there are problems
    "UnnecessaryMethodInvocationMatcher:OFF", // we don't use spring
    "UnnecessaryMethodReference:WARN",
    "UnnecessaryOptionalGet:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryParentheses:OFF", // noisy
    "UnnecessaryQualifier:OFF", // we don't use guava
    "UnnecessarySetDefault:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryStaticImport:OFF", // TODO: new, not checked if applicable to Lucene
    "UnnecessaryStringBuilder:WARN",
    "UnnecessaryTestMethodPrefix:OFF", // stylistic
    "UnnecessaryTypeArgument:ERROR",
    "UnrecognisedJavadocTag:WARN",
    "UnsafeFinalization:OFF", // we don't use finalizers, deprecated for removal, fails build
    "UnsafeLocaleUsage:OFF", // TODO: new, not checked if applicable to Lucene
    "UnsafeReflectiveConstructionCast:WARN",
    "UnsafeWildcard:ERROR",
    "UnsynchronizedOverridesSynchronized:OFF", // TODO: there are problems
    "UnusedAnonymousClass:ERROR",
    "UnusedCollectionModifiedInPlace:ERROR",
    "UnusedException:OFF", // TODO: new, not checked if applicable to Lucene
    "UnusedLabel:OFF", // TODO: there are problems
    "UnusedMethod:OFF", // TODO: there are problems
    "UnusedNestedClass:WARN",
    "UnusedTypeParameter:OFF", // TODO: there are problems
    "UnusedVariable:OFF", // noisy, can use ECJ
    "UrlInSee:OFF", // TODO: new, not checked if applicable to Lucene
    "UseBinds:OFF", // we don't use this annotation
    "UseCorrectAssertInTests:OFF", // noisy
    "UseEnumSwitch:OFF", // TODO: new, not checked if applicable to Lucene
    "UsingJsr305CheckReturnValue:OFF", // TODO: new, not checked if applicable to Lucene
    "Var:OFF", // TODO: new, not checked if applicable to Lucene
    "VarTypeName:ERROR",
    "VariableNameSameAsType:WARN",
    "Varifier:OFF", // TODO: new, not checked if applicable to Lucene
    "VoidMissingNullable:OFF", // TODO: new, not checked if applicable to Lucene
    "VoidUsed:WARN",
    "WaitNotInLoop:OFF", // TODO: there are problems
    "WakelockReleasedDangerously:OFF", // we don't use android
    "WildcardImport:OFF", // we use ast-grep for this check.
    "WithSignatureDiscouraged:OFF", // we aren't using this error-prone internal api
    "WrongOneof:OFF", // we don't use protobuf
    "XorPower:ERROR",
    "YodaCondition:OFF", // TODO: new, not checked if applicable to Lucene
    "ZoneIdOfZ:OFF", // we don't use ZoneId.of
  };

  private static final Map<String, CheckSeverity> ALL_CHECKS_PARSED =
      Stream.of(ALL_CHECKS)
          .map(
              check -> {
                var colon = check.indexOf(":");
                var checkName = check.substring(0, colon);
                var severity = CheckSeverity.valueOf(check.substring(colon + 1));
                return Map.entry(checkName, severity);
              })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  /** Filter out all checks which are disabled. */
  private static final Map<String, CheckSeverity> ENABLED_CHECKS =
      ALL_CHECKS_PARSED.entrySet().stream()
          .filter(e -> e.getValue() != CheckSeverity.OFF)
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  private void configureErrorProne(Project project) {
    // Apply the actual errorprone plugin.
    project.getPlugins().apply(net.ltgt.gradle.errorprone.ErrorPronePlugin.class);

    applyLocalDependency(project);

    project
        .getTasks()
        .withType(JavaCompile.class)
        .configureEach(
            t -> {
              t.dependsOn(":" + CheckEnvironmentPlugin.TASK_CHECK_JDK_INTERNALS_EXPOSED_TO_GRADLE);

              var epOptions =
                  ((ExtensionAware) t.getOptions())
                      .getExtensions()
                      .getByType(ErrorProneOptions.class);

              epOptions.getDisableWarningsInGeneratedCode().set(true);
              epOptions.getAllErrorsAsWarnings().set(true);

              // In case a named check is removed, fail.
              epOptions.getIgnoreUnknownCheckNames().set(false);

              // Turn off everything by default, then re-enable what's needed.
              epOptions.getDisableAllChecks().set(true);
              epOptions.getChecks().putAll(ENABLED_CHECKS);
              t.getLogger()
                  .info(
                      "Compiling with error prone, enabled checks: "
                          + String.join(", ", ENABLED_CHECKS.keySet()));

              // Exclude certain files (generated ones, mostly).
              List<String> excludedPaths =
                  switch (project.getPath()) {
                    case ":lucene:core" -> List.of(".*/StandardTokenizerImpl.java");
                    case ":lucene:analysis:common" ->
                        List.of(
                            ".*/HTMLStripCharFilter.java", ".*/UAX29URLEmailTokenizerImpl.java");
                    case ":lucene:test-framework" ->
                        List.of(
                            ".*/EmojiTokenizationTestUnicode_11_0.java",
                            ".*/WordBreakTestUnicode_9_0_0.java");
                    case ":lucene:queryparser" ->
                        List.of(
                            ".*/classic/ParseException.java",
                            ".*/classic/QueryParser.java",
                            ".*/classic/QueryParserConstants.java",
                            ".*/classic/QueryParserTokenManager.java",
                            ".*/classic/Token.java",
                            ".*/classic/TokenMgrError.java",
                            ".*/standard/parser/ParseException.java",
                            ".*/standard/parser/StandardSyntaxParser.java",
                            ".*/standard/parser/StandardSyntaxParserConstants.java",
                            ".*/standard/parser/StandardSyntaxParserTokenManager.java",
                            ".*/standard/parser/Token.java",
                            ".*/standard/parser/TokenMgrError.java",
                            ".*/surround/parser/ParseException.java",
                            ".*/surround/parser/QueryParser.java",
                            ".*/surround/parser/QueryParserConstants.java",
                            ".*/surround/parser/QueryParserTokenManager.java",
                            ".*/surround/parser/Token.java",
                            ".*/surround/parser/TokenMgrError.java");
                    default -> List.of();
                  };

              if (!excludedPaths.isEmpty()) {
                epOptions.getExcludedPaths().set(String.join("|", excludedPaths));
              }
            });
  }

  // Add the local version of errorprone to the errorprone configuration.
  private static void applyLocalDependency(Project project) {
    project
        .getDependencies()
        .add("errorprone", getVersionCatalog(project).findLibrary("errorprone").get());
  }
}
