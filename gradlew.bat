@rem
@rem Copyright 2015 the original author or authors.
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem
@rem SPDX-License-Identifier: Apache-2.0
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  Gradle startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables, and ensure extensions are enabled
setlocal EnableExtensions

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
@rem This is normally unused
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and GRADLE_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS="-Xmx64m" "-Xms64m"

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

"%COMSPEC%" /c exit 1

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

"%COMSPEC%" /c exit 1

:execute
@rem Setup the command line

@rem START OF LUCENE CUSTOMIZATION

@rem LUCENE-9471: workaround for gradle leaving junk temp. files behind.
SET GRADLE_TEMPDIR=%DIRNAME%\.gradle\tmp
IF NOT EXIST "%GRADLE_TEMPDIR%" MKDIR "%GRADLE_TEMPDIR%"
SET DEFAULT_JVM_OPTS=%DEFAULT_JVM_OPTS% "-Djava.io.tmpdir=%GRADLE_TEMPDIR%"

@rem Generate gradle.properties if it does not exist
IF NOT EXIST "%APP_HOME%\gradle.properties" (
  @rem local expansion is needed to check ERRORLEVEL inside control blocks.
  setlocal enableDelayedExpansion
  "%JAVA_EXE%" %JAVA_OPTS% "%APP_HOME%/build-tools/build-infra/src/main/java/org/apache/lucene/gradle/GradlePropertiesGenerator.java" "%APP_HOME%\gradle\template.gradle.properties" "%APP_HOME%\gradle.properties"
  IF %ERRORLEVEL% NEQ 0 goto fail
  endlocal
)

@rem A manually-installed gradle-wrapper.jar takes priority over our source-based bootstrap.
SET GRADLE_WRAPPER_JAR=%APP_HOME%\gradle\wrapper\gradle-wrapper.jar
SET GRADLE_WRAPPER_SRC=%APP_HOME%\gradle\wrapper\GradleWrapper.java
SET GRADLE_WRAPPER_CACHE=%APP_HOME%\.gradle\tmp\gradle-wrapper-classes
SET "JAVAC_EXE=%JAVA_EXE:java.exe=javac.exe%"

@rem Compile GradleWrapper.java once and reuse the compiled classes, instead of paying the
@rem single-file-source-launch recompile cost on every invocation. Falls back to source-launch
@rem (slower, but self-healing) if compilation isn't available or fails for any reason.
@rem No staleness check: if you edit GradleWrapper.java, delete %GRADLE_WRAPPER_CACHE% to force
@rem a recompile (this file changes rarely, so keeping this simple is worth that manual step).
IF NOT EXIST "%GRADLE_WRAPPER_JAR%" IF NOT EXIST "%GRADLE_WRAPPER_CACHE%\GradleWrapper.class" (
  mkdir "%GRADLE_WRAPPER_CACHE%" 2>nul
  "%JAVAC_EXE%" -d "%GRADLE_WRAPPER_CACHE%" "%GRADLE_WRAPPER_SRC%" >nul 2>nul
)

@rem END OF LUCENE CUSTOMIZATION

@rem Execute Gradle
@rem endlocal doesn't take effect until after the line is parsed and variables are expanded
@rem which allows us to clear the local environment before executing the java command
IF EXIST "%GRADLE_WRAPPER_JAR%" (
  endlocal & "%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %GRADLE_OPTS% "-Dorg.gradle.appname=%APP_BASE_NAME%" -jar "%GRADLE_WRAPPER_JAR%" %* & call :exitWithErrorLevel
) ELSE IF EXIST "%GRADLE_WRAPPER_CACHE%\GradleWrapper.class" (
  endlocal & "%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %GRADLE_OPTS% "-Dorg.gradle.appname=%APP_BASE_NAME%" -cp "%GRADLE_WRAPPER_CACHE%" GradleWrapper %* & call :exitWithErrorLevel
) ELSE (
  endlocal & "%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %GRADLE_OPTS% "-Dorg.gradle.appname=%APP_BASE_NAME%" "%GRADLE_WRAPPER_SRC%" %* & call :exitWithErrorLevel
)

:exitWithErrorLevel
@rem Use "%COMSPEC%" /c exit to allow operators to work properly in scripts
"%COMSPEC%" /c exit %ERRORLEVEL%
