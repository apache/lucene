@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@ECHO OFF

SETLOCAL
SET MODULES=%~dp0..

IF DEFINED LAUNCH_CMD GOTO testing
REM Windows 'start' command takes the first quoted argument to be the title of the started window. Since we
REM quote the LAUNCH_CMD (because it can contain spaces), it misinterprets it as the title and fails to run.
REM force the window title here.
SET LAUNCH_START=start "Lucene Luke"
SET LAUNCH_CMD=javaw
SET LAUNCH_OPTS=
goto launch

:testing
REM For distribution testing we don't use start and pass an explicit java command path,
REM This is required because otherwise we can't block on luke invocation and can't intercept
REM the return status. We also force UTF-8 encoding so that we don't have to interpret the output in
REM an unknown local platform encoding.
SET LAUNCH_START=
SET LAUNCH_OPTS=-Dfile.encoding=UTF-8

:launch
%LAUNCH_START% "%LAUNCH_CMD%" %LAUNCH_OPTS% --module-path "%MODULES%\modules;%MODULES%\modules-thirdparty" --module org.apache.lucene.luke %*
SET EXITVAL=%errorlevel%
EXIT /b %EXITVAL%
ENDLOCAL
