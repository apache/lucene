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

@echo off

SETLOCAL
SET MODULES=%~dp0..

IF DEFINED LAUNCH_CMD GOTO testing
SET LAUNCH_CMD=start javaw
SET LAUNCH_OPTS=
goto launch

:testing
REM For distribution testing we don't use start and pass an explicit launch ('java') command,
REM otherwise we can't block on luke invocation and can't intercept the return status.
REM We also force UTF-8 encoding.
SET LAUNCH_OPTS=-Dfile.encoding=UTF-8

:launch
"%LAUNCH_CMD%" %LAUNCH_OPTS% --module-path "%MODULES%\modules;%MODULES%\modules-thirdparty" --module org.apache.lucene.luke %*
SET EXITVAL=%errorlevel%
EXIT /b %EXITVAL%
ENDLOCAL
