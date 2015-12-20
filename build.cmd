@echo off
cd %~dp0

SETLOCAL
SET NUGET_VERSION=latest
SET CACHED_NUGET=%LocalAppData%\NuGet\nuget.%NUGET_VERSION%.exe
SET BUILDCMD_DNX_FEED=https://www.nuget.org/api/v2
SET BUILDCMD_DNX_VERSION=1.0.0-rc1-final 

IF EXIST %CACHED_NUGET% goto copynuget
echo Downloading latest version of NuGet.exe...
IF NOT EXIST %LocalAppData%\NuGet md %LocalAppData%\NuGet
@powershell -NoProfile -ExecutionPolicy unrestricted -Command "$ProgressPreference = 'SilentlyContinue'; Invoke-WebRequest 'https://dist.nuget.org/win-x86-commandline/%NUGET_VERSION%/nuget.exe' -OutFile '%CACHED_NUGET%'"

:copynuget
IF EXIST .nuget\nuget.exe goto sake
md .nuget
copy %CACHED_NUGET% .nuget\nuget.exe > nul

:sake
IF EXIST packages\Sake goto korebuild
.nuget\NuGet.exe install Sake -ExcludeVersion -Source https://www.nuget.org/api/v2/ -o packages

:korebuild
IF EXIST packages\KoreBuild goto dnx
.nuget\nuget.exe install KoreBuild -ExcludeVersion -Source https://www.myget.org/F/aspnetvnext/api/v2 -o packages -nocache -pre


:dnx
SET DNX_FEED=%BUILDCMD_DNX_FEED%
CALL packages\KoreBuild\build\dnvm install %BUILDCMD_DNX_VERSION% -runtime CoreCLR -arch x86
CALL packages\KoreBuild\build\dnvm install %BUILDCMD_DNX_VERSION% -runtime CLR -arch x86 -alias default

packages\Sake\tools\Sake.exe -I packages\KoreBuild\build -f makefile.shade %*