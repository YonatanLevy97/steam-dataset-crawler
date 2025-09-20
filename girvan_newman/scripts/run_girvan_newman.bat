@echo off
REM run_girvan_newman.bat - Windows batch version of Girvan-Newman wrapper
REM 
REM Usage: run_girvan_newman.bat --edges path\to\edges.csv.gz [OPTIONS]

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set GIRVAN_NEWMAN_SCRIPT=%SCRIPT_DIR%girvan_newman_analysis.py

REM Default parameters
set EDGES_PATH=
set OUT_DIR=.\out\girvan_newman_%date:~-4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set OUT_DIR=%OUT_DIR: =0%
set MAX_COMMUNITIES=10
set MIN_COMMUNITY_SIZE=5
set MIN_WEIGHT=0.7
set PYTHON_BIN=python

REM Additional options
set GIANT_ONLY=
set KCORE=
set MAX_NODES=
set MAX_EDGES=

:parse_args
if "%~1"=="" goto end_parse
if "%~1"=="-h" goto show_help
if "%~1"=="--help" goto show_help
if "%~1"=="--edges" (
    set EDGES_PATH=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--out-dir" (
    set OUT_DIR=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--max-communities" (
    set MAX_COMMUNITIES=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--min-community-size" (
    set MIN_COMMUNITY_SIZE=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--min-weight" (
    set MIN_WEIGHT=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--giant-only" (
    set GIANT_ONLY=--giant-only
    shift
    goto parse_args
)
if "%~1"=="--kcore" (
    set KCORE=--kcore %~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--max-nodes" (
    set MAX_NODES=--max-nodes %~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--max-edges" (
    set MAX_EDGES=--max-edges %~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--python" (
    set PYTHON_BIN=%~2
    shift
    shift
    goto parse_args
)
echo [ERROR] Unknown option: %~1
goto show_help

:end_parse

if "%EDGES_PATH%"=="" (
    echo [ERROR] --edges parameter is required
    goto show_help
)

if not exist "%EDGES_PATH%" (
    echo [ERROR] Edges file not found: %EDGES_PATH%
    exit /b 1
)

if not exist "%GIRVAN_NEWMAN_SCRIPT%" (
    echo [ERROR] Girvan-Newman script not found: %GIRVAN_NEWMAN_SCRIPT%
    exit /b 1
)

REM Create output directory
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

REM Build command
set CMD=%PYTHON_BIN% "%GIRVAN_NEWMAN_SCRIPT%" --edges "%EDGES_PATH%" --out-dir "%OUT_DIR%" --max-communities %MAX_COMMUNITIES% --min-community-size %MIN_COMMUNITY_SIZE% --min-weight %MIN_WEIGHT%

if not "%GIANT_ONLY%"=="" set CMD=%CMD% %GIANT_ONLY%
if not "%KCORE%"=="" set CMD=%CMD% %KCORE%
if not "%MAX_NODES%"=="" set CMD=%CMD% %MAX_NODES%
if not "%MAX_EDGES%"=="" set CMD=%CMD% %MAX_EDGES%

echo [INFO] Running Girvan-Newman community detection...
echo [CMD] %CMD%
echo.

%CMD%

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Girvan-Newman analysis failed
    exit /b 1
)

echo.
echo [SUCCESS] Analysis completed!
echo Results saved to: %OUT_DIR%
goto :eof

:show_help
echo Usage: %~nx0 --edges EDGES_CSV [OPTIONS]
echo.
echo Required:
echo   --edges PATH              Path to edges CSV file (supports .gz)
echo.
echo Output:
echo   --out-dir PATH            Output directory (default: timestamped directory)
echo.
echo Community Detection:
echo   --max-communities N       Maximum community levels to detect (default: 10)
echo   --min-community-size N    Minimum community size to keep (default: 5)
echo.
echo Graph Filtering:
echo   --min-weight X            Minimum edge weight/cosine similarity (default: 0.7)
echo   --giant-only              Only analyze largest connected component
echo   --kcore K                 Apply k-core filter before analysis
echo   --max-nodes N             Maximum nodes to analyze (degree-based sampling)
echo   --max-edges N             Maximum edges to load (useful for testing)
echo.
echo System:
echo   --python PATH             Python executable to use (default: python)
echo.
echo Examples:
echo   %~nx0 --edges ..\out\graph_runs\...\edges_top100.csv.gz --giant-only
echo   %~nx0 --edges ..\out\graph_runs\...\edges_top100.csv.gz --giant-only --kcore 3 --max-nodes 1000
goto :eof