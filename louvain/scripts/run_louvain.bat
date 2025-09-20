@echo off
REM run_louvain.bat
REM 
REM Windows wrapper script for Louvain community detection on Steam graph data
REM
REM Usage:
REM   run_louvain.bat --edges path\to\edges.csv.gz --out-dir .\out\louvain\
REM

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set LOUVAIN_SCRIPT=%SCRIPT_DIR%louvain_analysis.py

REM Default parameters
set EDGES_PATH=
set OUT_DIR=.\out\louvain_%date:~10,4%%date:~4,2%%date:~7,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set OUT_DIR=%OUT_DIR: =0%
set MIN_COMMUNITY_SIZE=5
set MIN_WEIGHT=0.7
set PYTHON_BIN=python

REM Louvain-specific parameters
set RESOLUTION=1.0
set RANDOM_SEED=42

REM Advanced options
set GIANT_ONLY=
set KCORE=
set MAX_NODES=
set MAX_EDGES=

:parse_args
if "%1"=="" goto end_parse
if "%1"=="-h" goto show_help
if "%1"=="--help" goto show_help
if "%1"=="--edges" (
  shift
  set EDGES_PATH=%2
  shift
  goto parse_args
)
if "%1"=="--out-dir" (
  shift
  set OUT_DIR=%2
  shift
  goto parse_args
)
if "%1"=="--min-community-size" (
  shift
  set MIN_COMMUNITY_SIZE=%2
  shift
  goto parse_args
)
if "%1"=="--min-weight" (
  shift
  set MIN_WEIGHT=%2
  shift
  goto parse_args
)
if "%1"=="--resolution" (
  shift
  set RESOLUTION=%2
  shift
  goto parse_args
)
if "%1"=="--random-seed" (
  shift
  set RANDOM_SEED=%2
  shift
  goto parse_args
)
if "%1"=="--giant-only" (
  set GIANT_ONLY=--giant-only
  shift
  goto parse_args
)
if "%1"=="--kcore" (
  shift
  set KCORE=--kcore %2
  shift
  goto parse_args
)
if "%1"=="--max-nodes" (
  shift
  set MAX_NODES=--max-nodes %2
  shift
  goto parse_args
)
if "%1"=="--max-edges" (
  shift
  set MAX_EDGES=--max-edges %2
  shift
  goto parse_args
)
if "%1"=="--python" (
  shift
  set PYTHON_BIN=%2
  shift
  goto parse_args
)
echo Unknown option: %1
echo Use --help for usage information.
exit /b 1

:show_help
echo Usage: %~nx0 --edges EDGES_CSV [OPTIONS]
echo.
echo Required:
echo   --edges PATH              Path to edges CSV file (supports .gz)
echo.
echo Output:
echo   --out-dir PATH            Output directory (default: .\out\louvain_TIMESTAMP)
echo.
echo Community Detection:
echo   --min-community-size N    Minimum community size to keep (default: 5)
echo   --resolution X            Resolution parameter - higher = smaller communities (default: 1.0)
echo   --random-seed N           Random seed for reproducibility (default: 42)
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
echo   REM Basic analysis on graph output
echo   %~nx0 --edges .\out\graph_runs\...\edges_top100.csv.gz
echo.
echo   REM More focused analysis with smaller communities
echo   %~nx0 --edges .\out\graph_runs\...\edges_top100.csv.gz ^
echo                    --giant-only --kcore 3 --max-nodes 1000 ^
echo                    --resolution 1.5
echo.
echo   REM Quick test on subset
echo   %~nx0 --edges .\out\graph_runs\...\edges_top100.csv.gz ^
echo                    --max-edges 50000 --max-nodes 500
echo.
echo Louvain vs Girvan-Newman:
echo   - Louvain is much faster (O(n log n) vs O(n³))
echo   - Louvain produces single-level communities (not hierarchical)
echo   - Resolution parameter controls community granularity
echo   - Better for large graphs and quick analysis
exit /b 0

:end_parse

REM Validate required parameters
if "%EDGES_PATH%"=="" (
  echo Error: --edges parameter is required
  echo Use --help for usage information.
  exit /b 1
)

if not exist "%EDGES_PATH%" (
  echo Error: Edges file not found: %EDGES_PATH%
  exit /b 1
)

REM Check Python availability
%PYTHON_BIN% --version >nul 2>&1
if %errorlevel% neq 0 (
  echo Error: Python executable not found: %PYTHON_BIN%
  echo Please install Python 3 or specify correct path with --python
  exit /b 1
)

REM Check if the Louvain script exists
if not exist "%LOUVAIN_SCRIPT%" (
  echo Error: Louvain analysis script not found: %LOUVAIN_SCRIPT%
  exit /b 1
)

REM Display configuration
echo ==================================================
echo LOUVAIN COMMUNITY DETECTION
echo ==================================================
echo Edges file:             %EDGES_PATH%
echo Output directory:       %OUT_DIR%
echo Python executable:      %PYTHON_BIN%
echo.
echo Parameters:
echo   Min community size:   %MIN_COMMUNITY_SIZE%
echo   Min edge weight:      %MIN_WEIGHT%
echo   Resolution:           %RESOLUTION%
echo   Random seed:          %RANDOM_SEED%
echo.
echo Filters:
if "%GIANT_ONLY%"=="" (
  echo   Giant component only: No
) else (
  echo   Giant component only: Yes
)
if "%KCORE%"=="" (
  echo   K-core filter:        None
) else (
  echo   K-core filter:        %KCORE:~7%
)
if "%MAX_NODES%"=="" (
  echo   Max nodes:            None
) else (
  echo   Max nodes:            %MAX_NODES:~11%
)
if "%MAX_EDGES%"=="" (
  echo   Max edges:            None
) else (
  echo   Max edges:            %MAX_EDGES:~11%
)
echo ==================================================
echo.

REM Create output directory
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

REM Build command
set CMD="%PYTHON_BIN%" "%LOUVAIN_SCRIPT%" --edges "%EDGES_PATH%" --out-dir "%OUT_DIR%" --min-community-size %MIN_COMMUNITY_SIZE% --min-weight %MIN_WEIGHT% --resolution %RESOLUTION% --random-seed %RANDOM_SEED%

if not "%GIANT_ONLY%"=="" set CMD=%CMD% %GIANT_ONLY%
if not "%KCORE%"=="" set CMD=%CMD% %KCORE%
if not "%MAX_NODES%"=="" set CMD=%CMD% %MAX_NODES%
if not "%MAX_EDGES%"=="" set CMD=%CMD% %MAX_EDGES%

REM Run the analysis
echo Starting Louvain community detection...
echo Command: %CMD%
echo.

%CMD%
if %errorlevel% equ 0 (
  echo.
  echo ==================================================
  echo SUCCESS: Louvain analysis completed!
  echo Results saved to: %OUT_DIR%
  echo ==================================================
  
  REM Show generated files
  echo.
  echo Generated files:
  for %%f in ("%OUT_DIR%\*") do (
    echo   - %%~nxf
  )
  
  echo.
  echo Next steps:
  echo   1. Review community_assignments.csv for detected communities
  echo   2. Check community_stats.json for summary statistics
  echo   3. View community_sizes.png for size distribution
  echo   4. Run detailed feature analysis:
  echo      python ..\scripts\detailed_community_feature_analysis.py ^
  echo        --communities %OUT_DIR%\community_assignments.csv ^
  echo        --metadata .\out\dead_labels_enriched.csv
  echo.
) else (
  echo.
  echo ==================================================
  echo ERROR: Louvain analysis failed!
  echo Check the error messages above for details.
  echo ==================================================
  exit /b 1
)

endlocal