@echo off
REM basic_usage.bat
REM 
REM Basic usage examples for Louvain community detection on Windows
REM Demonstrates common use cases and parameter combinations

setlocal enabledelayedexpansion

echo === Louvain Community Detection - Basic Usage Examples ===
echo.

REM Example data path - adjust this to your actual data location
set EDGES_FILE=..\..\out\graph_runs\example\edges_top100.csv.gz
set OUTPUT_BASE=..\..\out\louvain_examples

REM Check if example data exists
if not exist "%EDGES_FILE%" (
    echo Warning: Example edges file not found at: %EDGES_FILE%
    echo Please adjust the EDGES_FILE path to point to your actual edges data
    echo Expected format: CSV with columns src_appid,dst_appid,cosine
    echo.
    echo If you need to generate graph data first, run:
    echo   python ..\..\graph_scripts\build_cosine_similarity_graph.py
    echo.
    exit /b 1
)

echo Found edges file: %EDGES_FILE%
echo.

REM Example 1: Basic community detection with defaults
echo Example 1: Basic Community Detection
echo Running Louvain with default parameters...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\basic" ^
    --giant-only

if %errorlevel% neq 0 (
    echo Error in basic analysis
    exit /b 1
)

echo.
echo [✓] Basic analysis completed. Results in: %OUTPUT_BASE%\basic
echo.

REM Example 2: High-resolution analysis for detailed communities
echo Example 2: High-Resolution Analysis
echo Running with higher resolution for more detailed communities...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\high_resolution" ^
    --resolution 1.5 ^
    --min-community-size 3 ^
    --giant-only

if %errorlevel% neq 0 (
    echo Error in high-resolution analysis
    exit /b 1
)

echo.
echo [✓] High-resolution analysis completed. Results in: %OUTPUT_BASE%\high_resolution
echo.

REM Example 3: Focused analysis with filtering
echo Example 3: Focused Analysis with Strong Filtering
echo Running with strong filtering for high-quality communities...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\focused" ^
    --min-weight 0.8 ^
    --kcore 3 ^
    --giant-only ^
    --resolution 1.2

if %errorlevel% neq 0 (
    echo Error in focused analysis
    exit /b 1
)

echo.
echo [✓] Focused analysis completed. Results in: %OUTPUT_BASE%\focused
echo.

REM Example 4: Quick test on subset of data
echo Example 4: Quick Test on Data Subset
echo Running quick test with limited data for experimentation...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\quick_test" ^
    --max-edges 50000 ^
    --max-nodes 500 ^
    --resolution 1.0

if %errorlevel% neq 0 (
    echo Error in quick test
    exit /b 1
)

echo.
echo [✓] Quick test completed. Results in: %OUTPUT_BASE%\quick_test
echo.

REM Example 5: Low-resolution analysis for broad categories  
echo Example 5: Low-Resolution Analysis
echo Running with lower resolution for broader community categories...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\broad_categories" ^
    --resolution 0.6 ^
    --min-community-size 10 ^
    --giant-only

if %errorlevel% neq 0 (
    echo Error in broad categories analysis
    exit /b 1
)

echo.
echo [✓] Broad categories analysis completed. Results in: %OUTPUT_BASE%\broad_categories
echo.

REM Summary of results
echo === Summary of Generated Results ===
echo.

REM List the generated directories
for /d %%i in ("%OUTPUT_BASE%\*") do (
    set example_name=%%~ni
    echo !example_name!:
    
    REM Check if community_stats.json exists and show basic info
    if exist "%%i\community_stats.json" (
        echo   Files generated: community_assignments.csv, community_stats.json, etc.
        echo   Check %%i\community_stats.json for detailed statistics
    ) else (
        echo   No stats file found
    )
    echo.
)

REM Next steps information
echo === Next Steps ===
echo.
echo To analyze what each community represents, run feature analysis:
echo.
echo REM For any of the generated results:
echo python ..\scripts\detailed_community_feature_analysis.py ^
echo     --communities %OUTPUT_BASE%\basic\community_assignments.csv ^
echo     --metadata ..\..\out\dead_labels_enriched.csv ^
echo     --out-dir %OUTPUT_BASE%\basic_feature_analysis
echo.
echo REM Then generate summary:
echo python ..\scripts\community_feature_summary.py ^
echo     --analysis %OUTPUT_BASE%\basic_feature_analysis\detailed_feature_analysis.json ^
echo     --out-dir %OUTPUT_BASE%\basic_summary
echo.

echo REM Compare different resolution results:
echo dir /b %OUTPUT_BASE%\*\community_stats.json
echo.

echo [✓] All basic usage examples completed!
echo.
echo Generated results are in: %OUTPUT_BASE%\
echo Each subdirectory contains:
echo   - community_assignments.csv (node-to-community mapping)
echo   - community_stats.json (summary statistics)
echo   - community_sizes.png (size distribution plot)
echo   - modularity_info.json (modularity and parameters)

endlocal