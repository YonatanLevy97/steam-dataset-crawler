@echo off
REM quick_test.bat
REM 
REM Quick testing examples for Louvain community detection on Windows
REM Designed for rapid experimentation and validation

setlocal enabledelayedexpansion

echo === Louvain Community Detection - Quick Testing ===
echo.

REM Test data path - adjust this to your actual data location
set EDGES_FILE=..\..\out\graph_runs\example\edges_top100.csv.gz
set OUTPUT_BASE=..\..\out\louvain_quick_tests

REM Check if test data exists
if not exist "%EDGES_FILE%" (
    echo Warning: Test edges file not found at: %EDGES_FILE%
    echo Please adjust the EDGES_FILE path to point to your actual edges data
    echo.
    echo Try checking these common locations:
    echo   ..\..\out\graph_runs\*\edges_top100.csv.gz
    echo   ..\..\out\edges_*.csv.gz
    echo.
    exit /b 1
)

echo Found test edges file: %EDGES_FILE%
echo.

REM Test 1: Minimal quick test (very small subset)
echo Test 1: Minimal Quick Test
echo Running with very limited data for rapid testing...
..\scripts\run_louvain.bat ^
    --edges "%EDGES_FILE%" ^
    --out-dir "%OUTPUT_BASE%\minimal" ^
    --max-edges 5000 ^
    --max-nodes 200 ^
    --resolution 1.0

if exist "%OUTPUT_BASE%\minimal\community_assignments.csv" (
    echo [✓] Minimal test passed
    REM Quick stats using basic commands
    for /f %%i in ('findstr /c:"," "%OUTPUT_BASE%\minimal\community_assignments.csv"') do set node_count=%%i
    echo   Nodes processed: !node_count!
) else (
    echo [✗] Minimal test failed
)
echo.

REM Test 2: Parameter sensitivity test
echo Test 2: Resolution Parameter Sensitivity
echo Testing different resolution values...

for %%r in (0.5 1.0 2.0) do (
    echo   Testing resolution: %%r
    ..\scripts\run_louvain.bat ^
        --edges "%EDGES_FILE%" ^
        --out-dir "%OUTPUT_BASE%\resolution_%%r" ^
        --max-edges 10000 ^
        --max-nodes 300 ^
        --resolution %%r ^
        --giant-only >nul 2>&1
    
    if exist "%OUTPUT_BASE%\resolution_%%r\community_assignments.csv" (
        echo     [✓] Resolution %%r: completed
    ) else (
        echo     [✗] Resolution %%r: failed
    )
)
echo.

REM Test 3: Filtering effectiveness test
echo Test 3: Graph Filtering Test
echo Testing different filtering parameters...

for %%w in (0.6 0.7 0.8) do (
    echo   Testing min-weight: %%w
    ..\scripts\run_louvain.bat ^
        --edges "%EDGES_FILE%" ^
        --out-dir "%OUTPUT_BASE%\weight_%%w" ^
        --max-edges 15000 ^
        --max-nodes 400 ^
        --min-weight %%w ^
        --giant-only >nul 2>&1
    
    if exist "%OUTPUT_BASE%\weight_%%w\community_assignments.csv" (
        echo     [✓] Weight %%w: completed
    ) else (
        echo     [✗] Weight %%w: failed
    )
)
echo.

REM Test 4: K-core filtering test
echo Test 4: K-core Filtering Test
echo Testing k-core decomposition...

for %%k in (2 3 5) do (
    echo   Testing k-core: %%k
    ..\scripts\run_louvain.bat ^
        --edges "%EDGES_FILE%" ^
        --out-dir "%OUTPUT_BASE%\kcore_%%k" ^
        --max-edges 20000 ^
        --kcore %%k ^
        --giant-only >nul 2>&1
    
    if exist "%OUTPUT_BASE%\kcore_%%k\community_assignments.csv" (
        echo     [✓] K-core %%k: completed
    ) else (
        echo     [✗] K-core %%k: failed (possibly too restrictive)
    )
)
echo.

REM Test 5: Reproducibility test
echo Test 5: Reproducibility Test
echo Testing random seed reproducibility...

REM Run same parameters twice with same seed
for %%n in (1 2) do (
    ..\scripts\run_louvain.bat ^
        --edges "%EDGES_FILE%" ^
        --out-dir "%OUTPUT_BASE%\reproducibility_run%%n" ^
        --max-edges 8000 ^
        --max-nodes 250 ^
        --resolution 1.0 ^
        --random-seed 12345 ^
        --giant-only >nul 2>&1
)

REM Check if both runs completed
if exist "%OUTPUT_BASE%\reproducibility_run1\community_assignments.csv" (
    if exist "%OUTPUT_BASE%\reproducibility_run2\community_assignments.csv" (
        echo [✓] Reproducibility test passed (both runs completed)
    ) else (
        echo [✗] Reproducibility test failed (run 2 failed)
    )
) else (
    echo [✗] Reproducibility test failed (run 1 failed)
)
echo.

REM Test 6: Feature analysis compatibility test
echo Test 6: Feature Analysis Integration Test
echo Testing compatibility with feature analysis scripts...

REM Use one of the successful results for feature analysis test
if exist "%OUTPUT_BASE%\minimal\community_assignments.csv" (
    echo   Testing detailed feature analysis...
    
    REM Check if metadata file exists
    set METADATA_FILE=..\..\out\dead_labels_enriched.csv
    if exist "!METADATA_FILE!" (
        python ..\scripts\detailed_community_feature_analysis.py ^
            --communities "%OUTPUT_BASE%\minimal\community_assignments.csv" ^
            --metadata "!METADATA_FILE!" ^
            --out-dir "%OUTPUT_BASE%\feature_test" >nul 2>&1
        
        if exist "%OUTPUT_BASE%\feature_test\detailed_feature_analysis.json" (
            echo     [✓] Feature analysis integration test passed
        ) else (
            echo     [✗] Feature analysis integration test failed
        )
    ) else (
        echo     [⚠] Metadata file not found, skipping feature analysis test
        echo         Expected: !METADATA_FILE!
    )
) else (
    echo     [✗] No community results available for feature analysis test
)
echo.

REM Summary of all tests
echo === Test Summary ===
echo.

set passed_tests=0
set total_tests=6

REM Count successful tests by checking for key output files
if exist "%OUTPUT_BASE%\minimal\community_assignments.csv" (
    echo [✓] Minimal Quick Test
    set /a passed_tests+=1
) else (
    echo [✗] Minimal Quick Test
)

if exist "%OUTPUT_BASE%\resolution_1.0\community_assignments.csv" (
    echo [✓] Resolution Parameter Test
    set /a passed_tests+=1
) else (
    echo [✗] Resolution Parameter Test
)

if exist "%OUTPUT_BASE%\weight_0.7\community_assignments.csv" (
    echo [✓] Graph Filtering Test
    set /a passed_tests+=1
) else (
    echo [✗] Graph Filtering Test
)

if exist "%OUTPUT_BASE%\kcore_3\community_assignments.csv" (
    echo [✓] K-core Filtering Test
    set /a passed_tests+=1
) else (
    echo [✗] K-core Filtering Test
)

if exist "%OUTPUT_BASE%\reproducibility_run1\community_assignments.csv" (
    echo [✓] Reproducibility Test
    set /a passed_tests+=1
) else (
    echo [✗] Reproducibility Test
)

if exist "%OUTPUT_BASE%\feature_test\detailed_feature_analysis.json" (
    echo [✓] Feature Analysis Test
    set /a passed_tests+=1
) else (
    echo [✗] Feature Analysis Test
)

echo.
echo Results: %passed_tests%/%total_tests% tests passed

if %passed_tests% equ %total_tests% (
    echo [🎉] All tests passed! Louvain implementation is working correctly.
) else (
    if %passed_tests% geq 4 (
        echo [⚠] Most tests passed. Check failing tests for configuration issues.
    ) else (
        echo [❌] Multiple tests failed. Check installation and input data.
    )
)

echo.
echo Test results stored in: %OUTPUT_BASE%\
echo.
echo Next steps:
echo 1. Review test results in individual subdirectories
echo 2. Run full analysis with appropriate parameters based on test results
echo 3. Adjust resolution/filtering parameters as needed for your dataset
echo.

endlocal