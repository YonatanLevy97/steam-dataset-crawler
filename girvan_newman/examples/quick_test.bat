@echo off
REM quick_test.bat - Quick Windows test for Girvan-Newman

echo === Quick Girvan-Newman Test ===
echo Using small dataset for fast testing...
echo.

call scripts\run_girvan_newman.bat ^
    --edges ..\out\graph_runs\20250919_143520\edges\edges_top100.csv.gz ^
    --out-dir ..\out\quick_test ^
    --max-communities 3 ^
    --max-edges 5000 ^
    --max-nodes 200 ^
    --giant-only ^
    --kcore 2

echo.
echo Quick test complete! Results in ..\out\quick_test\
pause