@echo off
REM basic_usage.bat - Example usage of Girvan-Newman community detection for Windows

echo === Girvan-Newman Community Detection - Basic Usage Example ===
echo.

REM Example 1: Basic community detection
echo [1] Running basic community detection...
call scripts\run_girvan_newman.bat ^
    --edges ..\out\graph_runs\20250919_143520\edges\edges_top100.csv.gz ^
    --out-dir ..\out\example_analysis ^
    --giant-only ^
    --max-communities 5 ^
    --max-nodes 500

echo.

REM Example 2: Detailed feature analysis
echo [2] Running detailed feature analysis...
python scripts\detailed_community_feature_analysis.py ^
    --communities ..\out\example_analysis\community_assignments_best.csv ^
    --metadata ..\out\dead_labels_enriched.csv ^
    --out-dir ..\out\example_detailed_analysis

echo.

REM Example 3: Create readable summaries
echo [3] Creating readable summaries...
python scripts\community_feature_summary.py ^
    --analysis ..\out\example_detailed_analysis\detailed_feature_analysis.json ^
    --out-dir ..\out\example_summary

echo.
echo === Example Complete! ===
echo Results saved to:
echo   - Community assignments: ..\out\example_analysis\
echo   - Detailed analysis: ..\out\example_detailed_analysis\
echo   - Readable summaries: ..\out\example_summary\
pause