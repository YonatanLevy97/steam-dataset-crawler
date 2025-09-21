@echo off
REM Steam Dataset Crawler & Analysis Pipeline - Windows Batch Script
REM This script runs the complete pipeline from data collection to graph visualization

setlocal enabledelayedexpansion

echo ==========================================
echo Steam Dataset Crawler & Analysis Pipeline
echo ==========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.7+ and ensure it's in your PATH
    exit /b 1
)

REM Check if we're in the right directory
if not exist "research_crawler.py" (
    echo [ERROR] Please run this script from the project root directory
    echo [ERROR] Expected files: research_crawler.py, steamcharts_research_crawler.py
    exit /b 1
)

if not exist "steamcharts_research_crawler.py" (
    echo [ERROR] Please run this script from the project root directory
    echo [ERROR] Expected files: research_crawler.py, steamcharts_research_crawler.py
    exit /b 1
)

REM Create necessary directories
if not exist "data" mkdir data
if not exist "out" mkdir out
if not exist "data\features" mkdir data\features
if not exist "data\features\games_matrix" mkdir data\features\games_matrix

echo [INFO] Starting pipeline execution...
echo.

REM Step 1: Data Collection
echo [INFO] === STEP 1: DATA COLLECTION ===

REM Check if data collection is needed
if exist "data\games_metadata_merged.csv" if exist "data\players_data_merged.csv" (
    echo [WARNING] Data files already exist. Skipping data collection.
    echo [WARNING] If you want to re-collect data, delete the following files:
    echo [WARNING]   - data\games_metadata_merged.csv
    echo [WARNING]   - data\players_data_merged.csv
    echo.
    goto :skip_data_collection
)

echo [INFO] Collecting Steam game metadata...
echo [INFO] Command: python research_crawler.py
python research_crawler.py
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Steam metadata collection
    exit /b 1
)
echo [SUCCESS] Completed: Steam metadata collection
echo.

echo [INFO] Collecting SteamCharts player data...
echo [INFO] Command: python steamcharts_research_crawler.py
python steamcharts_research_crawler.py
if %errorlevel% neq 0 (
    echo [ERROR] Failed: SteamCharts player data collection
    exit /b 1
)
echo [SUCCESS] Completed: SteamCharts player data collection
echo.

REM Check if data files were created
if not exist "data\games_metadata_merged.csv" (
    echo [ERROR] Required file not found: data\games_metadata_merged.csv
    exit /b 1
)

if not exist "data\players_data_merged.csv" (
    echo [ERROR] Required file not found: data\players_data_merged.csv
    exit /b 1
)

:skip_data_collection

REM Step 2: Dead Game Detection
echo [INFO] === STEP 2: DEAD GAME DETECTION ===
echo [INFO] Command: python ./scripts/label_dead_games.py --players-csv data/players_data_merged.csv --out data/dead_labels.csv --window 6 --agg median --threshold 50 --min-months 3
python ./scripts/label_dead_games.py --players-csv data/players_data_merged.csv --out data/dead_labels.csv --window 6 --agg median --threshold 50 --min-months 3
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Dead game labeling
    exit /b 1
)
echo [SUCCESS] Completed: Dead game labeling
echo.

REM Step 3: Metadata Enrichment
echo [INFO] === STEP 3: METADATA ENRICHMENT ===
echo [INFO] Command: python ./scripts/enrich_dead_labels_with_metadata.py --dead-labels ./data/dead_labels.csv --games-csv ./data/games_metadata_merged.csv --out ./out/dead_labels_enriched.csv --metadata-cols type,is_free,required_age,release_date,coming_soon,developers,publishers,categories,genres,tags,windows,mac,linux,initial_price,final_price,discount_percent,metacritic_score,recommendations_total,achievements_total,supported_languages,pc_min_requirements,controller_support,has_dlc,dlc_count,crawl_timestamp,crawl_status
python ./scripts/enrich_dead_labels_with_metadata.py --dead-labels ./data/dead_labels.csv --games-csv ./data/games_metadata_merged.csv --out ./out/dead_labels_enriched.csv --metadata-cols type,is_free,required_age,release_date,coming_soon,developers,publishers,categories,genres,tags,windows,mac,linux,initial_price,final_price,discount_percent,metacritic_score,recommendations_total,achievements_total,supported_languages,pc_min_requirements,controller_support,has_dlc,dlc_count,crawl_timestamp,crawl_status
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Metadata enrichment
    exit /b 1
)
echo [SUCCESS] Completed: Metadata enrichment
echo.

REM Step 4: Filter Dead Games
echo [INFO] === STEP 4: FILTER DEAD GAMES ===
echo [INFO] Command: python ./scripts/filter_dead_games.py ./out/dead_labels_enriched.csv ./out/dead_games_only.csv
python ./scripts/filter_dead_games.py ./out/dead_labels_enriched.csv ./out/dead_games_only.csv
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Dead games filtering
    exit /b 1
)
echo [SUCCESS] Completed: Dead games filtering
echo.

REM Step 5: Train/Test Split
echo [INFO] === STEP 5: TRAIN/TEST SPLIT ===
echo [INFO] Command: python scripts/split_csv_train_test.py out/dead_games_only.csv
python scripts/split_csv_train_test.py out/dead_games_only.csv
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Train/test split
    exit /b 1
)
echo [SUCCESS] Completed: Train/test split
echo.

REM Step 6: Feature Engineering
echo [INFO] === STEP 6: FEATURE ENGINEERING ===
echo [INFO] Command: python ./graph_scripts/build_feature_vectors.py --in ./out/dead_games_only_train.csv --out-dir ./data/features/games_matrix --id-col appid --label-col label_dead_binary --infer-onehot --multi-cols genres,tags --hash-cols developers,publishers --hash-dims 64
python ./graph_scripts/build_feature_vectors.py --in ./out/dead_games_only_train.csv --out-dir ./data/features/games_matrix --id-col appid --label-col label_dead_binary --infer-onehot --multi-cols genres,tags --hash-cols developers,publishers --hash-dims 64
if %errorlevel% neq 0 (
    echo [ERROR] Failed: Feature vector construction
    exit /b 1
)
echo [SUCCESS] Completed: Feature vector construction
echo.

REM Step 7: Graph Construction
echo [INFO] === STEP 7: GRAPH CONSTRUCTION ===
echo [WARNING] Graph construction requires bash. Please run manually:
echo [WARNING] bash ./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100
echo.

REM Final summary
echo ==========================================
echo [SUCCESS] PIPELINE COMPLETED SUCCESSFULLY!
echo ==========================================
echo.
echo [INFO] Generated files:
echo   📊 Data files:
echo     - data\games_metadata_merged.csv (Steam game metadata)
echo     - data\players_data_merged.csv (Player activity data)
echo     - data\dead_labels.csv (Dead/alive labels)
echo     - out\dead_labels_enriched.csv (Labels + metadata)
echo     - out\dead_games_only.csv (Dead games only)
echo     - out\dead_games_only_train.csv (Training set - 80%%)
echo     - out\dead_games_only_test.csv (Test set - 20%%)
echo.
echo   🔢 Feature files:
echo     - data\features\games_matrix\X_csr.npz (Sparse feature matrix)
echo     - data\features\games_matrix\appids.npy (Game IDs)
echo     - data\features\games_matrix\labels.npy (Binary labels)
echo     - data\features\games_matrix\features_meta.json (Feature metadata)
echo.
echo   📈 Graph outputs:
echo     - Run graph construction manually with bash
echo.
echo [INFO] Pipeline execution completed at %date% %time%

pause