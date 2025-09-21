# Steam Dataset Crawler & Analysis Pipeline

A comprehensive data pipeline for collecting Steam game data, analyzing player activity patterns, and building similarity graphs for dead game detection and analysis.

## Overview

This pipeline collects data from Steam and SteamCharts, processes it to identify "dead" games (games with low player activity), and creates similarity graphs based on game features. The analysis helps understand patterns in game popularity and player engagement.

## Pipeline Components

### 1. Data Collection

#### Steam Game Metadata Crawler (`research_crawler.py`)
- **Purpose**: Collects comprehensive game metadata from Steam
- **Input**: CSV file with App IDs (batches)
- **Output**: `games_metadata_merged.csv`
- **Features Collected**:
  - Basic info: name, type, description, age rating
  - Release information: release date, coming soon status
  - Developer/publisher information
  - Categories, genres, tags
  - Platform support (Windows, Mac, Linux)
  - Pricing: initial price, final price, discount percentage
  - Reviews: Metacritic score, recommendations
  - Technical details: achievements, languages, requirements
  - DLC information

#### SteamCharts Player Data Crawler (`steamcharts_research_crawler.py`)
- **Purpose**: Collects historical player activity data from SteamCharts
- **Input**: CSV file with App IDs (batches)
- **Output**: `players_data_merged.csv`
- **Features Collected**:
  - Monthly average players
  - Peak player counts
  - Month-over-month change percentages
  - Historical data spanning multiple months

### 2. Data Processing & Labeling

#### Dead Game Detection (`scripts/label_dead_games.py`)
- **Purpose**: Identifies games with low player activity as "dead"
- **Parameters**:
  - `--window`: Number of recent months to analyze (default: 6)
  - `--agg`: Aggregation method - mean or median (default: median)
  - `--threshold`: Player count threshold for dead classification (default: 50)
  - `--min-months`: Minimum months of data required (default: 3)
- **Output**: `dead_labels.csv` with binary labels (Dead/Alive)

#### Metadata Enrichment (`scripts/enrich_dead_labels_with_metadata.py`)
- **Purpose**: Combines dead game labels with comprehensive game metadata
- **Output**: `dead_labels_enriched.csv`
- **Features**: All metadata columns plus dead/alive labels and player statistics

#### Dead Games Filtering (`scripts/filter_dead_games.py`)
- **Purpose**: Extracts only dead games from the enriched dataset
- **Output**: `dead_games_only.csv`

### 3. Dataset Preparation

#### Train/Test Split (`scripts/split_csv_train_test.py`)
- **Purpose**: Splits the dead games dataset into training and testing sets
- **Split**: 80% training, 20% testing
- **Output**: 
  - `dead_games_only_train.csv` (12,879 rows)
  - `dead_games_only_test.csv` (3,220 rows)

### 4. Feature Engineering

#### Feature Vector Construction (`graph_scripts/build_feature_vectors.py`)
- **Purpose**: Converts game metadata into numerical feature vectors for similarity analysis
- **Features**:
  - **Numeric**: Prices, scores, counts (standardized)
  - **One-hot**: Categorical features with low cardinality
  - **Multi-hot**: Multi-value features like genres and tags
  - **Hashed**: High-cardinality features like developers/publishers
- **Output**: 
  - `X_csr.npz`: Sparse feature matrix (L2-normalized)
  - `appids.npy`: Game IDs
  - `labels.npy`: Dead/alive labels
  - `features_meta.json`: Feature metadata

### 5. Graph Construction & Visualization

#### Cosine Similarity Graph (`graph_scripts/run_full_cosine_graph_v3.sh`)
- **Purpose**: Creates similarity graphs based on cosine similarity between game feature vectors
- **Parameters**:
  - `--threshold`: Cosine similarity threshold for edge creation (default: 0.70)
  - `--kcore`: Minimum node degree for graph filtering (default: 2)
  - `--topk-per-node`: Maximum neighbors per node (default: 100)
- **Process**:
  1. Computes cosine similarities between all game pairs
  2. Creates edges where similarity > threshold
  3. Prunes to top-K neighbors per node
  4. Filters by k-core to remove isolated nodes
  5. Generates visualization with node coloring by dead/alive status

## Quick Start

### 🚀 Run the Complete Pipeline

The easiest way to run the entire pipeline is using the provided main script:

#### **Linux/macOS/WSL:**
```bash
# Install dependencies
pip install -r requirements.txt

# Run the complete pipeline
./main.sh
```

#### **Windows (Native):**
```cmd
REM Install dependencies
pip install -r requirements.txt

REM Run the complete pipeline
main.bat
```

#### **Windows (Git Bash/WSL):**
```bash
# Install dependencies
pip install -r requirements.txt

# Run the complete pipeline
./main.sh
```

This will execute all steps automatically:
1. Data collection from Steam and SteamCharts
2. Dead game detection and labeling  
3. Metadata enrichment
4. Dead games filtering
5. Train/test split
6. Feature engineering
7. Graph construction and visualization

### 📋 Pipeline Options

#### **Bash Script (Linux/macOS/WSL/Git Bash):**
```bash
# Show help
./main.sh --help

# Skip data collection (use existing data files)
./main.sh --skip-data
```

#### **Windows Batch Script:**
```cmd
REM Show help (run without arguments)
main.bat

REM Note: Windows batch script doesn't support skip-data option
REM Delete data files manually if you want to re-collect data
```

## Manual Pipeline Execution

If you prefer to run individual steps manually:

### Step 1: Data Collection
```bash
# Collect Steam metadata
python research_crawler.py

# Collect SteamCharts player data
python steamcharts_research_crawler.py
```

### Step 2: Dead Game Detection
```bash
python ./scripts/label_dead_games.py \
  --players-csv data/players_data_merged.csv \
  --out data/dead_labels.csv \
  --window 6 \
  --agg median \
  --threshold 50 \
  --min-months 3
```

### Step 3: Metadata Enrichment
```bash
python ./scripts/enrich_dead_labels_with_metadata.py \
  --dead-labels ./data/dead_labels.csv \
  --games-csv ./data/games_metadata_merged.csv \
  --out ./out/dead_labels_enriched.csv \
  --metadata-cols type,is_free,required_age,release_date,coming_soon,developers,publishers,categories,genres,tags,windows,mac,linux,initial_price,final_price,discount_percent,metacritic_score,recommendations_total,achievements_total,supported_languages,pc_min_requirements,controller_support,has_dlc,dlc_count,crawl_timestamp,crawl_status
```

### Step 4: Filter Dead Games
```bash
python ./scripts/filter_dead_games.py \
  "./out/dead_labels_enriched.csv" \
  "./out/dead_games_only.csv"
```

### Step 5: Train/Test Split
```bash
python scripts/split_csv_train_test.py out/dead_games_only.csv
```

### Step 6: Feature Engineering
```bash
python ./graph_scripts/build_feature_vectors.py \
  --in ./out/dead_games_only_train.csv \
  --out-dir ./data/features/games_matrix \
  --id-col appid \
  --label-col label_dead_binary \
  --infer-onehot \
  --multi-cols genres,tags \
  --hash-cols developers,publishers \
  --hash-dims 64
```

### Step 7: Graph Construction
```bash
./graph_scripts/run_full_cosine_graph_v3.sh \
  --in ./out/dead_games_only_train.csv \
  --out-root ./out/graph_runs \
  --threshold 0.70 \
  --kcore 2 \
  --features ./data/features/games_matrix/X_csr.npz \
  --appids ./data/features/games_matrix/appids.npy \
  --topk-per-node 100
```

## Output Files

### Data Files
- `games_metadata_merged.csv`: Complete Steam game metadata
- `players_data_merged.csv`: Historical player activity data
- `dead_labels.csv`: Binary dead/alive labels
- `dead_labels_enriched.csv`: Labels + metadata
- `dead_games_only.csv`: Only dead games
- `dead_games_only_train.csv`: Training set (80%)
- `dead_games_only_test.csv`: Test set (20%)

### Feature Files
- `X_csr.npz`: Sparse feature matrix
- `appids.npy`: Game IDs
- `labels.npy`: Binary labels
- `features_meta.json`: Feature metadata

### Graph Outputs
- `cos0p70_k2_top100.jpg`: Graph visualization
- Edge files with cosine similarities
- Community detection results

## Key Features

### Dead Game Definition
Games are classified as "dead" if their median average players over the last 6 months is below 50, with at least 3 months of data available.

### Feature Engineering
- **Robust Price Parsing**: Handles various currency formats and "Free to Play" labels
- **Multi-value Handling**: Properly processes comma/semicolon-separated lists
- **Feature Hashing**: Reduces dimensionality for high-cardinality features
- **L2 Normalization**: Enables cosine similarity computation

### Graph Analysis
- **Cosine Similarity**: Measures feature vector similarity
- **Threshold Filtering**: Only keeps edges above similarity threshold
- **Top-K Pruning**: Limits neighbors per node for visualization
- **K-Core Filtering**: Removes isolated nodes
- **Community Detection**: Identifies clusters of similar games

## Technical Requirements

### System Requirements
- **Python**: 3.7 or higher
- **Operating System**: 
  - Linux (native support)
  - macOS (native support) 
  - Windows (native with `main.bat` or via WSL/Git Bash with `main.sh`)
- **Memory**: 8-16GB RAM recommended for large datasets
- **Storage**: 1-2GB free space for full dataset
- **Internet**: Required for data collection phase

### Windows-Specific Requirements
- **For `main.bat`**: Windows Command Prompt or PowerShell
- **For `main.sh`**: Git Bash, WSL, or MSYS2
- **Graph construction**: Requires bash (available in Git Bash, WSL, or MSYS2)

### Python Dependencies
Install all required packages with:
```bash
pip install -r requirements.txt
```

**Core packages:**
- pandas, numpy, scipy (data processing)
- scikit-learn (machine learning)
- BeautifulSoup4, requests (web scraping)
- NetworkX (graph analysis)
- matplotlib, seaborn (visualization)
- plotly (interactive plots)
- xgboost (gradient boosting)

### Main Script Features

The `main.sh` script provides:
- **Automatic execution** of all pipeline steps
- **Error handling** with clear error messages
- **Progress tracking** with colored output
- **File validation** to ensure required inputs exist
- **Directory creation** for output folders
- **Resume capability** - skips data collection if files exist
- **Help system** with `--help` option
- **Skip options** with `--skip-data` flag

## Use Cases

1. **Game Discovery**: Find similar games to popular titles
2. **Market Analysis**: Understand patterns in game success/failure
3. **Recommendation Systems**: Build game recommendation engines
4. **Community Detection**: Identify clusters of similar games
5. **Dead Game Analysis**: Study factors contributing to game abandonment

## Notes

- The pipeline is designed to handle large datasets efficiently
- All scripts include error handling and resume capabilities
- Data collection respects rate limits to avoid blocking
- Feature engineering excludes heavy text columns by default
- Graph visualization supports various customization options