#!/bin/bash

# Steam Dataset Crawler & Analysis Pipeline - Main Execution Script
# Cross-platform script that works on Windows (WSL/Git Bash), macOS, and Linux

# Detect operating system
detect_os() {
    case "$(uname -s)" in
        Linux*)     OS="linux";;
        Darwin*)    OS="macos";;
        CYGWIN*|MINGW*|MSYS*) OS="windows";;
        *)          OS="unknown";;
    esac
}

# Initialize OS detection
detect_os

# Colors for output (with Windows compatibility)
if [[ "$OS" == "windows" ]]; then
    # Windows may not support ANSI colors in all terminals
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
else
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
fi

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a file exists
check_file() {
    if [ ! -f "$1" ]; then
        print_error "Required file not found: $1"
        exit 1
    fi
}

# Function to check if a directory exists, create if not
ensure_dir() {
    if [ ! -d "$1" ]; then
        print_status "Creating directory: $1"
        mkdir -p "$1"
    fi
}

# Function to run a command with error handling
run_command() {
    local cmd="$1"
    local description="$2"
    
    print_status "Running: $description"
    print_status "Command: $cmd"
    
    if eval "$cmd"; then
        print_success "Completed: $description"
    else
        print_error "Failed: $description"
        exit 1
    fi
    echo ""
}

# Function to check Python availability
check_python() {
    local python_cmd=""
    
    # Try different Python commands
    if command -v python3 >/dev/null 2>&1; then
        python_cmd="python3"
    elif command -v python >/dev/null 2>&1; then
        python_cmd="python"
    else
        print_error "Python not found. Please install Python 3.7+ and ensure it's in your PATH"
        exit 1
    fi
    
    # Check Python version
    local version=$($python_cmd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    local major=$(echo $version | cut -d. -f1)
    local minor=$(echo $version | cut -d. -f2)
    
    if [ "$major" -lt 3 ] || ([ "$major" -eq 3 ] && [ "$minor" -lt 7 ]); then
        print_error "Python 3.7+ required, found $version"
        exit 1
    fi
    
    print_success "Found Python $version"
    echo "$python_cmd"
}

# Main execution
main() {
    echo "=========================================="
    echo "Steam Dataset Crawler & Analysis Pipeline"
    echo "=========================================="
    echo ""
    
    print_status "Detected OS: $OS"
    
    # Check Python availability
    PYTHON_CMD=$(check_python)
    
    # Check if we're in the right directory
    if [ ! -f "research_crawler.py" ] || [ ! -f "steamcharts_research_crawler.py" ]; then
        print_error "Please run this script from the project root directory"
        print_error "Expected files: research_crawler.py, steamcharts_research_crawler.py"
        exit 1
    fi
    
    # Create necessary directories
    ensure_dir "data"
    ensure_dir "out"
    ensure_dir "data/features"
    ensure_dir "data/features/games_matrix"
    
    print_status "Starting pipeline execution..."
    echo ""
    
    # Step 1: Data Collection
    print_status "=== STEP 1: DATA COLLECTION ==="
    
    # Check if data collection is needed
    if [ -f "data/games_metadata_merged.csv" ] && [ -f "data/players_data_merged.csv" ]; then
        print_warning "Data files already exist. Skipping data collection."
        print_warning "If you want to re-collect data, delete the following files:"
        print_warning "  - data/games_metadata_merged.csv"
        print_warning "  - data/players_data_merged.csv"
        echo ""
    else
        print_status "Collecting Steam game metadata..."
        run_command "$PYTHON_CMD research_crawler.py" "Steam metadata collection"
        
        print_status "Collecting SteamCharts player data..."
        run_command "$PYTHON_CMD steamcharts_research_crawler.py" "SteamCharts player data collection"
        
        # Check if data files were created
        check_file "data/games_metadata_merged.csv"
        check_file "data/players_data_merged.csv"
    fi
    
    # Step 2: Dead Game Detection
    print_status "=== STEP 2: DEAD GAME DETECTION ==="
    run_command "$PYTHON_CMD ./scripts/label_dead_games.py --players-csv data/players_data_merged.csv --out data/dead_labels.csv --window 6 --agg median --threshold 50 --min-months 3" "Dead game labeling"
    
    # Step 3: Metadata Enrichment
    print_status "=== STEP 3: METADATA ENRICHMENT ==="
    run_command "$PYTHON_CMD ./scripts/enrich_dead_labels_with_metadata.py --dead-labels ./data/dead_labels.csv --games-csv ./data/games_metadata_merged.csv --out ./out/dead_labels_enriched.csv --metadata-cols type,is_free,required_age,release_date,coming_soon,developers,publishers,categories,genres,tags,windows,mac,linux,initial_price,final_price,discount_percent,metacritic_score,recommendations_total,achievements_total,supported_languages,pc_min_requirements,controller_support,has_dlc,dlc_count,crawl_timestamp,crawl_status" "Metadata enrichment"
    
    # Step 4: Filter Dead Games
    print_status "=== STEP 4: FILTER DEAD GAMES ==="
    run_command "$PYTHON_CMD ./scripts/filter_dead_games.py ./out/dead_labels_enriched.csv ./out/dead_games_only.csv" "Dead games filtering"
    
    # Step 5: Train/Test Split
    print_status "=== STEP 5: TRAIN/TEST SPLIT ==="
    run_command "$PYTHON_CMD scripts/split_csv_train_test.py out/dead_games_only.csv" "Train/test split"
    
    # Step 6: Feature Engineering
    print_status "=== STEP 6: FEATURE ENGINEERING ==="
    run_command "$PYTHON_CMD ./graph_scripts/build_feature_vectors.py --in ./out/dead_games_only_train.csv --out-dir ./data/features/games_matrix --id-col appid --label-col label_dead_binary --infer-onehot --multi-cols genres,tags --hash-cols developers,publishers --hash-dims 64" "Feature vector construction"
    
    # Step 7: Graph Construction
    print_status "=== STEP 7: GRAPH CONSTRUCTION ==="
    
    # Check if we're on Windows and adjust the shell script call
    if [[ "$OS" == "windows" ]]; then
        # On Windows, try different ways to run the shell script
        if command -v bash >/dev/null 2>&1; then
            run_command "bash ./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100" "Cosine similarity graph construction"
        else
            print_warning "Bash not available on Windows. Please run the graph construction manually:"
            print_warning "bash ./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100"
        fi
    else
        run_command "./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100" "Cosine similarity graph construction"
    fi
    
    # Final summary
    echo "=========================================="
    print_success "PIPELINE COMPLETED SUCCESSFULLY!"
    echo "=========================================="
    echo ""
    print_status "Generated files:"
    echo "  📊 Data files:"
    echo "    - data/games_metadata_merged.csv (Steam game metadata)"
    echo "    - data/players_data_merged.csv (Player activity data)"
    echo "    - data/dead_labels.csv (Dead/alive labels)"
    echo "    - out/dead_labels_enriched.csv (Labels + metadata)"
    echo "    - out/dead_games_only.csv (Dead games only)"
    echo "    - out/dead_games_only_train.csv (Training set - 80%)"
    echo "    - out/dead_games_only_test.csv (Test set - 20%)"
    echo ""
    echo "  🔢 Feature files:"
    echo "    - data/features/games_matrix/X_csr.npz (Sparse feature matrix)"
    echo "    - data/features/games_matrix/appids.npy (Game IDs)"
    echo "    - data/features/games_matrix/labels.npy (Binary labels)"
    echo "    - data/features/games_matrix/features_meta.json (Feature metadata)"
    echo ""
    echo "  📈 Graph outputs:"
    echo "    - out/graph_runs/*/graphs/cos0p70_k2_top100.jpg (Graph visualization)"
    echo "    - out/graph_runs/*/edges/ (Edge files with similarities)"
    echo ""
    print_status "Pipeline execution completed in $(date)"
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Steam Dataset Crawler & Analysis Pipeline"
        echo ""
        echo "Usage: $0 [options]"
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --skip-data    Skip data collection (use existing data files)"
        echo ""
        echo "This script runs the complete pipeline:"
        echo "  1. Data collection from Steam and SteamCharts"
        echo "  2. Dead game detection and labeling"
        echo "  3. Metadata enrichment"
        echo "  4. Dead games filtering"
        echo "  5. Train/test split"
        echo "  6. Feature engineering"
        echo "  7. Graph construction and visualization"
        echo ""
        echo "Requirements:"
        echo "  - Python 3.7+ with required packages (see requirements.txt)"
        echo "  - Internet connection for data collection"
        echo "  - Sufficient disk space (~1-2GB for full dataset)"
        echo ""
        echo "Platform Support:"
        echo "  - Linux (native)"
        echo "  - macOS (native)"
        echo "  - Windows (WSL, Git Bash, or MSYS2)"
        echo ""
        exit 0
        ;;
    --skip-data)
        print_warning "Skipping data collection - using existing data files"
        # Modify the main function to skip data collection
        main() {
            echo "=========================================="
            echo "Steam Dataset Crawler & Analysis Pipeline"
            echo "=========================================="
            echo ""
            
            detect_os
            print_status "Detected OS: $OS"
            
            # Check Python availability
            PYTHON_CMD=$(check_python)
            
            # Check if we're in the right directory
            if [ ! -f "research_crawler.py" ] || [ ! -f "steamcharts_research_crawler.py" ]; then
                print_error "Please run this script from the project root directory"
                exit 1
            fi
            
            # Create necessary directories
            ensure_dir "data"
            ensure_dir "out"
            ensure_dir "data/features"
            ensure_dir "data/features/games_matrix"
            
            print_status "Starting pipeline execution (skipping data collection)..."
            echo ""
            
            # Check if required data files exist
            check_file "data/games_metadata_merged.csv"
            check_file "data/players_data_merged.csv"
            
            # Continue with remaining steps...
            # Step 2: Dead Game Detection
            print_status "=== STEP 2: DEAD GAME DETECTION ==="
            run_command "$PYTHON_CMD ./scripts/label_dead_games.py --players-csv data/players_data_merged.csv --out data/dead_labels.csv --window 6 --agg median --threshold 50 --min-months 3" "Dead game labeling"
            
            # Step 3: Metadata Enrichment
            print_status "=== STEP 3: METADATA ENRICHMENT ==="
            run_command "$PYTHON_CMD ./scripts/enrich_dead_labels_with_metadata.py --dead-labels ./data/dead_labels.csv --games-csv ./data/games_metadata_merged.csv --out ./out/dead_labels_enriched.csv --metadata-cols type,is_free,required_age,release_date,coming_soon,developers,publishers,categories,genres,tags,windows,mac,linux,initial_price,final_price,discount_percent,metacritic_score,recommendations_total,achievements_total,supported_languages,pc_min_requirements,controller_support,has_dlc,dlc_count,crawl_timestamp,crawl_status" "Metadata enrichment"
            
            # Step 4: Filter Dead Games
            print_status "=== STEP 4: FILTER DEAD GAMES ==="
            run_command "$PYTHON_CMD ./scripts/filter_dead_games.py ./out/dead_labels_enriched.csv ./out/dead_games_only.csv" "Dead games filtering"
            
            # Step 5: Train/Test Split
            print_status "=== STEP 5: TRAIN/TEST SPLIT ==="
            run_command "$PYTHON_CMD scripts/split_csv_train_test.py out/dead_games_only.csv" "Train/test split"
            
            # Step 6: Feature Engineering
            print_status "=== STEP 6: FEATURE ENGINEERING ==="
            run_command "$PYTHON_CMD ./graph_scripts/build_feature_vectors.py --in ./out/dead_games_only_train.csv --out-dir ./data/features/games_matrix --id-col appid --label-col label_dead_binary --infer-onehot --multi-cols genres,tags --hash-cols developers,publishers --hash-dims 64" "Feature vector construction"
            
            # Step 7: Graph Construction
            print_status "=== STEP 7: GRAPH CONSTRUCTION ==="
            
            # Check if we're on Windows and adjust the shell script call
            if [[ "$OS" == "windows" ]]; then
                # On Windows, try different ways to run the shell script
                if command -v bash >/dev/null 2>&1; then
                    run_command "bash ./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100" "Cosine similarity graph construction"
                else
                    print_warning "Bash not available on Windows. Please run the graph construction manually:"
                    print_warning "bash ./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100"
                fi
            else
                run_command "./graph_scripts/run_full_cosine_graph_v3.sh --in ./out/dead_games_only_train.csv --out-root ./out/graph_runs --threshold 0.70 --kcore 2 --features ./data/features/games_matrix/X_csr.npz --appids ./data/features/games_matrix/appids.npy --topk-per-node 100" "Cosine similarity graph construction"
            fi
            
            # Final summary
            echo "=========================================="
            print_success "PIPELINE COMPLETED SUCCESSFULLY!"
            echo "=========================================="
            echo ""
            print_status "Generated files:"
            echo "  📊 Data files:"
            echo "    - data/games_metadata_merged.csv (Steam game metadata)"
            echo "    - data/players_data_merged.csv (Player activity data)"
            echo "    - data/dead_labels.csv (Dead/alive labels)"
            echo "    - out/dead_labels_enriched.csv (Labels + metadata)"
            echo "    - out/dead_games_only.csv (Dead games only)"
            echo "    - out/dead_games_only_train.csv (Training set - 80%)"
            echo "    - out/dead_games_only_test.csv (Test set - 20%)"
            echo ""
            echo "  🔢 Feature files:"
            echo "    - data/features/games_matrix/X_csr.npz (Sparse feature matrix)"
            echo "    - data/features/games_matrix/appids.npy (Game IDs)"
            echo "    - data/features/games_matrix/labels.npy (Binary labels)"
            echo "    - data/features/games_matrix/features_meta.json (Feature metadata)"
            echo ""
            echo "  📈 Graph outputs:"
            echo "    - out/graph_runs/*/graphs/cos0p70_k2_top100.jpg (Graph visualization)"
            echo "    - out/graph_runs/*/edges/ (Edge files with similarities)"
            echo ""
            print_status "Pipeline execution completed in $(date)"
        }
        ;;
esac

# Run the main function
main