# Girvan-Newman Community Detection for Steam Dataset

Complete implementation of Girvan-Newman community detection algorithm for analyzing Steam game similarity graphs.

## 🚀 Quick Start

```bash
# Run basic community detection
./scripts/run_girvan_newman.sh --edges ./out/graph_runs/.../edges_top100.csv.gz --giant-only

# Analyze community features in detail  
python3 scripts/detailed_community_feature_analysis.py \
    --communities ./out/girvan_newman_.../community_assignments_best.csv \
    --metadata ./out/dead_labels_enriched.csv

# Create readable summaries
python3 scripts/community_feature_summary.py \
    --analysis ./out/detailed_analysis/detailed_feature_analysis.json
```

## 📁 Directory Structure

```
girvan_newman/
├── README.md                    # This file - main overview
├── scripts/                     # All executable scripts
│   ├── run_girvan_newman.sh          # Main wrapper script
│   ├── girvan_newman_analysis.py     # Core algorithm implementation  
│   ├── detailed_community_feature_analysis.py  # Feature analysis
│   ├── analyze_community_results.py            # Basic analysis
│   ├── community_feature_summary.py            # Readable summaries
│   ├── run_girvan_newman_binary.sh             # Binary classification
│   ├── interpret_binary_communities.py         # Binary interpretation
│   └── optimize_binary_separation.sh           # Parameter optimization
├── docs/                        # Documentation
│   ├── README_girvan_newman.md            # Detailed usage guide
│   ├── GIRVAN_NEWMAN_SUMMARY.md           # Implementation summary
│   ├── DETAILED_FEATURE_ANALYSIS_SUMMARY.md  # Feature analysis guide
│   └── BINARY_DEAD_ALIVE_GUIDE.md             # Binary classification guide
└── examples/                    # Example usage (you can add your own)
```

## 🎯 Main Scripts

### Core Analysis
- **`scripts/run_girvan_newman.sh`** - Main wrapper with all options
- **`scripts/girvan_newman_analysis.py`** - Core NetworkX implementation

### Feature Analysis  
- **`scripts/detailed_community_feature_analysis.py`** - Comprehensive feature analysis
- **`scripts/community_feature_summary.py`** - Readable summaries and CSV exports

### Optional Tools
- **`scripts/analyze_community_results.py`** - Basic community analysis
- **`scripts/run_girvan_newman_binary.sh`** - Binary dead/alive classification
- **`scripts/interpret_binary_communities.py`** - Binary result interpretation
- **`scripts/optimize_binary_separation.sh`** - Parameter optimization

## 📖 Documentation

- **[Complete Usage Guide](docs/README_girvan_newman.md)** - Detailed instructions and examples
- **[Implementation Summary](docs/GIRVAN_NEWMAN_SUMMARY.md)** - Technical overview and results
- **[Feature Analysis Guide](docs/DETAILED_FEATURE_ANALYSIS_SUMMARY.md)** - Understanding feature distributions
- **[Binary Classification Guide](docs/BINARY_DEAD_ALIVE_GUIDE.md)** - Dead vs alive analysis

## 🔧 Requirements

Install required Python packages:
```bash
pip install networkx matplotlib numpy pandas
```

## 💡 Example Workflows

### Standard Community Detection
```bash
cd girvan_newman
./scripts/run_girvan_newman.sh --edges ../out/graph_runs/.../edges_top100.csv.gz --giant-only --kcore 2
```

### Detailed Feature Analysis
```bash  
cd girvan_newman
python3 scripts/detailed_community_feature_analysis.py \
    --communities ../out/girvan_newman_.../community_assignments_best.csv \
    --metadata ../out/dead_labels_enriched.csv \
    --out-dir ../out/detailed_analysis

python3 scripts/community_feature_summary.py \
    --analysis ../out/detailed_analysis/detailed_feature_analysis.json \
    --out-dir ../out/community_summary
```

### Quick Test
```bash
cd girvan_newman  
./scripts/run_girvan_newman.sh \
    --edges ../out/graph_runs/.../edges_top100.csv.gz \
    --max-edges 10000 --max-nodes 500 --giant-only
```

## 🎯 Key Features

- **Hierarchical community detection** using NetworkX Girvan-Newman
- **Comprehensive feature analysis** showing percentage distributions
- **Multiple output formats** (JSON, CSV, visualizations)
- **Scalable implementation** with memory and performance optimizations
- **Integration ready** with your existing Steam dataset pipeline

## 🚀 Integration with Your Pipeline

This fits seamlessly into your existing workflow:

1. **Input**: Uses edge CSV files from `edges_to_graph.py`
2. **Filtering**: Same options as your graph visualization (k-core, giant component)
3. **Output**: Community assignments that can be joined with your game metadata
4. **Analysis**: Detailed breakdowns of game characteristics by community

## 📊 Results

The implementation successfully identifies meaningful communities in Steam game networks:

- **Genre clusters** (Action+RPG, Indie+Experimental, etc.)
- **Publisher ecosystems** (Major vs indie publishers)  
- **Market segments** (Premium vs mass market vs failed games)
- **Quality tiers** (High-scoring vs low-scoring games)

For detailed results and examples, see the documentation in the `docs/` folder.